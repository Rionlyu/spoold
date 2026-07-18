package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Rionlyu/spoold/internal/delivery"
)

func TestCreateIsIdempotentAndSurvivesReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spoold.journal")
	now := time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
	request := delivery.CreateRequest{
		IdempotencyKey: "invoice-42",
		TargetURL:      "https://example.com/hooks",
		Headers:        map[string]string{"content-type": "application/json"},
		Body:           json.RawMessage(`{"invoice":42}`),
		MaxAttempts:    4,
	}

	firstStore, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	first, created, err := firstStore.Create(request, now)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("first create was not reported as new")
	}
	duplicate, created, err := firstStore.Create(request, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if created || duplicate.ID != first.ID {
		t.Fatalf("duplicate = (%q, %v), want (%q, false)", duplicate.ID, created, first.ID)
	}
	if err := firstStore.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	replayed, created, err := reopened.Create(request, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if created || replayed.ID != first.ID {
		t.Fatalf("replayed duplicate = (%q, %v), want (%q, false)", replayed.ID, created, first.ID)
	}
}

func TestIdempotencyKeyRejectsDifferentRequest(t *testing.T) {
	store := openTestStore(t)
	now := time.Now()
	base := delivery.CreateRequest{
		IdempotencyKey: "same-key",
		TargetURL:      "https://example.com/a",
	}
	if _, _, err := store.Create(base, now); err != nil {
		t.Fatal(err)
	}
	base.TargetURL = "https://example.com/b"
	if _, _, err := store.Create(base, now); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("Create() error = %v, want %v", err, ErrIdempotencyConflict)
	}
}

func TestOpenRejectsConcurrentJournalOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spoold.journal")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); !errors.Is(err, ErrJournalLocked) {
		t.Fatalf("second Open() error = %v, want %v", err, ErrJournalLocked)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("Open() after owner closed: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestJournalAdmissionLimitRejectsOnlyNewDeliveries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spoold.journal")
	request := delivery.CreateRequest{
		IdempotencyKey: "first",
		TargetURL:      "https://example.com/first",
	}
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)

	initial, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := initial.Create(request, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := initial.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	bounded, err := OpenWithOptions(path, Options{MaxJournalBytes: info.Size()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bounded.Close() })
	duplicate, created, err := bounded.Create(request, now.Add(time.Minute))
	if err != nil || created || duplicate.ID != first.ID {
		t.Fatalf("idempotent Create() = (%q, %v, %v)", duplicate.ID, created, err)
	}
	if _, _, err := bounded.Create(delivery.CreateRequest{
		TargetURL: "https://example.com/second",
	}, now); !errors.Is(err, ErrJournalFull) {
		t.Fatalf("new Create() error = %v, want %v", err, ErrJournalFull)
	}
	if claimed, err := bounded.ClaimDue(now, time.Minute, 1); err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimDue() = %#v, %v", claimed, err)
	}
	if got := bounded.Stats().MaxJournalBytes; got != info.Size() {
		t.Fatalf("maximum journal bytes = %d, want %d", got, info.Size())
	}
}

func TestPersistenceFailureMarksStoreUnreadyUntilCompactionRepairsIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spoold.journal")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	original := store.file
	readOnly, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	store.file = readOnly
	if _, _, err := store.Create(delivery.CreateRequest{
		TargetURL: "https://example.com/fails",
	}, time.Now()); !errors.Is(err, ErrPersistence) {
		t.Fatalf("Create() error = %v, want %v", err, ErrPersistence)
	}
	if err := store.Ready(); !errors.Is(err, ErrPersistence) {
		t.Fatalf("Ready() error = %v, want %v", err, ErrPersistence)
	}

	if err := store.Compact(); err != nil {
		t.Fatalf("repairing Compact(): %v", err)
	}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Ready(); err != nil {
		t.Fatalf("Ready() after compaction: %v", err)
	}
	if _, created, err := store.Create(delivery.CreateRequest{
		TargetURL: "https://example.com/works",
	}, time.Now()); err != nil || !created {
		t.Fatalf("Create() after repair = (%v, %v)", created, err)
	}
}

func TestPruneTerminalRemovesStateAndIdempotencyKeysAcrossReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spoold.journal")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)

	succeeded := createTestDelivery(t, store, "succeeded", now)
	claimed, err := store.ClaimDue(now, time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim succeeded = %#v, %v", claimed, err)
	}
	if err := store.Succeed(succeeded.ID, claimed[0].Attempts, 204, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	failed := createTestDelivery(t, store, "failed", now.Add(time.Minute))
	claimed, err = store.ClaimDue(now.Add(time.Minute), time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim failed = %#v, %v", claimed, err)
	}
	if err := store.Fail(failed.ID, claimed[0].Attempts, 503, "failed", time.Time{}, true, now.Add(time.Minute+time.Second)); err != nil {
		t.Fatal(err)
	}

	canceled := createTestDelivery(t, store, "canceled", now.Add(2*time.Minute))
	if _, err := store.Cancel(canceled.ID, now.Add(2*time.Minute+time.Second)); err != nil {
		t.Fatal(err)
	}
	pending := createTestDelivery(t, store, "pending", now.Add(3*time.Minute))

	pruned, err := store.PruneTerminal(now.Add(10 * time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 3 {
		t.Fatalf("pruned deliveries = %d, want 3", pruned)
	}
	for _, id := range []string{succeeded.ID, failed.ID, canceled.ID} {
		if _, err := store.Get(id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get(%q) error = %v, want %v", id, err, ErrNotFound)
		}
	}
	if _, err := store.Get(pending.ID); err != nil {
		t.Fatalf("pending delivery was pruned: %v", err)
	}

	replacement, created, err := store.Create(delivery.CreateRequest{
		IdempotencyKey: "succeeded",
		TargetURL:      "https://example.com/succeeded",
	}, now.Add(time.Hour))
	if err != nil || !created || replacement.ID == succeeded.ID {
		t.Fatalf("replacement Create() = (%q, %v, %v)", replacement.ID, created, err)
	}
	if got := store.Stats().PrunedDeliveries; got != 3 {
		t.Fatalf("pruned delivery metric = %d, want 3", got)
	}

	before := store.List("")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	if got := reopened.List(""); !reflect.DeepEqual(got, before) {
		t.Fatalf("replayed state differs:\ngot  %#v\nwant %#v", got, before)
	}
}

func TestExpiredLeaseCanBeReclaimedAndRejectsStaleCompletion(t *testing.T) {
	store := openTestStore(t)
	now := time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
	item, _, err := store.Create(delivery.CreateRequest{
		TargetURL: "https://example.com/hook",
	}, now)
	if err != nil {
		t.Fatal(err)
	}

	first, err := store.ClaimDue(now, time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.ClaimDue(now.Add(2*time.Minute), time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || len(second) != 1 || second[0].Attempts != 2 {
		t.Fatalf("claims = %#v then %#v", first, second)
	}
	if err := store.Succeed(item.ID, 1, 200, now.Add(2*time.Minute)); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale Succeed() error = %v, want %v", err, ErrStaleLease)
	}
	if err := store.Succeed(item.ID, 2, 204, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(item.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != delivery.StatusSucceeded || got.LastResponseCode != 204 {
		t.Fatalf("delivery = %#v", got)
	}
}

func TestReplayIgnoresInterruptedFinalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spoold.journal")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	item, _, err := store.Create(delivery.CreateRequest{
		TargetURL: "https://example.com/hook",
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(`{"version":1,"delivery":`); err != nil {
		t.Fatal(err)
	}
	file.Close()

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	if _, err := reopened.Get(item.ID); err != nil {
		t.Fatal(err)
	}
}

func TestReplayAcceptsCompleteFinalRecordWithoutNewline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spoold.journal")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	item, _, err := store.Create(delivery.CreateRequest{
		TargetURL: "https://example.com/hook",
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data[:len(data)-1], 0o600); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	if _, err := reopened.Get(item.ID); err != nil {
		t.Fatal(err)
	}
}

func TestCompactPreservesEveryDeliveryAndIdempotencyHash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spoold.journal")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)

	succeeded := createTestDelivery(t, store, "succeeded", now)
	claim, err := store.ClaimDue(now, time.Minute, 1)
	if err != nil || len(claim) != 1 {
		t.Fatalf("claim succeeded delivery = %#v, %v", claim, err)
	}
	if err := store.Succeed(succeeded.ID, claim[0].Attempts, 204, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	failed := createTestDelivery(t, store, "failed", now.Add(time.Minute))
	claim, err = store.ClaimDue(now.Add(time.Minute), time.Minute, 1)
	if err != nil || len(claim) != 1 {
		t.Fatalf("claim failed delivery = %#v, %v", claim, err)
	}
	if err := store.Fail(failed.ID, claim[0].Attempts, 503, "unavailable", time.Time{}, true, now.Add(time.Minute+time.Second)); err != nil {
		t.Fatal(err)
	}

	canceled := createTestDelivery(t, store, "canceled", now.Add(2*time.Minute))
	if _, err := store.Cancel(canceled.ID, now.Add(2*time.Minute+time.Second)); err != nil {
		t.Fatal(err)
	}

	inFlight := createTestDelivery(t, store, "in-flight", now.Add(3*time.Minute))
	claim, err = store.ClaimDue(now.Add(3*time.Minute), time.Minute, 1)
	if err != nil || len(claim) != 1 || claim[0].ID != inFlight.ID {
		t.Fatalf("claim in-flight delivery = %#v, %v", claim, err)
	}
	createTestDelivery(t, store, "pending", now.Add(4*time.Minute))

	before := store.List("")
	beforeStats := store.Stats()
	if beforeStats.JournalRecords != 11 || beforeStats.LiveDeliveries != 5 {
		t.Fatalf("stats before compaction = %#v", beforeStats)
	}
	if err := store.Compact(); err != nil {
		t.Fatal(err)
	}
	afterStats := store.Stats()
	if afterStats.JournalRecords != 5 || afterStats.LiveDeliveries != 5 {
		t.Fatalf("stats after compaction = %#v", afterStats)
	}
	if afterStats.JournalSizeBytes >= beforeStats.JournalSizeBytes {
		t.Fatalf("journal size after compaction = %d, want less than %d", afterStats.JournalSizeBytes, beforeStats.JournalSizeBytes)
	}
	if afterStats.CompactionsSucceeded != 1 || afterStats.CompactionsFailed != 0 {
		t.Fatalf("compaction counters = %#v", afterStats)
	}
	if got := store.List(""); !reflect.DeepEqual(got, before) {
		t.Fatalf("state after compaction differs:\ngot  %#v\nwant %#v", got, before)
	}

	assertJournalIDsSorted(t, path)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	if got := reopened.List(""); !reflect.DeepEqual(got, before) {
		t.Fatalf("replayed state differs:\ngot  %#v\nwant %#v", got, before)
	}

	request := delivery.CreateRequest{
		IdempotencyKey: "failed",
		TargetURL:      "https://example.com/failed",
	}
	duplicate, created, err := reopened.Create(request, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if created || duplicate.ID != failed.ID {
		t.Fatalf("idempotent create after compaction = (%q, %v), want (%q, false)", duplicate.ID, created, failed.ID)
	}
	request.TargetURL = "https://example.com/different"
	if _, _, err := reopened.Create(request, now.Add(time.Hour)); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting create after compaction error = %v, want %v", err, ErrIdempotencyConflict)
	}
}

func TestCompactFailureBeforeRenameLeavesOriginalJournalWritable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spoold.journal")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	first := createTestDelivery(t, store, "first", time.Now())
	store.compactionHook = func(stage compactionStage) error {
		if stage == compactionBeforeRename {
			return errors.New("injected failure")
		}
		return nil
	}
	if err := store.Compact(); err == nil {
		t.Fatal("Compact() error = nil, want injected failure")
	}
	store.compactionHook = nil
	second := createTestDelivery(t, store, "second", time.Now().Add(time.Second))
	stats := store.Stats()
	if stats.CompactionsFailed != 1 || stats.CompactionsSucceeded != 0 || stats.JournalRecords != 2 {
		t.Fatalf("stats = %#v", stats)
	}
	assertNoCompactionFiles(t, path)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	assertReplaysIDs(t, path, first.ID, second.ID)
}

func TestCompactFailureAfterReplacementKeepsSubsequentAppendsRecoverable(t *testing.T) {
	for _, failureStage := range []compactionStage{compactionAfterRename, compactionAfterDirSync} {
		t.Run(string(failureStage), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "spoold.journal")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			first := createTestDelivery(t, store, "first", time.Now())
			claimed, err := store.ClaimDue(time.Now().Add(time.Second), time.Minute, 1)
			if err != nil || len(claimed) != 1 {
				t.Fatalf("ClaimDue() = %#v, %v", claimed, err)
			}
			if err := store.Succeed(first.ID, claimed[0].Attempts, 200, time.Now().Add(2*time.Second)); err != nil {
				t.Fatal(err)
			}

			store.compactionHook = func(stage compactionStage) error {
				if stage == failureStage {
					return errors.New("injected failure")
				}
				return nil
			}
			if err := store.Compact(); err == nil {
				t.Fatal("Compact() error = nil, want injected failure")
			}
			store.compactionHook = nil
			second := createTestDelivery(t, store, "second", time.Now().Add(3*time.Second))
			stats := store.Stats()
			if stats.CompactionsFailed != 1 || stats.CompactionsSucceeded != 0 || stats.JournalRecords != 2 {
				t.Fatalf("stats = %#v", stats)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			assertReplaysIDs(t, path, first.ID, second.ID)
		})
	}
}

func TestOpenRemovesAbandonedCompactionFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spoold.journal")
	stale := filepath.Join(dir, compactionPrefix(path)+"stale")
	if err := os.WriteFile(stale, []byte("abandoned"), 0o600); err != nil {
		t.Fatal(err)
	}
	unrelated := filepath.Join(dir, ".other.journal.compact-stale")
	if err := os.WriteFile(unrelated, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale compaction file still exists: %v", err)
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Fatalf("unrelated file was removed: %v", err)
	}
}

func TestConcurrentMutationAndCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spoold.journal")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	const deliveries = 24

	var wg sync.WaitGroup
	errs := make(chan error, deliveries+12)
	for i := range deliveries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := store.Create(delivery.CreateRequest{
				IdempotencyKey: string(rune('a' + i)),
				TargetURL:      "https://example.com/hook",
			}, now.Add(time.Duration(i)*time.Millisecond))
			if err != nil {
				errs <- err
			}
		}()
	}
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := store.Compact(); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := len(store.List("")); got != deliveries {
		t.Fatalf("live deliveries = %d, want %d", got, deliveries)
	}
	if err := store.Compact(); err != nil {
		t.Fatal(err)
	}
	if got := store.Stats().JournalRecords; got != deliveries {
		t.Fatalf("journal records = %d, want %d", got, deliveries)
	}
	before := store.List("")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	if got := reopened.List(""); !reflect.DeepEqual(got, before) {
		t.Fatalf("replayed state differs:\ngot  %#v\nwant %#v", got, before)
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "spoold.journal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func createTestDelivery(t *testing.T, store *Store, key string, now time.Time) delivery.Delivery {
	t.Helper()
	item, _, err := store.Create(delivery.CreateRequest{
		IdempotencyKey: key,
		TargetURL:      "https://example.com/" + key,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	return item
}

func assertJournalIDsSorted(t *testing.T, path string) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var ids []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record journalRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, record.Delivery.ID)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if !sort.StringsAreSorted(ids) {
		t.Fatalf("compacted journal IDs are not sorted: %v", ids)
	}
}

func assertNoCompactionFiles(t *testing.T, path string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), compactionPrefix(path)) {
			t.Fatalf("abandoned compaction file %q", entry.Name())
		}
	}
}

func assertReplaysIDs(t *testing.T, path string, ids ...string) {
	t.Helper()
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, id := range ids {
		if _, err := reopened.Get(id); err != nil {
			t.Fatalf("Get(%q) after replay: %v", id, err)
		}
	}
}
