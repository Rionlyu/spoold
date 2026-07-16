package store

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "spoold.journal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}
