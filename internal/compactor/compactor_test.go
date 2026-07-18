package compactor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Rionlyu/spoold/internal/store"
)

func TestCompactIfNeededAppliesThresholdAndRedundancyRatio(t *testing.T) {
	tests := []struct {
		name      string
		stats     store.Stats
		threshold int64
		wantCalls int
	}{
		{
			name: "below size threshold",
			stats: store.Stats{
				JournalSizeBytes: 99,
				JournalRecords:   20,
				LiveDeliveries:   10,
			},
			threshold: 100,
		},
		{
			name: "below redundancy ratio",
			stats: store.Stats{
				JournalSizeBytes: 100,
				JournalRecords:   19,
				LiveDeliveries:   10,
			},
			threshold: 100,
		},
		{
			name: "at both boundaries",
			stats: store.Stats{
				JournalSizeBytes: 100,
				JournalRecords:   20,
				LiveDeliveries:   10,
			},
			threshold: 100,
			wantCalls: 1,
		},
		{
			name: "empty live set",
			stats: store.Stats{
				JournalSizeBytes: 100,
			},
			threshold: 100,
			wantCalls: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			journal := &fakeStore{stats: test.stats}
			compactor := New(journal, discardLogger(), Config{
				ThresholdBytes: test.threshold,
			})
			compactor.compactIfNeeded()
			if got := journal.compactCalls(); got != test.wantCalls {
				t.Fatalf("Compact() calls = %d, want %d", got, test.wantCalls)
			}
		})
	}
}

func TestDisabledCompactorDoesNotStart(t *testing.T) {
	journal := &fakeStore{
		stats: store.Stats{
			JournalSizeBytes: 100,
			JournalRecords:   2,
			LiveDeliveries:   1,
		},
	}
	compactor := New(journal, discardLogger(), Config{
		ThresholdBytes: 0,
		CheckInterval:  time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	compactor.Start(ctx)
	time.Sleep(10 * time.Millisecond)
	cancel()
	compactor.Wait()
	if got := journal.compactCalls(); got != 0 {
		t.Fatalf("Compact() calls = %d, want 0", got)
	}
}

func TestCompactorStopsGracefully(t *testing.T) {
	journal := &fakeStore{
		stats: store.Stats{
			JournalSizeBytes: 100,
			JournalRecords:   2,
			LiveDeliveries:   1,
		},
	}
	compactor := New(journal, discardLogger(), Config{
		ThresholdBytes: 100,
		CheckInterval:  time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	compactor.Start(ctx)
	waitForCompactions(t, journal, 1)
	cancel()

	done := make(chan struct{})
	go func() {
		compactor.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait() did not return after cancellation")
	}
	calls := journal.compactCalls()
	time.Sleep(5 * time.Millisecond)
	if got := journal.compactCalls(); got != calls {
		t.Fatalf("Compact() calls after Wait = %d, want %d", got, calls)
	}
}

func TestCompactionFailureIsNonFatal(t *testing.T) {
	journal := &fakeStore{
		stats: store.Stats{
			JournalSizeBytes: 100,
			JournalRecords:   2,
			LiveDeliveries:   1,
		},
		failures: 1,
	}
	compactor := New(journal, discardLogger(), Config{
		ThresholdBytes: 100,
		CheckInterval:  time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	compactor.Start(ctx)
	waitForCompactions(t, journal, 2)
	cancel()
	compactor.Wait()
}

func TestMaintenanceRepairsUnreadyJournalBelowThreshold(t *testing.T) {
	journal := &fakeStore{
		stats: store.Stats{
			JournalSizeBytes: 10,
			JournalRecords:   1,
			LiveDeliveries:   1,
		},
		readyErr: errors.New("injected persistence failure"),
	}
	compactor := New(journal, discardLogger(), Config{
		ThresholdBytes: 100,
	})

	compactor.maintain()

	if got := journal.compactCalls(); got != 1 {
		t.Fatalf("Compact() calls = %d, want 1", got)
	}
	if err := journal.Ready(); err != nil {
		t.Fatalf("Ready() after repair = %v, want nil", err)
	}
}

func TestRetentionPrunesTerminalDeliveriesAndCompactsTombstones(t *testing.T) {
	now := time.Now()
	journal := &fakeStore{
		stats: store.Stats{
			JournalSizeBytes: 100,
			JournalRecords:   2,
			LiveDeliveries:   2,
		},
		items: []time.Time{
			now.Add(-2 * time.Hour),
			now.Add(-30 * time.Minute),
		},
	}
	compactor := New(journal, discardLogger(), Config{
		ThresholdBytes: 0,
		Retention:      time.Hour,
	})
	compactor.maintain()

	if got := journal.compactCalls(); got != 1 {
		t.Fatalf("Compact() calls = %d, want 1", got)
	}
	stats := journal.Stats()
	if stats.LiveDeliveries != 1 || stats.JournalRecords != 1 || stats.PrunedDeliveries != 1 {
		t.Fatalf("stats = %#v", stats)
	}
}

type fakeStore struct {
	mu       sync.Mutex
	stats    store.Stats
	calls    int
	failures int
	items    []time.Time
	readyErr error
}

func (s *fakeStore) Stats() store.Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *fakeStore) Ready() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readyErr
}

func (s *fakeStore) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failures > 0 {
		s.failures--
		s.stats.CompactionsFailed++
		return errors.New("injected failure")
	}
	s.stats.JournalSizeBytes /= 2
	s.stats.JournalRecords = s.stats.LiveDeliveries
	s.stats.CompactionsSucceeded++
	s.readyErr = nil
	return nil
}

func (s *fakeStore) compactCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *fakeStore) PruneTerminal(before time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	kept := s.items[:0]
	pruned := 0
	for _, updatedAt := range s.items {
		if updatedAt.After(before) {
			kept = append(kept, updatedAt)
		} else {
			pruned++
		}
	}
	s.items = kept
	s.stats.LiveDeliveries -= uint64(pruned)
	s.stats.JournalRecords += uint64(pruned)
	s.stats.PrunedDeliveries += uint64(pruned)
	return pruned, nil
}

func waitForCompactions(t *testing.T, store *fakeStore, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if store.compactCalls() >= count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Compact() calls = %d, want at least %d", store.compactCalls(), count)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
