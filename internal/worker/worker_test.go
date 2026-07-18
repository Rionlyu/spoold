package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Rionlyu/spoold/internal/delivery"
	"github.com/Rionlyu/spoold/internal/store"
	"github.com/Rionlyu/spoold/internal/target"
)

func TestPoolRetriesThenSucceeds(t *testing.T) {
	var requests atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := requests.Add(1)
		if r.Header.Get("X-Spoold-Delivery-ID") == "" {
			t.Error("missing delivery id header")
		}
		if attempt == 1 {
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer destination.Close()

	journal, err := store.Open(filepath.Join(t.TempDir(), "journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	item, _, err := journal.Create(delivery.CreateRequest{
		TargetURL:   destination.URL,
		Body:        json.RawMessage(`{"event":"ready"}`),
		MaxAttempts: 3,
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	pool := New(journal, target.NewClient(true, time.Second), slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		Concurrency:   1,
		PollInterval:  time.Millisecond,
		LeaseDuration: time.Second,
		BaseBackoff:   time.Millisecond,
		MaxBackoff:    5 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)
	defer func() {
		cancel()
		pool.Wait()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, err := journal.Get(item.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status == delivery.StatusSucceeded {
			if got.Attempts != 2 || requests.Load() != 2 {
				t.Fatalf("delivery = %#v, requests = %d", got, requests.Load())
			}
			metrics := pool.Metrics()
			if metrics.Attempts != 2 || metrics.Succeeded != 1 || metrics.RetryableFailure != 1 {
				t.Fatalf("metrics = %#v", metrics)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("delivery did not succeed before timeout")
}

func TestPoolDoesNotRetryClientError(t *testing.T) {
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "invalid", http.StatusBadRequest)
	}))
	defer destination.Close()

	journal, err := store.Open(filepath.Join(t.TempDir(), "journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	item, _, err := journal.Create(delivery.CreateRequest{
		TargetURL:   destination.URL,
		MaxAttempts: 5,
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	pool := New(journal, target.NewClient(true, time.Second), slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		Concurrency:  1,
		PollInterval: time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)
	defer func() {
		cancel()
		pool.Wait()
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		got, err := journal.Get(item.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status == delivery.StatusFailed {
			if got.Attempts != 1 || got.LastResponseCode != http.StatusBadRequest {
				t.Fatalf("delivery = %#v", got)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("delivery did not fail before timeout")
}

func TestPoolDoesNotLetOneTargetConsumeAllWorkers(t *testing.T) {
	blocked := make(chan struct{})
	firstTargetStarted := make(chan struct{}, 1)
	firstTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case firstTargetStarted <- struct{}{}:
		default:
		}
		<-blocked
		w.WriteHeader(http.StatusNoContent)
	}))
	defer firstTarget.Close()

	secondTargetCalled := make(chan struct{}, 1)
	secondTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondTargetCalled <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer secondTarget.Close()

	journal, err := store.Open(filepath.Join(t.TempDir(), "journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	now := time.Now()
	for index, targetURL := range []string{firstTarget.URL, firstTarget.URL, secondTarget.URL} {
		if _, _, err := journal.Create(delivery.CreateRequest{
			TargetURL: targetURL,
		}, now.Add(time.Duration(index)*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	}

	pool := New(journal, target.NewClient(true, time.Second), slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		Concurrency:  2,
		PerTarget:    1,
		PollInterval: time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)
	defer func() {
		close(blocked)
		cancel()
		pool.Wait()
	}()

	select {
	case <-firstTargetStarted:
	case <-time.After(time.Second):
		t.Fatal("first target was not called")
	}
	select {
	case <-secondTargetCalled:
	case <-time.After(time.Second):
		t.Fatal("second target was starved by the first target")
	}
}

func TestSendDefaultsBinaryBodyContentType(t *testing.T) {
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Content-Type"); got != "application/octet-stream" {
			t.Errorf("Content-Type = %q, want application/octet-stream", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		if string(body) != "\x00\xff" {
			t.Errorf("body = %v", body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer destination.Close()

	pool := New(nil, target.NewClient(true, time.Second), slog.New(slog.NewTextHandler(io.Discard, nil)), Config{})
	status, err := pool.send(context.Background(), delivery.Delivery{
		ID:        "binary",
		TargetURL: destination.URL,
		Method:    http.MethodPost,
		Body:      []byte{0x00, 0xff},
		Attempts:  1,
	})
	if err != nil || status != http.StatusNoContent {
		t.Fatalf("send() = (%d, %v)", status, err)
	}
}

func TestBackoffIsBounded(t *testing.T) {
	for attempt := 1; attempt <= 20; attempt++ {
		got := Backoff(time.Second, 10*time.Second, "delivery", attempt)
		if got <= 0 || got > 10*time.Second {
			t.Fatalf("Backoff(attempt=%d) = %s", attempt, got)
		}
	}
}
