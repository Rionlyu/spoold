package spoolctl

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Rionlyu/spoold/internal/api"
	"github.com/Rionlyu/spoold/internal/delivery"
	"github.com/Rionlyu/spoold/internal/store"
)

func TestSendCreatesDeliveryThroughAPI(t *testing.T) {
	server, journal := newSpooldServer(t)
	var stdout, stderr bytes.Buffer
	code := Run(context.Background(), []string{
		"send",
		"--server", server.URL,
		"--idempotency-key", "edge-reading-42",
		"--header", "X-Event-Type: sensor.reading",
		"--data", `{"temperature":21.5}`,
		"https://example.com/readings",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "queued") || !strings.Contains(stdout.String(), "https://example.com/readings") {
		t.Fatalf("stdout = %q", stdout.String())
	}

	items := journal.List("")
	if len(items) != 1 {
		t.Fatalf("deliveries = %d, want 1", len(items))
	}
	item := items[0]
	if item.IdempotencyKey != "edge-reading-42" ||
		item.Headers["X-Event-Type"] != "sensor.reading" ||
		string(item.Body) != `{"temperature":21.5}` {
		t.Fatalf("delivery = %#v", item)
	}
}

func TestSendReportsIdempotentReuse(t *testing.T) {
	server, journal := newSpooldServer(t)
	args := []string{
		"send",
		"--server", server.URL,
		"--idempotency-key", "same",
		"https://example.com/hook",
	}
	for index, expected := range []string{"queued", "existing"} {
		var stdout, stderr bytes.Buffer
		if code := Run(context.Background(), args, strings.NewReader(""), &stdout, &stderr); code != 0 {
			t.Fatalf("run %d: exit code = %d, stderr = %s", index, code, stderr.String())
		}
		if !strings.Contains(stdout.String(), expected) {
			t.Fatalf("run %d: stdout = %q, want %q", index, stdout.String(), expected)
		}
	}
	if got := len(journal.List("")); got != 1 {
		t.Fatalf("deliveries = %d, want 1", got)
	}
}

func TestListJSONReturnsMachineReadableAPIShape(t *testing.T) {
	server, journal := newSpooldServer(t)
	if _, _, err := journal.Create(delivery.CreateRequest{
		TargetURL: "https://example.com/hook",
	}, testTime()); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := Run(context.Background(), []string{
		"list",
		"--server", server.URL,
		"--status", "pending",
		"--json",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	var response listResponse
	if err := json.Unmarshal(stdout.Bytes(), &response); err != nil {
		t.Fatalf("decode stdout: %v\n%s", err, stdout.String())
	}
	if response.Count != 1 || len(response.Deliveries) != 1 {
		t.Fatalf("response = %#v", response)
	}
}

func TestSendReadsBodyFromStdin(t *testing.T) {
	server, journal := newSpooldServer(t)
	var stdout, stderr bytes.Buffer
	code := Run(context.Background(), []string{
		"send",
		"--server", server.URL,
		"--data-file", "-",
		"https://example.com/hook",
	}, strings.NewReader(`{"offline":true}`), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	if got := string(journal.List("")[0].Body); got != `{"offline":true}` {
		t.Fatalf("body = %q", got)
	}
}

func TestGetAndCancelUseDeliveryLifecycleAPI(t *testing.T) {
	server, journal := newSpooldServer(t)
	item, _, err := journal.Create(delivery.CreateRequest{
		TargetURL: "https://example.com/hook",
	}, testTime())
	if err != nil {
		t.Fatal(err)
	}

	for _, command := range []string{"get", "cancel"} {
		var stdout, stderr bytes.Buffer
		code := Run(context.Background(), []string{
			command,
			"--server", server.URL,
			item.ID,
		}, strings.NewReader(""), &stdout, &stderr)
		if code != 0 {
			t.Fatalf("%s: exit code = %d, stderr = %s", command, code, stderr.String())
		}
		if !strings.Contains(stdout.String(), item.ID) {
			t.Fatalf("%s: stdout = %q", command, stdout.String())
		}
	}

	stored, err := journal.Get(item.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != delivery.StatusCanceled {
		t.Fatalf("status = %s, want canceled", stored.Status)
	}
}

func TestGetShowsLastDeliveryErrorAndRetryRequeues(t *testing.T) {
	server, journal := newSpooldServer(t)
	item, _, err := journal.Create(delivery.CreateRequest{
		TargetURL: "https://example.com/hook",
	}, testTime())
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := journal.ClaimDue(testTime(), time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Fail(
		item.ID,
		claimed[0].Attempts,
		http.StatusServiceUnavailable,
		"destination unavailable",
		time.Time{},
		true,
		testTime().Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}

	var getOutput, getErrors bytes.Buffer
	if code := Run(context.Background(), []string{
		"get",
		"--server", server.URL,
		item.ID,
	}, strings.NewReader(""), &getOutput, &getErrors); code != 0 {
		t.Fatalf("get: exit code = %d, stderr = %s", code, getErrors.String())
	}
	if !strings.Contains(getOutput.String(), "last error: destination unavailable") {
		t.Fatalf("get stdout = %q", getOutput.String())
	}

	var retryOutput, retryErrors bytes.Buffer
	if code := Run(context.Background(), []string{
		"retry",
		"--server", server.URL,
		item.ID,
	}, strings.NewReader(""), &retryOutput, &retryErrors); code != 0 {
		t.Fatalf("retry: exit code = %d, stderr = %s", code, retryErrors.String())
	}
	stored, err := journal.Get(item.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != delivery.StatusPending || stored.Attempts != 0 || stored.LastError != "" {
		t.Fatalf("retried delivery = %#v", stored)
	}
}

func TestListHumanOutputIncludesTable(t *testing.T) {
	server, journal := newSpooldServer(t)
	if _, _, err := journal.Create(delivery.CreateRequest{
		TargetURL: "https://example.com/hook",
		Method:    http.MethodPut,
	}, testTime()); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := Run(context.Background(), []string{
		"list",
		"--server", server.URL,
		"--limit", "1",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	for _, text := range []string{"ID", "STATUS", "ATTEMPTS", "PUT", "https://example.com/hook"} {
		if !strings.Contains(stdout.String(), text) {
			t.Fatalf("stdout = %q, want %q", stdout.String(), text)
		}
	}
}

func TestCommandUsageExitCodes(t *testing.T) {
	tests := []struct {
		name string
		args []string
		code int
		text string
	}{
		{name: "no command", code: 2, text: "Usage:"},
		{name: "help", args: []string{"help"}, code: 0, text: "crash-safe HTTP deliveries"},
		{name: "unknown", args: []string{"unknown"}, code: 2, text: "unknown command"},
		{name: "send missing URL", args: []string{"send"}, code: 2, text: "requires exactly one target URL"},
		{name: "list extra argument", args: []string{"list", "extra"}, code: 2, text: "does not accept"},
		{name: "invalid limit", args: []string{"list", "--limit", "0"}, code: 2, text: "between 1 and 500"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := Run(context.Background(), test.args, strings.NewReader(""), &stdout, &stderr)
			if code != test.code {
				t.Fatalf("exit code = %d, want %d", code, test.code)
			}
			if combined := stdout.String() + stderr.String(); !strings.Contains(combined, test.text) {
				t.Fatalf("output = %q, want %q", combined, test.text)
			}
		})
	}
}

func TestSendValidatesClientOptions(t *testing.T) {
	tests := []struct {
		name string
		args []string
		text string
	}{
		{
			name: "two body sources",
			args: []string{"send", "--data", "{}", "--data-file", "-", "https://example.com"},
			text: "cannot be used together",
		},
		{
			name: "invalid server",
			args: []string{"send", "--server", "unix:///tmp/spoold.sock", "https://example.com"},
			text: "must use http or https",
		},
		{
			name: "invalid header",
			args: []string{"send", "--header", "missing-colon", "https://example.com"},
			text: "Name: value",
		},
		{
			name: "duplicate header",
			args: []string{"send", "--header", "x-test: one", "--header", "X-Test: two", "https://example.com"},
			text: "provided more than once",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := Run(context.Background(), test.args, strings.NewReader("{}"), &stdout, &stderr)
			if code != 2 {
				t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), test.text) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), test.text)
			}
		})
	}
}

func TestSendRejectsInvalidJSONBeforeContactingServer(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests++
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := Run(context.Background(), []string{
		"send",
		"--server", server.URL,
		"--data", "{",
		"https://example.com/hook",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != 2 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	if requests != 0 {
		t.Fatalf("server requests = %d, want 0", requests)
	}
	if !strings.Contains(stderr.String(), "valid JSON") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestRemoteAPIErrorIsActionable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		io.WriteString(w, `{"error":{"code":"idempotency_conflict","message":"key already has different content"}}`)
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := Run(context.Background(), []string{
		"send",
		"--server", server.URL,
		"https://example.com/hook",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	for _, text := range []string{"409", "idempotency_conflict", "different content"} {
		if !strings.Contains(stderr.String(), text) {
			t.Fatalf("stderr = %q, want %q", stderr.String(), text)
		}
	}
}

func newSpooldServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	journal, err := store.Open(filepath.Join(t.TempDir(), "journal"))
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := httptest.NewServer(api.New(journal, nil, logger))
	t.Cleanup(func() {
		server.Close()
		journal.Close()
	})
	return server, journal
}

func testTime() (result time.Time) {
	return time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
}
