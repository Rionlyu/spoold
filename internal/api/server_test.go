package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Rionlyu/spoold/internal/delivery"
	"github.com/Rionlyu/spoold/internal/store"
)

func TestCreateAndReadDelivery(t *testing.T) {
	server, journal := newTestServer(t)
	body := `{
		"idempotencyKey":"order-42",
		"targetUrl":"https://example.com/hooks",
		"body":{"order":42},
		"maxAttempts":3
	}`

	first := request(t, server, http.MethodPost, "/v1/deliveries", body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, body = %s", first.Code, first.Body.String())
	}
	var created delivery.Delivery
	if err := json.Unmarshal(first.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	second := request(t, server, http.MethodPost, "/v1/deliveries", body)
	if second.Code != http.StatusOK {
		t.Fatalf("second status = %d, body = %s", second.Code, second.Body.String())
	}

	get := request(t, server, http.MethodGet, "/v1/deliveries/"+created.ID, "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", get.Code, get.Body.String())
	}
	stored, err := journal.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != delivery.StatusPending || stored.MaxAttempts != 3 {
		t.Fatalf("stored delivery = %#v", stored)
	}
}

func TestConflictingIdempotencyKeyReturnsConflict(t *testing.T) {
	server, _ := newTestServer(t)
	first := `{"idempotencyKey":"same","targetUrl":"https://example.com/a"}`
	second := `{"idempotencyKey":"same","targetUrl":"https://example.com/b"}`
	if got := request(t, server, http.MethodPost, "/v1/deliveries", first); got.Code != http.StatusCreated {
		t.Fatalf("first status = %d", got.Code)
	}
	if got := request(t, server, http.MethodPost, "/v1/deliveries", second); got.Code != http.StatusConflict {
		t.Fatalf("second status = %d, body = %s", got.Code, got.Body.String())
	}
}

func TestCreateAcceptsBase64Body(t *testing.T) {
	server, journal := newTestServer(t)
	got := request(t, server, http.MethodPost, "/v1/deliveries", `{
		"targetUrl":"https://example.com/upload",
		"bodyBase64":"AP8Q"
	}`)
	if got.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", got.Code, got.Body.String())
	}
	items := journal.List("")
	if len(items) != 1 || !bytes.Equal(items[0].Body, []byte{0x00, 0xff, 0x10}) {
		t.Fatalf("deliveries = %#v", items)
	}
	if !strings.Contains(got.Body.String(), `"bodyBase64":"AP8Q"`) {
		t.Fatalf("response body = %s", got.Body.String())
	}
}

func TestCreateRejectsAmbiguousOrInvalidBase64Body(t *testing.T) {
	server, _ := newTestServer(t)
	for _, body := range []string{
		`{"targetUrl":"https://example.com","body":{},"bodyBase64":"e30="}`,
		`{"targetUrl":"https://example.com","bodyBase64":"not base64"}`,
	} {
		got := request(t, server, http.MethodPost, "/v1/deliveries", body)
		if got.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, body = %s", got.Code, got.Body.String())
		}
	}
}

func TestCancelDeliveryAndFilterList(t *testing.T) {
	server, _ := newTestServer(t)
	created := request(t, server, http.MethodPost, "/v1/deliveries", `{"targetUrl":"https://example.com"}`)
	var item delivery.Delivery
	if err := json.Unmarshal(created.Body.Bytes(), &item); err != nil {
		t.Fatal(err)
	}

	canceled := request(t, server, http.MethodPost, "/v1/deliveries/"+item.ID+"/cancel", "")
	if canceled.Code != http.StatusOK {
		t.Fatalf("cancel status = %d, body = %s", canceled.Code, canceled.Body.String())
	}
	list := request(t, server, http.MethodGet, "/v1/deliveries?status=canceled", "")
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), item.ID) {
		t.Fatalf("list status = %d, body = %s", list.Code, list.Body.String())
	}
}

func TestListOmitsPayloadWhileGetReturnsIt(t *testing.T) {
	server, _ := newTestServer(t)
	created := request(t, server, http.MethodPost, "/v1/deliveries", `{
		"targetUrl":"https://example.com",
		"headers":{"X-Secret":"value"},
		"bodyBase64":"AP8Q"
	}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", created.Code, created.Body.String())
	}
	var item delivery.Delivery
	if err := json.Unmarshal(created.Body.Bytes(), &item); err != nil {
		t.Fatal(err)
	}

	list := request(t, server, http.MethodGet, "/v1/deliveries", "")
	if strings.Contains(list.Body.String(), "bodyBase64") || strings.Contains(list.Body.String(), "X-Secret") {
		t.Fatalf("list exposed payload: %s", list.Body.String())
	}
	get := request(t, server, http.MethodGet, "/v1/deliveries/"+item.ID, "")
	if !strings.Contains(get.Body.String(), `"bodyBase64":"AP8Q"`) ||
		!strings.Contains(get.Body.String(), `"X-Secret":"value"`) {
		t.Fatalf("get omitted payload: %s", get.Body.String())
	}
}

func TestRejectsUnknownJSONField(t *testing.T) {
	server, _ := newTestServer(t)
	got := request(t, server, http.MethodPost, "/v1/deliveries", `{
		"targetUrl":"https://example.com",
		"maxRetires":3
	}`)
	if got.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", got.Code, got.Body.String())
	}
}

func TestReadinessReflectsJournalAvailability(t *testing.T) {
	server, journal := newTestServer(t)
	if got := request(t, server, http.MethodGet, "/readyz", ""); got.Code != http.StatusOK {
		t.Fatalf("ready status = %d, body = %s", got.Code, got.Body.String())
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	if got := request(t, server, http.MethodGet, "/healthz", ""); got.Code != http.StatusOK {
		t.Fatalf("health status = %d, body = %s", got.Code, got.Body.String())
	}
	if got := request(t, server, http.MethodGet, "/readyz", ""); got.Code != http.StatusServiceUnavailable {
		t.Fatalf("unready status = %d, body = %s", got.Code, got.Body.String())
	}
}

func TestCreateReturnsInsufficientStorageAtAdmissionLimit(t *testing.T) {
	journal, err := store.Open(filepath.Join(t.TempDir(), "journal"), store.Options{
		MaxJournalBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { journal.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := New(journal, nil, logger)

	got := request(t, server, http.MethodPost, "/v1/deliveries", `{"targetUrl":"https://example.com"}`)
	if got.Code != http.StatusInsufficientStorage {
		t.Fatalf("status = %d, body = %s", got.Code, got.Body.String())
	}
	if !strings.Contains(got.Body.String(), "journal_full") {
		t.Fatalf("body = %s", got.Body.String())
	}
}

func TestStatusRecorderKeepsFirstResponseStatus(t *testing.T) {
	response := httptest.NewRecorder()
	recorder := &statusRecorder{
		ResponseWriter: response,
		status:         http.StatusOK,
	}
	recorder.WriteHeader(http.StatusCreated)
	recorder.WriteHeader(http.StatusInternalServerError)

	if recorder.status != http.StatusCreated {
		t.Fatalf("recorded status = %d, want %d", recorder.status, http.StatusCreated)
	}
	if response.Code != http.StatusCreated {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusCreated)
	}
}

func TestStatusRecorderCapturesImplicitOKStatus(t *testing.T) {
	response := httptest.NewRecorder()
	recorder := &statusRecorder{
		ResponseWriter: response,
		status:         http.StatusOK,
	}
	if _, err := recorder.Write([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	recorder.WriteHeader(http.StatusInternalServerError)

	if recorder.status != http.StatusOK {
		t.Fatalf("recorded status = %d, want %d", recorder.status, http.StatusOK)
	}
	if response.Code != http.StatusOK {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusOK)
	}
}

func TestMetricsExposeDeliveryAndJournalHealth(t *testing.T) {
	server, journal := newTestServer(t)
	request(t, server, http.MethodPost, "/v1/deliveries", `{"targetUrl":"https://example.com"}`)
	if _, err := journal.ClaimDue(time.Now().Add(time.Minute), time.Minute, 1); err != nil {
		t.Fatal(err)
	}
	if err := journal.Compact(); err != nil {
		t.Fatal(err)
	}
	stats := journal.Stats()

	got := request(t, server, http.MethodGet, "/metrics", "")
	if got.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", got.Code, got.Body.String())
	}
	body := got.Body.String()
	for _, metric := range []string{
		`spoold_deliveries{status="in_flight"} 1`,
		"spoold_journal_size_bytes " + strconv.FormatInt(stats.JournalSizeBytes, 10),
		"spoold_journal_records 1",
		"spoold_journal_max_bytes 0",
		"spoold_journal_pruned_deliveries_total 0",
		`spoold_journal_compactions_total{result="succeeded"} 1`,
		`spoold_journal_compactions_total{result="failed"} 0`,
	} {
		if !strings.Contains(body, metric) {
			t.Errorf("metrics do not contain %q:\n%s", metric, body)
		}
	}
}

func newTestServer(t *testing.T) (*Server, *store.Store) {
	t.Helper()
	journal, err := store.Open(filepath.Join(t.TempDir(), "journal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { journal.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(journal, nil, logger), journal
}

func request(t *testing.T, handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	return response
}
