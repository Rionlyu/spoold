package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

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

func TestMetricsExposeDeliveryCounts(t *testing.T) {
	server, _ := newTestServer(t)
	request(t, server, http.MethodPost, "/v1/deliveries", `{"targetUrl":"https://example.com"}`)
	got := request(t, server, http.MethodGet, "/metrics", "")
	if got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `spoold_deliveries{status="pending"} 1`) {
		t.Fatalf("status = %d, body = %s", got.Code, got.Body.String())
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
