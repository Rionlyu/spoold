package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Rionlyu/spoold/internal/delivery"
	"github.com/Rionlyu/spoold/internal/store"
	"github.com/Rionlyu/spoold/internal/worker"
)

const maxRequestBody = 1 << 20

type metricsSource interface {
	Metrics() worker.Metrics
}

type Server struct {
	store   *store.Store
	metrics metricsSource
	log     *slog.Logger
	handler http.Handler
}

type createRequest struct {
	IdempotencyKey string            `json:"idempotencyKey"`
	TargetURL      string            `json:"targetUrl"`
	Method         string            `json:"method"`
	Headers        map[string]string `json:"headers"`
	Body           json.RawMessage   `json:"body"`
	MaxAttempts    int               `json:"maxAttempts"`
}

type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func New(journal *store.Store, metrics metricsSource, logger *slog.Logger) *Server {
	server := &Server{
		store:   journal,
		metrics: metrics,
		log:     logger,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.health)
	mux.HandleFunc("GET /readyz", server.health)
	mux.HandleFunc("GET /metrics", server.renderMetrics)
	mux.HandleFunc("POST /v1/deliveries", server.create)
	mux.HandleFunc("GET /v1/deliveries", server.list)
	mux.HandleFunc("GET /v1/deliveries/{id}", server.get)
	mux.HandleFunc("POST /v1/deliveries/{id}/cancel", server.cancel)
	mux.HandleFunc("POST /v1/deliveries/{id}/retry", server.retry)
	server.handler = server.accessLog(mux)
	return server
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var payload createRequest
	if err := decodeJSON(w, r, &payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	item, created, err := s.store.Create(delivery.CreateRequest{
		IdempotencyKey: payload.IdempotencyKey,
		TargetURL:      payload.TargetURL,
		Method:         payload.Method,
		Headers:        payload.Headers,
		Body:           payload.Body,
		MaxAttempts:    payload.MaxAttempts,
	}, time.Now())
	if err != nil {
		if errors.Is(err, store.ErrIdempotencyConflict) {
			writeError(w, http.StatusConflict, "idempotency_conflict", err.Error())
			return
		}
		if errors.Is(err, store.ErrPersistence) {
			s.log.Error("persist delivery", "error", err)
			writeError(w, http.StatusInternalServerError, "persistence_failed", "delivery could not be persisted")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_delivery", err.Error())
		return
	}

	w.Header().Set("Location", "/v1/deliveries/"+item.ID)
	if created {
		writeJSON(w, http.StatusCreated, item)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	status := delivery.Status(r.URL.Query().Get("status"))
	if status != "" && !validStatus(status) {
		writeError(w, http.StatusBadRequest, "invalid_status", "unknown delivery status")
		return
	}

	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 500 {
			writeError(w, http.StatusBadRequest, "invalid_limit", "limit must be between 1 and 500")
			return
		}
		limit = parsed
	}

	items := s.store.List(status)
	if len(items) > limit {
		items = items[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"deliveries": items,
		"count":      len(items),
	})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	item, err := s.store.Get(r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	item, err := s.store.Cancel(r.PathValue("id"), time.Now())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) retry(w http.ResponseWriter, r *http.Request) {
	item, err := s.store.Retry(r.PathValue("id"), time.Now())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) renderMetrics(w http.ResponseWriter, _ *http.Request) {
	counts := s.store.Counts()
	journal := s.store.Stats()
	runtime := worker.Metrics{}
	if s.metrics != nil {
		runtime = s.metrics.Metrics()
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintln(w, "# HELP spoold_deliveries Current deliveries by status.")
	fmt.Fprintln(w, "# TYPE spoold_deliveries gauge")
	for _, status := range []delivery.Status{
		delivery.StatusPending,
		delivery.StatusInFlight,
		delivery.StatusSucceeded,
		delivery.StatusFailed,
		delivery.StatusCanceled,
	} {
		fmt.Fprintf(w, "spoold_deliveries{status=%q} %d\n", status, counts[status])
	}
	fmt.Fprintln(w, "# HELP spoold_delivery_attempts_total Outbound delivery attempts.")
	fmt.Fprintln(w, "# TYPE spoold_delivery_attempts_total counter")
	fmt.Fprintf(w, "spoold_delivery_attempts_total %d\n", runtime.Attempts)
	fmt.Fprintln(w, "# HELP spoold_delivery_results_total Recorded delivery results.")
	fmt.Fprintln(w, "# TYPE spoold_delivery_results_total counter")
	fmt.Fprintf(w, "spoold_delivery_results_total{result=\"succeeded\"} %d\n", runtime.Succeeded)
	fmt.Fprintf(w, "spoold_delivery_results_total{result=\"retryable_failure\"} %d\n", runtime.RetryableFailure)
	fmt.Fprintf(w, "spoold_delivery_results_total{result=\"terminal_failure\"} %d\n", runtime.TerminalFailure)
	fmt.Fprintln(w, "# HELP spoold_journal_size_bytes Current journal file size in bytes.")
	fmt.Fprintln(w, "# TYPE spoold_journal_size_bytes gauge")
	fmt.Fprintf(w, "spoold_journal_size_bytes %d\n", journal.JournalSizeBytes)
	fmt.Fprintln(w, "# HELP spoold_journal_records Current physical journal record count.")
	fmt.Fprintln(w, "# TYPE spoold_journal_records gauge")
	fmt.Fprintf(w, "spoold_journal_records %d\n", journal.JournalRecords)
	fmt.Fprintln(w, "# HELP spoold_journal_compactions_total Journal compaction attempts by result.")
	fmt.Fprintln(w, "# TYPE spoold_journal_compactions_total counter")
	fmt.Fprintf(w, "spoold_journal_compactions_total{result=\"succeeded\"} %d\n", journal.CompactionsSucceeded)
	fmt.Fprintf(w, "spoold_journal_compactions_total{result=\"failed\"} %d\n", journal.CompactionsFailed)
}

func (s *Server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		s.log.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.status,
			"duration_ms", time.Since(started).Milliseconds(),
		)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("request body must contain one JSON object")
	}
	return nil
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, store.ErrInvalidTransition):
		writeError(w, http.StatusConflict, "invalid_transition", err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", "delivery state could not be updated")
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	var response errorResponse
	response.Error.Code = code
	response.Error.Message = message
	writeJSON(w, status, response)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func validStatus(status delivery.Status) bool {
	switch status {
	case delivery.StatusPending,
		delivery.StatusInFlight,
		delivery.StatusSucceeded,
		delivery.StatusFailed,
		delivery.StatusCanceled:
		return true
	default:
		return false
	}
}
