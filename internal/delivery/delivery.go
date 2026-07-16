package delivery

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusInFlight  Status = "in_flight"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusCanceled  Status = "canceled"
)

type Delivery struct {
	ID               string            `json:"id"`
	IdempotencyKey   string            `json:"idempotencyKey,omitempty"`
	TargetURL        string            `json:"targetUrl"`
	Method           string            `json:"method"`
	Headers          map[string]string `json:"headers,omitempty"`
	Body             json.RawMessage   `json:"body,omitempty"`
	Status           Status            `json:"status"`
	Attempts         int               `json:"attempts"`
	MaxAttempts      int               `json:"maxAttempts"`
	NextAttemptAt    time.Time         `json:"nextAttemptAt,omitempty"`
	LeaseUntil       time.Time         `json:"leaseUntil,omitempty"`
	LastError        string            `json:"lastError,omitempty"`
	LastResponseCode int               `json:"lastResponseCode,omitempty"`
	CreatedAt        time.Time         `json:"createdAt"`
	UpdatedAt        time.Time         `json:"updatedAt"`
}

type CreateRequest struct {
	IdempotencyKey string
	TargetURL      string
	Method         string
	Headers        map[string]string
	Body           json.RawMessage
	MaxAttempts    int
}

func New(req CreateRequest, now time.Time) (Delivery, string, error) {
	normalized, err := normalize(req)
	if err != nil {
		return Delivery{}, "", err
	}

	id, err := newID()
	if err != nil {
		return Delivery{}, "", fmt.Errorf("generate delivery id: %w", err)
	}

	hash := fingerprint(normalized)
	return Delivery{
		ID:             id,
		IdempotencyKey: normalized.IdempotencyKey,
		TargetURL:      normalized.TargetURL,
		Method:         normalized.Method,
		Headers:        normalized.Headers,
		Body:           normalized.Body,
		Status:         StatusPending,
		MaxAttempts:    normalized.MaxAttempts,
		NextAttemptAt:  now.UTC(),
		CreatedAt:      now.UTC(),
		UpdatedAt:      now.UTC(),
	}, hash, nil
}

func Fingerprint(req CreateRequest) (string, error) {
	normalized, err := normalize(req)
	if err != nil {
		return "", err
	}
	return fingerprint(normalized), nil
}

func Clone(d Delivery) Delivery {
	d.Headers = cloneHeaders(d.Headers)
	d.Body = append(json.RawMessage(nil), d.Body...)
	return d
}

func (d Delivery) MarshalJSON() ([]byte, error) {
	type alias Delivery
	value := struct {
		alias
		NextAttemptAt *time.Time `json:"nextAttemptAt,omitempty"`
		LeaseUntil    *time.Time `json:"leaseUntil,omitempty"`
	}{
		alias: alias(d),
	}
	if !d.NextAttemptAt.IsZero() {
		value.NextAttemptAt = &d.NextAttemptAt
	}
	if !d.LeaseUntil.IsZero() {
		value.LeaseUntil = &d.LeaseUntil
	}
	return json.Marshal(value)
}

func normalize(req CreateRequest) (CreateRequest, error) {
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	if len(req.IdempotencyKey) > 256 {
		return CreateRequest{}, errors.New("idempotency key must not exceed 256 bytes")
	}
	req.TargetURL = strings.TrimSpace(req.TargetURL)
	if req.TargetURL == "" {
		return CreateRequest{}, errors.New("target URL is required")
	}
	if len(req.TargetURL) > 4096 {
		return CreateRequest{}, errors.New("target URL must not exceed 4096 bytes")
	}

	parsed, err := url.Parse(req.TargetURL)
	if err != nil {
		return CreateRequest{}, fmt.Errorf("parse target URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return CreateRequest{}, errors.New("target URL must use http or https")
	}
	if parsed.Host == "" {
		return CreateRequest{}, errors.New("target URL must include a host")
	}
	if parsed.User != nil {
		return CreateRequest{}, errors.New("target URL must not include user information")
	}
	if parsed.Fragment != "" {
		return CreateRequest{}, errors.New("target URL must not include a fragment")
	}

	req.Method = strings.ToUpper(strings.TrimSpace(req.Method))
	if req.Method == "" {
		req.Method = http.MethodPost
	}
	if !validMethod(req.Method) {
		return CreateRequest{}, errors.New("method contains invalid characters")
	}

	if req.MaxAttempts == 0 {
		req.MaxAttempts = 8
	}
	if req.MaxAttempts < 1 || req.MaxAttempts > 100 {
		return CreateRequest{}, errors.New("max attempts must be between 1 and 100")
	}

	normalizedHeaders := make(map[string]string, len(req.Headers))
	for name, value := range req.Headers {
		canonical := http.CanonicalHeaderKey(strings.TrimSpace(name))
		if canonical == "" || !validHeaderName(canonical) {
			return CreateRequest{}, fmt.Errorf("invalid header name %q", name)
		}
		if reservedHeader(canonical) {
			return CreateRequest{}, fmt.Errorf("header %q is managed by spoold", name)
		}
		if strings.ContainsAny(value, "\r\n") {
			return CreateRequest{}, fmt.Errorf("header %q contains a newline", name)
		}
		if _, exists := normalizedHeaders[canonical]; exists {
			return CreateRequest{}, fmt.Errorf("header %q is duplicated after canonicalization", name)
		}
		normalizedHeaders[canonical] = value
	}
	req.Headers = normalizedHeaders

	if len(req.Body) > 0 && !json.Valid(req.Body) {
		return CreateRequest{}, errors.New("body must be valid JSON")
	}
	req.Body = append(json.RawMessage(nil), req.Body...)
	return req, nil
}

func fingerprint(req CreateRequest) string {
	hash := sha256.New()
	fmt.Fprintf(hash, "%s\n%s\n%d\n", req.Method, req.TargetURL, req.MaxAttempts)
	names := make([]string, 0, len(req.Headers))
	for name := range req.Headers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintf(hash, "%s:%s\n", name, req.Headers[name])
	}
	hash.Write(req.Body)
	return hex.EncodeToString(hash.Sum(nil))
}

func newID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func cloneHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	result := make(map[string]string, len(headers))
	for name, value := range headers {
		result[name] = value
	}
	return result
}

func validMethod(method string) bool {
	if method == "" {
		return false
	}
	for _, r := range method {
		if r <= 32 || r >= 127 || strings.ContainsRune("()<>@,;:\\\"/[]?={}", r) {
			return false
		}
	}
	return true
}

func validHeaderName(name string) bool {
	return validMethod(name)
}

func reservedHeader(name string) bool {
	switch name {
	case "Connection",
		"Content-Length",
		"Host",
		"Proxy-Connection",
		"Trailer",
		"Transfer-Encoding",
		"Upgrade",
		"X-Spoold-Attempt",
		"X-Spoold-Delivery-Id":
		return true
	default:
		return false
	}
}
