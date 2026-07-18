package delivery

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestNewNormalizesRequest(t *testing.T) {
	item, _, err := New(CreateRequest{
		TargetURL:   "https://example.com/hook",
		Method:      "post",
		Headers:     map[string]string{"content-type": "application/json"},
		Body:        json.RawMessage(`{"ok":true}`),
		MaxAttempts: 3,
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if item.Method != "POST" || item.Headers["Content-Type"] != "application/json" {
		t.Fatalf("normalized delivery = %#v", item)
	}
}

func TestNewRejectsUnsafeOrMalformedInput(t *testing.T) {
	tests := []CreateRequest{
		{TargetURL: "file:///tmp/out"},
		{TargetURL: "https://user:pass@example.com/hook"},
		{TargetURL: "https://example.com/hook#fragment"},
		{TargetURL: "https://example.com", Headers: map[string]string{"X-Test": "bad\nvalue"}},
		{TargetURL: "https://example.com", Headers: map[string]string{"Content-Length": "5"}},
		{TargetURL: "https://example.com", Headers: map[string]string{"x-test": "a", "X-Test": "b"}},
		{TargetURL: "https://example.com", Body: make([]byte, MaxBodyBytes+1)},
	}
	for _, request := range tests {
		if _, _, err := New(request, time.Now()); err == nil {
			t.Fatalf("New(%#v) succeeded", request)
		}
	}
}

func TestBinaryBodyUsesBase64JSONAndRoundTrips(t *testing.T) {
	item, _, err := New(CreateRequest{
		TargetURL: "https://example.com/hook",
		Body:      []byte{0x00, 0xff, 0x10},
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte(`"bodyBase64":"AP8Q"`)) {
		t.Fatalf("JSON = %s", data)
	}

	var decoded Delivery
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Body, item.Body) {
		t.Fatalf("decoded body = %v, want %v", decoded.Body, item.Body)
	}
}

func TestBinaryFingerprintSeparatesHeadersFromBody(t *testing.T) {
	_, firstHash, err := New(CreateRequest{
		TargetURL: "https://example.com/hook",
		Body:      []byte("X-Test:value\npayload"),
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	_, secondHash, err := New(CreateRequest{
		TargetURL: "https://example.com/hook",
		Headers:   map[string]string{"X-Test": "value"},
		Body:      []byte("payload"),
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if firstHash == secondHash {
		t.Fatal("fingerprints collide across header/body boundary")
	}
}

func TestMarshalOmitsInactiveScheduleFields(t *testing.T) {
	item := Delivery{
		ID:          "test",
		TargetURL:   "https://example.com",
		Method:      "POST",
		Status:      StatusSucceeded,
		MaxAttempts: 1,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	data, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "nextAttemptAt") || strings.Contains(string(data), "leaseUntil") {
		t.Fatalf("JSON contains inactive schedule fields: %s", data)
	}
}
