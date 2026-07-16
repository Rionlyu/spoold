package delivery

import (
	"encoding/json"
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
		{TargetURL: "https://example.com", Body: json.RawMessage(`{`)},
		{TargetURL: "https://example.com", Headers: map[string]string{"X-Test": "bad\nvalue"}},
	}
	for _, request := range tests {
		if _, _, err := New(request, time.Now()); err == nil {
			t.Fatalf("New(%#v) succeeded", request)
		}
	}
}
