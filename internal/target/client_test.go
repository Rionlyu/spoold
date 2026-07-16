package target

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientBlocksLoopbackByDefault(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()

	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewClient(false, time.Second).Do(request)
	if err == nil || !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("Do() error = %v, want %v", err, ErrBlockedAddress)
	}
}

func TestClientCanExplicitlyAllowLoopback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	response, err := NewClient(true, time.Second).Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d", response.StatusCode)
	}
}

func TestBlockedAddresses(t *testing.T) {
	for _, value := range []string{
		"127.0.0.1",
		"10.0.0.1",
		"169.254.1.1",
		"100.64.0.1",
		"::1",
		"fc00::1",
	} {
		if !blocked(net.ParseIP(value)) {
			t.Fatalf("blocked(%s) = false", value)
		}
	}
	if blocked(net.ParseIP("8.8.8.8")) {
		t.Fatal("public address was blocked")
	}
}
