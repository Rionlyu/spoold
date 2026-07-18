package main

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunReturnsFailureWhenAddressIsAlreadyInUse(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-listen", listener.Addr().String(),
		"-journal", filepath.Join(t.TempDir(), "journal"),
	}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit code = %d, stdout = %s, stderr = %s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), `"msg":"listen"`) {
		t.Fatalf("stdout = %s", stdout.String())
	}
}

func TestRunPrintsVersionWithoutStarting(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-version"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "spoold dev") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestParseConfigRejectsUnsafeValues(t *testing.T) {
	tests := [][]string{
		{"-workers", "0"},
		{"-workers", "2", "-per-target-workers", "3"},
		{"-max-journal-bytes", "-1"},
		{"-terminal-retention", "-1s"},
		{"-request-timeout", "0"},
		{"unexpected"},
	}
	for _, args := range tests {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			var stderr bytes.Buffer
			if _, code := parseConfig(args, &stderr); code != 2 {
				t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
			}
		})
	}
}

func TestOpenListenerCreatesOwnerOnlyUnixSocket(t *testing.T) {
	socketDir, err := os.MkdirTemp("/tmp", "spoold-listener-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(socketDir) })
	socketPath := filepath.Join(socketDir, "spoold.sock")
	listener, cleanup, err := openListener("", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("socket permissions = %o, want 600", got)
	}
	if listener.Addr().Network() != "unix" {
		t.Fatalf("network = %q, want unix", listener.Addr().Network())
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(socketPath); !os.IsNotExist(err) {
		t.Fatalf("socket still exists after cleanup: %v", err)
	}
}
