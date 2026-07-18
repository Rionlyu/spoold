package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Rionlyu/spoold/internal/delivery"
)

func TestProcessRecoversDeliveryAfterSIGKILL(t *testing.T) {
	apiAddress := unusedTCPAddress(t)
	targetAddress := unusedTCPAddress(t)
	journalPath := filepath.Join(t.TempDir(), "spoold.journal")
	args := []string{
		"-listen", apiAddress,
		"-journal", journalPath,
		"-allow-private-targets",
		"-workers", "1",
		"-per-target-workers", "1",
		"-request-timeout", "100ms",
		"-compact-threshold-bytes", "0",
		"-terminal-retention", "0",
	}

	first := startSpooldProcess(t, args)
	waitForHealth(t, apiAddress, first)
	item := enqueueDelivery(t, apiAddress, "http://"+targetAddress+"/events")
	waitForDelivery(t, apiAddress, item.ID, first, func(item delivery.Delivery) bool {
		return item.Status == delivery.StatusPending && item.Attempts >= 1
	})

	conflictArgs := append([]string(nil), args...)
	conflictArgs[1] = unusedTCPAddress(t)
	conflict := newSpooldProcess(conflictArgs)
	conflictOutput, err := conflict.CombinedOutput()
	if err == nil {
		t.Fatal("second process unexpectedly opened the same journal")
	}
	if !strings.Contains(string(conflictOutput), "already owned") {
		t.Fatalf("second process output = %s", conflictOutput)
	}

	if err := first.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := first.cmd.Wait(); err == nil {
		t.Fatal("SIGKILL process exited successfully")
	}
	first.finished = true

	var received atomic.Int32
	targetListener, err := net.Listen("tcp", targetAddress)
	if err != nil {
		t.Fatal(err)
	}
	targetServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})}
	go targetServer.Serve(targetListener)
	t.Cleanup(func() { targetServer.Close() })

	second := startSpooldProcess(t, args)
	waitForHealth(t, apiAddress, second)
	succeeded := waitForDelivery(t, apiAddress, item.ID, second, func(item delivery.Delivery) bool {
		return item.Status == delivery.StatusSucceeded
	})
	if succeeded.Attempts < 2 {
		t.Fatalf("attempts after recovery = %d, want at least 2", succeeded.Attempts)
	}
	if received.Load() != 1 {
		t.Fatalf("destination requests = %d, want 1", received.Load())
	}
	stopSpooldProcess(t, second)
}

func TestSpooldHelperProcess(t *testing.T) {
	if os.Getenv("SPOOLD_TEST_HELPER") != "1" {
		return
	}
	separator := -1
	for index, argument := range os.Args {
		if argument == "--" {
			separator = index
			break
		}
	}
	if separator == -1 {
		os.Exit(2)
	}
	os.Exit(run(os.Args[separator+1:], os.Stdout, os.Stderr))
}

type spooldProcess struct {
	cmd      *exec.Cmd
	output   lockedBuffer
	finished bool
}

type lockedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

func newSpooldProcess(args []string) *exec.Cmd {
	commandArgs := append([]string{"-test.run=^TestSpooldHelperProcess$", "--"}, args...)
	cmd := exec.Command(os.Args[0], commandArgs...)
	cmd.Env = append(os.Environ(), "SPOOLD_TEST_HELPER=1")
	return cmd
}

func startSpooldProcess(t *testing.T, args []string) *spooldProcess {
	t.Helper()
	process := &spooldProcess{cmd: newSpooldProcess(args)}
	process.cmd.Stdout = &process.output
	process.cmd.Stderr = &process.output
	if err := process.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if process.finished {
			return
		}
		_ = process.cmd.Process.Kill()
		_ = process.cmd.Wait()
	})
	return process
}

func stopSpooldProcess(t *testing.T, process *spooldProcess) {
	t.Helper()
	if err := process.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := process.cmd.Wait(); err != nil {
		t.Fatalf("stop spoold: %v\n%s", err, process.output.String())
	}
	process.finished = true
}

func unusedTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitForHealth(t *testing.T, address string, process *spooldProcess) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get("http://" + address + "/healthz")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("spoold did not become healthy\n%s", process.output.String())
}

func enqueueDelivery(t *testing.T, address, targetURL string) delivery.Delivery {
	t.Helper()
	payload := fmt.Sprintf(`{"targetUrl":%q,"body":{"crash":"recovery"}}`, targetURL)
	response, err := http.Post(
		"http://"+address+"/v1/deliveries",
		"application/json",
		strings.NewReader(payload),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("enqueue status = %d, body = %s", response.StatusCode, body)
	}
	var item delivery.Delivery
	if err := json.NewDecoder(response.Body).Decode(&item); err != nil {
		t.Fatal(err)
	}
	return item
}

func waitForDelivery(
	t *testing.T,
	address string,
	id string,
	process *spooldProcess,
	done func(delivery.Delivery) bool,
) delivery.Delivery {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get("http://" + address + "/v1/deliveries/" + id)
		if err == nil {
			var item delivery.Delivery
			decodeErr := json.NewDecoder(response.Body).Decode(&item)
			response.Body.Close()
			if response.StatusCode == http.StatusOK && decodeErr == nil && done(item) {
				return item
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("delivery did not reach expected state\n%s", process.output.String())
	return delivery.Delivery{}
}
