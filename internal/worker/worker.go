package worker

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rionlyu/spoold/internal/delivery"
	"github.com/Rionlyu/spoold/internal/store"
)

type Config struct {
	Concurrency   int
	PollInterval  time.Duration
	LeaseDuration time.Duration
	BaseBackoff   time.Duration
	MaxBackoff    time.Duration
}

type Metrics struct {
	Attempts         uint64
	Succeeded        uint64
	RetryableFailure uint64
	TerminalFailure  uint64
}

type Pool struct {
	store  *store.Store
	client *http.Client
	log    *slog.Logger
	config Config

	attempts         atomic.Uint64
	succeeded        atomic.Uint64
	retryableFailure atomic.Uint64
	terminalFailure  atomic.Uint64
	wg               sync.WaitGroup
}

func New(store *store.Store, client *http.Client, logger *slog.Logger, config Config) *Pool {
	if config.Concurrency < 1 {
		config.Concurrency = 4
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 250 * time.Millisecond
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = 30 * time.Second
	}
	if config.BaseBackoff <= 0 {
		config.BaseBackoff = time.Second
	}
	if config.MaxBackoff <= 0 {
		config.MaxBackoff = 5 * time.Minute
	}
	return &Pool{
		store:  store,
		client: client,
		log:    logger,
		config: config,
	}
}

func (p *Pool) Start(ctx context.Context) {
	for range p.config.Concurrency {
		p.wg.Add(1)
		go p.run(ctx)
	}
}

func (p *Pool) Wait() {
	p.wg.Wait()
}

func (p *Pool) Metrics() Metrics {
	return Metrics{
		Attempts:         p.attempts.Load(),
		Succeeded:        p.succeeded.Load(),
		RetryableFailure: p.retryableFailure.Load(),
		TerminalFailure:  p.terminalFailure.Load(),
	}
}

func (p *Pool) run(ctx context.Context) {
	defer p.wg.Done()
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		claimed, err := p.store.ClaimDue(time.Now(), p.config.LeaseDuration, 1)
		if err != nil {
			p.log.Error("claim delivery", "error", err)
		} else if len(claimed) == 1 {
			p.deliver(ctx, claimed[0])
			timer.Reset(0)
			continue
		}
		timer.Reset(p.config.PollInterval)
	}
}

func (p *Pool) deliver(ctx context.Context, item delivery.Delivery) {
	p.attempts.Add(1)
	started := time.Now()
	statusCode, err := p.send(ctx, item)
	elapsed := time.Since(started)

	if err == nil && statusCode >= 200 && statusCode < 300 {
		if err := p.store.Succeed(item.ID, item.Attempts, statusCode, time.Now()); err != nil {
			p.log.Error("record delivery success", "delivery_id", item.ID, "attempt", item.Attempts, "error", err)
			return
		}
		p.succeeded.Add(1)
		p.log.Info("delivery succeeded", "delivery_id", item.ID, "attempt", item.Attempts, "status", statusCode, "duration_ms", elapsed.Milliseconds())
		return
	}

	message := failureMessage(statusCode, err)
	retryable := retryableStatus(statusCode) ||
		(err != nil && (statusCode == 0 || statusCode >= 200 && statusCode < 300))
	terminal := !retryable || item.Attempts >= item.MaxAttempts
	next := time.Now().Add(Backoff(p.config.BaseBackoff, p.config.MaxBackoff, item.ID, item.Attempts))
	if err := p.store.Fail(item.ID, item.Attempts, statusCode, message, next, terminal, time.Now()); err != nil {
		p.log.Error("record delivery failure", "delivery_id", item.ID, "attempt", item.Attempts, "error", err)
		return
	}

	if terminal {
		p.terminalFailure.Add(1)
		p.log.Warn("delivery failed", "delivery_id", item.ID, "attempt", item.Attempts, "status", statusCode, "error", message, "duration_ms", elapsed.Milliseconds())
		return
	}
	p.retryableFailure.Add(1)
	p.log.Info("delivery scheduled for retry", "delivery_id", item.ID, "attempt", item.Attempts, "status", statusCode, "next_attempt_at", next, "error", message)
}

func (p *Pool) send(ctx context.Context, item delivery.Delivery) (int, error) {
	request, err := http.NewRequestWithContext(ctx, item.Method, item.TargetURL, bytes.NewReader(item.Body))
	if err != nil {
		return 0, err
	}
	for name, value := range item.Headers {
		request.Header.Set(name, value)
	}
	if len(item.Body) > 0 && request.Header.Get("Content-Type") == "" {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("X-Spoold-Delivery-ID", item.ID)
	request.Header.Set("X-Spoold-Attempt", strconv.Itoa(item.Attempts))

	response, err := p.client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
	if readErr != nil {
		return response.StatusCode, fmt.Errorf("read response: %w", readErr)
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return response.StatusCode, nil
	}
	detail := strings.TrimSpace(string(body))
	if detail == "" {
		return response.StatusCode, fmt.Errorf("HTTP %d", response.StatusCode)
	}
	return response.StatusCode, fmt.Errorf("HTTP %d: %s", response.StatusCode, detail)
}

func Backoff(base, maximum time.Duration, id string, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := base
	for i := 1; i < attempt && delay < maximum; i++ {
		if delay > maximum/2 {
			delay = maximum
			break
		}
		delay *= 2
	}
	if delay > maximum {
		delay = maximum
	}

	hash := fnv.New32a()
	fmt.Fprintf(hash, "%s:%d", id, attempt)
	factor := 0.8 + float64(hash.Sum32()%401)/1000
	jittered := time.Duration(float64(delay) * factor)
	if jittered > maximum {
		return maximum
	}
	return jittered
}

func retryableStatus(status int) bool {
	return status == http.StatusRequestTimeout ||
		status == http.StatusTooEarly ||
		status == http.StatusTooManyRequests ||
		status >= 500
}

func failureMessage(status int, err error) string {
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("HTTP %d", status)
}
