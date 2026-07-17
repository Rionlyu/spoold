# spoold

[![CI](https://github.com/Rionlyu/spoold/actions/workflows/ci.yml/badge.svg)](https://github.com/Rionlyu/spoold/actions/workflows/ci.yml)

`spoold` is a small, durable HTTP delivery daemon. An application enqueues an
outbound request once; `spoold` synchronizes it to disk before acknowledging
the request, delivers it in the background, and retries transient failures with
bounded exponential backoff.

The project is intentionally narrow. It demonstrates the reliability mechanics
behind webhook delivery without hiding them behind a database or queueing
framework:

- append-only mutation persistence, deterministic replay, and online compaction;
- idempotent enqueueing with conflicting-reuse detection;
- lease-based workers and stale-attempt protection;
- at-least-once delivery with bounded retry cycles;
- SSRF-aware DNS resolution and outbound dialing;
- a strict JSON API, Prometheus-format metrics, and structured logs;
- no runtime dependencies outside the Go standard library.

## Quick start

Go 1.26 or newer is required.

```sh
make build
./bin/spoold -allow-private-targets
```

Private targets are disabled by default. The flag is needed for the local
receiver:

```sh
go run ./examples/receiver
```

Enqueue a delivery:

```sh
curl -i http://127.0.0.1:8080/v1/deliveries \
  -H 'Content-Type: application/json' \
  -d '{
    "idempotencyKey": "build-2026-07-16",
    "targetUrl": "http://127.0.0.1:9090/events",
    "headers": {
      "X-Event-Type": "build.completed"
    },
    "body": {
      "repository": "spoold",
      "result": "passed"
    },
    "maxAttempts": 5
  }'
```

The response is `201 Created` after the journal entry has been synchronized.
Submitting the same request and idempotency key returns the original delivery
with `200 OK`. Reusing that key for different content returns `409 Conflict`.

Inspect the delivery and metrics:

```sh
curl http://127.0.0.1:8080/v1/deliveries/<id>
curl http://127.0.0.1:8080/metrics
```

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/deliveries` | Persist and enqueue a delivery |
| `GET` | `/v1/deliveries?status=&limit=` | List deliveries |
| `GET` | `/v1/deliveries/{id}` | Inspect one delivery |
| `POST` | `/v1/deliveries/{id}/cancel` | Cancel pending or leased work |
| `POST` | `/v1/deliveries/{id}/retry` | Start a new retry cycle for failed work |
| `GET` | `/healthz`, `/readyz` | Process health |
| `GET` | `/metrics` | Prometheus text exposition |

Request bodies are limited to 1 MiB and reject unknown fields. Delivery bodies
must contain valid JSON. The method defaults to `POST`, and `maxAttempts`
defaults to `8`.

Workers treat transport errors, `408`, `425`, `429`, and `5xx` responses as
retryable. Other non-success responses fail immediately. Backoff starts at one
second, doubles up to five minutes, and includes stable per-delivery jitter.

Every outbound request includes:

```text
X-Spoold-Delivery-ID: <stable delivery id>
X-Spoold-Attempt: <current attempt number>
```

Destinations should use the delivery ID as their deduplication key.

## Delivery semantics

`spoold` provides at-least-once delivery. A worker records a lease before making
the HTTP request. If the process stops after the destination accepts the
request but before success is journaled, that lease eventually expires and the
request is delivered again.

Canceling an in-flight delivery prevents its worker from recording a later
result, but cannot retract an HTTP request that the destination already
processed.

The journal stores a complete versioned delivery record for every state
transition. Replay accepts a complete final record even if its newline was lost
during shutdown, ignores an incomplete final append, and rejects corruption in
any earlier complete record.

Online compaction replaces superseded records with one current record per
delivery. It is lossless: pending, in-flight, succeeded, failed, and canceled
deliveries remain present, as do request fingerprints used for idempotency.
Compaction briefly pauses store mutations while it writes and synchronizes a
snapshot, then atomically replaces the journal. The default background check
runs once per minute and compacts only when the journal is at least 64 MiB and
contains at least twice as many physical records as live deliveries. A
compaction failure is logged and counted but does not stop delivery processing.

See [the design document](docs/design.md) for the state machine and durability
boundary and replacement sequence.

## Network safety

Production-default outbound dialing rejects targets that resolve to loopback,
private, link-local, unspecified, multicast, or carrier-grade NAT addresses.
Redirects are limited and pass through the same checks. Environment HTTP proxy
settings are deliberately ignored so a proxy cannot bypass target validation.

Use `-allow-private-targets` only in a trusted development environment. `spoold`
does not provide authentication or tenant isolation in v0.1 and should not be
exposed directly to an untrusted network.

## Configuration

```text
-listen string
      HTTP listen address (default "127.0.0.1:8080")
-journal string
      append-only journal path (default "data/spoold.journal")
-workers int
      number of delivery workers (default 4)
-allow-private-targets
      allow private and loopback delivery targets
-request-timeout duration
      outbound request timeout (default 10s)
-shutdown-timeout duration
      graceful shutdown timeout (default 10s)
-compact-threshold-bytes int
      minimum journal size for compaction; 0 disables (default 67108864)
-compact-check-interval duration
      journal compaction check interval (default 1m)
```

Container builds listen on `0.0.0.0:8080` and store the journal under
`/var/lib/spoold`.

## Development

```sh
make fmt-check
make vet
make test
make test-race
make build
make verify
```

## Project structure

```text
cmd/spoold/          process configuration and graceful shutdown
internal/api/        strict HTTP API and metrics exposition
internal/compactor/  background journal compaction policy
internal/delivery/   delivery model and request fingerprinting
internal/store/      append-only journal and state transitions
internal/target/     SSRF-aware HTTP transport
internal/worker/     leases, delivery, retry policy, and counters
examples/receiver/   local HTTP destination for the quick start
docs/design.md       semantics and deliberate non-goals
```

## Limitations

- One process owns one journal file.
- Compaction retains terminal deliveries; there is no retention-based purge or
  administrative compaction endpoint.
- There is no authentication, tenant isolation, rate limiting, or per-target
  concurrency control.
- Response bodies are retained only as a bounded error string, not as durable
  artifacts.
- `spoold` does not claim distributed coordination or exactly-once delivery.

## License

MIT
