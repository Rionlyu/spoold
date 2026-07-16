# spoold

`spoold` is a small, durable HTTP delivery daemon. Applications enqueue an
outbound request once; `spoold` persists it before acknowledging the request,
delivers it in the background, and retries transient failures with bounded
exponential backoff.

The project is an exercise in reliability-oriented backend engineering:
append-only persistence, crash recovery, idempotency, worker leases, safe
outbound networking, and operational visibility.

## Planned v0.1 contract

- A JSON API for enqueueing and inspecting deliveries.
- An append-only journal with replay on startup.
- Idempotency keys that reject conflicting reuse.
- Lease-based workers with at-least-once delivery semantics.
- Bounded retries for transport errors, `408`, `425`, `429`, and `5xx`.
- SSRF-aware target validation, with an explicit local-development override.
- Prometheus-format metrics and structured logs.
- No runtime dependencies outside the Go standard library.

The implementation intentionally targets one process and one journal file.
It does not claim distributed coordination or exactly-once delivery.

## Development

Go 1.26 or newer is required.

```sh
make verify
```

See [the design document](docs/design.md) for the delivery state machine,
durability boundary, and deliberate limitations.

## License

MIT

