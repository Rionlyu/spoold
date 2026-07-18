# spoold

[![CI](https://github.com/Rionlyu/spoold/actions/workflows/ci.yml/badge.svg)](https://github.com/Rionlyu/spoold/actions/workflows/ci.yml)

`spoold` is durable curl for programs that cannot afford to lose an outbound
HTTP request. Submit a delivery to the local daemon; `spoold` synchronizes it to
disk before acknowledging the request, delivers it in the background, and
resumes bounded retries after process termination, reboot, destination failure,
or network loss.

It is intentionally a local delivery spool, not a hosted webhook platform or
general message broker. One self-contained daemon and one owner-only journal
provide the reliability boundary without PostgreSQL, Redis, Kafka, a cloud
account, or a language-specific SDK:

- append-only mutation persistence, deterministic replay, and online compaction;
- idempotent enqueueing with conflicting-reuse detection;
- lease-based workers and stale-attempt protection;
- at-least-once delivery with bounded retry cycles;
- SSRF-aware DNS resolution and outbound dialing;
- a strict JSON API, Prometheus-format metrics, and structured logs;
- no runtime dependencies outside the Go standard library.

Use it when a cron job must report completion after a reboot, an edge agent
must survive hours offline, an appliance must retain callbacks without shipping
a database, or a small service needs a local outbound reliability boundary.
Unlike `curl --retry`, the retry lifecycle is independent of the submitting
process. Do not use it when enqueueing must be atomic with an application
database transaction or when the queue must be highly available across hosts.

## Install

Release archives support Linux and macOS on amd64 and arm64. Every archive
contains both `spoold` and `spoolctl`, plus the license and security policy.

```sh
curl -fsSL https://raw.githubusercontent.com/Rionlyu/spoold/main/scripts/install.sh | sh
```

Set `SPOOLD_INSTALL_DIR` to install somewhere other than `/usr/local/bin`, or
set `SPOOLD_VERSION` to install a specific tag. The installer verifies the
archive against the release checksum. Release checksums are keylessly signed
with Sigstore, and the release workflow also publishes SBOMs and GitHub build
provenance.

To authenticate a downloaded checksum file with Cosign:

```sh
tag=v0.1.0
cosign verify-blob checksums.txt \
  --bundle checksums.txt.sigstore.json \
  --certificate-identity \
    "https://github.com/Rionlyu/spoold/.github/workflows/release.yml@refs/tags/${tag}" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com"
```

Tagged multi-architecture images are published to
`ghcr.io/rionlyu/spoold`. The container must have a persistent volume mounted at
`/var/lib/spoold`.

```sh
docker run -d \
  --name spoold \
  --restart unless-stopped \
  --publish 127.0.0.1:8080:8080 \
  --volume spoold-data:/var/lib/spoold \
  ghcr.io/rionlyu/spoold:v0.1.0
```

To build from source instead, Go 1.26 or newer is required:

```sh
make build
```

Source-built binaries are written to `./bin`; prefix the following `spoold` and
`spoolctl` commands with `./bin/` when they are not installed on `PATH`.

## Quick start

```sh
spoold -allow-private-targets
```

Private targets are disabled by default. The flag is needed for the local
receiver:

```sh
go run ./examples/receiver
```

Enqueue a delivery:

```sh
spoolctl send \
  --idempotency-key build-2026-07-16 \
  --header 'X-Event-Type: build.completed' \
  --data '{"repository":"spoold","result":"passed"}' \
  --max-attempts 5 \
  http://127.0.0.1:9090/events
```

`spoolctl` prints `queued` only after the journal entry has been synchronized.
Submitting the same request and idempotency key prints `existing` with the
original delivery. Reusing that key for different content fails with an
idempotency conflict.

Inspect the delivery and metrics:

```sh
spoolctl list
spoolctl get <id>
spoolctl retry <failed-id>
curl http://127.0.0.1:8080/metrics
```

Every `spoolctl` command accepts `--server`; `SPOOLD_URL` changes its default.
Use `--json` for machine-readable output and `spoolctl help` for the complete
command list. The HTTP API remains the stable integration surface for programs
that do not use the CLI.

For an owner-only local endpoint instead of TCP:

```sh
spoold \
  -unix-socket "$HOME/.spoold/spoold.sock" \
  -journal "$HOME/.spoold/spoold.journal"

export SPOOLD_URL="unix://$HOME/.spoold/spoold.sock"
spoolctl list
```

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/deliveries` | Persist and enqueue a delivery |
| `GET` | `/v1/deliveries?status=&limit=` | List deliveries |
| `GET` | `/v1/deliveries/{id}` | Inspect one delivery |
| `POST` | `/v1/deliveries/{id}/cancel` | Cancel pending or leased work |
| `POST` | `/v1/deliveries/{id}/retry` | Start a new retry cycle for failed work |
| `GET` | `/healthz`, `/readyz` | Liveness and persistence readiness |
| `GET` | `/metrics` | Prometheus text exposition |

Requests reject unknown fields. A delivery body can use `body` for JSON or
`bodyBase64` for arbitrary bytes; the fields are mutually exclusive and the
decoded body is limited to 1 MiB. `spoolctl send --data-binary <file>` handles
the base64 transport automatically. The method defaults to `POST`, and
`maxAttempts` defaults to `8`.

List responses omit headers and bodies to remain bounded and avoid exposing
secrets in bulk output. `GET /v1/deliveries/{id}` returns the complete retained
request.

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
retained delivery. Compaction itself is lossless and briefly pauses store
mutations while it writes and synchronizes a snapshot, then atomically replaces
the journal. Separately, terminal retention defaults to seven days. Expired
succeeded, failed, and canceled deliveries are tombstoned, their idempotency
keys expire with them, and the journal is compacted. Pending and in-flight work
is never removed by retention.

The default background check runs once per minute and also compacts when the
journal is at least 64 MiB and contains at least twice as many physical records
as retained deliveries. A maintenance failure is logged and counted but does
not terminate the process. If replacement was renamed but its directory could
not be synchronized, readiness fails and mutations pause until the next enabled
maintenance pass repairs the journal.

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

Only one process can own a journal. A second process fails startup rather than
risk interleaved writes. On Linux and macOS, `-unix-socket` creates an
owner-only `0600` socket and is the preferred boundary on a multi-user machine.

## Configuration

```text
-listen string
      HTTP listen address (default "127.0.0.1:8080")
-journal string
      append-only journal path (default "data/spoold.journal")
-max-journal-bytes int
      reject new deliveries at this physical journal size; 0 disables
      (default 1073741824)
-terminal-retention duration
      retain terminal delivery history; 0 retains forever (default 168h)
-workers int
      number of delivery workers (default 4)
-per-target-workers int
      maximum concurrent requests to one target origin (default 1)
-unix-socket string
      owner-only Unix socket path; overrides -listen
-allow-private-targets
      allow private and loopback delivery targets
-request-timeout duration
      outbound request timeout (default 10s)
-shutdown-timeout duration
      graceful shutdown timeout (default 10s)
-compact-threshold-bytes int
      minimum journal size for redundancy compaction; 0 disables the size
      trigger (default 67108864)
-compact-check-interval duration
      journal maintenance interval (default 1m)
```

Container builds listen on `0.0.0.0:8080`, store the journal under
`/var/lib/spoold`, and enforce the same 1 GiB admission limit and seven-day
terminal retention defaults.

## Development

```sh
make fmt-check
make vet
make test
make test-race
make build
make verify

# Exercise a real daemon kill, journal reopen, and recovered delivery.
go test ./cmd/spoold -run TestProcessRecoversDeliveryAfterSIGKILL -v
```

## Project structure

```text
cmd/spoold/          process configuration and graceful shutdown
cmd/spoolctl/        curl-like local delivery client
internal/api/        strict HTTP API and metrics exposition
internal/buildinfo/  release version, commit, and build metadata
internal/compactor/  background journal compaction policy
internal/delivery/   delivery model and request fingerprinting
internal/spoolctl/   client commands, API transport, and output
internal/store/      append-only journal and state transitions
internal/target/     SSRF-aware HTTP transport
internal/worker/     leases, delivery, retry policy, and counters
examples/receiver/   local HTTP destination for the quick start
scripts/install.sh   checksummed release installer
docs/design.md       semantics and deliberate non-goals
docs/launch-readiness.md  public release gate and positioning brief
```

The focused product decision and competitive boundary are documented in
[product direction](docs/product-direction.md). The pre-release checks and
market boundary are in [public launch readiness](docs/launch-readiness.md).

## Limitations

- One process owns one journal file; concurrent ownership is rejected.
- Terminal history and its idempotency keys expire after seven days by default.
  There is no administrative purge endpoint.
- There is no authentication, tenant isolation, rate limiting, or per-target
  rate shaping.
- Response bodies are retained only as a bounded error string, not as durable
  artifacts.
- `spoold` does not claim distributed coordination or exactly-once delivery.
- Release binaries currently target Linux and macOS.

## License

MIT
