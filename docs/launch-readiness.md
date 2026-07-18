# Public launch readiness

This document is the release gate and positioning brief for the first public
`spoold` release. It intentionally separates work required to make the
durability promise credible from features that would dilute the product.

## Launch decision

Launch `spoold` as **durable curl**: the smallest self-hosted component that
accepts an arbitrary outbound HTTP request only after synchronizing it to local
disk, then keeps delivering it across destination outages and process
restarts.

The promise is deliberately narrow:

> Accept the request now, deliver it later, and require no database, broker, or
> cloud service.

Do not position it as a webhook platform, message broker, transactional outbox,
or exactly-once system. Those products solve larger or different problems.

## Market boundary

The adjacent products validate the underlying store-and-forward need but make
different tradeoffs:

| Category | Examples | Why `spoold` remains distinct |
|---|---|---|
| General HTTP forwarders | [aswh](https://github.com/pepmartinez/aswh) | Similar arbitrary-HTTP goal, but its supported backends are MongoDB, PostgreSQL, and Redis. |
| Self-hosted webhook platforms | [Hookdeck Outpost](https://github.com/hookdeck/outpost), [Convoy](https://github.com/frain-dev/convoy) | Broader routing and platform scope with additional runtime infrastructure. |
| Hosted delivery queues | [Svix](https://docs.svix.com/retries), [QStash](https://upstash.com/docs/qstash/features/retry) | The provider owns the queue; `spoold` is local, offline-capable, and account-free. |
| Telemetry agents | [Vector](https://vector.dev/docs/reference/configuration/sinks/http/), [Fluent Bit](https://docs.fluentbit.io/manual/data-pipeline/buffering) | Disk buffering serves a telemetry pipeline rather than an explicit fsync-backed API for caller-selected HTTP requests. |
| Transactional outboxes | Framework- and database-specific implementations | They can be atomic with application state; `spoold` begins its guarantee only after a separate enqueue reaches the daemon. |

The defensible claim is not that HTTP retries are new. It is that this
combination is unusually small: arbitrary HTTP, synchronization before
acknowledgement, deterministic local replay, and no runtime service dependency.

## Technical release gates

The first release must preserve every checked property below:

- [x] A successful enqueue means the journal append has been synchronized.
- [x] Replay rejects complete-record corruption and tolerates only an
  interrupted final append.
- [x] A process killed during an outage can restart from the same journal and
  complete the accepted delivery.
- [x] A second process cannot concurrently own the same journal.
- [x] Storage failure makes readiness fail and blocks further mutations until
  a synchronized replacement journal repairs the persistence boundary.
- [x] New admission is bounded while transitions for accepted work remain
  available.
- [x] Pending and in-flight work is never removed by retention.
- [x] Idempotency survives restart and compaction, then expires with retained
  terminal history.
- [x] One unavailable target cannot occupy every worker.
- [x] Arbitrary byte bodies and JSON bodies are both round-tripped losslessly.
- [x] The default network policy blocks private target classes and redirects
  cannot bypass it.
- [x] Loopback and owner-only Unix socket control-plane boundaries are
  available.
- [x] Binaries embed version metadata and ship together in checksummed
  Linux/macOS archives for amd64 and arm64.
- [x] The release workflow is configured to produce SBOMs, keyless signatures,
  provenance, and a non-root multi-architecture container.

These gates are exercised by unit, race, process, packaging, and container
checks in CI. A tag is publishable only from a green `main`.

## Known alpha constraints

These are disclosure requirements, not launch blockers:

- delivery is at-least-once; a destination must deduplicate by the stable
  delivery ID when duplicate effects matter;
- `spoold` cannot atomically join an application's database transaction;
- one process and one host own a journal; there is no high availability;
- the control API has no network authentication and must remain on loopback, an
  owner-only Unix socket, or another trusted boundary;
- headers and bodies are stored in the journal and should be protected as
  sensitive data;
- terminal history and its idempotency keys expire after seven days by default;
- request bodies are limited to 1 MiB, response bodies are not archived, and
  Windows is not a release target;
- durability ultimately depends on the host filesystem and storage honoring
  synchronization calls.

Authentication, a dashboard, distributed workers, transformations, scheduling,
fan-out, and a hosted control plane should not delay the first release.

## Release procedure

1. Merge only after both CI jobs pass on the launch pull request.
2. Confirm `main` CI is green and the release notes describe at-least-once
   behavior, the trust boundary, retention, and platform support.
3. Tag the immutable commit as `v0.1.0` and let the release workflow publish
   archives, checksums, signatures, SBOMs, provenance, and the GHCR image.
4. On a clean supported host, install with `scripts/install.sh`, confirm
   `spoold -version` and `spoolctl version`, enqueue while the destination is
   unavailable, terminate the daemon, restart it, and observe successful
   delivery.
5. Verify the checksum signature and GitHub attestations, pull both container
   architectures, and confirm the container writes successfully to a mounted
   persistent volume and fails clearly when the journal location is read-only.
6. Publish the announcement only after the released artifacts—not local
   builds—pass the smoke test.

Never move or overwrite a published tag. If the release is defective, document
the defect and publish a new patch version.

## Launch message and demo

The launch should lead with the failure it removes:

> `curl` tells you whether a request worked now. `spoold` tells you the request
> is safely queued and keeps trying after your script, destination, or machine
> comes back.

The proof should be a short terminal recording, not a feature tour:

1. start `spoold` while the example destination is offline;
2. enqueue one request and show `queued`;
3. kill the daemon with `SIGKILL`;
4. start the destination and restart `spoold`;
5. show the same delivery reach `succeeded`.

Link directly to the durability design and the process-level test so technical
readers can verify the promise.

## Post-launch validation

The goal is useful adoption, not raw impressions. During the first 30 days,
seek:

- three unrelated users who reproduce the crash-recovery demo;
- one real use in a script, edge agent, appliance, or small self-hosted service;
- one report from a prolonged destination outage or forced reboot;
- installation feedback from every supported OS and architecture;
- concrete reasons from users who considered `spoold` but chose another tool.

Prioritize defects that threaten acknowledged work, installation failures, and
confusing durability semantics. Add features only when repeated real workflows
show that the focused contract is insufficient.
