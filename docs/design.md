# Design

## Purpose

`spoold` separates accepting an outbound HTTP delivery from performing it. The
caller receives success only after the delivery has been appended to a journal
and synchronized to disk. Background workers then own network retries.

This is useful when a process should not lose a webhook because its destination
is slow or temporarily unavailable.

## State machine

```text
             claim
pending --------------> in_flight
   ^                         |
   | retryable failure       | success
   |                         v
   +-------------------- succeeded
   |
   +---- manual retry <---- failed

pending/in_flight ---- manual cancel ----> canceled
```

An expired `in_flight` lease is eligible to be claimed again after restart or
worker failure. This gives at-least-once delivery: a destination may process a
request even if `spoold` crashes before recording the successful response.
Every request includes stable delivery and attempt headers so a destination can
deduplicate.

Canceling leased work is best effort. It invalidates the worker's lease and
prevents a later journal update, but an HTTP request already processed by the
destination cannot be retracted.

## Persistence

The journal is newline-delimited JSON. Each mutation appends the complete
current delivery record under a versioned envelope and calls `fsync` before
returning. Creation of a new journal also synchronizes its directory before the
first enqueue can be acknowledged. Replay keeps the last valid record for each
delivery.

A final partial line is treated as an interrupted append and ignored. Corrupt
complete records fail startup rather than silently discarding acknowledged
state.

One process owns a journal through a stable adjacent lock file. A competing
process fails startup instead of interleaving writes. An append or sync failure
makes persistence readiness sticky: new mutations fail until a successful
compaction establishes and synchronizes a replacement journal. The next enabled
maintenance pass attempts that repair even below the size threshold. Liveness
remains separate so an operator can distinguish a running process from a
durable one.

Journal format version 2 can record either a complete delivery or a deletion
tombstone. Replay remains compatible with version 1 records.

### Capacity and retention

The physical journal has a configurable admission limit, 1 GiB by default.
Crossing it rejects only new deliveries; idempotent lookup and transitions for
already accepted work remain available. This keeps existing work recoverable
without allowing unbounded new commitments.

Terminal retention defaults to seven days. Maintenance writes deletion
tombstones for expired succeeded, failed, and canceled deliveries, then
compacts them away. Their idempotency keys expire at the same time. Pending and
in-flight deliveries are never removed by retention. Setting retention to zero
keeps terminal deliveries indefinitely.

### Online compaction

Compaction is a lossless replacement of superseded physical records, not a
retention policy: retention decides which terminal records are still live
before compaction begins. The store mutex remains held for the snapshot, so
creates, claims, completions, retries, and cancellations pause briefly. Every
retained delivery is written exactly once in delivery-ID order, including
pending, in-flight, succeeded, failed, and canceled deliveries. The associated
request fingerprint is carried forward so idempotency behavior is unchanged.

The atomic replacement sequence is:

1. Create a temporary compaction file in the journal directory.
2. Write the current record for every delivery in deterministic order.
3. Synchronize the temporary file.
4. Atomically rename it over the journal path.
5. Synchronize the journal directory.
6. Switch future appends to the replacement descriptor, then close the old
   descriptor.

If compaction fails before the rename, the original journal remains active and
the temporary file is removed. If an error occurs after the rename but before
directory synchronization, the store adopts the replacement descriptor but
marks persistence unready and blocks mutations until a later compaction repairs
the durability boundary. An error after directory synchronization still adopts
the durable replacement and subsequent appends remain recoverable. Abandoned
temporary compaction files are removed when the store opens. Failures are
counted and logged rather than terminating the process.

The background compactor checks once per minute by default. Size-based
compaction runs only when the journal is at least 64 MiB and its physical record
count is at least twice the live-delivery count. Retention and persistence
repair are evaluated independently. The byte threshold and check interval are
configurable, and a zero byte threshold disables size-based compaction. There
is deliberately no administrative HTTP endpoint and no purge operation.

## Concurrency

State transitions are serialized by the store. Claims record a lease and
increment the attempt number before a worker performs network I/O. Completion
updates include the attempt number; a late worker cannot overwrite a newer
claim. Compaction uses the same serialization boundary, trading a brief mutation
pause for a snapshot that cannot omit or reorder a concurrent state change.

Workers also reserve target origins while claiming work. The default allows one
active request per scheme/host/port while still using the global worker pool,
so an unavailable destination cannot occupy every worker. The limit is
configurable.

## Networking

Only HTTP and HTTPS targets are accepted. User information and URL fragments
are rejected. The production-default dialer rejects loopback, private,
link-local, unspecified, and multicast addresses after DNS resolution.

Local targets can be enabled explicitly for development and integration tests.
The control API listens on loopback by default. On Linux and macOS it can
instead use an owner-only Unix socket, which is the preferred boundary on a
multi-user host.

## Non-goals for v0.1

- Sharing one journal between processes.
- Distributed worker coordination.
- Exactly-once delivery.
- Arbitrary scheduling or recurring jobs.
- Authentication and tenant isolation.
- Unbounded response-body storage.
