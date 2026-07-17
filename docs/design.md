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
returning. Replay keeps the last valid record for each delivery.

A final partial line is treated as an interrupted append and ignored. Corrupt
complete records fail startup rather than silently discarding acknowledged
state.

### Online compaction

Compaction is a lossless replacement of superseded physical records, not a
retention policy. The store mutex remains held for the snapshot, so creates,
claims, completions, retries, and cancellations pause briefly. Every live
delivery is written exactly once in delivery-ID order, including pending,
in-flight, succeeded, failed, and canceled deliveries. The associated request
fingerprint is carried forward so idempotency behavior is unchanged.

The atomic replacement sequence is:

1. Create a temporary compaction file in the journal directory.
2. Write the current record for every delivery in deterministic order.
3. Synchronize the temporary file.
4. Atomically rename it over the journal path.
5. Synchronize the journal directory.
6. Switch future appends to the replacement descriptor, then close the old
   descriptor.

If compaction fails before the rename, the original journal remains active and
the temporary file is removed. If an error occurs after the rename, the store
adopts the replacement descriptor before returning the error, so subsequent
appends remain recoverable. Abandoned temporary compaction files are removed
when the store opens. Failures are counted and logged but are non-fatal to the
service.

The background compactor checks once per minute by default. It runs only when
the journal is at least 64 MiB and its physical record count is at least twice
the live-delivery count. The byte threshold and check interval are configurable,
and a zero byte threshold disables automatic compaction. There is deliberately
no administrative HTTP endpoint and no purge operation.

## Concurrency

State transitions are serialized by the store. Claims record a lease and
increment the attempt number before a worker performs network I/O. Completion
updates include the attempt number; a late worker cannot overwrite a newer
claim. Compaction uses the same serialization boundary, trading a brief mutation
pause for a snapshot that cannot omit or reorder a concurrent state change.

## Networking

Only HTTP and HTTPS targets are accepted. User information and URL fragments
are rejected. The production-default dialer rejects loopback, private,
link-local, unspecified, and multicast addresses after DNS resolution.

Local targets can be enabled explicitly for development and integration tests.

## Non-goals for v0.1

- Multi-process access to one journal.
- Distributed worker coordination.
- Exactly-once delivery.
- Arbitrary scheduling or recurring jobs.
- Authentication and tenant isolation.
- Unbounded response-body storage.
