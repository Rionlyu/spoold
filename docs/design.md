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

## Persistence

The journal is newline-delimited JSON. Each mutation appends the complete
current delivery record under a versioned envelope and calls `fsync` before
returning. Replay keeps the last valid record for each delivery.

A final partial line is treated as an interrupted append and ignored. Corrupt
complete records fail startup rather than silently discarding acknowledged
state.

## Concurrency

State transitions are serialized by the store. Claims record a lease and
increment the attempt number before a worker performs network I/O. Completion
updates include the attempt number; a late worker cannot overwrite a newer
claim.

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
