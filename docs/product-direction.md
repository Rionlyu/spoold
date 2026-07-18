# Product direction

## Decision

`spoold` is a durable local HTTP delivery spool for programs that cannot afford
to lose an outbound request when a process terminates, a machine reboots, a
destination fails, or connectivity disappears.

The shortest description is **durable curl**:

1. submit an HTTP request to a local `spoold`;
2. receive success only after the request is synchronized to disk;
3. allow `spoold` to deliver it in the background and resume after restarts.

The project is proceeding in this direction. It is not becoming a hosted
webhook platform, a general message broker, or a distributed workflow engine.

## Intended users

The primary users are:

- shell scripts, cron jobs, and CI tasks that need durable callbacks;
- small self-hosted applications that do not operate a database or broker;
- on-premise software and appliances reporting to an external control plane;
- edge agents that must retain HTTP deliveries through connectivity loss;
- legacy or language-constrained programs that can call a local HTTP API.

This is not a transactional outbox for an application's database. `spoold`
cannot make an application state change atomic with a separate enqueue request.
It provides a clear durability boundary once the caller submits the delivery.

## Differentiation

The underlying store-and-forward pattern is established. The product
differentiation is the combination of:

- arbitrary caller-selected HTTP requests rather than one telemetry schema;
- synchronization before enqueue acknowledgement;
- deterministic crash replay and lease recovery;
- idempotent submission with conflicting-reuse detection;
- safe outbound target resolution;
- one self-contained daemon and one local journal;
- no PostgreSQL, MongoDB, Redis, broker, cloud account, or language SDK.

Current adjacent categories make different tradeoffs:

- hosted HTTP queues and webhook platforms require a third-party service and
  focus on customer endpoints, portals, routing, or multi-tenancy;
- transactional outbox frameworks are tied to an application framework and its
  database;
- telemetry agents provide disk buffers for log or metric pipelines rather
  than an explicit fsync-backed API for arbitrary HTTP requests;
- general store-and-forward proxies commonly introduce a database-backed queue
  and a larger operational surface.

`spoold` wins only when the absence of that extra stack matters. It should not
attempt to match broader systems feature for feature.

## Confidence and stop condition

The direction decision was re-evaluated on 2026-07-18 with an estimated 92%
confidence. This is a product judgment, not a statistical measurement. The
research included:

- [aswh](https://codeberg.org/pepmartinez/aswh), the closest general
  store-and-forward HTTP proxy, which requires MongoDB, Redis, or PostgreSQL;
- [Svix](https://docs.svix.com/retries) and
  [QStash](https://upstash.com/docs/qstash/features/retry), which provide
  hosted delivery infrastructure;
- [Vector](https://vector.dev/docs/reference/configuration/sinks/http/) and
  [Fluent Bit](https://docs.fluentbit.io/manual/administration/buffering-and-storage),
  which provide disk-buffered telemetry pipelines;
- framework-specific transactional outbox implementations that share the
  application's database.

The direction remains worthy because no maintained alternative found in the
audit combined arbitrary HTTP submission, synchronization before
acknowledgement, single-host crash replay, and zero runtime infrastructure.
This is a differentiation claim, not a claim that store-and-forward HTTP has no
prior art.

Development should stop or reposition if a maintained, dependency-free local
HTTP spool with comparable durability becomes clearly established and `spoold`
cannot show a simpler contract or stronger failure behavior. It should also
stop expanding if real users do not adopt the release after deliberate
distribution and feedback work.

## Product contract

The durable local-spool direction requires:

1. a curl-like client suitable for humans and scripts;
2. a local trust boundary that does not require a public API;
3. bounded retention and explicit disk-full behavior;
4. per-destination backpressure so one target cannot monopolize delivery;
5. packaging as signed binaries and a minimal container;
6. failure tests covering abrupt termination, restart, and storage faults.

The first product milestone adds `spoolctl`, a dependency-free client for
submitting, inspecting, retrying, and canceling deliveries.

## Deliberate non-goals

- inbound webhook ingestion from the public internet;
- hosted accounts, billing, teams, or tenant isolation;
- a graphical dashboard;
- transformations, fan-out, or workflow orchestration;
- distributed workers or high-availability coordination;
- exactly-once HTTP effects;
- replacing MQTT, Kafka, NATS, or a transactional application outbox.

## Validation

Popularity is not the initial proof. The direction is validated when unrelated
users can install a release without Go, reproduce the crash-recovery demo, and
operate it for a real script, appliance, self-hosted service, or edge agent.
External deployments, issues, and contributions are stronger evidence than
stars alone.
