# 3. Technology Stack

## Design rule

Use the smallest stack that can prove the systems concepts. Dependencies must support a measurable requirement, not decorate the repository.

## C++20 — planning and verification core

Use for:

- resource/domain model;
- deterministic planner;
- constraint evaluation;
- independent verifier library or separately linked verifier target;
- placement and local search;
- replay hot path;
- microbenchmarks.

Recommended:

- CMake;
- GoogleTest;
- Google Benchmark;
- standard library first;
- protobuf-generated types at process boundaries;
- optional OR-Tools **only in offline benchmark tooling** as an exact small-instance quality oracle, never as a mandatory production dependency.

Quality:

- `-Wall -Wextra -Wpedantic` plus project-appropriate warnings;
- clang-format;
- clang-tidy;
- ASan/UBSan;
- TSan on concurrency-focused targets;
- fuzz harnesses for parsers/contracts/resource arithmetic.

## Python 3.11+ — control and experimentation

Use for:

- FastAPI control API;
- Prometheus adapter;
- immutable snapshot construction;
- forecast models;
- reconciliation orchestration;
- Docker/Kubernetes adapters;
- experiment runner;
- statistical reports.

Recommended:

- FastAPI + Pydantic;
- httpx;
- psycopg;
- prometheus-client;
- Kubernetes Python client for the local adapter;
- NumPy for offline/forecast math if needed;
- pandas only for offline analysis;
- matplotlib for reports;
- pytest;
- Hypothesis for property-based Python tests;
- ruff + pyright/mypy.

## Contracts

Preferred runtime boundary:

- gRPC;
- Protocol Buffers;
- explicit schema versions;
- integer/strongly typed units at the canonical model boundary where possible.

A local subprocess adapter using protobuf is acceptable during early Phase 1, but the final release should exercise the same versioned request/response contracts used by replay.

## Telemetry

Mandatory:

- Prometheus;
- native histograms where supported or appropriately bucketed classic histograms;
- recording rules only where they do not erase required raw evidence.

Optional:

- OpenTelemetry traces for correlating control actions with workload behavior;
- Grafana for demo visualization.

Never compute fleet latency by averaging per-instance quantiles.

## Durable state

PostgreSQL stores:

- policies and versions;
- plan metadata;
- state-machine transitions;
- action attempts;
- idempotency keys;
- snapshot/profile content references;
- compensation events;
- experiment manifests;
- audit records.

Large immutable benchmark/snapshot payloads may be stored as compressed local artifacts referenced by content hash; PostgreSQL stores metadata and integrity hashes.

SQLite may be used for isolated unit tests only.

## Local targets

- Docker Compose is mandatory for developer/test workflows;
- kind or k3d is mandatory for the final Kubernetes integration gate;
- Linux is the reference host.

## Cost model

Checked-in synthetic catalog is the canonical benchmark source:

```text
small   2 vCPU   4 GiB   $0.05/hour
medium  4 vCPU   8 GiB   $0.09/hour
large   8 vCPU  16 GiB   $0.17/hour
xlarge 16 vCPU  32 GiB   $0.31/hour
```

Every catalog has a version/hash. Optional provider adapters may import public prices, but headline reproducibility cannot depend on a live pricing API.

## CI

GitHub Actions should run:

- clean C++ configure/build/test;
- Python lint/type/test;
- ASan/UBSan jobs;
- targeted TSan;
- fuzz smoke corpus;
- deterministic replay test;
- Docker Compose integration test;
- kind/k3d Kubernetes semantic test on a gated workflow;
- benchmark smoke/regression check with intentionally loose hardware-sensitive thresholds;
- documentation contract/version consistency checks.

## Dependency exclusions

Do not add Kafka, Redis, service mesh, cloud queueing, workflow engines or an operator framework without a measured need. A single controller plus PostgreSQL is enough for v1.
