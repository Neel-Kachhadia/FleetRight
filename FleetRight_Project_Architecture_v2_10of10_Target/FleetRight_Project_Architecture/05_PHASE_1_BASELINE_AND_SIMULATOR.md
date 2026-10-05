# 5. Phase 1 — Measurement Foundation, Baselines and Simulator

## Goal

Build the scientific foundation for every later claim. Phase 1 is complete only when FleetRight can generate reproducible load, measure latency correctly, characterize safe capacity, and compare policies under the same deterministic workload trace.

## 1. Synthetic workloads

Implement at least three stateless services:

1. CPU-bound API;
2. memory-pressure-sensitive API;
3. latency-sensitive queued service.

Each service supports configurable:

- service-time distribution;
- concurrency;
- memory footprint;
- burst behavior;
- cold-start/warm-up duration;
- failure/error injection.

## 2. Workload traces

Mandatory trace families:

- steady;
- ramp;
- diurnal/seasonal;
- bursty;
- flash crowd;
- correlated multi-service spike;
- noisy tenant;
- memory-heavy;
- slow-starting service.

Every trace has a schema version, generator seed and content hash.

## 3. Correct latency telemetry

Instrument request latency using native histograms where practical or SLO-aware classic histogram buckets.

Record:

- count;
- histogram distribution;
- p50/p95/p99 derived from aggregate histogram data;
- error/reject count;
- queue depth/growth.

Do not aggregate by averaging per-replica p99 values.

For low traffic, mark tail-latency evidence low-confidence based on sample count rather than treating a noisy p99 as authoritative.

## 4. Capacity characterization lab

For every supported workload/resource shape, run a controlled load sweep:

```text
low load
  -> step demand upward
  -> hold each level long enough to stabilize
  -> record latency/error/queue/CPU/memory
  -> detect sustainable boundary
  -> repeat across seeds/runs
```

The output is a versioned `CapacityProfile`, including a conservative safe throughput/concurrency bound and confidence metadata.

A profile must be invalidated/reprofiled when workload code, request mix or important runtime configuration changes.

## 5. Deterministic event-driven simulator

Simulator state includes:

- nodes and machine types;
- replicas and placements;
- demand traces;
- service capacity profiles;
- queues;
- startup/warm-up delay;
- actuation delay;
- node activation delay;
- eviction/restart delay;
- failures;
- cost accumulation.

Simulation must model nonlinear behavior near capacity. If offered load exceeds effective service capacity, queueing and latency must worsen rather than remaining linearly proportional to CPU.

Simulator assumptions are documented and calibrated against real Docker workload experiments for critical behaviors.

## 6. Baseline policies

### A. Static peak provisioning

Provision for known peak plus declared reserve.

### B. Naive threshold controller

Educational baseline only:

```text
CPU > threshold for N windows -> scale up
CPU < threshold for M windows -> scale down
```

### C. HPA-like target tracker

Serious reactive baseline. It must implement/document:

- target metric ratio;
- tolerance/deadband;
- separate scale-up/down velocity policies;
- downscale stabilization window;
- startup/readiness exclusion;
- conservative treatment of missing metrics;
- multi-metric max recommendation when configured.

The exact implementation is local and deterministic; it is not claimed to be byte-for-byte Kubernetes HPA. Its documented semantics must be sufficiently close to make the comparison fair.

### D. Offline lookahead oracle policy

Not a deployable controller. It receives the future demand trace and computes a lower-bound/reference capacity schedule under the same action delays and safety model. It frames how close FleetRight gets to ideal foresight.

## Phase-1 exit gates

- deterministic trace replay from seed/hash;
- aggregate histogram latency instrumentation validated;
- at least three workload profiles characterized;
- capacity envelopes repeatable within declared tolerance;
- simulator behavior calibrated against Docker measurements for saturation and startup;
- static, naive and HPA-like baselines implemented;
- oracle/reference framework available for bounded scenarios;
- one command generates raw manifest, metrics and baseline report;
- no later phase is allowed to invent SLO safety without referencing a versioned capacity profile.

## Evidence artifact

`results/phase1/<run-id>/` contains:

```text
manifest.json
trace.parquet
capacity_profiles/
raw_metrics/
policy_outputs/
summary.json
report.md
plots/
```
