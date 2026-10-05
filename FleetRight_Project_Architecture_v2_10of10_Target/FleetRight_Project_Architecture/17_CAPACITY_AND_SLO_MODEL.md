# 17. Capacity and SLO Model

## Why this document exists

The most dangerous shortcut in a capacity optimizer is assuming that “low CPU” proves that fewer replicas are safe. Tail latency is often nonlinear near saturation and workloads can bottleneck on memory, concurrency, queueing or downstream behavior.

FleetRight therefore bases destructive rightsizing on **empirically validated performance envelopes**.

## Definitions

### Offered load

The demand arriving at a workload:

```text
request_rate_rps
concurrency
request_mix
```

### Service capacity

The maximum offered load a given resource/replica shape can sustain while satisfying configured latency, error and queue-stability criteria with a declared confidence margin.

### SLO-safe boundary

For a profile candidate, a load point is considered safe only when all are satisfied:

```text
p99_upper_bound <= latency_slo
error_rate_upper_bound <= error_slo
queue_growth_upper_bound <= 0
memory_pressure <= policy_threshold
cpu_throttling <= policy_threshold
```

The safe capacity is deliberately below the first observed unstable/saturated region.

## Characterization experiment

For each service build/request mix/resource shape:

1. warm service to steady state;
2. start at low demand;
3. increase demand in controlled steps;
4. hold each step for a predefined interval;
5. repeat each point across runs/seeds;
6. aggregate latency via histograms;
7. record CPU, memory, throttling, errors, queue behavior;
8. identify the highest load level satisfying all criteria with required confidence;
9. apply an explicit safety haircut;
10. persist profile with version/hash.

## Profile identity

A profile key includes factors that materially change capacity:

```text
workload build/image digest
request-mix ID
CPU allocation
memory allocation
runtime configuration class
architecture/platform class if relevant
profile schema version
```

A code or request-mix change cannot silently reuse an incompatible profile.

## Confidence and sample sufficiency

Tail metrics need enough requests. A profile point with insufficient samples cannot be promoted to a high-confidence safety boundary.

For a threshold-form latency SLO such as `p99 <= 120 ms`, FleetRight should also evaluate the directly observable success fraction:

```text
latency_success_fraction = requests_with_latency_<=_120ms / total_requests
```

A `p99 <= 120 ms` target corresponds to at least 99% of requests meeting the threshold. The characterization harness should compute a conservative statistical lower bound for that success fraction (for example a Wilson/binomial bound) and require the lower bound to meet the target before the point is considered profile-safe. This avoids declaring a boundary safe from a noisy point estimate of p99 alone.

For error-rate SLOs, use the analogous conservative upper confidence bound on failures. Across repeated runs, also report dispersion and reject unstable points even when the pooled average passes.

The profile stores:

- request count;
- number of repeated runs;
- histogram configuration/version;
- tail-SLO success fraction and conservative bound;
- error-rate conservative bound;
- per-run dispersion;
- observed variance;
- generated-at/valid-until.

## Conservative effective capacity

Planning capacity is lower than raw measured boundary:

```text
effective_capacity = measured_safe_capacity
                     * profile_confidence_factor
                     * recent_model_accuracy_factor
                     * degraded_cluster_factor
```

Every factor is in `(0, 1]` and its value/reason is shown in plan explainability.

## Predicting p99 safely

FleetRight does not need a magical globally accurate p99 regression model. It can use a conservative profile envelope:

- find the profile/resource shape compatible with the candidate;
- ensure per-replica planned load stays inside the safe region;
- optionally interpolate only within empirically supported points;
- treat extrapolation toward/above saturation as unsafe;
- expose `predicted_p99_upper_bound` from the envelope/confidence model.

This makes SLO prediction auditable.

## Queueing guard

Even if current p99 is below SLO, positive queue growth indicates unsustainable capacity. The verifier therefore rejects a candidate whose conservative planned load is expected to produce a growing queue.

## Cold-start model

Profiles include startup/readiness/warm-up distributions. For scale-out planning:

```text
capacity is not counted as available until readiness + configured warm-up
```

For compensation, recovery time starts when the compensating action begins and ends when capacity is ready and SLO health has recovered.

## Memory safety

Memory is treated differently from CPU because short-term low use does not imply a safe lower limit.

Downsize requires:

- stable working-set history;
- no recent OOM;
- no severe reclaim/page-fault indicator in supported telemetry;
- configured reserve;
- compatible load-tested profile where practical.

## Profile fallback behavior

| Profile state | Scale out | Scale in / downsize |
|---|---|---|
| valid/high confidence | allowed if verifier passes | allowed if verifier passes |
| stale | allowed conservatively | blocked |
| missing | reactive emergency scale-out allowed by policy | blocked |
| low sample count | allowed conservatively | blocked |
| incompatible build/request mix | allowed conservatively | blocked until reprofiled |

## Calibration against real workloads

Simulator capacity curves must be compared with Docker workload measurements. The simulator may simplify reality, but the release report must state the observed calibration error and avoid claiming more precision than measured.
