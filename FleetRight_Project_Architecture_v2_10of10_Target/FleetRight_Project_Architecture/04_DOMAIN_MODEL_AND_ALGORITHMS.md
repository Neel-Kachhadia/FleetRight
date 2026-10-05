# 4. Domain Model and Algorithms

## Canonical units

FleetRight does not use ambiguous percentages as optimizer inputs.

```text
CpuMillicores      integer
MemoryMiB          integer
NetworkMbps        integer/decimal
RequestsPerSecond  decimal
Concurrency        integer/decimal
LatencyMs          decimal
DurationMs         integer
MoneyMicrosPerHour integer
```

Percent utilization can be a telemetry display field but is derived from explicit numerator/denominator quantities.

## Resource vector

```text
ResourceVector {
    cpu_millicores
    memory_mib
    network_mbps
}
```

A placement is feasible only when every supported dimension plus configured reserve fits allocatable capacity.

## Workload model

```text
WorkloadSpec {
    workload_id
    priority_class
    min_replicas
    max_replicas
    startup_ms
    readiness_stabilization_ms
    latency_slo_ms
    error_rate_slo
    topology_constraints[]
    disruption_policy
    failure_tolerance_policy
}

WorkloadObservation {
    allocated_cpu_millicores
    allocated_memory_mib
    observed_cpu_millicores_p50/p95/p99
    observed_memory_mib_p50/p95/p99
    cpu_throttling_rate
    request_rate
    concurrency
    queue_depth
    queue_growth_rate
    latency_histogram_ref
    error_rate
    sample_count
    confidence
}
```

## Node model

```text
NodeState {
    node_id
    machine_type
    allocatable_resources
    reserved_resources
    hourly_cost
    topology_labels
    taints
    health
    lifecycle_state
    placements[]
    generation
}
```

## Capacity profile

A `CapacityProfile` is the empirical bridge between a proposed allocation and SLO feasibility.

```text
CapacityProfile {
    workload_profile_id
    cpu_allocation
    memory_allocation
    replica_shape
    request_mix_id
    sustainable_qps_at_slo
    sustainable_concurrency_at_slo
    p99_latency_at_boundary
    error_rate_at_boundary
    queue_growth_at_boundary
    cold_start_distribution
    sample_count
    confidence_interval
    valid_until
    profile_version
}
```

See `17_CAPACITY_AND_SLO_MODEL.md`.

## Forecast model

```text
DemandForecast {
    workload_id
    horizon_ms
    expected_qps
    qps_upper_bound
    expected_concurrency
    concurrency_upper_bound
    calibration_error
    confidence
    model_version
}
```

## Replica calculation

For each candidate resource shape, derive a **conservative effective capacity** from the measured profile.

```text
effective_qps_per_replica =
    profile.sustainable_qps_at_slo
    * degradation_factor(profile_confidence, recent_error, cluster_health)

replicas_for_qps = ceil(forecast.qps_upper_bound / effective_qps_per_replica)
replicas_for_concurrency = ceil(forecast.concurrency_upper_bound /
                                effective_concurrency_per_replica)

required_replicas = max(
    replicas_for_qps,
    replicas_for_concurrency,
    availability_floor,
    failure_reserve_floor
)
```

CPU and memory remain hard placement dimensions; they are not substitutes for SLO capacity. A candidate resource shape without a sufficiently confident profile cannot be used for destructive rightsizing.

## Vertical rightsizing

Vertical recommendations are chosen from profiled resource shapes, not merely `usage_p95 * multiplier`.

A candidate shape is legal only if:

- recent memory working-set bound fits with reserve;
- no evidence of recent OOM/reclaim instability;
- throttling stays below policy threshold;
- a compatible capacity profile exists;
- profile-predicted SLO bounds remain feasible at demand UCB;
- startup/recovery cost is acceptable.

Quantiles can seed which shapes to profile, but they do not alone prove safety.

## Placement heuristic

### Initial deterministic solution

1. calculate normalized demand for every resource dimension;
2. order replicas by dominant normalized demand, priority and stable ID tie-breakers;
3. evaluate feasible nodes;
4. score residual fragmentation across CPU/memory/network;
5. include topology, failure reserve and migration penalty;
6. place into the lowest deterministic score.

Example residual score:

```text
fragmentation(node_after) =
    w_cpu * square(free_cpu_ratio)
  + w_mem * square(free_mem_ratio)
  + w_net * square(free_net_ratio)
  + w_imbalance * resource_imbalance
```

### Bounded local improvement

Consider a deterministic bounded set of:

- single-replica moves;
- pair swaps;
- node evacuation sequences.

Stop on iteration/time budget. Stable ordering guarantees reproducibility.

### Exact offline oracle

For small scenarios only, solve the equivalent mixed/integer placement model offline to measure heuristic quality:

```text
optimality_gap = (heuristic_cost - exact_cost) / exact_cost
```

This is benchmark evidence, not the production path.

## N-1 feasibility

A topology may look diverse but still be unrecoverable after a failure. For workloads/pools configured `N_MINUS_1`, the verifier removes each required failure domain in turn and checks whether surviving allocatable capacity can host the critical recovery set while maintaining minimum replicas and declared SLO-safe capacity.

The implementation may use conservative aggregate checks first and an explicit recovery-placement feasibility check for critical plans.

## Objective function

```text
score(plan) =
    infrastructure_cost
  + lambda_move        * migration_cost
  + lambda_churn       * action_churn
  + lambda_uncertainty * uncertainty_exposure
  + lambda_boundary    * distance_to_safety_boundary_penalty
```

Hard constraints are evaluated separately and override the score.

## Hard constraints

A candidate is infeasible if any of the following fails:

- explicit resource capacity;
- SLO envelope at demand upper bound;
- error-rate bound;
- non-growing queue requirement at planned peak;
- minimum/maximum replicas;
- topology spread / anti-affinity;
- supported taint/toleration rules;
- disruption budget;
- migration/action budget;
- telemetry freshness;
- capacity-profile confidence/freshness;
- failure tolerance requirement;
- state-generation/action preconditions.

## Explainability

Every action includes:

- current state;
- proposed state;
- demand UCB;
- capacity profile/version used;
- expected cost delta;
- hard constraints and margins;
- rejected more-aggressive alternative and exact reason;
- action blast radius;
- observation interval;
- compensation action.

Example:

```text
payments-api: 8 -> 7 replicas
Demand UCB (10m): 3,640 rps
Validated effective capacity: 585 rps/replica
Required by demand: 7
Availability floor: 4
N-1 recovery check: PASS
Predicted p99 upper bound: 106 ms <= 120 ms SLO
Expected saving: $0.018/hour
6 replicas rejected: demand capacity margin would be -130 rps
```
