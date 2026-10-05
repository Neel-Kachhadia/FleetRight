# 13. API and Data Contracts

## Contract design rules

- every mutable external object has a version/generation;
- every replay-critical input is versioned/content-addressed;
- units are explicit in field names or strong wrapper types;
- timestamps use UTC for audit; elapsed timers use monotonic clocks locally;
- destructive actions carry preconditions;
- unknown enum values fail safely at safety boundaries.

## Optimization snapshot

Conceptual protobuf:

```text
OptimizationSnapshot {
  string snapshot_id;
  uint64 generation;
  Timestamp observed_at;
  Timestamp expires_at;
  ClusterState cluster;
  repeated WorkloadState workloads;
  repeated CapacityProfileRef profiles;
  repeated PolicyRef policies;
  MachineCatalogRef machine_catalog;
  TelemetryQuality telemetry_quality;
}
```

## Optimization request

```text
OptimizationRequest {
  OptimizationSnapshot snapshot;
  repeated DemandForecast forecasts;
  PlanningMode mode; // REACTIVE or PREDICTIVE
  PlanningBudget budget;
}
```

## Optimization response

```text
OptimizationPlan {
  string plan_id;
  string base_snapshot_id;
  uint64 base_generation;
  Timestamp created_at;
  Timestamp expires_at;
  ObjectiveBreakdown current_objective;
  ObjectiveBreakdown proposed_objective;
  repeated Action actions;
  repeated RejectedAlternative rejected_alternatives;
  repeated ConstraintMargin constraint_margins;
  Explanation explanation;
  string optimizer_build_id;
}
```

## Verifier response

```text
VerificationResult {
  string plan_id;
  bool accepted;
  repeated InvariantResult invariants;
  repeated ConstraintMargin margins;
  string verifier_build_id;
  bytes canonical_result_hash;
}
```

## Action types

```text
ResizeWorkload
SetReplicaCount
EvictReplica
CordonNode
DrainNodeStep
DeactivateNode
ActivateNode
NoOp
```

Avoid a generic `MoveReplica` in the canonical live contract because Kubernetes movement is normally eviction/recreation.

Every action includes:

```text
action_id
conflict_keys[]
preconditions[]
expected_postconditions[]
max_blast_radius
observation_window
compensation_action
reason_codes[]
```

## Policy example

```yaml
schema_version: 2
workload: payments-api
priority: critical
latency_slo_ms: 120
error_rate_slo: 0.001
min_replicas: 4
max_replicas: 20
min_cpu_millicores: 250
min_memory_mib: 256
min_headroom_fraction: 0.25
max_scale_down_fraction_per_step: 0.10
downscale_stabilization_seconds: 300
post_action_observation_seconds: 120
forecast_scale_in_allowed: false
profile_min_confidence: 0.95
telemetry_max_age_seconds: 30
failure_tolerance: N_MINUS_1
max_concurrent_disruptions: 1
topology:
  key: topology.kubernetes.io/zone
  min_domains: 3
  max_skew: 1
```

## Control-plane HTTP API

Read endpoints:

```text
GET /healthz
GET /readyz
GET /v1/state
GET /v1/plans
GET /v1/plans/{id}
GET /v1/experiments
GET /v1/audit
```

Mutating endpoints:

```text
POST /v1/optimize
POST /v1/plans/{id}/approve
POST /v1/plans/{id}/execute
POST /v1/plans/{id}/pause
POST /v1/plans/{id}/cancel
POST /v1/plans/{id}/compensate
```

There is no unauthenticated generic `/rollback`; compensation is a journaled state-machine transition.

## Authentication and authorization

- read-only endpoints may have a separate viewer role;
- optimization/recommendation role cannot execute;
- operator can approve/execute bounded plans;
- admin can change policy/security configuration;
- mutating requests require authentication, authorization and audit identity.

Local development may use static dev credentials, but “auth disabled” cannot be the default execute-mode configuration.

## Idempotency

Mutating client requests include an `Idempotency-Key`. Action attempts also use stable `action_id`.

If a transport response is lost, the caller queries/reconciles by action ID. Duplicate requests never intentionally replay a side effect.

## Schema evolution

- protobuf field numbers never reused;
- unknown safety-critical enum values fail closed;
- policy files carry schema version;
- migrations are versioned;
- benchmark traces/snapshots/profile artifacts carry schema version;
- replay tool supports only explicitly declared compatible versions.
