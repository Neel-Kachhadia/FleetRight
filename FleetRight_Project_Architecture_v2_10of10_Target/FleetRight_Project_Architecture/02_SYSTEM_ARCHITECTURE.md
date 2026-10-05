# 2. System Architecture

## High-level architecture

```text
                         Synthetic / Real Workloads
                                   │
                                   ▼
                          Prometheus / Exporters
                                   │
                                   ▼
                         Telemetry Normalizer
                                   │
                                   ▼
                     Immutable Snapshot Builder
                       snapshot_id / generation
                                   │
                  ┌────────────────┴────────────────┐
                  ▼                                 ▼
          Forecast Engine                  Capacity Profile Store
      demand + uncertainty              measured SLO-safe envelopes
                  │                                 │
                  └────────────────┬────────────────┘
                                   ▼
                         C++ Planning Engine
             rightsizing + replicas + placement + nodes
                                   │
                            candidate plan
                                   ▼
                    Independent Plan Verifier
             ┌─────────────────────────────────────────┐
             │ resource capacity / explicit units      │
             │ SLO capacity envelope                   │
             │ availability / topology                 │
             │ N-1 feasibility where required          │
             │ disruption and migration budgets        │
             │ telemetry/profile freshness             │
             │ action/blast-radius budgets             │
             │ snapshot generation preconditions       │
             └─────────────────────────────────────────┘
                                   │
                         verified immutable plan
                                   ▼
                           Decision Journal
                                   │
                  ┌────────────────┴────────────────┐
                  ▼                                 ▼
          Simulator / Shadow                 Reconciler / Executor
                                                    │
                                 Docker / local Kubernetes adapters
                                                    │
                                                    ▼
                                             Outcome Verifier
                                                    │
                                      ┌─────────────┴─────────────┐
                                      ▼                           ▼
                                   COMMIT                     COMPENSATE
                                      │                           │
                                      └─────────────┬─────────────┘
                                                    ▼
                                          Evidence / Reports
```

## Architectural principles

### 1. Immutable decision inputs

A plan never depends on an implicit “current state.” The Snapshot Builder emits a content-addressed, immutable snapshot containing normalized telemetry, workload/node inventory, active policies, topology, disruption state and references to capacity profiles.

Every plan records:

```text
base_snapshot_id
base_generation
policy_version
machine_catalog_version
profile_set_version
forecast_version
optimizer_build_id
```

### 2. Planner and verifier are separate trust domains

The planner searches for a good plan. The verifier does **not** trust planner scoring or planner-internal feasibility flags. It recomputes hard constraints from the canonical plan and snapshot.

A planner bug may produce a bad plan. A bad plan must still fail independent verification.

### 3. Execution is reconciliation, not fire-and-forget commands

The controller records desired actions and repeatedly compares desired state with observed state. A timeout means “unknown outcome,” not “failed, retry blindly.” This is essential for Kubernetes/API partial failure and process crashes.

### 4. Downscale is asymmetric

Scale-out may use forecast upper bounds to prepare early. Destructive scale-in/consolidation requires stronger evidence from observed demand, stable capacity profiles, cooldowns and recovery reserve.

### 5. Compensation, not magical rollback

Capacity removal is not transactionally reversible. Re-adding replicas or nodes can take time. The system therefore models compensating actions, recovery time, warm-up and spare capacity.

## Major subsystems

### Telemetry plane

Collects:

- CPU usage in millicores / CPU-seconds;
- CPU throttling;
- memory working set/RSS;
- QPS and concurrency;
- request-duration histogram;
- errors and rejects;
- queue depth and queue growth;
- network throughput;
- replica readiness;
- restart count;
- node health and allocatable resources.

Latency is represented with aggregatable histograms. FleetRight does not average per-replica p99 values.

Each normalized metric carries:

```text
window_start
window_end
sample_count
source
freshness
coverage
confidence
```

### Capacity profile store

Stores experimentally derived safe operating regions for each service/resource configuration. See `17_CAPACITY_AND_SLO_MODEL.md`.

### Forecast engine

Produces expected demand and an upper confidence bound. Forecast output cannot override hard limits.

### C++ planner

Outputs candidate plans for:

- vertical resource requests;
- replica count;
- placement;
- node activation/removal;
- bounded migrations.

It also records rejected alternatives and objective components.

### Independent verifier

Verifier code should be smaller and simpler than the planner. It validates canonical state transitions and returns machine-readable failure codes.

### Reconciler/executor

Supported adapters:

- simulator;
- Docker/local processes;
- Kubernetes.

Kubernetes semantics:

- replica changes operate on controller desired state;
- “move replica” is modeled as eviction/recreation, not live migration;
- voluntary disruptions use the Eviction API when applicable;
- PodDisruptionBudgets are checked and rechecked;
- topology-spread constraints, anti-affinity, taints/tolerations and readiness are respected for supported scenarios;
- node drain is a sequence of reconcilable evictions, not a single atomic action.

### Outcome verifier

After each progressive step, compare observed state against expected bounds:

- latency/error/queue behavior;
- readiness;
- saturation;
- forecast miss;
- cost change;
- recovery time.

Only after the observation interval succeeds does the controller continue to the next step.

## Plan lifecycle

```text
PROPOSED
  -> VERIFIED
  -> APPROVED           optional manual gate by policy
  -> EXECUTING
  -> VERIFYING
  -> COMMITTED

failure paths:
  -> REJECTED
  -> STALE
  -> PAUSED
  -> COMPENSATING
  -> COMPENSATED
  -> FAILED_SAFE
```

Transitions are persisted before externally visible side effects whenever practical.

## Concurrency control

FleetRight must assume that state can change while planning.

Before each action:

1. fetch current generation/resource version;
2. evaluate action preconditions;
3. verify relevant safety facts are still fresh;
4. execute or mark the plan `STALE`;
5. reconcile actual state.

Only one active destructive plan is allowed per workload/node conflict domain unless an explicit concurrency protocol proves independence.

## Degraded-mode behavior

| Condition | Allowed behavior |
|---|---|
| Prometheus unavailable | no destructive optimization; recovery scale-out may use trusted last-known state only if policy permits |
| snapshot expired | plan becomes stale |
| profile confidence low | block rightsizing/downscale; allow safe scale-out |
| forecast unavailable | fall back to reactive conservative policy |
| PostgreSQL unavailable before action | do not start new mutation |
| PostgreSQL loss after unknown action result | reconcile target first |
| verifier unavailable | no execution |
| planner unavailable | maintain/recover current desired state; no optimization |
