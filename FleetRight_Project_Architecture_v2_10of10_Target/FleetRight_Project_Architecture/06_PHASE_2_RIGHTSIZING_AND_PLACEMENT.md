# 6. Phase 2 — Rightsizing, Placement and Consolidation

## Goal

Build a deterministic C++ optimizer that reduces cost while preserving hard capacity, SLO, availability, topology, disruption and failure-recovery constraints.

## Inputs

Phase 2 consumes one immutable `OptimizationSnapshot` containing:

- workload specs and observations;
- node inventory and allocatable capacity;
- current placements;
- capacity profiles;
- policies;
- cost catalog;
- active disruption state;
- snapshot generation and freshness.

Phase 2 may use an observed-demand conservative bound even before Phase 3 forecasting is enabled.

## Rightsizer

For each workload, enumerate only supported/profiled resource shapes.

Reject a candidate shape if:

- memory reserve is insufficient;
- recent OOM/reclaim/throttling evidence violates policy;
- the profile is stale/low-confidence;
- demand bound exceeds SLO-safe profile capacity;
- startup/restart risk violates policy.

Output:

```text
recommended_shape
safe_replica_range
binding_constraints[]
confidence
rejected_shapes[]
```

## Replica planner

Compute demand-driven replicas from capacity profiles, then apply:

- minimum/maximum replicas;
- availability floor;
- N-1 reserve policy;
- scale-direction/action budget;
- recovery capacity floor.

## Placement engine

Use deterministic multi-dimensional best-fit placement with stable tie-breaking.

Constraints:

- CPU/memory/network capacity;
- topology spread;
- anti-affinity;
- supported taints/tolerations;
- unhealthy/cordoned nodes;
- workload priority;
- reserved headroom;
- migration budget;
- N-1 recovery feasibility where configured.

## Consolidation planner

A node is removable only if the planner can produce a full evacuation sequence that:

1. preserves all hard constraints during the sequence, not just in the final state;
2. fits within disruption/migration budgets;
3. respects recovery capacity requirements;
4. leaves enough spare capacity for configured failures;
5. is independently verifiable.

Node removal is represented as a plan of eviction/recreation actions plus final node deactivation, never as an atomic “move everything” action.

## Local improvement

After deterministic initial placement, run bounded move/swap/evacuation search under a strict time/iteration budget.

A move is retained only if:

```text
hard_constraints == PASS
and objective improves by >= minimum_improvement_threshold
```

## Exact small-instance oracle

For benchmark-only small scenarios, formulate the same constraints in an exact solver and record:

- exact objective;
- heuristic objective;
- optimality gap;
- exact solve time;
- heuristic solve time.

This prevents claims that “lower cost” implies “good optimizer” without a quality reference.

## Independent invariant checker

The Phase-2 test harness calls the verifier on every produced plan. Planner-internal assertions do not count as independent verification.

## Phase-2 exit gates

- zero accepted invalid placements across deterministic regression corpus;
- property-based random topologies find no verifier-accepted resource/topology violation;
- heuristic optimality gap reported for small-instance suite;
- p95 planner latency target met for 1,000 workloads / 500 nodes on reference host;
- verifier p95 latency target met independently;
- deterministic output for identical snapshot/build;
- consolidation sequences preserve intermediate-state safety;
- every destructive recommendation names a valid capacity profile and safety margin;
- cost improvement demonstrated versus static baseline on appropriate traces without worsening declared SLO constraints.
