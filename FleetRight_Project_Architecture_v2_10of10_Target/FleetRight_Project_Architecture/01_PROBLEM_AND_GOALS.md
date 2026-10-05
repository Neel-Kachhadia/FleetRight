# 1. Problem, Scope and Engineering Goals

## Problem statement

Compute fleets are expensive because operators must continuously trade reliability margin against waste. Overprovisioning protects tail latency and failure recovery but burns node-hours, licenses, power and capacity. Underprovisioning causes saturation, queue growth, throttling, OOMs, retry storms and SLO violations.

A useful controller therefore has to reason about more than average CPU:

- heterogeneous CPU, memory and network capacity;
- nonlinear latency near saturation;
- request-rate and concurrency changes;
- startup and warm-up delay;
- noisy or missing telemetry;
- forecast error;
- workload priority;
- topology/failure domains;
- disruption budgets;
- migration/restart cost;
- node loss and recovery headroom;
- conflicting controllers and stale plans.

## Primary engineering question

> How much capacity can be rightsized, consolidated, or removed while proving that the resulting state remains feasible for a conservative demand bound and defined failure model?

## Product goals

FleetRight must:

1. build immutable, versioned snapshots of cluster state;
2. distinguish allocation, observed usage and inferred demand using explicit units;
3. empirically profile service capacity and SLO behavior;
4. forecast near-term demand with calibrated uncertainty;
5. jointly recommend resource sizes, replica counts, placement and active nodes;
6. independently verify every candidate plan against hard invariants;
7. execute only against the state version the plan was verified for;
8. use progressive actions with bounded blast radius;
9. reconcile actual state after timeout/crash instead of blindly retrying;
10. compensate for harmful actions and measure recovery time;
11. explain every accepted and rejected decision;
12. preserve enough input state to deterministically replay experiments;
13. fail safe on stale, malformed or low-confidence inputs;
14. compare against realistic reactive baselines on identical traces;
15. generate defensible cost, latency and reliability evidence.

## Explicit scope

Version 1 optimizes **stateless replicated services**. Stateful services are intentionally excluded because safe movement introduces quorum, data replication, persistent-volume and durability semantics that would turn FleetRight into a different project.

Supported resource dimensions for v1:

```text
CPU      millicores
Memory   MiB
Network  Mbps or bytes/s where measured
Demand   requests/s and concurrent requests
Latency  milliseconds
```

GPU is not part of the v1 resource vector unless a real GPU workload and benchmark are later added.

## Non-goals

FleetRight is not:

- a replacement Kubernetes scheduler;
- a cloud provider;
- a universal cluster autoscaler;
- a full billing platform;
- a stateful workload orchestrator;
- a reinforcement-learning research project;
- a dashboard-first project;
- an LLM infrastructure agent.

## Safety hierarchy

The controller follows this priority ordering:

```text
1. preserve correctness and declared hard safety invariants
2. preserve availability / SLO feasibility
3. restore capacity after faults
4. reduce uncertainty
5. reduce infrastructure cost
```

Cost optimization never outranks safety.

## Quantitative targets

Targets are goals until measured; they must never be copied to a resume as facts before reproducible validation.

- zero verifier-accepted plans that violate modeled hard constraints;
- zero dangerous scale-downs on expired or insufficient telemetry;
- deterministic planner output for identical normalized inputs and build version;
- p99 SLO target met in >=99% of normal evaluation windows for selected benchmark scenarios, or a stricter configured target;
- optimizer p95 decision time <=250 ms for 1,000 workloads / 500 nodes on the reference host;
- verifier p95 decision time <=25 ms for the same scenario;
- control-plane restart recovers without duplicate side effects;
- measurable cost reduction versus static peak provisioning;
- measurable cost reduction versus a serious HPA-like reactive baseline at equivalent SLO constraints;
- predictive mode reduces late scale-out events without using forecast-only evidence to perform aggressive scale-in;
- small-instance heuristic optimality gap quantified against an exact offline oracle;
- repeated benchmark runs report median plus dispersion, not cherry-picked best runs.

## Success definition

FleetRight succeeds when it can answer both questions with evidence:

1. **Why is this plan cheaper?**
2. **Why is this plan safe enough to execute under the declared model?**
