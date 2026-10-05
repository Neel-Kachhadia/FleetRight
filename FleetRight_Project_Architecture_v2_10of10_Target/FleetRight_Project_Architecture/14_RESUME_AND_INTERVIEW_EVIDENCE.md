# 14. Resume and Interview Evidence

## What FleetRight should prove

A completed FleetRight demonstrates four distinct engineering strengths.

### Algorithms

- multidimensional bin packing;
- deterministic local search;
- exact-oracle comparison;
- constraint modeling;
- forecast uncertainty.

### Distributed/control systems

- immutable snapshots;
- stale-state/TOCTOU protection;
- idempotency;
- reconciliation after unknown outcomes;
- leader/lease ownership;
- partial failure;
- state-machine recovery.

### Reliability engineering

- empirical SLO capacity envelopes;
- tail-latency-aware safety;
- disruption budgets;
- N-1 recovery feasibility;
- progressive rollout;
- compensation and recovery-time measurement;
- fail-safe degraded modes.

### Performance/measurement

- reproducible benchmarks;
- realistic HPA-like baseline;
- small-instance optimality gap;
- simulator calibration;
- statistical reporting.

## Resume bullet templates

Use only measured numbers from a tagged benchmark release.

### Systems version

> Built FleetRight, a C++20/Python SLO-constrained capacity controller that jointly optimized rightsizing, replica counts, placement, and node consolidation using empirically calibrated service-capacity profiles and an independent safety verifier; reduced median compute cost by **X%** versus an HPA-like reactive baseline while maintaining **Y** latency/error target across **N** predefined scenarios.

### Reliability version

> Designed a crash-safe reconciliation control plane with immutable snapshots, stale-plan rejection, progressive disruption, PDB/topology-aware Kubernetes execution, and compensating recovery; passed **N** injected failure cases including controller crash after unknown action outcome and node loss during consolidation.

### Algorithms/performance version

> Implemented deterministic multi-resource placement and bounded local search for **N workloads / M nodes** with p95 solve time of **T ms**; measured a median **G%** optimality gap against an exact small-instance oracle.

## Best demo narrative

1. show an overprovisioned but safe cluster;
2. show measured capacity profile for a service;
3. run FleetRight reactive planning;
4. inspect why the verifier accepts 8 -> 7 but rejects 8 -> 6;
5. execute one progressive step;
6. show lower cost and unchanged SLO;
7. inject traffic spike and show recovery/scale-out;
8. trigger a deliberately harmful test plan and show compensation;
9. kill the controller after an external action but before acknowledgement;
10. restart and show reconciliation without duplicate side effect;
11. display benchmark table against HPA-like baseline and offline oracle.

## Interview questions the project must support

- Why is CPU utilization insufficient to prove latency safety?
- How did you derive the service-capacity envelope?
- Why do you use demand UCB instead of expected demand?
- Why can predictive scale-out be safer than predictive scale-in?
- What is the planner/verifier trust boundary?
- How do you prevent stale-plan execution?
- What if Kubernetes applied an action but your process never received the response?
- Why is “rollback” an inaccurate word for removed capacity?
- How do PDBs/topology spread affect consolidation?
- How did you validate simulator assumptions?
- How close is your heuristic to the exact optimum?
- What scenario makes FleetRight worse than the baseline?
- Which guarantees exist only under your model, and which were tested end to end?

## Evidence rule

The strongest portfolio signal is not more technologies. It is showing that each claim is tied to a reproducible experiment, declared assumptions, and a failure case that could falsify it.
