# FleetRight Phase 1 measurement and planning demo

Run: `20261005T151113.299621Z-seed-42` (seed `42`)

This reproducible integration run uses explicitly synthetic CapacityProfile fixtures. It does not establish production capacity; Docker measurements are retained in separate timestamped characterization runs.

Planner feasibility: **True**; independent verifier: **True**.
Plan cost: 500000 -> 250000 micros/hour; actions: 14.
Fixed-horizon objective: 1530000 -> 340520 micros, including movement, churn, uncertainty, and capacity-boundary costs.

## Workload decisions

| Workload | Replicas | Effective QPS/replica (milli) | Profile |
|---|---:|---:|---|
| cpu-api | 4 -> 3 | 465500 | `cpu-api-500m-512-v1` |
| queued-api | 3 -> 2 | 279300 | `queued-api-750m-512-v1` |
| memory-api | 3 -> 2 | 372400 | `memory-api-500m-768-v1` |

## Exact bounded oracle

Exact objective: 100000 micros over its one-hour horizon; heuristic: 100000; gap: 0.000000%; exhaustive combinations examined: 3.

## Baseline limitations

All four baseline controllers received identical per-workload traces and modeled actuation/startup delays. Simulator cost is a replica-cost proxy and is reported separately from the node-cost optimizer objective; these figures are not presented as a head-to-head infrastructure savings comparison.
Raw trace and window data are retained in `traces/` and `raw_metrics/`; protobuf inputs and outputs are under `policy_outputs/`.
