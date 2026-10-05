# Prompt 1 implementation

The canonical domain and optimizer boundary is Protocol Buffers v1 under `proto/fleetright/v1/`.
C++20 owns unit-safe resource arithmetic, deterministic planning, placement, and an independently
implemented verifier. Python 3.11+ owns trace generation, histogram/profile statistics, workload
experiments, simulation, baseline policies, and artifact orchestration. Python uses the installed
`protoc` executable to encode/decode protobuf artifacts, so it does not require the Python protobuf
runtime.

Snapshots, capacity profiles, and machine catalogs are content-hashed. Planning uses the snapshot's
fixed evaluation time; it does not read the wall clock. Snapshot verification rejects unknown root
fields before validating the canonical final hash field. Node prices and allocatable resources must
match the checked-in synthetic catalog. Capacity evidence is tied to image, request mix, resource
allocation, runtime class, platform, version, and validity interval. Synthetic fixtures are marked
and accepted only when the policy explicitly enables them.

The planner uses sorted workload/replica identifiers and stable node tie-breaks. Its bounded
single-replica local search has a deterministic size-scaled iteration cap and rejects any candidate
that fails hard placement checks. For N-1 workloads it simulates each retained failure domain and
keeps idle recovery nodes active when the recovery placement needs them. The verifier separately
recomputes snapshot binding, empirical profile bounds, node capacity, topology, action limits,
evacuation intermediates, N-1 recovery, and catalog cost. It does not call planner feasibility code.

The ranking score is integer micros over `planning_horizon_seconds`: active-node micros/hour are
prorated over the horizon, then one-time moved-replica and action-churn charges are added, along
with policy-weighted profile-uncertainty and effective-capacity-boundary exposure. Uncertainty is
the complement of the lowest profile/factor confidence. Boundary risk is the larger of QPS or
concurrency demand divided by aggregate effective per-replica capacity. Those scores rank plans
after hard safety checks; they never relax an SLO or feasibility condition. The existing cluster's
score uses fresh matching profiles for its current resource shapes and assigns full uncertainty and
boundary exposure when such evidence is absent. If the current state fails a hard check, a
safety-restoring plan may bypass the minimum cost-improvement threshold. Otherwise that threshold
is independently recomputed by the verifier. All objective multiplications, horizon conversion,
and additions use checked wide-integer arithmetic.

The simulator is deterministic and event-driven at trace-window granularity. It models nonlinear
near-saturation latency, queue growth, rejection/error response, readiness, startup, actuation,
node activation, eviction/restart, and accumulated replica-cost proxies. It is a model, not a
measurement; its cost proxy is not directly comparable to node-hour optimizer cost.

No Kubernetes API, persistent control plane, prediction service, or mutation executor is included.
Container calibration emits a blocked evidence run when the Docker daemon is inaccessible. The
current-host run contains hashed profiles for the three checked-in workloads at one resource
allocation and one safe concurrency point; it does not calibrate the simulator's startup or
saturation error. An unconstrained host process never promotes a resource-shape profile.
