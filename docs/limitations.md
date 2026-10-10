# Current limitations

- Current-host Docker calibration generated three hashed profiles at one resource allocation and
  one concurrency-two load point per service, with three 60-second repeats. This provides a narrow
  measured envelope; it does not locate a saturation boundary or establish behavior at other
  resource shapes. Hosts without an accessible Docker daemon receive a BLOCKED run manifest.
- The deterministic simulator models nonlinear saturation and startup/actuation delays, but the
  model-to-Docker latency error and cold-start prediction error were not numerically calibrated in
  this milestone. It remains a comparison model, not evidence for production capacity.
- Offline lookahead uses 55% of profiled capacity under the current convex simulator model. It still
  misses one memory-api burst window because the simulator applies that trace's memory-pressure
  penalty independently of replica count; treat that case as replica-only infeasibility, not a safe
  reference plan.
- The optimizer uses deterministic greedy multidimensional placement, a bounded single-replica
  neighborhood, and a bounded portfolio of single-profile alternatives. It does not provide a
  general global optimum or evaluate every cross-workload profile combination; the exact oracle is
  limited to small, single-workload, non-N-1 cases.
- C++ planner/verifier code is single-threaded. TSan is not applicable until concurrency is added.
- The fixed-horizon objective supports movement, churn, uncertainty, and capacity-boundary costs,
  but the checked-in coefficient defaults are synthetic. Operators must calibrate policy weights
  against their own economic and operational costs before using them outside this benchmark.
- Capacity characterization currently covers the three checked-in workload shapes at one
  resource allocation per service. Additional allocations require new Docker measurements.
- Python protobuf generated runtime bindings and Hypothesis are not installed in this offline host;
  Python boundary encoding uses `protoc`, and property checks use a fixed-seed standard-library
  corpus. Ruff lint is in CI. Ruff format is not a CI gate; repository-wide formatting still flags
  pre-existing unformatted lines in `python/fleetright/experiment.py`, outside the current change.
  Mypy was evaluated locally but reports unresolved dynamic JSON and
  protobuf-contract typing in existing modules; the focused Prompt 2A check also reports dynamic
  JSONB/dict-row typing in control-plane paths. No `mypy`/`pyright` CI gate is configured, so this
  is recorded as a limitation rather than a passing type check.

Prompt 2A adds baseline forecasting, a PostgreSQL-backed plan lifecycle, and a local authenticated
control API. Forecast coverage is evaluated on historical test windows only; no production workload
history has been supplied for model calibration. The control API uses a single cluster-wide
workload/node lease set, so parallel planning for independent workloads is not supported yet.

Prompt 2B exposes live Docker Compose execution and a Kubernetes `FleetRightWorkload` target behind
explicit `EXECUTE` configuration. The Docker fence is host-local, and the checked-in Compose
services publish fixed host ports, so those services remain single-replica. The adapter allows
multiple replicas only after `docker compose config` confirms the selected service has no published
host ports; an isolated portless fixture exercises 3→4 scaling. On Kubernetes, only monotonic
scale-up on a new `cpu-api` object marked `monotonic-scale-up-v1` is admitted; direct Deployment
execution and scale-down are disabled. Kubernetes execution requires an explicit namespace-scoped
kubeconfig and narrowly scoped RBAC.

The current session's dedicated safety-lab API access is blocked because its Kubernetes v1.37.0
certificate is not yet valid at the session time; no TLS verification bypass or cluster mutation was
attempted. The saved live negative control from 2026-10-08 demonstrates that an unready same-UID Pod
under `IfHealthyBudget` can be evicted after its resourceVersion changes even when the PDB reports
zero allowed disruptions. PostgreSQL leases, a Pod UID, and a resourceVersion check do not fence
that separate final mutation. In the healthy-Pod path, Kubernetes can update PDB disruption
bookkeeping before rejecting a stale same-name request on UID mismatch; in the unready-Pod path, it
can skip that bookkeeping. A PDB status observation therefore cannot prove that old requests are
fenced. A request already accepted by PDB admission can also delete its source after a later
readiness regression. No eviction or cordon mutator exists in the current target.

An irrevocable exact-UID retirement could tolerate delayed duplicate Evictions in principle only if
all states during `OUTCOME_UNKNOWN` preserve the availability floor, capacity, topology, and allowed
failure reserve. The current verifier/runtime does not establish those conditions. More directly,
FleetRightWorkload reconciliation normally recreates an evicted ordinal while that ordinal remains
below desired replicas, using the immutable old allowed-node set; a stale in-flight CREATE can do the
same after the source UID is observed absent. The replacement may return to the source node. This
prevents the current V2 source-exclusion and node-drain postconditions.

The V2 inventory verifier binds source Pods to a Deployment and one active ReplicaSet UID, while the
current execution target uses FleetRightWorkload-owned Pods. The existing V2 contract cannot bind to
that target. The FleetRightWorkload PDB also uses `replicas - availabilityFloor` for `maxUnavailable`;
it does not encode the profile-derived minimum replicas or N-1 reserve required by V2. Keep staged
replacement, Eviction, and node drain disabled.

V2 `MOVE_REPLICA`, `DEACTIVATE_NODE`, and resource resizing are rejected during planning admission
and again before execution-journal creation. Durable staged replacement, PDB-safe eviction,
partial-drain recovery, and live compensation are not implemented. The V2 contract and independent
C++ verifier cover bounded placement/resource/topology/N-1 outcomes, but V2 plans remain inadmissible
for execution because no target enforces the required operation authority and staged mutation
protocol. The complete authenticated API-to-live-Kubernetes-workload takeover path is also not yet
qualified. Docker process-crash takeover is tested only for one scale-up boundary; its target timeout
and compensation paths remain unqualified.
The local Deployment recipe is a health fixture, not a current EXECUTE target; there is no checked-in
sample `FleetRightWorkload` resource or turnkey live setup recipe yet.
See [ADR-008](adr/008-staged-evacuation-execution.md).

The separate V2 contract and independent C++ verifier prove a bounded
constraint-based evacuation for one source Pod UID. A verified V2 plan remains
inadmissible for execution: the current adapter does not enforce the approved
node set and hard anti-affinity, provide an atomic UID/PDB eviction fence,
recheck owner identities, or reconcile the staged protocol durably. No live
evacuation is supported. V2 enumerates up to eight approved nodes and rejects
larger outcome sets rather than sampling them. Its bounded N-1 proof supports
one N-1 workload per plan and checks serving availability at each modeled
stage plus every recovery insertion order using an explicit node-label failure
domain. See
[ADR-008](adr/008-staged-evacuation-execution.md) for the stage protocol and
adapter requirements. The API runs as a loopback-bound local process and
invokes the checked-out planner/verifier binaries.
