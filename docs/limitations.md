# Current limitations

## v1.0.0 limitations (authoritative; supersedes any contradictory milestone text below)

Evidence and claims
- Policy-comparison results are **simulated**: synthetic traces, the declared convex latency model
  (never calibrated against Docker latency), and a synthetic cost model (`cost-model-v1`). They say
  nothing about real traffic or cloud bills. No production deployment exists.
- The tuning set shares trace *families* with four evaluation scenarios (disjoint seeds and load);
  hold-out is seed-level, not family-level. A first evaluation run was inspected before two fairness
  additions (see `docs/benchmark-methodology.md`).
- n = 20 paired seeds per scenario supports large effects only; steady and flash-crowd families are
  nearly deterministic across seeds, so their intervals say little about sampling variability.
- Latency is the simulator's per-window p99; there is no per-request latency distribution.

Where FleetRight loses or misses a target (see `docs/RELEASE_REPORT.md`)
- Versus the tuned HPA-like baseline FleetRight was more expensive in S05, S09, S13, S14 and traded
  extra SLO-violation windows for lower cost in most other scenarios. It is not shown to dominate.
- Predictive FleetRight was never better than reactive (ablation at the reactive policy's knob: 0 better, 5 costlier with no SLO benefit, 8 trade-offs,
  2 no difference; with its own tuned knob: 0 better, 5 worse, 10 trade-offs). With the bundled models the 95% bound is wide (mean overhead 0.7-1.1) and the seasonal model
  cannot activate at a 60 s cadence (needs 48 distinct hours).
- All online policies, FleetRight included, take hard-SLO violations on a 5x single-window flash crowd;
  FleetRight reactive also on slow-start (S08), bursty multi-tenant (S09) and correlated surges (S11);
  predictive also on S05 and S15. FleetRight has no online recalibration for silent capacity drift
  (S15: 71 soft violation windows/seed for reactive versus 1.4 for HPA-like).
- The C++ planner sizes replicas at 100% of profile-effective capacity. The published FleetRight numbers
  use a tuned 60-70% utilization target that the planner does not expose; the shipped sizing alone had
  154 hard + 3,611 soft violation windows of 14,400 on the tuning set. Operators obtain headroom only via
  the effective-capacity factors.
- With an unhealthy in-use node (S16) the planner returns infeasible instead of an emergency repair plan.
- Optimizer: the greedy + bounded local search is not globally optimal. After the Prompt 3 staging rule
  (no move onto a node that currently hosts a sibling replica) it was optimal in 23/30 small oracle
  instances with a non-binding move budget (mean gap +7.6%, max +40%). The exact oracle covers one
  workload, no N-1, at most 12 nodes. The planner and verifier are single-threaded; TSan is not applicable.
- Verifier latency margin is 17% (20.7 ms p95 vs 25 ms); a loaded host measured 26.0 ms. The planner
  margin is 21% (196.9 vs 250 ms).

Execution (Kubernetes) - scope and unproven areas
- Supported: `SCALE_UP`, `MOVE_REPLICA`, `EVICT_SOURCE`, `DRAIN_NODE` (2-4 Pods), `COMPENSATE` on the
  `FleetRightWorkload` target, one Kind lab. Disabled: `SCALE_DOWN`. Unsupported: `DEACTIVATE_NODE`,
  resource resizing, Deployments.
- Not proven live: a real Kubernetes API outage; the three verification-deadline outcomes (10-minute
  waits); an unsupported Pod owner during a live drain; the `get secrets` denial probe (needs an executor
  kubeconfig); the 6-node compensation-with-capacity case in this phase. PostgreSQL connection loss is
  proven deterministically only.
- Compensation needs a free destination; otherwise `FAILED_SAFE` leaves a Pending recovery Pod and a raised
  desired count for an operator. A PDB-blocked retirement waits without a deadline. FleetRight never
  uncordons a Node. Deadlines mix controller wall-clock with PostgreSQL timestamps.
- The control API is a loopback-bound local process with static bearer tokens, no TLS, no rate limiting.
  It is not hardened for network exposure.
- The live suites share one lab and drained Nodes stay cordoned; reset the lab between modules.

## Historical limitations by milestone (kept for the record)

Statements below were true when written. Where they say eviction, drain, compensation or staged
replacement is unimplemented, Milestone 4 superseded them (`docs/V2_EXECUTION_PROGRESS.md`, 2026-10-13).

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

The 2026-10-10 live Kind qualification passed authenticated API-to-PostgreSQL-to-Kubernetes
scale-up and process takeover. A read on 2026-10-11 found all three lab Nodes Ready, all system
Pods Running/Ready, and the existing `cpu-api` workload at 4/4 Ready. The opt-in scale-up test
requires a 3/3 baseline and was not rerun. The saved live negative control from
2026-10-08 demonstrates that an unready same-UID Pod
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

As of 2026-10-11, V2 owner contract version 1 explicitly binds
`FleetRightWorkload` kind/name/UID/resourceVersion/generation, controller type/protocol, and Pod
owner/logical-slot identity. The API, C++ planner, and independent verifier reject legacy
Deployment/ReplicaSet owner fields. This resolves the inventory owner-shape mismatch for planning;
it does not establish destructive execution authority. The FleetRightWorkload PDB still uses
`replicas - availabilityFloor` for `maxUnavailable`; it does not encode the profile-derived minimum
replicas or N-1 reserve required by V2. The 2026-10-11 worktree journals new Pod CREATE intents in
workload status, retains unresolved outcomes, and blocks scale-authority updates while an intent is
pending. The additive CRD rule is installed in the isolated lab and a live server-side update probe
confirmed it rejects spec changes while the marker exists; the new controller code itself was not
deployed. This does not cancel a pre-upgrade or already admitted CREATE and does not add a
retired-slot tombstone; the delayed-CREATE regression for a removed ordinal remains a safety
failure. An unresolved marker can block reconciliation indefinitely. Keep staged replacement,
Eviction, and node drain disabled.

V2 `MOVE_REPLICA`, `DEACTIVATE_NODE`, and resource resizing are rejected during planning admission
and again before execution-journal creation. Durable staged replacement, PDB-safe eviction,
partial-drain recovery, and live compensation are not implemented. The V2 contract and independent
C++ verifier cover bounded placement/resource/topology/N-1 outcomes, but V2 plans remain inadmissible
for execution because no target enforces the required operation authority and staged mutation
protocol. The authenticated API-to-live-Kubernetes scale-up and takeover path passed on 2026-10-10;
it does not qualify destructive operations. Docker process-crash takeover and target timeout after
a real scale-up are tested, but only for monotonic scale-up; destructive-operation recovery and
compensation remain unqualified.
The local Deployment recipe is a health fixture, not a current EXECUTE target; there is no checked-in
sample `FleetRightWorkload` resource or turnkey live setup recipe yet.
See [ADR-008](adr/008-staged-evacuation-execution.md).

The separate V2 contract and independent C++ verifier prove a bounded
constraint-based evacuation for one source Pod UID. A verified V2 plan remains
inadmissible for execution: the current target does not enforce approved
placement through a destructive mutation protocol, provide an atomic UID/PDB
eviction fence, or reconcile staged actions durably. Owner identities are bound
in the plan but are not revalidated by a destructive target because that target
does not exist. No live evacuation is supported. V2 enumerates up to eight approved nodes and rejects
larger outcome sets rather than sampling them. Its bounded N-1 proof supports
one N-1 workload per plan and checks serving availability at each modeled
stage plus every recovery insertion order using an explicit node-label failure
domain. See
[ADR-008](adr/008-staged-evacuation-execution.md) for the stage protocol and
adapter requirements. The API runs as a loopback-bound local process and
invokes the checked-out planner/verifier binaries.

## Prompt 2B operational notes (2026-10-13)

- FleetRight never uncordons a Node. A completed or failed drain leaves the Node cordoned for the
  operator; only `plan-id`, `operation-id` and `cordon-owned` are released, and
  `fleetright.io/fencing-token` remains as an inert monotonic high-water mark.
- A PDB-blocked retirement waits without a deadline (it performs no destructive action) until the
  budget opens or an operator cancels the plan.
- Failed compensation (`FAILED_SAFE`) leaves the unplaceable recovery Pod `Pending` and the
  workload's desired replica count raised; an operator must resolve capacity or delete the Pod.
- Deadlines use controller wall-clock against PostgreSQL timestamps; clock skew shifts them.
- Drain completion remains a point-in-time inventory read.
