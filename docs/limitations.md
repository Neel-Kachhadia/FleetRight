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

Prompt 2B adds live Docker Compose and Kubernetes Deployment replica scaling behind explicit
`EXECUTE` configuration. The Docker fence is host-local and Docker Compose's fixed published ports
limit the checked-in service harness to one replica per service. Kubernetes execution must use an
explicit namespace-scoped kubeconfig. The dedicated `fleetright-safety-lab` kind cluster and its
three nodes were healthy at preflight, but no evacuation action was run: the v1.37.0 Eviction API
does not preserve the supplied Pod resourceVersion precondition on every applicable unhealthy-Pod
PDB branch, so a stale controller is not fenced at the final mutation. The safety-lab has no
`fleetright` namespace or installed workload/RBAC; Kubernetes readiness, topology, PDB admission,
ServiceAccount identity, and RBAC were not exercised against this cluster during this focused
session. The earlier Prompt 2B cluster and adapter checks recorded below in
[the specialist review log](specialist-review-log.md) were run on a different disposable cluster
and did not validate durable evacuation. The documented optimize-to-approve-to-execute API runbook
was not exercised end-to-end; API idempotency was tested with a fake target, and the real Docker
adapter was exercised separately.

Only immutable replica-count actions are executable. A Deployment scale action does not identify a
Pod or use the Eviction API; it cannot implement a verified per-Pod move. `MOVE_REPLICA`, node
deactivation/drain, and resource resizing are rejected during planning admission and rechecked at
approval before execution-journal creation. Although the Kubernetes target code has PDB-aware
eviction helpers, eviction and node drain are not wired to the durable reconciler, so PDB-blocked
retry and partial-drain recovery are not supported. SHADOW produces recommendations without a
separate simulated reconciliation pass. The exact-destination evacuation proof cannot be faithfully
carried out through the current Deployment scaling interface; see
[ADR-008](adr/008-staged-evacuation-execution.md). The API runs as a loopback-bound local process
and invokes the checked-out planner/verifier binaries.
