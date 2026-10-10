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

The Prompt 1 milestone had no Kubernetes API, persistent control plane, prediction service, or
mutation executor. Prompt 2A adds the local forecast and durable control-plane foundation described
below; it still has no infrastructure mutation executor.
Container calibration emits a blocked evidence run when the Docker daemon is inaccessible. The
current-host run contains hashed profiles for the three checked-in workloads at one resource
allocation and one safe concurrency point; it does not calibrate the simulator's startup or
saturation error. An unconstrained host process never promotes a resource-shape profile.

## Prompt 2A predictive control foundation

`python/fleetright/forecast.py` defines version-1 `DemandForecast` values. Demand uses milli-QPS
and integer concurrent requests; all timestamps and horizons use milliseconds. Each forecast
retains its model name/version, training window bounds/hash, expected demand, calibrated upper
bounds, confidence, MAE/WAPE, empirical bound coverage, underprediction severity, and a reserve
cost proxy.

The rolling quantile model takes the median of up to 60 recent observations. EWMA/trend uses
alpha 0.3 by default and only projects positive trend. The seasonal model uses the median of prior
observations in the matching UTC hour, aggregates multiple samples within each day, and needs 48
distinct UTC-hour buckets plus two prior matching daily cycles. Temporal evaluation keeps order:
the first 60% forms the initial training history,
the next 20% calibrates one-sided residual margins, and the last 20% is an untouched chronological
holdout. Each holdout prediction sees only earlier observations. Finite-sample quantiles use the
`ceil((n+1)*coverage)` order statistic. The reported confidence is measured joint QPS/concurrency
coverage on the holdout; a forecast below its requested coverage is disabled for that workload.
Walk-forward calibration uses the requested forecast horizon and chooses only origins at least that
far before each target; standalone evaluation defaults to the median sample cadence.
This is empirical calibration on the supplied history, not a production guarantee. The test
fixture's constant series had 0 MAE/WAPE and 100% joint coverage; a later shift intentionally
undercovers and falls back to reactive planning.

History without a usable sample window, a stale last observation, model exception, invalid model
output, low measured confidence, or undercoverage records a fallback reason and preserves the
snapshot's observed demand. By default, the latest observation may be at most the greater of
60 seconds and twice the median sample cadence old. Forecast upper bounds may raise the demand
passed to the existing C++ planner over startup + readiness stabilization + scheduling buffer.
The independent C++ verifier still checks the resulting plan.

Scale-out may be prepared from the forecast upper bound. Scale-in requires fresh observed samples,
stabilization duration, minimum sample coverage, a valid profile matching the current resources,
no recent bad-health signal, and recovery reserve. A plan containing any replica/resource reduction
or node deactivation without qualifying evidence is persisted as `REJECTED`, so it cannot be
approved. The forecast never substitutes for those observations.

The control API stores plans, state transitions, action intents, artifact bytes, idempotency rows,
leases, and audits in PostgreSQL. `migrations/001_control_plane.sql` creates the schema; apply it
explicitly:

```sh
PYTHONPATH=python FLEETRIGHT_DATABASE_URL="$FLEETRIGHT_DATABASE_URL" \
  .venv/bin/python -m fleetright.control migrate
```

Content-addressed artifact bytes are stored in PostgreSQL and checked against SHA-256 when loaded.
A hash alone is not treated as replay content. Plan records bind the source snapshot ID/generation,
policy/profile/catalog hashes, forecast hashes, planner/verifier build IDs, verifier result hash,
and expiry. The durable plan expires at the earliest of its native snapshot plan, forecast, and
referenced capacity-profile validity deadlines. Approval checks the current binding and expiry
using the database clock, reloads and hashes replay artifacts, reruns the independent verifier,
then records `APPROVED` and acquires conflict leases in the same transaction.

The lifecycle is `PROPOSED -> VERIFIED -> APPROVED`, with declared transitions to `PAUSED`,
`STALE`, `REJECTED`, or `CANCELLED`; invalid transitions fail closed. Mutating routes require
`Idempotency-Key`. PostgreSQL uniqueness and row locking make a repeated principal/operation/key
return the stored result and reject a changed request. Workload and node ownership uses PostgreSQL
leases with database-clock expiry and monotonically increasing fencing tokens. Prompt 2A uses one
cluster-wide workload/node lease set, which serializes planning across otherwise independent
workloads.

The roles are `viewer` (read), `recommender` (optimize), and `operator`/`admin` (approve, pause,
cancel). Development credentials are supplied through `FLEETRIGHT_TOKENS_JSON`; the repository
contains no credential values. Keep that variable and the PostgreSQL password in a local secret
store or process environment. Run Compose with `--profile control-plane` to start loopback-only
PostgreSQL, then run the API locally:

```sh
PYTHONPATH=python FLEETRIGHT_DATABASE_URL="$FLEETRIGHT_DATABASE_URL" \
  FLEETRIGHT_TOKENS_JSON="$FLEETRIGHT_TOKENS_JSON" \
  .venv/bin/uvicorn fleetright.api:app --host 127.0.0.1 --port 8080
```

The API is a local process because it invokes the checked-out planner/verifier binaries; Compose
manages only its PostgreSQL dependency in this profile.

Read routes are `/healthz`, `/readyz`, `/v1/state`, `/v1/plans`, `/v1/plans/{id}`, and
`/v1/audit`. Mutating routes are `/v1/optimize`, `/v1/plans/{id}/approve`, `/pause`, and `/cancel`.
OBSERVE stores snapshot/control-state provenance without planning; RECOMMEND and SHADOW persist
recommendations. Prompt 2B adds the explicitly configured live execution path below.

## Prompt 2B safe execution

Fresh installs still default to `RECOMMEND`. Live execution requires both
`FLEETRIGHT_MODE=EXECUTE` and `FLEETRIGHT_EXECUTION_TARGET=docker` or
`kubernetes-workload`. The
authenticated operator-only `POST /v1/plans/{id}/execute` replays the approved artifacts through
the independent verifier, checks the current plan binding and expiry, checks supported action
types, records a PostgreSQL execution claim and returns `202`. An identical principal/key/body
replays the same response. The API starts a bounded background reconciliation loop; it does not
wait for target convergence in the HTTP request.

The durable plan states include `EXECUTING`, `VERIFYING`, `COMMITTED`, `COMPENSATING`,
`COMPENSATED`, and `FAILED_SAFE`. Each action records a sequence, action type, immutable approved
payload, attempt number, target observations, dispatch intent, target fencing token, error, and
timestamps. `/v1/plans/{id}` includes the execution owner/fence/lease and per-action execution
state. Controller leases and workload conflict leases use PostgreSQL time and monotonically
increasing fencing tokens.

Before a replica change the controller rereads the configured target, verifies the approved replica
count and target identity/generation, rechecks plan expiry and snapshot/policy binding, verifies its
leases, then commits the dispatch intent. After a crash or timeout, a
persisted dispatch with no recorded response is treated as unknown. A new controller fences the
actual target before it takes over; the active owner observes the target before deciding whether an
unknown operation already applied or may be retried. Retries are limited to two attempts and occur
only after observation proves the prior requested count was not applied. A known successful response
followed by an external revert fails safe without redispatch. Targets that admit scale-down change
one replica per step, wait for observed readiness, then require a fresh health window before
continuing; the Kubernetes workload target rejects scale-down. The first sample after controller
restart cannot authorize scale-down. A fresh latency, error, or queue
regression after scale-down creates a durable compensation action restoring the pre-action replica
count; `COMPENSATED` is written only after capacity and a fresh healthy window are observed. A
scale-out health regression leaves the added capacity in place and ends `FAILED_SAFE`. Known target
state conflicts fail the action and release its leases; an unknown outcome remains active until
observation is available. Pause and cancel stop later dispatches; cancellation does not undo
completed changes. If an action has an unknown outcome, the controller first yields ownership and
publishes a durable target fence. It then records an already-applied step as complete (or failed if
it stopped partway through a multi-step reduction), or records a proven unapplied step as failed,
before transitioning to `PAUSED` or `CANCELLED`.

Execution health thresholds are bounded by the checked-in local workload contracts: CPU 120 ms,
memory 150 ms, queued 80 ms p99, and 1,000 ppm errors. A supplied snapshot may request stricter
limits but cannot relax these ceilings. Queue growth must be nonpositive.

The Docker adapter uses `docker compose up --scale` with absolute replica targets and a private
host-local `flock` file for controller fencing. It uses Docker healthcheck status as readiness when
the service defines a healthcheck, otherwise it counts running containers. Before scaling above one
replica, it checks that `docker compose config` reports no published host ports. The checked-in
Compose services publish fixed ports and remain single-replica; the isolated portless fixture covers
multi-replica execution. Docker Engine does not provide the Deployment-style resource-version CAS
used by Kubernetes. Keep this adapter on one host and do not allow other Docker clients to mutate the
same Compose project while FleetRight executes.

The Kubernetes target supports only `SET_REPLICA_COUNT` for the checked-in `cpu-api`
`FleetRightWorkload`, and only for objects created with `monotonic-scale-up-v1`. It reads workload
UID, generation, resourceVersion, desired/observed/ready/available counts, and the durable execution
request/status. One resourceVersion-guarded Merge Patch binds plan ID, action ID, generation, and
expected/target replica counts to the new desired count. The executor Role permits GET/PATCH on that
named custom resource only; the separate workload-controller Role creates Pods but cannot delete
Pods, submit Evictions, patch workload spec, or mutate Nodes. Direct Kubernetes Deployment execution
is disabled. Kubernetes EXECUTE requires an explicit mode-0600 kubeconfig and
`FLEETRIGHT_KUBERNETES_NAMESPACE=fleetright-v2`.

The workload controller reports `APPLIED` after all desired deterministic Pod ordinals exist and
pass identity/template/placement validation; this does not imply readiness. The API reconciler
separately requires matching operation identity, observed generation/count, Ready/Available counts,
and a fresh healthy workload-metrics window before it completes the action. A later operation also
requires the prior operation to be `APPLIED`, the current availability floor to be met, and the
workload to report `Reconciled=True`.

Earlier authenticated API and direct-adapter evidence used a Kubernetes Deployment target on a
different disposable kind cluster. That target is disabled at this HEAD, so those results do not
qualify the current `FleetRightWorkload` target. The current API approval/journal/takeover flow has
not yet been run end-to-end against a live `FleetRightWorkload` controller under crash and lost
response.

At this HEAD, the API rejects `MOVE_REPLICA`, `DEACTIVATE_NODE`, and resource resizing before
claiming execution. No Pod eviction or Node-cordon mutator is present in the current target, and the
RBAC manifests grant neither permission. Durable PDB-blocked execution, staged replacement, bounded
node drain, partial-drain restart recovery, and live compensation therefore remain unimplemented.
The saved Kubernetes 1.37.0 negative control shows that an unready same-UID Pod can be evicted after
the caller's resourceVersion changes, so Pod UID and the workload CAS are not a final-mutation fence.
The PDB does not govern the supported scale-up operation.

## V2 workload model: Phase 1

`deploy/kubernetes/fleetrightworkload-crd.yaml` defines the namespaced
`fleetright.io/v1alpha1` `FleetRightWorkload` CRD. Its scale subresource maps
desired replicas to `.spec.replicas`, observed replicas to `.status.replicas`, and
the PDB selector to the serialized `.status.labelSelector`. The controller in
`python/fleetright/workload_controller.py` owns deterministic logical replica
Pods through `FleetRightWorkload` owner references and reconstructs them from API
state on every reconcile. Conflicting owner/replica identity or a changed Pod
template blocks reconciliation; no orphan replacement is adopted.
The controller canonicalizes only order-insensitive map lists (`allowedNodes`,
`containers`, and `ports`) in its template fingerprint. The CRD keeps `env`
atomic and the fingerprint preserves its order because environment expansion
can depend on earlier entries.

Ordinary reconciliation creates missing deterministic ordinals up to the
desired replica high-water mark. It does not delete Pods. If live count exceeds
desired, it keeps the Pods and reports `Reconciled=False` with
`ScaleDownRequiresStagedExecution`. A terminal Pod keeps its logical identity
reserved and reports `TerminalPodRequiresStagedExecution`; terminal replacement
also needs a staged protocol. A workload with `deletionTimestamp` is ignored so
its owner deletion can remove dependents without the controller recreating
them. `surgeBudget` is reserved for a future staged-replacement protocol and
ordinary reconciliation never creates over-desired Pods.

The workload records allowed Node `{name, uid}` identities. Pods use required
node affinity on `metadata.name` so the Kubernetes scheduler enforces the allowed
name set. Each reconcile fetches one Node inventory and requires every allowed
name to retain its recorded UID before creating Pods; later reconciles check
scheduled Pod nodes against a fresh inventory. A same-name Node replacement
between an inventory read and scheduler binding can race detection until the next
reconcile. Kubernetes cannot match Node UID in native Pod affinity, so the
controller detects a same-name Node replacement when it next observes the
scheduled Pod but cannot claim atomic Node UID placement. The monotonic
scale-up protocol enforces allowed Node names and checks the current Node UID
before and after scheduling; exact UID placement remains unsupported. Optional
topology spread uses `DoNotSchedule`; optional Pod anti-affinity
uses `requiredDuringSchedulingIgnoredDuringExecution`. No soft affinity or
custom scheduler is used.

The `fleetright-v2` namespace enforces, audits, and warns at the Kubernetes
`restricted` Pod Security level. Generated workload Pods set
`runAsNonRoot: true`, default to UID/GID `10001`, and use the `RuntimeDefault`
seccomp profile. A workload may request a different positive UID/GID or
`fsGroup`; UID/GID zero and a user supplied `runAsNonRoot` override are rejected.
Every generated container sets `allowPrivilegeEscalation: false` and drops all
capabilities. The `Ready` condition reports Kubernetes Pod readiness only,
including the configured container readiness probe; it is not a separate
application health check.

The controller runs only in the isolated `fleetright-v2` namespace. Its namespaced
Role can get/list/create Pods and manage PDBs there; a separate cluster role
grants read-only access to Nodes. The `fleetright` API executor account can
only get/patch the named `cpu-api` FleetRightWorkload and cannot mutate Pods,
Evictions, Deployments, PDBs, or Nodes. The controller keeps an owner-matched
PodDisruptionBudget whose selector matches the serialized scale selector and
whose integer `maxUnavailable` is capped by both the availability floor and
maximum concurrent disruptions. Status reports observed, ready, available,
and surge replicas plus the selector, reconciliation condition, and execution
identity/phase.
CPU and memory requests are sent to the scheduler. `networkMbps` accepts only
zero because this cluster has no network-bandwidth scheduling resource; no
network headroom is inferred. The controller's service account is separate from
the API executor and has no Deployment, Node mutation, Pod deletion, or Pod
Eviction permission. It is the only FleetRight principal that creates workload
Pods. Kubernetes EXECUTE uses `kubernetes-workload` and only newly created
objects marked `monotonic-scale-up-v1`; direct Kubernetes Deployment execution
is disabled. The independent verifier is unchanged.
An API 400, 403, or 422 while creating a generated Pod sets a sanitized
workload condition; transient request errors and failed status writes still
surface as controller errors.

The Docker API E2E passes on a separate portless Compose fixture: a verified 3→4 plan is authenticated,
approved, executed, observed ready across all four Docker healthchecks, and committed with a fresh
workload SLO window. It kills a spawned controller process after the real scale but before the target
response returns, leaving the journal `RUNNING`. After lease expiry, a second process commits from
observed state without redispatch; the journal contains one dispatch. This covers one crash boundary.
Docker target timeout, health-regression compensation, and compensation recovery remain unverified.
The checked-in Compose services still publish fixed ports and support only one replica each. The
adapter fails closed on multiple replicas unless Compose configuration confirms there are no
published host ports.

Docker local operation commands:

```sh
export FLEETRIGHT_POSTGRES_PASSWORD="$(openssl rand -hex 24)"
docker compose --profile control-plane up -d control-postgres
export PGPASSWORD="$FLEETRIGHT_POSTGRES_PASSWORD"
export FLEETRIGHT_DATABASE_URL='postgresql://fleetright@127.0.0.1:55432/fleetright'
PYTHONPATH=python .venv/bin/python -m fleetright.control migrate
docker compose up -d --build cpu-service memory-service queued-service
export FLEETRIGHT_MODE=EXECUTE
export FLEETRIGHT_EXECUTION_TARGET=docker
export FLEETRIGHT_OPERATOR_BEARER_TOKEN="$(openssl rand -hex 24)"
export FLEETRIGHT_TOKENS_JSON="{\"${FLEETRIGHT_OPERATOR_BEARER_TOKEN}\":{\"principal\":\"operator\",\"role\":\"operator\"}}"
PYTHONPATH=python .venv/bin/uvicorn fleetright.api:app --host 127.0.0.1 --port 8080
```

Use an `optimize-request.json` that matches the `/v1/optimize` request schema, then approve and
execute the returned plan with the operator token already set above:

```sh
curl --fail-with-body -sS -H "Authorization: Bearer $FLEETRIGHT_OPERATOR_BEARER_TOKEN" \
  -H 'Idempotency-Key: optimize-local-1' -H 'Content-Type: application/json' \
  --data-binary @optimize-request.json http://127.0.0.1:8080/v1/optimize > plan.json
PLAN_ID=$(jq -r .plan_id plan.json)
curl --fail-with-body -sS -H "Authorization: Bearer $FLEETRIGHT_OPERATOR_BEARER_TOKEN" \
  -H 'Idempotency-Key: approve-local-1' -H 'Content-Type: application/json' \
  --data '{}' "http://127.0.0.1:8080/v1/plans/$PLAN_ID/approve"
curl --fail-with-body -sS -H "Authorization: Bearer $FLEETRIGHT_OPERATOR_BEARER_TOKEN" \
  -H 'Idempotency-Key: execute-local-1' -H 'Content-Type: application/json' \
  --data '{}' "http://127.0.0.1:8080/v1/plans/$PLAN_ID/execute"
curl --fail-with-body -sS -H "Authorization: Bearer $FLEETRIGHT_OPERATOR_BEARER_TOKEN" \
  "http://127.0.0.1:8080/v1/plans/$PLAN_ID"
```

The following Deployment setup is a legacy service-health fixture. It is not the current Kubernetes
execution target: `FLEETRIGHT_EXECUTION_TARGET=kubernetes` is disabled, and a Deployment in
`fleetright` cannot be used by the `FleetRightWorkload` target in `fleetright-v2`.

For a disposable local kind cluster, the legacy service-health fixture can be started with:

```sh
kind create cluster --config deploy/kubernetes/kind.yaml
docker build -t fleetright-workload:local -f workloads/Dockerfile .
kind load docker-image fleetright-workload:local
kubectl apply -f deploy/kubernetes/rbac.yaml -f deploy/kubernetes/workloads.yaml
kubectl -n fleetright rollout status deployment/cpu-api --timeout=120s
kubectl -n fleetright port-forward service/cpu-api 8081:8080
```

The current scale-up target additionally requires the CRD, the separate workload controller, and a
new `FleetRightWorkload` object created with `executionProtocol: monotonic-scale-up-v1` and current
allowed-node names and UIDs. The repository has no checked-in sample object or turnkey setup for
that target, so the commands above do not qualify its live execution path.

When that target has been provisioned in an isolated cluster, mint a short-lived ServiceAccount
token from the kind administrator context, write a private kubeconfig for the bounded namespace
Role, and start EXECUTE:

```sh
export FLEETRIGHT_KUBE_SERVER="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
export FLEETRIGHT_KUBE_CA_DATA="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')"
export FLEETRIGHT_SA_TOKEN="$(kubectl -n fleetright create token fleetright-controller --duration=10m)"
umask 077
cat > /tmp/fleetright-kubeconfig <<EOF
apiVersion: v1
kind: Config
clusters:
- name: fleetright-kind
  cluster:
    server: ${FLEETRIGHT_KUBE_SERVER}
    certificate-authority-data: ${FLEETRIGHT_KUBE_CA_DATA}
contexts:
- name: fleetright-controller
  context:
    cluster: fleetright-kind
    namespace: fleetright-v2
    user: fleetright-controller
current-context: fleetright-controller
users:
- name: fleetright-controller
  user:
    token: ${FLEETRIGHT_SA_TOKEN}
EOF
unset FLEETRIGHT_KUBE_SERVER FLEETRIGHT_KUBE_CA_DATA FLEETRIGHT_SA_TOKEN
export FLEETRIGHT_KUBERNETES_KUBECONFIG=/tmp/fleetright-kubeconfig
export FLEETRIGHT_KUBERNETES_NAMESPACE=fleetright-v2
export FLEETRIGHT_HEALTH_TARGETS_JSON='{"cpu-api":"http://127.0.0.1:8081"}'
export FLEETRIGHT_MODE=EXECUTE
export FLEETRIGHT_EXECUTION_TARGET=kubernetes-workload
PYTHONPATH=python .venv/bin/uvicorn fleetright.api:app --host 127.0.0.1 --port 8080
```

Never point Kubernetes EXECUTE at the kind administrator kubeconfig. The Role grants only GET/PATCH
on the named `cpu-api` FleetRightWorkload; the controller Role grants Pod CREATE but no Pod DELETE
or Eviction. Existing unmarked FleetRightWorkload objects cannot be promoted in place. Pod
Eviction, Deployment mutation, scale-down, and Node cordoning remain disabled by the checked-in
RBAC and application configuration. Delete the temporary kubeconfig when finished.
