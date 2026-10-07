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
`/v1/plans/{id}/execute` returns HTTP 501 with `execution-layer-not-enabled`. OBSERVE stores
snapshot/control-state provenance without planning; RECOMMEND and SHADOW persist recommendations.
Every mode reports execution disabled. **Live execution is not enabled in Prompt 2A.**
