# FleetRight

FleetRight is an SLO-constrained capacity optimizer. It measures how much load a service shape can carry
within its latency and error SLOs, forecasts short-term demand, and proposes lower-cost replica and
placement plans under hard availability, N-1, topology and resource constraints. Plans are checked by a
separately implemented verifier and, when execution is explicitly enabled, applied to Kubernetes through
a journaled, fenced, PDB-aware protocol that can fail safe or compensate.

**Status: v1.0.0 (local release candidate; see [`docs/RELEASE_REPORT.md`](docs/RELEASE_REPORT.md)).**
It is a research-grade, portfolio-scale system: validated against a simulator, deterministic fixtures and
a disposable Kind lab. It has never run in production and its cost figures are synthetic.

## Why not just an HPA?

CPU utilization is telemetry, not proof that a smaller shape still meets p99. An HPA-style controller
adds replicas only after load arrives (startup, readiness and node activation take tens of seconds),
cannot reason about which node shape or placement is cheapest, and says nothing about N-1 survival or
topology spread. Static peak provisioning is safe but pays for the peak all day. FleetRight sizes from
*measured* capacity profiles and treats SLO, availability and placement as hard constraints, then trades
off cost. What the evidence actually shows (and does not) is in the release report: against tuned
reactive baselines it is often cheaper but frequently by taking more SLO-violation windows; forecasting did
not help in the simulated model.

## How it works

```
telemetry -> capacity profile -> forecast -> C++ planner -> C++ verifier -> PostgreSQL plan + journal
         -> Kubernetes executor -> health observation -> commit | compensate | fail safe
```

* **Capacity profiles** (`python/fleetright/characterize.py`): hashed, tied to image/request mix/resource
  shape; a missing, stale or unconstrained profile blocks rightsizing and scale-in.
* **Forecast** (`forecast.py`): rolling-median, EWMA-trend and seasonal models with temporal calibration;
  insufficient/stale/uncalibrated forecasts fall back to reactive.
* **Planner** (`cpp/src/planner.cpp`): deterministic greedy placement + bounded local search over a
  fixed-horizon integer objective (node cost, movement, churn, uncertainty, capacity-boundary risk).
* **Verifier** (`cpp/src/verifier.cpp`): independent recomputation of every hard invariant, including
  intermediate states during evacuation and N-1 recovery (ADR-003).
* **Control plane** (`api.py`, `control.py`): authenticated role-based API, idempotent plan lifecycle,
  leases, fencing tokens and audit events in PostgreSQL. Default mode is `RECOMMEND`.
* **Execution** (`execution.py`, `workload_controller.py`, `pod_admission.py`): staged replacement
  (surge Pod first), PDB-respecting Eviction, Node drain with cordon ownership, bounded verification and
  health-driven compensation; unknown outcomes are observed, never guessed. Operations: `SCALE_UP`,
  `MOVE_REPLICA`, `EVICT_SOURCE`, `DRAIN_NODE`, `COMPENSATE`. `SCALE_DOWN` disabled, `DEACTIVATE_NODE`
  unsupported. See [`docs/ENGINEERING_WALKTHROUGH.md`](docs/ENGINEERING_WALKTHROUGH.md) and ADR-008.

### Example (conceptual until each step is read in the evidence)

A service has variable traffic. FleetRight keeps a measured capacity profile for it, forecasts the next
minutes, and the planner finds a cheaper verified placement. The verifier checks the plan, including
every intermediate state. The controller creates a replacement Pod on the approved Node; once it is Ready
the source is retired through the Eviction API so the PDB is honoured; the controller verifies availability
and placement before completing. If health regresses it restores capacity; if it cannot, the plan ends
`FAILED_SAFE` for an operator. Each step is exercised live on Kind or deterministically (see the report);
no cloud deployment exists.

## Quickstart

Requirements: Linux, CMake >= 3.20, a C++20 compiler, Protobuf compiler and runtime, OpenSSL dev files,
GoogleTest, Python >= 3.11 (network access for `pip` unless `FLEETRIGHT_SKIP_VENV=1`), Docker (PostgreSQL
tests, Docker E2E and the Kind labs), optional `kind` + `kubectl` for live Kubernetes tests.

```sh
./scripts/bootstrap.sh                 # venv + Release build + C++ tests + Python tests + ruff
./scripts/start_test_db.sh             # disposable PostgreSQL on 127.0.0.1 (credentials stay in a 0600 file)
source "${FLEETRIGHT_STATE_DIR:-$HOME/.cache/fleetright}/test-db.env"
PYTHONPATH=python .venv/bin/python -m pytest -q python/tests      # includes PostgreSQL integration
./scripts/run_demo.sh                  # plan -> verify -> replay -> baselines -> exact oracle (results/phase1/)
./scripts/run_benchmarks.sh --iterations 100   # planner/verifier latency vs the 250 ms / 25 ms targets
./scripts/run_qualification.sh         # full paired benchmark + quality + forecast + placement (results/phase3/)
```

Docker E2E: `FLEETRIGHT_RUN_DOCKER_E2E=1 pytest python/tests/test_prompt2a_control_api.py -k docker_api`.
Live Kubernetes tests (`FLEETRIGHT_RUN_KIND_E2E=1`, `FLEETRIGHT_LAB_ADMIN_KUBECONFIG`,
`FLEETRIGHT_LAB_CONTEXT`) require a disposable Kind lab built from `deploy/kubernetes/`
(`install-pod-create-admission.sh`); never point them at a cluster you care about. Run each live module
with a lab reset in between (drained Nodes stay cordoned by design).

Where things are written: `results/phase1|2|3/` (git-ignored run output), compact published artifacts in
[`docs/benchmarks/v1.0.0/`](docs/benchmarks/v1.0.0/), the qualification ledger in
[`docs/PROMPT3_QUALIFICATION.md`](docs/PROMPT3_QUALIFICATION.md), execution history in
[`docs/V2_EXECUTION_PROGRESS.md`](docs/V2_EXECUTION_PROGRESS.md).

## Headline evidence (v1.0.0, this host)

| Item | Result | Class |
|---|---|---|
| Planner API p95, 1000 workloads / 500 nodes | **196.9 ms** (target 250 ms) | measured, Release |
| Verifier API p95 | **20.7 ms** (target 25 ms) | measured, Release |
| Planner/verifier agreement on 60 random instances | 60/60 (was 49/60 before fixes) | real planner |
| Mutated plans rejected by the verifier | 360/360 | real planner |
| Reactive FleetRight vs HPA-like (15 simulated scenarios) | median cost -8.6%; 6 better / 3 worse / 6 trade-off; hard-violation-free in 11/15 scenarios (HPA-like 10/15, static peak 15/15) | **simulated** |
| Predictive vs reactive FleetRight | 0 better, 5 worse, 10 trade-off | **simulated** |
| Live Kind qualification | 15 live tests + 3 Docker E2E passed | live, one lab |
| Tests | C++ 52/52, Python 496 passed / 20 live-gated skips (live suites run separately), ASan+UBSan clean | |

## Documentation

[`docs/RELEASE_REPORT.md`](docs/RELEASE_REPORT.md) results and limits - [`docs/benchmark-methodology.md`](docs/benchmark-methodology.md)
protocol - [`docs/ENGINEERING_WALKTHROUGH.md`](docs/ENGINEERING_WALKTHROUGH.md) design tour -
[`docs/limitations.md`](docs/limitations.md) - [`docs/implementation.md`](docs/implementation.md),
[`docs/capacity-model.md`](docs/capacity-model.md), ADRs in `docs/adr/`. The original frozen architecture
is retained under `FleetRight_Project_Architecture_v2_10of10_Target/`.

## Safety boundary

The planner consumes a sealed, versioned snapshot; the verifier independently rechecks it. Live mutation
needs `FLEETRIGHT_MODE=EXECUTE` plus an explicit target and a namespace-scoped kubeconfig with narrowly
scoped RBAC (no Pod delete, no Secret access, constrained Node patches). The API listens on loopback with
static bearer tokens and is not hardened for network exposure.
