# 12. Repository Structure

```text
fleetright/
├── CMakeLists.txt
├── README.md
├── LICENSE
├── docker-compose.yml
├── proto/
│   ├── model.proto
│   ├── optimizer.proto
│   ├── verifier.proto
│   └── control.proto
├── cpp/
│   ├── include/fleetright/
│   │   ├── model/
│   │   ├── units/
│   │   ├── planner/
│   │   ├── verifier/
│   │   ├── placement/
│   │   ├── policy/
│   │   ├── cost/
│   │   └── replay/
│   ├── src/
│   ├── tests/
│   ├── fuzz/
│   └── benchmarks/
├── control/
│   ├── api/
│   ├── auth/
│   ├── telemetry/
│   ├── snapshots/
│   ├── profiles/
│   ├── forecast/
│   ├── planner_client/
│   ├── verifier_client/
│   ├── reconciler/
│   ├── executor/
│   │   ├── simulator/
│   │   ├── docker/
│   │   └── kubernetes/
│   ├── persistence/
│   └── tests/
├── simulator/
│   ├── engine/
│   ├── scenarios/
│   ├── traces/
│   ├── calibration/
│   └── generators/
├── workloads/
│   ├── cpu_service/
│   ├── memory_service/
│   └── queued_latency_service/
├── deploy/
│   ├── compose/
│   └── kind/
├── policies/
│   ├── default.yaml
│   └── strict.yaml
├── capacity_profiles/
│   └── fixtures/
├── baselines/
│   ├── static/
│   ├── threshold/
│   └── hpa_like/
├── oracle/
│   └── exact_small_instance/
├── migrations/
├── scripts/
│   ├── bootstrap.sh
│   ├── run_demo.sh
│   ├── characterize_capacity.sh
│   ├── run_benchmarks.sh
│   ├── run_chaos.sh
│   └── reproduce_release.sh
├── results/
│   └── .gitkeep
├── docs/
│   ├── architecture.md
│   ├── benchmark-methodology.md
│   ├── capacity-model.md
│   ├── failure-model.md
│   ├── security.md
│   ├── operations.md
│   ├── limitations.md
│   └── adr/
│       ├── ADR-001-deterministic-planner.md
│       ├── ADR-002-cpp-python-boundary.md
│       ├── ADR-003-independent-verifier.md
│       ├── ADR-004-immutable-snapshots.md
│       ├── ADR-005-progressive-compensation.md
│       ├── ADR-006-forecast-scaleout-asymmetry.md
│       └── ADR-007-stateless-v1-scope.md
└── .github/workflows/
    ├── ci.yml
    ├── sanitizers.yml
    ├── fuzz-smoke.yml
    ├── integration-compose.yml
    ├── integration-kind.yml
    └── benchmark-smoke.yml
```

## Boundary rules

- C++ planner/verifier do not depend on Prometheus or Kubernetes SDKs.
- External systems are translated into immutable versioned contracts.
- Simulator and live adapters consume the same canonical plan semantics.
- Verifier does not call planner-internal feasibility helpers that could duplicate the same bug.
- Policies are data, not hidden control-flow branches.
- Capacity profiles are versioned evidence, not magic constants.
- Experiment output is append-only by run ID; raw evidence is never overwritten.
- No executor may bypass the journal/state machine.
- Security/auth code is not mixed into optimization algorithms.

## ADR rule

Any new dependency or architecture mechanism that changes safety, reproducibility or trust boundaries requires an ADR before implementation.
