# FleetRight

FleetRight measures stateless service capacity and proposes lower-cost resource, replica, and placement plans under hard SLO and failure constraints. CPU utilization is telemetry; it is not proof that a smaller service shape preserves tail latency.

The first implementation milestone contains a generated C++20 protobuf contract, deterministic planner, separately implemented verifier, Python trace/profile/simulation tools, Prometheus exposition for three synthetic workloads, and reproducible run artifacts. The original frozen architecture is retained under [`FleetRight_Project_Architecture_v2_10of10_Target/FleetRight_Project_Architecture`](FleetRight_Project_Architecture_v2_10of10_Target/FleetRight_Project_Architecture).

## Build and run

Requirements: Linux, CMake, C++20 compiler, Protobuf compiler and C++ runtime, OpenSSL development files, GoogleTest, Python 3.11+, and Docker Compose for container calibration.

```sh
./scripts/bootstrap.sh
./scripts/run_demo.sh
./scripts/characterize_capacity.sh
```

The demo writes append-only evidence to `results/phase1/<run-id>/`. It uses explicitly marked synthetic profile fixtures for optimizer integration; such fixtures are disabled in the default planning policy. A profile from a real service is eligible for destructive planning only when its workload identity matches and its resource limits were enforced during characterization.

`characterize_capacity.sh` measures the supplied service process and records whether Docker resource limits were available. An unconstrained process run is evidence about that run only; it cannot establish a capacity profile for a smaller CPU or memory allocation. Docker access is needed for resource-shape calibration.

## Safety boundary

The planner consumes a sealed, versioned snapshot and emits a protobuf plan. The verifier independently checks hard invariants from the snapshot and plan. A missing, stale, incompatible, low-confidence, or unconstrained capacity profile blocks rightsizing and scale-in. Planning is deterministic for identical canonical inputs and build configuration.

The local simulator is a declared model, not a replacement for workload measurements. Its near-saturation queue and latency behavior is nonlinear. No forecasting, live Kubernetes controller, or destructive executor is included in this milestone.

Evacuation schema V2 is a separate, constraint-based planning protocol. Its independent C++ verifier evaluates every supported scheduler placement outcome and keeps mathematical verification separate from execution admission. The Python API rejects V2 execution until a durable Kubernetes adapter enforces the recorded placement, identity, PDB, readiness, and fencing requirements. V1 behavior remains unchanged; see [ADR-008](docs/adr/008-staged-evacuation-execution.md).

See [`docs/implementation.md`](docs/implementation.md), [`docs/capacity-model.md`](docs/capacity-model.md), and [`docs/benchmark-methodology.md`](docs/benchmark-methodology.md) for contracts, assumptions, and evidence limits.
