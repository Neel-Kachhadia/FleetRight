# FleetRight planner scale benchmark

Scenario: 1000 workloads, 500 nodes, 50 measured core API runs and CLI runs.

Planner API p95: 332.36 ms (target 250 ms: FAIL).
Verifier API p95: 18.24 ms (target 25 ms: PASS).
End-to-end CLI p95 including process start and protobuf file I/O: planner 351.97 ms, verifier 38.26 ms.
Independent verifier: PASS; deterministic planner/verifier replay: True/True.

Raw timings and canonical protobuf inputs/outputs are retained beside this report.
