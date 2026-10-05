# FleetRight planner scale benchmark

Scenario: 1000 workloads, 500 nodes, 20 measured core API runs and CLI runs.

Planner API p95: 313.99 ms (target 250 ms: FAIL).
Verifier API p95: 17.00 ms (target 25 ms: PASS).
End-to-end CLI p95 including process start and protobuf file I/O: planner 409.10 ms, verifier 36.91 ms.
Independent verifier: PASS; deterministic planner/verifier replay: True/True.

Raw timings and canonical protobuf inputs/outputs are retained beside this report.
