# FleetRight planner scale benchmark

Scenario: 1000 workloads, 500 nodes, 100 measured core API runs and CLI runs.

Planner API p95: 196.88 ms (target 250 ms: PASS).
Verifier API p95: 20.66 ms (target 25 ms: PASS).
End-to-end CLI p95 including process start and protobuf file I/O: planner 209.92 ms, verifier 37.70 ms.
Independent verifier: PASS; deterministic planner/verifier replay: True/True.

Raw timings and canonical protobuf inputs/outputs are retained beside this report.
