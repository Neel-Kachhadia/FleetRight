Provenance: planner/verifier scale benchmark (`scripts/run_benchmarks.sh --iterations 50`) run on this
host on the unmodified 06b448b planner (branch phase5/final-qualification before commit c88f24b),
Release build, GCC 13.3.0. Planner API p95 332.36 ms (target 250 ms: FAIL); the historical Prompt 2 record
was 313.99 ms. After the `min_element` change alone (c88f24b) an ad hoc 50-iteration run measured planner
p50 152.5 / p95 155.9 ms with a byte-identical plan (no artifact kept); the published after-state
(`../planner/`) additionally includes the two correctness rules of 1cf56ac.
