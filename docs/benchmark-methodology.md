# Benchmark methodology

`scripts/run_demo.sh` creates an append-only `results/phase1/<run-id>/` artifact containing seeded
traces, synthetic profile fixtures, raw simulator windows, static/threshold/HPA-like/lookahead
outputs, protobuf snapshot/plan/verifier artifacts, deterministic replay hashes, and the bounded
exact-oracle comparison. It labels all capacity inputs as synthetic. Simulator replica-cost proxies
are reported separately from node-hour optimizer cost.

`scripts/run_benchmarks.sh` generates the declared 1,000-workload/500-node synthetic snapshot with
one additional eligible shape for a workload. This exercises the bounded full-plan profile
comparison. It measures 30 direct C++ planner/verifier calls after loading protobuf inputs once, and separately
measures 30 end-to-end CLI runs including process startup and file I/O. The 250 ms planner and 25 ms
verifier targets apply to the direct API measurements; CLI p95 is retained as an end-to-end view.
Every output is checked for deterministic replay and independent verifier acceptance. The manifest
records commit, build IDs, compiler/build type, host/kernel/CPU/RAM, schema, policy/profile/catalog
hashes, and raw direct/CLI timings. Runs do not overwrite prior run IDs.

The exact oracle exhaustively enumerates eligible node subsets for a single workload on at most 12
nodes. It uses the same compatible profile point, effective capacity, resource shape, headroom,
catalog price, and anti-affinity rules. Its bounded comparison uses a one-hour horizon and zero
movement/churn/uncertainty/boundary weights, so the exact and heuristic objective both reduce to
infrastructure micros/hour. It deliberately rejects N-1 and topology cases it does not model. The
reported gap is heuristic objective relative to the exact optimum; a heuristic result below the
oracle is treated as a model mismatch error.

Baselines receive identical generated traces and simulator delays. HPA-like behavior includes
metric-ratio targeting, deadband, scale-up/down velocity, downscale stabilization, startup/readiness
scale-in exclusion, and conservative missing metrics. It is representative reactive behavior, not a
claim of byte-for-byte Kubernetes HPA equivalence. The lookahead schedule is offline and not
deployable policy.
