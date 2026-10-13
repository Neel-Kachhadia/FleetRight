#!/usr/bin/env bash
# Reproduce the v1.0.0 scientific evidence: planner/verifier latency, optimizer quality against
# the exact oracle, placement scenarios, tuned paired policy comparison and forecast evaluation.
# Everything runs against the Release build in ./build (run scripts/bootstrap.sh first) and writes
# to results/phase3/ (git-ignored). Compact copies are published with:
#   python -m fleetright.qualify publish results/phase3/qualification-<id> docs/benchmarks/v1.0.0
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python_bin="${PYTHON:-$repo_root/.venv/bin/python}"
[[ -x "$python_bin" ]] || python_bin=python3
export PYTHONPATH="$repo_root/python${PYTHONPATH:+:$PYTHONPATH}"
out="${FLEETRIGHT_RESULTS:-$repo_root/results}"
workers="${FLEETRIGHT_WORKERS:-8}"
"$python_bin" -m fleetright.benchmark --output "$out" --iterations "${FLEETRIGHT_BENCH_ITERATIONS:-100}"
"$python_bin" -m fleetright.optimizer_quality --output "$out/phase3/optimizer-quality.json"
"$python_bin" -m fleetright.qualify_placement --output "$out/phase3/placement.json"
"$python_bin" -m fleetright.qualify tune --output "$out/phase3" --workers "$workers"
run_dir="$("$python_bin" -m fleetright.qualify run --output "$out/phase3" --workers "$workers" \
  --tuned "$out/phase3/tuning.json")"
"$python_bin" -m fleetright.qualify_forecast --output "$out/phase3/forecast.json" \
  --summary "$run_dir/summary.json"
echo "$run_dir"
