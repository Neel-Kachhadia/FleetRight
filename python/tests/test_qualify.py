from __future__ import annotations

import random
import re
import subprocess
from copy import deepcopy
from pathlib import Path

import pytest
from fleetright import qualify
from fleetright.contracts import decode_text, encode_snapshot
from fleetright.policies import required_replicas
from fleetright.scenario import demo_snapshot_fields

ROOT = Path(__file__).resolve().parents[2]
PLANNER = ROOT / "build/fleetright-plan"


def _plan_decisions(tmp_path: Path, demands: dict[str, tuple[int, int]]) -> dict[str, dict]:
    fields = deepcopy(demo_snapshot_fields())
    for workload in fields["workloads"]:
        qps, concurrency = demands[workload["workload_id"]]
        workload["demand_qps_milli"], workload["concurrency_bound"] = qps, concurrency
    payload, _ = encode_snapshot(fields)
    snapshot, plan = tmp_path / "snapshot.pb", tmp_path / "plan.pb"
    snapshot.write_bytes(payload)
    subprocess.run([str(PLANNER), str(snapshot), str(plan)], check=True, capture_output=True)
    text = decode_text(
        "fleetright.v1.OptimizationPlan", "fleetright/v1/optimizer.proto", plan.read_bytes()
    )
    decisions = {}
    for block in re.findall(r"workload_decisions \{(.*?)\n\}", text, re.S):
        workload_id = re.search(r'workload_id: "([^"]+)"', block).group(1)
        decisions[workload_id] = {
            "proposed": int(re.search(r"proposed_replicas: (\d+)", block).group(1)),
            "current": int(re.search(r"current_replicas: (\d+)", block).group(1)),
            "eff_qps": int(re.search(r"effective_qps_per_replica_milli: (\d+)", block).group(1)),
        }
    return decisions


@pytest.mark.skipif(not PLANNER.is_file(), reason="C++ planner is not built")
def test_policy_sizing_rule_matches_the_cpp_planner(tmp_path: Path) -> None:
    rng = random.Random(7)
    contexts = qualify._contexts()
    checked = 0
    for _ in range(12):
        demands = {w: (rng.randint(100_000, 3_000_000), rng.randint(5, 400)) for w in contexts}
        decisions = _plan_decisions(tmp_path, demands)
        for workload_id, ctx in contexts.items():
            workload = ctx["workload"]
            expected = required_replicas(
                *demands[workload_id], ctx["eff_qps"], ctx["eff_conc"], workload["min_replicas"],
                workload["availability_floor"], workload["max_replicas"],
                workload["failure_tolerance"] == "FAILURE_TOLERANCE_N_MINUS_1",
            )  # fmt: skip
            got = decisions[workload_id]
            assert got["eff_qps"] == ctx["eff_qps"]
            if expected >= got["current"]:  # scale-in is separately gated by the planner
                assert got["proposed"] == expected, (workload_id, demands[workload_id])
                checked += 1
    assert checked >= 10


def test_case_is_deterministic_and_trace_is_shared_across_policies() -> None:
    scenario = qualify.SCENARIOS[2]
    first = qualify.run_case(
        scenario, 100, "fleetright_reactive", 600_000, qualify.EVAL_BASE_QPS_MILLI
    )
    second = qualify.run_case(
        scenario, 100, "fleetright_reactive", 600_000, qualify.EVAL_BASE_QPS_MILLI
    )
    other = qualify.run_case(scenario, 100, "hpa_like", 500_000, qualify.EVAL_BASE_QPS_MILLI)
    assert first == second
    assert first["trace_sha256"] == other["trace_sha256"]


def test_oracle_is_labelled_offline_and_hard_violation_counts_availability() -> None:
    scenario = qualify.SCENARIOS[3]  # flash crowd: only the offline oracle may anticipate it
    online = qualify.run_case(
        scenario, 100, "fleetright_reactive", 600_000, qualify.EVAL_BASE_QPS_MILLI
    )
    oracle = qualify.run_case(
        scenario, 100, "offline_lookahead_oracle", 0, qualify.EVAL_BASE_QPS_MILLI
    )
    assert online["hard_violation_windows"] > 0 and oracle["hard_violation_windows"] == 0
    assert all(kind in {"hard", "soft"} for _, _, kind in online["violation_windows"])


def test_bootstrap_ci_brackets_a_known_difference_and_is_seeded() -> None:
    diffs = [-10.0 + random.Random(i).random() for i in range(20)]
    low, high = qualify.bootstrap_ci(diffs, random.Random(1))
    assert high < 0 and low < sum(diffs) / 20 < high
    assert qualify.bootstrap_ci(diffs, random.Random(1)) == (low, high)
    flat = qualify.bootstrap_ci([0.0] * 20, random.Random(1))
    assert flat == (0.0, 0.0)


@pytest.mark.skipif(not PLANNER.is_file(), reason="C++ planner is not built")
def test_placement_suite_meets_its_declared_expectations() -> None:
    from fleetright import qualify_placement

    report = qualify_placement.run()
    failed = [row["id"] for row in report["cases"] if not row["pass"]]
    assert not failed, failed
    assert report["total"] >= 10


@pytest.mark.skipif(not PLANNER.is_file(), reason="C++ planner is not built")
def test_verifier_accepts_clean_planner_plans_and_rejects_every_mutation(monkeypatch) -> None:
    import tempfile

    from fleetright import optimizer_quality

    monkeypatch.setattr(optimizer_quality, "SEEDS", (1, 2, 3, 12, 42, 50))
    with tempfile.TemporaryDirectory() as tmp:
        report = optimizer_quality.run(Path(tmp))
    assert report["verifier_accepted_clean_plans"] == report["planner_feasible"] == 6
    assert report["deterministic_replay"] == "6/6"
    for kind, outcome in report["mutation_rejection"].items():
        assert outcome["rejected"] == outcome["trials"] > 0, kind


def test_forecast_evaluation_reports_calibrated_coverage_on_a_steady_series() -> None:
    from fleetright import qualify_forecast
    from fleetright.forecast import EwmaTrendModel

    case = qualify_forecast.CASES[0][1]
    metrics = qualify_forecast.evaluate_series(
        qualify_forecast._series(case, 100), EwmaTrendModel(), qualify.WARMUP, None
    )
    assert metrics["n"] > 100 and metrics["wape"] < 0.05
    assert 0.85 <= metrics["upper_bound_coverage_95"] <= 1.0
