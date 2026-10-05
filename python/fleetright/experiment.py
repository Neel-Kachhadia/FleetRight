from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
import sys
import time
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fleetright.baselines import (
    FixedTarget,
    HpaLikeBaseline,
    LookaheadTarget,
    ThresholdBaseline,
    lookahead_schedule,
    static_peak_replicas,
)
from fleetright.contracts import decode_text, encode_profile, encode_snapshot
from fleetright.oracle import exact_single_workload
from fleetright.scenario import demo_snapshot_fields, oracle_snapshot_fields
from fleetright.simulator import SimulationConfig, simulate
from fleetright.traces import Trace, generate_trace, validate_trace
from fleetright.units import multiply_ppm_floor

ROOT = Path(__file__).resolve().parents[2]
TRACE_FAMILIES = (
    "steady",
    "ramp",
    "diurnal",
    "bursty",
    "flash_crowd",
    "correlated_spike",
    "noisy_tenant",
    "memory_heavy",
    "slow_start",
)


def _run(command: list[str], cwd: Path = ROOT) -> tuple[subprocess.CompletedProcess[bytes], float]:
    started = time.perf_counter()
    result = subprocess.run(command, cwd=cwd, capture_output=True, check=False)
    return result, time.perf_counter() - started


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _blocks(text: str, field: str) -> list[str]:
    blocks: list[str] = []
    start = 0
    needle = field + " {"
    while True:
        index = text.find(needle, start)
        if index < 0:
            break
        opening = index + len(needle) - 1
        depth = 0
        in_string = False
        escaped = False
        for end in range(opening, len(text)):
            char = text[end]
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    in_string = False
                continue
            if char == '"':
                in_string = True
            elif char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    blocks.append(text[opening + 1 : end])
                    start = end + 1
                    break
        else:
            raise ValueError(f"unterminated protobuf text block: {field}")
    return blocks


def _field(block: str, field: str, default: str | None = None) -> str:
    match = re.search(rf"(?m)^\s*{re.escape(field)}:\s*(.+?)\s*$", block)
    if not match:
        if default is not None:
            return default
        raise ValueError(f"missing protobuf text field {field}")
    return match.group(1).strip().strip('"')


def _host() -> dict[str, object]:
    memory_kib = 0
    try:
        for line in Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
            if line.startswith("MemTotal:"):
                memory_kib = int(line.split()[1])
                break
    except (OSError, ValueError, IndexError):
        pass
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=False
    )
    status = subprocess.run(
        ["git", "status", "--short"], cwd=ROOT, capture_output=True, text=True, check=False
    )
    compiler = subprocess.run(
        ["cmake", "-LA", "-N", str(ROOT / "build")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    compiler_line = next(
        (
            line
            for line in compiler.stdout.splitlines()
            if line.startswith("CMAKE_CXX_COMPILER:FILEPATH=")
        ),
        "",
    )
    return {
        "kernel": platform.release(),
        "platform": platform.platform(),
        "cpu": platform.processor() or platform.machine(),
        "logical_cpu_count": os.cpu_count(),
        "memory_bytes": memory_kib * 1024,
        "python": sys.version.split()[0],
        "git_commit": commit.stdout.strip() if commit.returncode == 0 else "unknown",
        "git_status": status.stdout.splitlines() if status.returncode == 0 else [],
        "cxx_compiler": compiler_line.split("=", 1)[-1] if compiler_line else "unknown",
        "build_type": "Release"
        if "CMAKE_BUILD_TYPE:STRING=Release" in compiler.stdout
        else "unknown",
    }


def _run_baselines(run_dir: Path, seed: int) -> dict[str, object]:
    results: dict[str, object] = {}
    raw_dir = run_dir / "raw_metrics"
    trace_dir = run_dir / "traces"
    trace_dir.mkdir()
    workloads = demo_snapshot_fields()["workloads"]
    all_traces: list[Trace] = []
    for trace_index, family in enumerate(TRACE_FAMILIES):
        trace = generate_trace(family, seed + trace_index)
        if not validate_trace(trace):
            raise RuntimeError("generated trace failed its canonical hash check")
        all_traces.append(trace)
        _json(trace_dir / f"{family}.json", trace.as_dict())
    for workload in workloads:
        profile = next(
            p
            for p in demo_snapshot_fields()["capacity_profiles"]
            if p["workload_id"] == workload["workload_id"]
        )
        point = profile["safe_points"][0]
        factors = (
            workload["profile_confidence_factor_ppm"],
            workload["model_accuracy_factor_ppm"],
            workload["degraded_cluster_factor_ppm"],
        )
        effective_qps = multiply_ppm_floor(point["safe_qps_milli"], *factors)
        effective_concurrency = multiply_ppm_floor(point["safe_concurrency"], *factors)
        results[workload["workload_id"]] = {}
        for trace in all_traces:
            trace_workload = next(
                w for w in trace.workloads if w.workload_id == workload["workload_id"]
            )
            peak_qps = max(p.qps_milli for p in trace_workload.points)
            peak_concurrency = max(p.concurrency for p in trace_workload.points)
            initial = sum(
                1
                for item in demo_snapshot_fields()["current_placements"]
                if item["workload_id"] == workload["workload_id"]
            )
            config = SimulationConfig(
                workload_id=workload["workload_id"],
                safe_qps_per_replica_milli=effective_qps,
                safe_concurrency_per_replica=effective_concurrency,
                base_latency_ms=max(1, int(point["p99_upper_ms"] * 0.55)),
                latency_slo_ms=workload["latency_slo_ms"],
                initial_replicas=initial,
                min_replicas=workload["min_replicas"],
                max_replicas=workload["max_replicas"],
                actuation_delay_ms=5_000,
                node_activation_ms=10_000,
                startup_ms=workload["startup_ms"],
                readiness_stabilization_ms=workload["readiness_stabilization_ms"],
                replica_cost_micros_per_hour=1_000,
                injected_error_ppm=0,
            )
            reserve_static = static_peak_replicas(
                peak_qps,
                peak_concurrency,
                effective_qps,
                effective_concurrency,
                workload["min_replicas"],
                reserve_ppm=200_000,
            )
            steps = len(trace_workload.points)
            lookahead_delay = (
                config.actuation_delay_ms
                + config.node_activation_ms
                + config.startup_ms
                + config.readiness_stabilization_ms
                + trace.window_ms
                - 1
            ) // trace.window_ms
            demand_qps = [p.qps_milli for p in trace_workload.points]
            demand_concurrency = [p.concurrency for p in trace_workload.points]
            schedules = lookahead_schedule(
                demand_qps,
                demand_concurrency,
                effective_qps,
                effective_concurrency,
                workload["min_replicas"],
                workload["max_replicas"],
                lookahead_delay,
                0,
            )
            policy_objects = {
                "static_peak_reserve": FixedTarget(min(workload["max_replicas"], reserve_static)),
                "naive_threshold": ThresholdBaseline(
                    min_replicas=workload["min_replicas"], max_replicas=workload["max_replicas"]
                ),
                "hpa_like": HpaLikeBaseline(
                    target_cpu_ppm=650_000,
                    target_qps_per_replica_milli=max(1, effective_qps * 8 // 10),
                    target_concurrency_per_replica=max(1, effective_concurrency * 8 // 10),
                    min_replicas=workload["min_replicas"],
                    max_replicas=workload["max_replicas"],
                ),
                "offline_lookahead": LookaheadTarget(tuple(schedules[:steps])),
            }
            per_trace: dict[str, object] = {}
            for policy_name, policy in policy_objects.items():
                output = simulate(trace, config, policy)
                windows = [asdict(window) for window in output.windows]
                artifact = {
                    "schema_version": 1,
                    "workload_id": workload["workload_id"],
                    "trace_id": trace.family,
                    "trace_sha256": trace.content_sha256,
                    "policy": policy_name,
                    "request_count": output.request_count,
                    "failed_requests": output.failed_requests,
                    "max_queue_milli_requests": output.max_queue_milli_requests,
                    "cost_micros": output.replica_cost_micros,
                    "latency_slo_violations": sum(
                        window["latency_p99_ms"] > workload["latency_slo_ms"] for window in windows
                    ),
                    "max_latency_ms": max(window["latency_p99_ms"] for window in windows),
                    "max_ready_replicas": max(window["ready_replicas"] for window in windows),
                    "max_desired_replicas": max(window["desired_replicas"] for window in windows),
                }
                per_trace[policy_name] = artifact
                _json(
                    raw_dir / f"sim-{trace.family}-{workload['workload_id']}-{policy_name}.json",
                    {**artifact, "windows": windows},
                )
            results[workload["workload_id"]][trace.family] = per_trace
    return results


def run_demo(output: Path, seed: int = 42) -> Path:
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ") + f"-seed-{seed}"
    run_dir = output / "phase1" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    for child in ("capacity_profiles", "raw_metrics", "policy_outputs", "plots"):
        (run_dir / child).mkdir()
    started = datetime.now(UTC)
    fields = demo_snapshot_fields()
    snapshot_bytes, finalized = encode_snapshot(fields)
    snapshot_text = decode_text(
        "fleetright.v1.OptimizationSnapshot", "fleetright/v1/model.proto", snapshot_bytes
    )
    (run_dir / "policy_outputs/snapshot.pb").write_bytes(snapshot_bytes)
    (run_dir / "policy_outputs/snapshot.textproto").write_text(snapshot_text, encoding="utf-8")
    profile_hashes = []
    for profile in finalized["capacity_profiles"]:
        payload, final_profile = encode_profile(profile)
        name = str(profile["workload_id"])
        (run_dir / "capacity_profiles" / f"{name}.pb").write_bytes(payload)
        (run_dir / "capacity_profiles" / f"{name}.textproto").write_text(
            decode_text("fleetright.v1.CapacityProfile", "fleetright/v1/model.proto", payload),
            encoding="utf-8",
        )
        profile_hashes.append(final_profile["profile_sha256"])

    binaries = {
        name: ROOT / "build" / binary
        for name, binary in (("plan", "fleetright-plan"), ("verify", "fleetright-verify"))
    }
    if any(not path.is_file() for path in binaries.values()):
        raise RuntimeError("C++ binaries missing; run scripts/bootstrap.sh first")
    plan_path = run_dir / "policy_outputs/plan.pb"
    plan_result, plan_seconds = _run(
        [str(binaries["plan"]), str(run_dir / "policy_outputs/snapshot.pb"), str(plan_path)]
    )
    if plan_result.returncode:
        raise RuntimeError(plan_result.stderr.decode(errors="replace"))
    plan_bytes = plan_path.read_bytes()
    replay_path = run_dir / "policy_outputs/plan-replay.pb"
    replay, replay_seconds = _run(
        [str(binaries["plan"]), str(run_dir / "policy_outputs/snapshot.pb"), str(replay_path)]
    )
    if replay.returncode or replay_path.read_bytes() != plan_bytes:
        raise RuntimeError("optimizer replay did not reproduce byte-identical output")
    plan_text = decode_text(
        "fleetright.v1.OptimizationPlan", "fleetright/v1/optimizer.proto", plan_bytes
    )
    (run_dir / "policy_outputs/plan.textproto").write_text(plan_text, encoding="utf-8")
    verification_path = run_dir / "policy_outputs/verification.pb"
    verify_result, verify_seconds = _run(
        [
            str(binaries["verify"]),
            str(run_dir / "policy_outputs/snapshot.pb"),
            str(plan_path),
            str(verification_path),
        ]
    )
    if verify_result.returncode:
        raise RuntimeError(verify_result.stderr.decode(errors="replace"))
    verification_bytes = verification_path.read_bytes()
    verification_text = decode_text(
        "fleetright.v1.VerificationResult", "fleetright/v1/optimizer.proto", verification_bytes
    )
    (run_dir / "policy_outputs/verification.textproto").write_text(
        verification_text, encoding="utf-8"
    )
    accepted = _field(verification_text, "accepted", "false") == "true"
    if not accepted:
        raise RuntimeError(
            "independent verifier rejected demo plan: "
            + verify_result.stderr.decode(errors="replace")
        )
    verify_replay_path = run_dir / "policy_outputs/verification-replay.pb"
    verify_replay, _ = _run(
        [
            str(binaries["verify"]),
            str(run_dir / "policy_outputs/snapshot.pb"),
            str(plan_path),
            str(verify_replay_path),
        ]
    )
    if verify_replay.returncode or verify_replay_path.read_bytes() != verification_bytes:
        raise RuntimeError("verifier replay did not reproduce byte-identical output")

    baseline_results = _run_baselines(run_dir, seed)
    _json(run_dir / "policy_outputs/baselines.json", baseline_results)

    oracle_fields = oracle_snapshot_fields()
    oracle_bytes, _ = encode_snapshot(oracle_fields)
    oracle_path = run_dir / "policy_outputs/oracle-snapshot.pb"
    oracle_path.write_bytes(oracle_bytes)
    exact = exact_single_workload(oracle_fields)
    oracle_plan_path = run_dir / "policy_outputs/oracle-plan.pb"
    oracle_plan, oracle_heuristic_seconds = _run(
        [str(binaries["plan"]), str(oracle_path), str(oracle_plan_path)]
    )
    if oracle_plan.returncode:
        raise RuntimeError(oracle_plan.stderr.decode(errors="replace"))
    oracle_plan_text = decode_text(
        "fleetright.v1.OptimizationPlan",
        "fleetright/v1/optimizer.proto",
        oracle_plan_path.read_bytes(),
    )
    heuristic_cost = int(_field(oracle_plan_text, "proposed_objective_cost_micros"))
    if heuristic_cost < exact.objective_micros_per_hour:
        raise RuntimeError(
            "heuristic objective is lower than exact optimum; constraints/objectives differ"
        )
    gap = (heuristic_cost - exact.objective_micros_per_hour) / exact.objective_micros_per_hour
    exact_result = {
        "oracle": "enumerate node subsets for one workload; exact infrastructure hourly cost",
        "scenario_workloads": 1,
        "scenario_nodes": len(oracle_fields["nodes"]),
        "scenario_size_bound": 12,
        "replicas": exact.replicas,
        "exact_cost_micros_per_hour": exact.objective_micros_per_hour,
        "heuristic_objective_cost_micros": heuristic_cost,
        "optimality_gap": gap,
        "exact_solve_seconds": exact.duration_seconds,
        "heuristic_solve_seconds": oracle_heuristic_seconds,
        "combinations_examined": exact.scenarios_examined,
        "exact_node_ids": list(exact.node_ids),
    }
    (run_dir / "policy_outputs/oracle-exact.json").write_text(
        json.dumps(exact_result, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )

    catalog_hash = finalized["machine_catalog"]["catalog_sha256"]
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "scenario_id": "demo-three-workload-v1",
        "seed": seed,
        "started_at_utc": started.isoformat(),
        "completed_at_utc": datetime.now(UTC).isoformat(),
        "git_commit": _host()["git_commit"],
        "optimizer_build_id": _field(plan_text, "optimizer_build_id"),
        "verifier_build_id": _field(verification_text, "verifier_build_id"),
        "snapshot_schema_version": 1,
        "snapshot_sha256": finalized["content_sha256"],
        "policy_sha256": _sha256(json.dumps(fields["policy"], sort_keys=True).encode()),
        "capacity_profile_sha256": profile_hashes,
        "machine_catalog_sha256": catalog_hash,
        "trace_hashes": {
            family: json.loads((run_dir / "traces" / f"{family}.json").read_text())[
                "content_sha256"
            ]
            for family in TRACE_FAMILIES
        },
        "host": _host(),
        "optimizer_seconds": plan_seconds,
        "optimizer_replay_seconds": replay_seconds,
        "verifier_seconds": verify_seconds,
        "planner_deterministic_replay": _sha256(plan_bytes) == _sha256(replay_path.read_bytes()),
        "verifier_deterministic_replay": _sha256(verification_bytes)
        == _sha256(verify_replay_path.read_bytes()),
        "capacity_measurement_kind": "synthetic_benchmark_fixture",
        "docker_capacity_calibration": "BLOCKED: see dedicated characterization run artifact",
    }
    manifest["manifest_sha256"] = _sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    )
    _json(run_dir / "manifest.json", manifest)
    plan = {
        "plan_id": _field(plan_text, "plan_id"),
        "feasible": _field(plan_text, "feasible", "false") == "true",
        "accepted": accepted,
        "current_cost_micros_per_hour": int(_field(plan_text, "current_cost_micros_per_hour")),
        "proposed_cost_micros_per_hour": int(_field(plan_text, "proposed_cost_micros_per_hour")),
        "current_objective_cost_micros": int(
            _field(plan_text, "current_objective_cost_micros")
        ),
        "proposed_objective_cost_micros": int(
            _field(plan_text, "proposed_objective_cost_micros")
        ),
        "objective_costs": {
            "movement_micros": int(_field(plan_text, "proposed_movement_cost_micros", "0")),
            "churn_micros": int(_field(plan_text, "proposed_churn_cost_micros", "0")),
            "uncertainty_micros": int(
                _field(plan_text, "proposed_uncertainty_cost_micros", "0")
            ),
            "boundary_micros": int(_field(plan_text, "proposed_boundary_cost_micros", "0")),
            "current_uncertainty_micros": int(
                _field(plan_text, "current_uncertainty_cost_micros", "0")
            ),
            "current_boundary_micros": int(
                _field(plan_text, "current_boundary_cost_micros", "0")
            ),
        },
        "action_count": int(_field(plan_text, "action_count")),
        "workload_decisions": [
            {
                "workload_id": _field(block, "workload_id"),
                "current_replicas": int(_field(block, "current_replicas")),
                "proposed_replicas": int(_field(block, "proposed_replicas")),
                "profile_id": _field(block, "capacity_profile_id"),
                "effective_qps_per_replica_milli": int(
                    _field(block, "effective_qps_per_replica_milli")
                ),
                "profile_point_index": int(_field(block, "profile_point_index", "0")),
                "uncertainty_exposure_ppm": int(
                    _field(block, "uncertainty_exposure_ppm", "0")
                ),
                "boundary_risk_ppm": int(_field(block, "boundary_risk_ppm", "0")),
            }
            for block in _blocks(plan_text, "workload_decisions")
        ],
        "verifier_invariants": [
            {
                "invariant": _field(block, "invariant"),
                "passed": _field(block, "passed", "false") == "true",
            }
            for block in _blocks(verification_text, "invariants")
        ],
        "oracle": exact_result,
        "baselines": baseline_results,
    }
    _json(run_dir / "summary.json", plan)
    report_lines = [
        "# FleetRight Phase 1 measurement and planning demo",
        "",
        f"Run: `{run_id}` (seed `{seed}`)",
        "",
        "This reproducible integration run uses explicitly synthetic CapacityProfile fixtures. "
        "The result does not establish production capacity; Docker calibration is separately "
        "blocked "
        "on this host.",
        "",
        f"Planner feasibility: **{plan['feasible']}**; independent verifier: **{accepted}**.",
        f"Plan cost: {plan['current_cost_micros_per_hour']} -> "
        f"{plan['proposed_cost_micros_per_hour']} micros/hour; actions: {plan['action_count']}.",
        f"Fixed-horizon objective: {plan['current_objective_cost_micros']} -> "
        f"{plan['proposed_objective_cost_micros']} micros, including movement, churn, "
        "uncertainty, and capacity-boundary costs.",
        "",
        "## Workload decisions",
        "",
        "| Workload | Replicas | Effective QPS/replica (milli) | Profile |",
        "|---|---:|---:|---|",
    ]
    for item in plan["workload_decisions"]:
        report_lines.append(
            f"| {item['workload_id']} | {item['current_replicas']} -> "
            f"{item['proposed_replicas']} | "
            f"{item['effective_qps_per_replica_milli']} | `{item['profile_id']}` |"
        )
    report_lines.extend(
        (
            "",
            "## Exact bounded oracle",
            "",
            f"Exact objective: {exact.objective_micros_per_hour} micros over its one-hour "
            f"horizon; heuristic: "
            f"{heuristic_cost}; gap: {gap:.6%}; exhaustive combinations examined: "
            f"{exact.scenarios_examined}.",
            "",
            "## Baseline limitations",
            "",
            "All four baseline controllers received identical per-workload traces and modeled "
            "actuation/startup delays. Simulator cost is a replica-cost proxy and is reported "
            "separately from the node-cost optimizer objective; these figures are not "
            "presented as a "
            "head-to-head infrastructure savings comparison.",
            "Raw trace and window data are retained in `traces/` "
            "and `raw_metrics/`; protobuf inputs and outputs are under `policy_outputs/`.",
        )
    )
    (run_dir / "report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run deterministic measurement, simulation, optimizer and verifier evidence"
    )
    parser.add_argument("--output", type=Path, default=ROOT / "results")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    result = run_demo(args.output, args.seed)
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
