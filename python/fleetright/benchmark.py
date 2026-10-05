from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import subprocess
import sys
import tempfile
import time
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fleetright.contracts import decode_text, encode_snapshot
from fleetright.scenario import demo_snapshot_fields

ROOT = Path(__file__).resolve().parents[2]


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _stress_snapshot(workload_count: int, node_count: int) -> dict[str, Any]:
    if not 1 <= workload_count <= 1_000 or not 1 <= node_count <= 500:
        raise ValueError("benchmark bounds are 1,000 workloads and 500 nodes")
    fields = demo_snapshot_fields()
    templates = fields["workloads"]
    profile_templates = fields["capacity_profiles"]
    epoch = fields["evaluation_time_ms"]
    workloads: list[dict[str, Any]] = []
    profiles: list[dict[str, Any]] = []
    placements: list[dict[str, str]] = []
    for index in range(workload_count):
        template_index = index % len(templates)
        workload = deepcopy(templates[template_index])
        workload_id = f"benchmark-api-{index:04d}"
        workload["workload_id"] = workload_id
        workload["failure_tolerance"] = "FAILURE_TOLERANCE_NONE"
        workload["min_topology_domains"] = 0
        workload["max_moves"] = 100
        workloads.append(workload)
        profile = deepcopy(profile_templates[template_index])
        profile["profile_id"] = f"benchmark-profile-{index:04d}"
        profile["workload_id"] = workload_id
        profile["generated_at_ms"] = epoch - 60_000
        profiles.append(profile)
        if index == 0:
            alternative = deepcopy(profile)
            alternative["profile_id"] = f"benchmark-profile-{index:04d}-alternative"
            alternative["allocation"] = {
                "cpu_millicores": 750,
                "memory_mib": 768,
                "network_mbps": 75,
            }
            alternative["safe_points"][0]["safe_qps_milli"] = 700_000
            alternative["safe_points"][0]["safe_concurrency"] = 200
            profiles.append(alternative)
        for replica in range(3):
            placements.append(
                {
                    "workload_id": workload_id,
                    "replica_id": f"{workload_id}-{replica}",
                    "node_id": f"node-{(index * 3 + replica) % node_count:03d}",
                }
            )

    machine = next(
        item for item in fields["machine_catalog"]["machines"] if item["machine_type"] == "xlarge"
    )
    nodes = []
    for index in range(node_count):
        nodes.append(
            {
                "node_id": f"node-{index:03d}",
                "machine_type": "xlarge",
                "allocatable": deepcopy(machine["allocatable"]),
                "reserved": {"cpu_millicores": 100, "memory_mib": 256, "network_mbps": 50},
                "hourly_cost_micros": machine["hourly_cost_micros"],
                "labels": [("zone", f"zone-{index % 5}"), ("rack", f"rack-{index % 20}")],
                "taints": [],
                "health": "NODE_HEALTH_HEALTHY",
                "active": True,
                "generation": 2,
            }
        )
    fields.update(
        {
            "snapshot_id": f"benchmark-{workload_count}w-{node_count}n-v1",
            "workloads": workloads,
            "capacity_profiles": profiles,
            "nodes": nodes,
            "current_placements": placements,
        }
    )
    fields["policy"]["maximum_plan_actions"] = 10_000
    fields["policy"]["maximum_local_search_iterations"] = 256
    return fields


def _host() -> dict[str, object]:
    memory_kib = 0
    try:
        for line in Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
            if line.startswith("MemTotal:"):
                memory_kib = int(line.split()[1])
                break
    except (OSError, ValueError, IndexError):
        pass
    compiler = subprocess.run(
        ["cmake", "-LA", "-N", str(ROOT / "build")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "kernel": platform.release(),
        "platform": platform.platform(),
        "cpu": platform.processor() or platform.machine(),
        "logical_cpu_count": os.cpu_count(),
        "memory_bytes": memory_kib * 1024,
        "python": sys.version.split()[0],
        "git_commit": subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=False
        ).stdout.strip(),
        "git_status": subprocess.run(
            ["git", "status", "--short"], cwd=ROOT, capture_output=True, text=True, check=False
        ).stdout.splitlines(),
        "cxx_compiler": next(
            (
                line.split("=", 1)[-1]
                for line in compiler.stdout.splitlines()
                if line.startswith("CMAKE_CXX_COMPILER:FILEPATH=")
            ),
            "unknown",
        ),
        "build_type": "Release"
        if "CMAKE_BUILD_TYPE:STRING=Release" in compiler.stdout
        else "unknown",
    }


def _invoke(command: list[str], timeout: int = 90) -> float:
    started = time.perf_counter()
    result = subprocess.run(command, cwd=ROOT, capture_output=True, timeout=timeout, check=False)
    elapsed = time.perf_counter() - started
    if result.returncode:
        detail = (result.stdout + result.stderr).decode(errors="replace")
        raise RuntimeError(f"{command[0]} exited {result.returncode}: {detail}")
    return elapsed


def _p95(values: list[float]) -> float:
    return sorted(values)[math.ceil(len(values) * 0.95) - 1]


def run_benchmark(
    output_root: Path, workload_count: int = 1_000, node_count: int = 500, iterations: int = 30
) -> Path:
    if iterations < 20 or iterations > 200:
        raise ValueError("benchmark requires 20 to 200 measured iterations")
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
    run_dir = output_root / "phase2" / f"{run_id}-{workload_count}w-{node_count}n"
    run_dir.mkdir(parents=True, exist_ok=False)
    (run_dir / "policy_outputs").mkdir()
    fields = _stress_snapshot(workload_count, node_count)
    snapshot_bytes, finalized = encode_snapshot(fields)
    snapshot_path = run_dir / "policy_outputs/snapshot.pb"
    snapshot_path.write_bytes(snapshot_bytes)
    snapshot_text = decode_text(
        "fleetright.v1.OptimizationSnapshot", "fleetright/v1/model.proto", snapshot_bytes
    )
    (run_dir / "policy_outputs/snapshot.textproto").write_text(snapshot_text, encoding="utf-8")
    planner = ROOT / "build/fleetright-plan"
    verifier = ROOT / "build/fleetright-verify"
    core_benchmark = ROOT / "build/fleetright-core-benchmark"
    if not planner.is_file() or not verifier.is_file() or not core_benchmark.is_file():
        raise RuntimeError("C++ planner/verifier missing; run scripts/bootstrap.sh first")

    planner_seconds: list[float] = []
    verifier_seconds: list[float] = []
    plan_hashes: list[str] = []
    verifier_hashes: list[str] = []
    first_plan: bytes | None = None
    first_verification: bytes | None = None
    with tempfile.TemporaryDirectory(prefix="fleetright-benchmark-") as temporary:
        plan_path = Path(temporary) / "plan.pb"
        result_path = Path(temporary) / "verification.pb"
        for _ in range(iterations):
            planner_seconds.append(_invoke([str(planner), str(snapshot_path), str(plan_path)]))
            plan_bytes = plan_path.read_bytes()
            plan_hashes.append(_sha256(plan_bytes))
            if first_plan is None:
                first_plan = plan_bytes
            elif plan_bytes != first_plan:
                raise RuntimeError("optimizer output changed during identical benchmark replay")
            verifier_seconds.append(
                _invoke([str(verifier), str(snapshot_path), str(plan_path), str(result_path)])
            )
            result_bytes = result_path.read_bytes()
            verifier_hashes.append(_sha256(result_bytes))
            if first_verification is None:
                first_verification = result_bytes
            elif result_bytes != first_verification:
                raise RuntimeError("verifier output changed during identical benchmark replay")
    assert first_plan is not None and first_verification is not None
    plan_text = decode_text(
        "fleetright.v1.OptimizationPlan", "fleetright/v1/optimizer.proto", first_plan
    )
    verification_text = decode_text(
        "fleetright.v1.VerificationResult", "fleetright/v1/optimizer.proto", first_verification
    )
    accepted_line = next(
        (
            line.strip()
            for line in verification_text.splitlines()
            if line.strip().startswith("accepted:")
        ),
        "accepted: false",
    )
    accepted = accepted_line == "accepted: true"
    if not accepted:
        raise RuntimeError("independent verifier rejected the scale benchmark plan")
    (run_dir / "policy_outputs/plan.pb").write_bytes(first_plan)
    core_run = subprocess.run(
        [
            str(core_benchmark),
            str(snapshot_path),
            str(run_dir / "policy_outputs/plan.pb"),
            str(iterations),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    if core_run.returncode:
        raise RuntimeError(f"C++ core benchmark failed: {core_run.stderr.strip()}")
    core_timings = json.loads(core_run.stdout)
    if (
        core_timings.get("iterations") != iterations
        or not core_timings.get("accepted")
        or len(core_timings.get("planner_seconds", [])) != iterations
        or len(core_timings.get("verifier_seconds", [])) != iterations
    ):
        raise RuntimeError("C++ core benchmark returned incomplete or unverified timings")
    (run_dir / "policy_outputs/plan.textproto").write_text(plan_text, encoding="utf-8")
    (run_dir / "policy_outputs/verification.pb").write_bytes(first_verification)
    (run_dir / "policy_outputs/verification.textproto").write_text(
        verification_text, encoding="utf-8"
    )
    raw = {
        "schema_version": 1,
        "planner_process_seconds": planner_seconds,
        "verifier_process_seconds": verifier_seconds,
        "planner_sha256": plan_hashes,
        "verifier_sha256": verifier_hashes,
    }
    _json(run_dir / "raw_timings.json", raw)
    _json(run_dir / "core_timings.json", core_timings)
    planner_core_p95 = _p95(core_timings["planner_seconds"])
    verifier_core_p95 = _p95(core_timings["verifier_seconds"])
    summary = {
        "scenario_workloads": workload_count,
        "scenario_nodes": node_count,
        "profile_alternatives": 1,
        "iterations": iterations,
        "planner_p50_seconds": sorted(planner_seconds)[iterations // 2],
        "planner_p95_seconds": _p95(planner_seconds),
        "verifier_p50_seconds": sorted(verifier_seconds)[iterations // 2],
        "verifier_p95_seconds": _p95(verifier_seconds),
        "planner_core_p50_seconds": sorted(core_timings["planner_seconds"])[iterations // 2],
        "planner_core_p95_seconds": planner_core_p95,
        "verifier_core_p50_seconds": sorted(core_timings["verifier_seconds"])[iterations // 2],
        "verifier_core_p95_seconds": verifier_core_p95,
        "planner_target_seconds": 0.250,
        "verifier_target_seconds": 0.025,
        "planner_target_met": planner_core_p95 <= 0.250,
        "verifier_target_met": verifier_core_p95 <= 0.025,
        "planner_cli_target_met": _p95(planner_seconds) <= 0.250,
        "verifier_cli_target_met": _p95(verifier_seconds) <= 0.025,
        "independent_verifier_accepted": accepted,
        "planner_deterministic_replay": len(set(plan_hashes)) == 1,
        "verifier_deterministic_replay": len(set(verifier_hashes)) == 1,
        "plan_id": next(
            (
                line.strip().split(":", 1)[1].strip().strip('"')
                for line in plan_text.splitlines()
                if line.strip().startswith("plan_id:")
            ),
            "",
        ),
    }
    _json(run_dir / "summary.json", summary)
    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "scenario_id": "optimizer-scale-profile-portfolio-v1",
        "git_commit": _host()["git_commit"],
        "host": _host(),
        "started_at_utc": run_id,
        "completed_at_utc": datetime.now(UTC).isoformat(),
        "core_benchmark_iterations": iterations,
        "benchmark_method": "direct C++ API timing plus end-to-end CLI timing",
        "snapshot_sha256": finalized["content_sha256"],
        "policy_sha256": _sha256(json.dumps(fields["policy"], sort_keys=True).encode()),
        "capacity_profile_sha256": [
            profile["profile_sha256"] for profile in finalized["capacity_profiles"]
        ],
        "machine_catalog_sha256": finalized["machine_catalog"]["catalog_sha256"],
        "optimizer_build_id": next(
            (
                line.split(":", 1)[1].strip().strip('"')
                for line in plan_text.splitlines()
                if line.strip().startswith("optimizer_build_id:")
            ),
            "",
        ),
        "verifier_build_id": next(
            (
                line.split(":", 1)[1].strip().strip('"')
                for line in verification_text.splitlines()
                if line.strip().startswith("verifier_build_id:")
            ),
            "",
        ),
        "plan_sha256": _sha256(first_plan),
        "verification_sha256": _sha256(first_verification),
    }
    manifest["manifest_sha256"] = _sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    )
    _json(run_dir / "manifest.json", manifest)
    report = [
        "# FleetRight planner scale benchmark",
        "",
        f"Scenario: {workload_count} workloads, {node_count} nodes, "
        f"{iterations} measured core API runs and CLI runs.",
        "",
        f"Planner API p95: {summary['planner_core_p95_seconds'] * 1000:.2f} ms "
        f"(target 250 ms: {'PASS' if summary['planner_target_met'] else 'FAIL'}).",
        f"Verifier API p95: {summary['verifier_core_p95_seconds'] * 1000:.2f} ms "
        f"(target 25 ms: {'PASS' if summary['verifier_target_met'] else 'FAIL'}).",
        f"End-to-end CLI p95 including process start and protobuf file I/O: planner "
        f"{summary['planner_p95_seconds'] * 1000:.2f} ms, verifier "
        f"{summary['verifier_p95_seconds'] * 1000:.2f} ms.",
        "Independent verifier: PASS; deterministic planner/verifier replay: "
        f"{summary['planner_deterministic_replay']}/{summary['verifier_deterministic_replay']}.",
        "",
        "Raw timings and canonical protobuf inputs/outputs are retained beside this report.",
    ]
    (run_dir / "report.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure deterministic planner/verifier CLI p95")
    parser.add_argument("--output", type=Path, default=ROOT / "results")
    parser.add_argument("--workloads", type=int, default=1_000)
    parser.add_argument("--nodes", type=int, default=500)
    parser.add_argument("--iterations", type=int, default=30)
    args = parser.parse_args()
    print(run_benchmark(args.output, args.workloads, args.nodes, args.iterations))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
