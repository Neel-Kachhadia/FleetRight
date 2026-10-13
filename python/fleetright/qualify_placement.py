"""Placement/feasibility scenarios on the real C++ planner and independent verifier.

These are deterministic snapshot cases (no simulator): the demo cluster with one targeted change
each. `expect` is declared before running; a case passes only if the actual outcome matches.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tempfile
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path

from fleetright.contracts import decode_text, encode_snapshot
from fleetright.scenario import demo_snapshot_fields

ROOT = Path(__file__).resolve().parents[2]
PLAN, VERIFY = ROOT / "build/fleetright-plan", ROOT / "build/fleetright-verify"


def _no_change(fields: dict) -> None:
    return None


def _node_unavailable(fields: dict) -> None:
    for node in fields["nodes"]:
        if node["node_id"] == "node-00":
            node["health"] = "NODE_HEALTH_UNHEALTHY"


def _one_zone(fields: dict) -> None:
    for node in fields["nodes"]:
        node["labels"] = [("zone", "zone-0"), ("rack", node["labels"][1][1])]


def _tiny_cluster(fields: dict) -> None:
    fields["nodes"] = fields["nodes"][:1]
    fields["current_placements"] = [
        p for p in fields["current_placements"] if p["node_id"] == "node-00"
    ]


def _demand_beyond_max(fields: dict) -> None:
    fields["workloads"][0]["demand_qps_milli"] = 50_000_000


def _stale_profiles(fields: dict) -> None:
    for profile in fields["capacity_profiles"]:
        profile["generated_at_ms"] -= 10 * 3_600_000


def _conflicting_bounds(fields: dict) -> None:
    fields["workloads"][0]["min_replicas"] = 13  # > max_replicas 12


def _extreme_valid(fields: dict) -> None:
    for workload in fields["workloads"]:
        workload["max_replicas"] = 100_000
        workload["demand_qps_milli"] = 2**40


def _single_feasible_node(fields: dict) -> None:
    for workload in fields["workloads"][1:]:
        workload["demand_qps_milli"] = 1
        workload["concurrency_bound"] = 1
    fields["workloads"] = fields["workloads"][:1]
    fields["capacity_profiles"] = fields["capacity_profiles"][:1]
    fields["workloads"][0]["failure_tolerance"] = "FAILURE_TOLERANCE_NONE"
    fields["workloads"][0]["min_topology_domains"] = 0
    fields["workloads"][0]["min_replicas"] = fields["workloads"][0]["availability_floor"] = 1
    fields["workloads"][0]["demand_qps_milli"] = 100_000
    fields["workloads"][0]["concurrency_bound"] = 10
    fields["nodes"] = fields["nodes"][:3]
    fields["current_placements"] = [
        {**p, "node_id": "node-00"} for p in fields["current_placements"][:1]
    ]


def _identical_nodes(fields: dict) -> None:
    _single_feasible_node(fields)
    fields["workloads"][0]["min_replicas"] = fields["workloads"][0]["availability_floor"] = 2
    fields["workloads"][0]["demand_qps_milli"] = 700_000
    fields["workloads"][0]["concurrency_bound"] = 100
    fields["current_placements"] = [
        {"workload_id": "cpu-api", "replica_id": f"cpu-api-{i}", "node_id": f"node-0{i}"}
        for i in range(2)
    ]


CASES: tuple[tuple[str, str, Callable[[dict], None], str], ...] = (
    ("S07", "mixed CPU/memory workloads on the demo cluster", _no_change, "accepted"),
    ("S16", "one in-use node unexpectedly unhealthy", _node_unavailable, "accepted_or_rejected"),
    ("S17a", "topology: 3 zones, min 2 domains (baseline)", _no_change, "accepted"),
    ("S17b", "topology bottleneck: every node in one zone", _one_zone, "infeasible"),
    ("S18a", "insufficient capacity: one node for the whole fleet", _tiny_cluster, "infeasible"),
    ("S18b", "demand beyond the workload's max replicas", _demand_beyond_max, "infeasible"),
    ("E01", "conflicting bounds (min > max replicas)", _conflicting_bounds, "infeasible"),
    ("E02", "stale capacity profiles block rightsizing", _stale_profiles, "hold_or_infeasible"),
    ("E03", "extreme valid inputs (2^40 milli-qps, 100k max)", _extreme_valid, "infeasible"),
    ("E04", "single workload, one replica, three nodes", _single_feasible_node, "accepted"),
    ("E05", "many equal-cost placements are deterministic", _identical_nodes, "accepted"),
)  # fmt: skip


def _field(text: str, name: str) -> str | None:
    match = re.search(rf"^{name}: (.+)$", text, re.M)
    return match.group(1).strip('"') if match else None


def run_case(workdir: Path, fields: dict) -> dict:
    snapshot, plan, replay, result = (workdir / n for n in ("s.pb", "p.pb", "r.pb", "v.pb"))
    snapshot.write_bytes(encode_snapshot(fields)[0])
    first = subprocess.run([str(PLAN), str(snapshot), str(plan)], capture_output=True, text=True)
    subprocess.run([str(PLAN), str(snapshot), str(replay)], capture_output=True, text=True)
    if first.returncode:
        return {"outcome": "planner_error", "detail": first.stderr.strip()[:200]}
    text = decode_text(
        "fleetright.v1.OptimizationPlan", "fleetright/v1/optimizer.proto", plan.read_bytes()
    )
    verified = subprocess.run(
        [str(VERIFY), str(snapshot), str(plan), str(result)], capture_output=True, text=True
    )
    accepted = "accepted=true" in verified.stdout
    feasible = _field(text, "feasible") == "true"
    used = sorted(set(re.findall(r'placements \{[^}]*node_id: "([^"]+)"', text, re.S)))
    return {
        "outcome": "accepted" if accepted else "infeasible" if not feasible else "rejected",
        "planner_feasible": feasible,
        "verifier_accepted": accepted,
        "deterministic": plan.read_bytes() == replay.read_bytes(),
        "infeasible_reason": _field(text, "infeasible_reason"),
        "rejection_codes": re.findall(r"rejection_codes: (\w+)", text),
        "current_cost_micros_per_hour": int(_field(text, "current_cost_micros_per_hour") or 0),
        "proposed_cost_micros_per_hour": int(_field(text, "proposed_cost_micros_per_hour") or 0),
        "nodes_used": len(used),
        "verifier_failed_invariants": [
            line.split(":", 1)[0].strip() for line in verified.stderr.splitlines() if ":" in line
        ],
    }


def run() -> dict:
    rows = []
    with tempfile.TemporaryDirectory(prefix="fleetright-placement-") as tmp:
        for case_id, description, change, expect in CASES:
            fields = deepcopy(demo_snapshot_fields())
            change(fields)
            try:
                actual = run_case(Path(tmp), fields)
            except Exception as error:  # input rejected before planning is a valid rejection
                actual = {"outcome": "input_rejected", "detail": str(error)[:200]}
            ok = {
                "accepted": actual["outcome"] == "accepted",
                "infeasible": actual["outcome"] in {"infeasible", "input_rejected"},
                "accepted_or_rejected": actual["outcome"] in {"accepted", "infeasible", "rejected"},
                "hold_or_infeasible": actual["outcome"] in {"accepted", "infeasible"}
                and (
                    actual["outcome"] == "infeasible"
                    or actual["proposed_cost_micros_per_hour"]
                    >= actual["current_cost_micros_per_hour"]
                ),
            }[expect]
            ok = ok and actual.get("deterministic", True)
            rows.append(
                {"id": case_id, "case": description, "expect": expect, "pass": ok, **actual}
            )
    return {"cases": rows, "passed": sum(r["pass"] for r in rows), "total": len(rows)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "results/phase3/placement.json")
    args = parser.parse_args()
    report = run()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
