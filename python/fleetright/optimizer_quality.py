"""Optimizer quality evidence on the real C++ planner/verifier (no simulation involved).

(a) optimality gap against the exact oracle on seeded small instances, (b) infeasibility
agreement, (c) deterministic replay, (d) independent-verifier rejection of mutated plans/snapshots.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import statistics
import subprocess
import tempfile
from copy import deepcopy
from pathlib import Path

from fleetright.contracts import decode_text, encode_snapshot, encode_text
from fleetright.oracle import exact_single_workload
from fleetright.scenario import oracle_snapshot_fields

ROOT = Path(__file__).resolve().parents[2]
PLAN, VERIFY = ROOT / "build/fleetright-plan", ROOT / "build/fleetright-verify"
SEEDS = tuple(range(1, 61))
MUTATIONS = (
    "colocate_replicas",
    "unknown_node",
    "drop_placement",
    "tamper_plan_id",
    "tamper_cost",
    "unhealthy_node_in_snapshot",
)


def _instance(seed: int) -> dict:
    rng = random.Random(seed)
    fields = oracle_snapshot_fields()
    catalog = {m["machine_type"]: m for m in fields["machine_catalog"]["machines"]}
    template = fields["nodes"][0]
    nodes = []
    for index in range(rng.randint(3, 10)):
        kind = rng.choice(("small", "medium", "large"))
        machine = catalog[kind]
        nodes.append(
            {
                **deepcopy(template),
                "node_id": f"node-{index:02d}",
                "machine_type": kind,
                "allocatable": dict(machine["allocatable"]),
                "hourly_cost_micros": machine["hourly_cost_micros"],
                "labels": [("zone", f"zone-{index % 3}"), ("rack", f"rack-{index}")],
            }
        )
    fields["nodes"] = nodes
    fields["workloads"][0]["demand_qps_milli"] = rng.randint(150_000, 1_500_000)
    fields["workloads"][0]["concurrency_bound"] = rng.randint(30, 400)
    fields["current_placements"] = [
        {**p, "node_id": nodes[i % len(nodes)]["node_id"]}
        for i, p in enumerate(fields["current_placements"])
    ]
    # even seeds leave the per-workload move budget non-binding (a fair oracle comparison, the
    # oracle ignores it); odd seeds bind it at 2 to exercise planner/verifier agreement.
    fields["workloads"][0]["max_moves"] = 100 if seed % 2 == 0 else 2
    fields["snapshot_id"] = f"snapshot-quality-{seed:03d}"
    return fields


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, capture_output=True, text=True, check=False, timeout=60)


def _verify(snapshot: Path, plan: Path, out: Path) -> tuple[bool, list[str]]:
    """Return (accepted, names of failed invariants) from the verifier CLI's summary lines."""
    result = _run([str(VERIFY), str(snapshot), str(plan), str(out)])
    failed = [line.split(":", 1)[0].strip() for line in result.stderr.splitlines() if ":" in line]
    return result.returncode == 0 and "accepted=true" in result.stdout, failed


def _field(text: str, name: str) -> str | None:
    match = re.search(rf"^{name}: (.+)$", text, re.M)
    return match.group(1).strip('"') if match else None


def _reseal(text: str) -> str:
    """Recompute plan_id (SHA-256 of the deterministic encoding without it) after a mutation."""
    body = re.sub(r'(?m)^plan_id: "[0-9a-f]+"\n', "", text, count=1)
    payload = encode_text("fleetright.v1.OptimizationPlan", "fleetright/v1/optimizer.proto", body)
    return re.sub(
        r"(?m)^schema_version: 1\n",
        f'schema_version: 1\nplan_id: "{hashlib.sha256(payload).hexdigest()}"\n',
        body,
        count=1,
    )


def _mutate(text: str, kind: str) -> str:
    if kind == "colocate_replicas":
        nodes = re.findall(r'node_id: "([^"]+)"', text)
        return text.replace(f'node_id: "{nodes[1]}"', f'node_id: "{nodes[0]}"', 1)
    if kind == "unknown_node":
        return re.sub(
            r'(placements \{.*?node_id: )"[^"]+"',
            r'\1"node-does-not-exist"',
            text,
            count=1,
            flags=re.S,
        )
    if kind == "drop_placement":
        return re.sub(r"placements \{.*?\n\}\n", "", text, count=1, flags=re.S)
    if kind == "tamper_plan_id":
        return re.sub(r'(?m)^plan_id: "([0-9a-f]{8})', r'plan_id: "ffffffff', text, count=1)
    return re.sub(
        r"(?m)^proposed_cost_micros_per_hour: \d+",
        "proposed_cost_micros_per_hour: 1",
        text,
        count=1,
    )


def run(workdir: Path) -> dict:
    gaps: list[float] = []
    budget_gaps: list[float] = []
    cases: list[dict] = []
    rejection = {kind: {"trials": 0, "rejected": 0} for kind in MUTATIONS}
    clean_accept = replay_same = 0
    for seed in SEEDS:
        fields = _instance(seed)
        payload, _ = encode_snapshot(fields)
        snapshot, plan, replay, result = (workdir / n for n in ("s.pb", "p.pb", "r.pb", "v.pb"))
        snapshot.write_bytes(payload)
        planned = _run([str(PLAN), str(snapshot), str(plan)])
        _run([str(PLAN), str(snapshot), str(replay)])
        same = plan.read_bytes() == replay.read_bytes()
        replay_same += same
        text = decode_text(
            "fleetright.v1.OptimizationPlan", "fleetright/v1/optimizer.proto", plan.read_bytes()
        )
        feasible = _field(text, "feasible") == "true"
        assert _reseal(text) == text, "plan_id reseal must reproduce the planner's identifier"
        try:
            exact = exact_single_workload(fields).objective_micros_per_hour
        except ValueError:
            exact = None
        case = {"seed": seed, "nodes": len(fields["nodes"]), "planner_feasible": feasible,
                "oracle_feasible": exact is not None, "deterministic": same,
                "planner_exit": planned.returncode}  # fmt: skip
        if feasible:
            accepted, _ = _verify(snapshot, plan, result)
            clean_accept += accepted
            case["verifier_accepted"] = accepted
            heuristic = int(_field(text, "proposed_objective_cost_micros") or 0)
            case["heuristic_cost"], case["oracle_cost"] = heuristic, exact
            if exact:
                case["gap"] = heuristic / exact - 1
                case["move_budget_binding"] = seed % 2 == 1
                (gaps if seed % 2 == 0 else budget_gaps).append(case["gap"])
            for kind in MUTATIONS:
                rejection[kind]["trials"] += 1
                bad_snapshot, bad_plan = snapshot, workdir / "bad.pb"
                if kind == "unhealthy_node_in_snapshot":
                    stale = deepcopy(fields)
                    used = re.findall(r'node_id: "([^"]+)"', text)[0]
                    for node in stale["nodes"]:
                        if node["node_id"] == used:
                            node["health"] = "NODE_HEALTH_UNHEALTHY"
                    bad_snapshot = workdir / "bad-snapshot.pb"
                    bad_snapshot.write_bytes(encode_snapshot(stale)[0])
                    bad_plan = plan
                else:
                    try:
                        mutated = _mutate(text, kind)
                        if kind != "tamper_plan_id":  # isolate the semantic invariant under test
                            mutated = _reseal(mutated)
                    except IndexError:
                        rejection[kind]["trials"] -= 1  # plan too small for this mutation
                        continue
                    bad_plan.write_bytes(
                        encode_text(
                            "fleetright.v1.OptimizationPlan",
                            "fleetright/v1/optimizer.proto",
                            mutated,
                        )
                    )
                accepted_bad, failed = _verify(bad_snapshot, bad_plan, result)
                rejection[kind]["rejected"] += not accepted_bad
                for name in failed:
                    reasons = rejection[kind].setdefault("failed_invariants", {})
                    reasons[name] = reasons.get(name, 0) + 1
        cases.append(case)
    both_infeasible = sum(not c["planner_feasible"] and not c["oracle_feasible"] for c in cases)
    disagree = [c["seed"] for c in cases if c["planner_feasible"] != c["oracle_feasible"]]
    return {
        "instances": len(cases),
        "planner_feasible": sum(c["planner_feasible"] for c in cases),
        "oracle_feasible": sum(c["oracle_feasible"] for c in cases),
        "feasibility_agreement": both_infeasible
        + sum(c["planner_feasible"] and c["oracle_feasible"] for c in cases),
        "feasibility_disagreement_seeds": disagree,
        "verifier_accepted_clean_plans": clean_accept,
        "deterministic_replay": f"{replay_same}/{len(cases)}",
        "gap_with_move_budget_binding": {
            "n": len(budget_gaps),
            "optimal": sum(g == 0 for g in budget_gaps),
            "mean": statistics.fmean(budget_gaps) if budget_gaps else None,
            "max": max(budget_gaps) if budget_gaps else None,
        },
        "gap": {
            "n": len(gaps),
            "optimal": sum(g == 0 for g in gaps),
            "mean": statistics.fmean(gaps) if gaps else None,
            "max": max(gaps) if gaps else None,
            "below_oracle_model_mismatch": sum(g < 0 for g in gaps),
        },
        "mutation_rejection": rejection,
        "cases": cases,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=ROOT / "results/phase3/optimizer-quality.json"
    )
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="fleetright-quality-") as tmp:
        report = run(Path(tmp))
    report["seeds"] = [SEEDS[0], SEEDS[-1]]
    report["sha256"] = hashlib.sha256(
        json.dumps(report["cases"], sort_keys=True).encode()
    ).hexdigest()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
