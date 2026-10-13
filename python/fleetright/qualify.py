"""Paired, seeded policy qualification on the declared simulator (Prompt 3, scientific validation).

Everything here is SIMULATED: synthetic traces, the declared convex latency model and an
illustrative cost model. It is not production or cloud-billing evidence. See
docs/benchmark-methodology.md for the protocol; run `python -m fleetright.qualify --help`.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import random
import shutil
import statistics
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from functools import cache, lru_cache
from pathlib import Path

from fleetright.baselines import (
    FixedTarget,
    HpaLikeBaseline,
    LookaheadTarget,
    ThresholdBaseline,
    lookahead_schedule,
    static_peak_replicas,
)
from fleetright.policies import FleetRightPolicy, required_replicas
from fleetright.scenario import demo_snapshot_fields
from fleetright.simulator import SimulationConfig, simulate
from fleetright.traces import DemandPoint, Trace, generate_trace, transform_trace
from fleetright.units import multiply_ppm_floor

ROOT = Path(__file__).resolve().parents[2]
COST_MODEL_VERSION = "cost-model-v1"  # synthetic: demo machine catalog 'small' = 0.05 USD/h
WINDOW_MS = 60_000
WINDOWS = 360
WARMUP = 120  # windows 0..119 run for every policy but are not scored
EVENT = 200  # window of level shift / drift / node loss (inside the scored region)
EVAL_SEEDS = tuple(range(100, 120))
TUNE_SEEDS = tuple(range(1, 6))
EVAL_BASE_QPS_MILLI = 600_000
TUNE_BASE_QPS_MILLI = 450_000
JITTER_PERMILLE = 30
POLICIES = (
    "static_peak_reserve",
    "naive_threshold",
    "hpa_like",
    "fleetright_reactive",
    "fleetright_predictive",
    "fleetright_predictive_same_headroom",  # ablation: predictive at reactive's tuned knob
    "offline_lookahead_oracle",
)
FLEETRIGHT = ("fleetright_reactive", "fleetright_predictive")
DEFAULT_PARAMS = {
    "static_peak_reserve": 200_000,
    "naive_threshold": 800_000,
    "hpa_like": 650_000,
    "fleetright_reactive": 1_000_000,
    "fleetright_predictive": 1_000_000,
}
GRIDS = {
    "static_peak_reserve": (0, 50_000, 100_000, 200_000, 300_000, 400_000, 500_000),
    "naive_threshold": (500_000, 600_000, 700_000, 800_000),
    "hpa_like": (500_000, 600_000, 700_000, 800_000),
    "fleetright_reactive": (600_000, 700_000, 800_000, 900_000, 1_000_000),
    "fleetright_predictive": (600_000, 700_000, 800_000, 900_000, 1_000_000),
}
TUNE_FAMILIES = ("steady", "ramp", "diurnal", "bursty")
VIOLATION_BUDGET = 0.03  # tuning rule: (hard+soft) violation windows <= 3% of scored windows


@dataclass(frozen=True)
class Scenario:
    sid: str
    name: str
    family: str
    workloads: tuple[str, ...] = ("cpu-api", "memory-api", "queued-api")
    startup_ms: int | None = None
    missing_every: int = 0
    shift_permille: int | None = None
    drift_ppm: int | None = None
    loss_replicas: int | None = None
    note: str = ""


ALL = ("cpu-api", "memory-api", "queued-api")
SCENARIOS = (
    Scenario("S01", "steady", "steady"),
    Scenario("S02", "gradual increase", "ramp"),
    Scenario("S03", "daily peaks (compressed 12-window day)", "diurnal"),
    Scenario("S04", "flash crowd 5x", "flash_crowd"),
    Scenario("S05", "CPU-intensive workload", "bursty", ("cpu-api",)),
    Scenario("S06", "memory-intensive workload", "memory_heavy", ("memory-api",)),
    Scenario("S08", "slow-starting replicas (150 s)", "diurnal", startup_ms=150_000),
    Scenario("S09", "multiple tenants, different SLOs", "bursty"),
    Scenario("S10", "noisy-neighbour demand", "noisy_tenant"),
    Scenario("S11", "correlated surges", "correlated_spike"),
    Scenario("S12", "forecast underprediction (+80% level shift)", "diurnal", shift_permille=1_800),
    Scenario("S13", "forecast overprediction (-50% level shift)", "diurnal", shift_permille=500),
    Scenario("S14", "stale telemetry (1 of 3 windows missing)", "diurnal", missing_every=3),
    Scenario("S15", "capacity profile drift (-20% true capacity)", "diurnal", drift_ppm=800_000),
    Scenario("S16", "one node lost (1 replica per tenant)", "steady", loss_replicas=1),
)
TUNING_SCENARIOS = tuple(Scenario(f"T-{f}", f, f) for f in TUNE_FAMILIES)


def _sha(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


@lru_cache(maxsize=1)
def _contexts() -> dict[str, dict]:
    fields = demo_snapshot_fields()
    result: dict[str, dict] = {}
    for workload in fields["workloads"]:
        profile = next(
            p for p in fields["capacity_profiles"] if p["workload_id"] == workload["workload_id"]
        )
        point = profile["safe_points"][0]
        factors = (
            workload["profile_confidence_factor_ppm"],
            workload["model_accuracy_factor_ppm"],
            workload["degraded_cluster_factor_ppm"],
        )
        result[workload["workload_id"]] = {
            "workload": workload,
            "allocation": profile["allocation"],
            "eff_qps": multiply_ppm_floor(point["safe_qps_milli"], *factors),
            "eff_conc": multiply_ppm_floor(point["safe_concurrency"], *factors),
            "base_latency_ms": max(1, int(point["p99_upper_ms"] * 0.55)),
        }
    return result


def build_trace(scenario: Scenario, seed: int, base_qps_milli: int) -> Trace:
    trace = generate_trace(
        scenario.family, seed, WINDOWS, WINDOW_MS, scenario.workloads, base_qps_milli,
        jitter_permille=JITTER_PERMILLE,
    )  # fmt: skip
    if scenario.shift_permille is not None:
        permille = scenario.shift_permille

        def shift(index: int, point: DemandPoint) -> DemandPoint:
            if index < EVENT:
                return point
            qps = point.qps_milli * permille // 1_000
            return replace(
                point, qps_milli=qps, concurrency=max(1, point.concurrency * permille // 1_000)
            )

        trace = transform_trace(trace, f"{scenario.family}+shift{permille}", shift)
    return trace


def _config(ctx: dict, scenario: Scenario) -> SimulationConfig:
    workload = ctx["workload"]
    initial = max(workload["min_replicas"], 1)
    drift = (EVENT * WINDOW_MS, scenario.drift_ppm) if scenario.drift_ppm else None
    loss = (EVENT * WINDOW_MS, scenario.loss_replicas) if scenario.loss_replicas else None
    return SimulationConfig(
        workload_id=workload["workload_id"],
        safe_qps_per_replica_milli=ctx["eff_qps"],
        safe_concurrency_per_replica=ctx["eff_conc"],
        base_latency_ms=ctx["base_latency_ms"],
        latency_slo_ms=workload["latency_slo_ms"],
        initial_replicas=initial,
        min_replicas=workload["min_replicas"],
        max_replicas=workload["max_replicas"],
        startup_ms=scenario.startup_ms or workload["startup_ms"],
        readiness_stabilization_ms=workload["readiness_stabilization_ms"],
        capacity_drift=drift,
        replica_loss=loss,
    )


def _lead_ms(config: SimulationConfig) -> int:
    return (
        config.actuation_delay_ms
        + config.node_activation_ms
        + config.startup_ms
        + config.readiness_stabilization_ms
    )


def make_policy(name: str, ctx: dict, config: SimulationConfig, trace_points, param: int):
    workload = ctx["workload"]
    lo, hi = workload["min_replicas"], workload["max_replicas"]
    eff_q, eff_c = ctx["eff_qps"], ctx["eff_conc"]
    if name == "static_peak_reserve":
        peak_q = max(p.qps_milli for p in trace_points)
        peak_c = max(p.concurrency for p in trace_points)
        return FixedTarget(min(hi, static_peak_replicas(peak_q, peak_c, eff_q, eff_c, lo, param)))
    if name == "naive_threshold":
        return ThresholdBaseline(scale_up_threshold_ppm=param, min_replicas=lo, max_replicas=hi)
    if name == "hpa_like":
        return HpaLikeBaseline(
            target_cpu_ppm=param,
            target_qps_per_replica_milli=max(1, eff_q * param // 1_000_000),
            target_concurrency_per_replica=max(1, eff_c * param // 1_000_000),
            min_replicas=lo,
            max_replicas=hi,
        )
    if name == "offline_lookahead_oracle":
        delay = -(-_lead_ms(config) // WINDOW_MS)
        schedule = lookahead_schedule(
            [p.qps_milli for p in trace_points],
            [p.concurrency for p in trace_points],
            eff_q, eff_c, lo, hi, delay, 0,
        )  # fmt: skip
        return LookaheadTarget(tuple(schedule))
    return FleetRightPolicy(
        effective_qps_milli=eff_q,
        effective_concurrency=eff_c,
        min_replicas=lo,
        max_replicas=hi,
        availability_floor=workload["availability_floor"],
        n_minus_1=workload["failure_tolerance"] == "FAILURE_TOLERANCE_N_MINUS_1",
        max_scale_down_ppm=workload["max_scale_down_fraction_ppm"],
        window_ms=WINDOW_MS,
        lead_ms=_lead_ms(config),
        target_utilization_ppm=param,
        predictive=name != "fleetright_reactive",
    )


@cache
def _nodes_needed(counts: tuple[int, ...], order: tuple[str, ...]) -> int:
    """First-fit-decreasing node count with the planner's one-replica-per-workload-per-node rule."""
    fields = demo_snapshot_fields()
    node = next(n for n in fields["nodes"] if n["machine_type"] == "small")
    keep = 1_000_000 - fields["policy"]["headroom_ppm"]
    cap = [
        (node["allocatable"][k] - node["reserved"][k]) * keep // 1_000_000
        for k in ("cpu_millicores", "memory_mib", "network_mbps")
    ]
    items = []
    for workload_id, count in zip(order, counts, strict=True):
        a = _contexts()[workload_id]["allocation"]
        shape = (a["cpu_millicores"], a["memory_mib"], a["network_mbps"])
        items += [(max(s * 1000 // c for s, c in zip(shape, cap, strict=True)), workload_id, shape)
                  for _ in range(count)]  # fmt: skip
    nodes: list[list] = []
    for _, workload_id, shape in sorted(items, key=lambda x: (-x[0], x[1])):
        for used in nodes:
            if workload_id not in used[3] and all(
                u + s <= c for u, s, c in zip(used[:3], shape, cap, strict=True)
            ):
                for i in range(3):
                    used[i] += shape[i]
                used[3].add(workload_id)
                break
        else:
            nodes.append([*shape, {workload_id}])
    return len(nodes)


def _percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, int(q * len(ordered) + 0.999999) - 1))]


def run_case(scenario: Scenario, seed: int, policy: str, param: int, base: int) -> dict:
    trace = build_trace(scenario, seed, base)
    node_price = next(n for n in demo_snapshot_fields()["nodes"] if n["machine_type"] == "small")[
        "hourly_cost_micros"
    ]
    soft = hard = 0
    violations: list[list] = []
    requests = failed = 0
    overload_windows = 0
    max_queue = 0
    scale_actions = replicas_added = 0
    latencies: list[float] = []
    capacity_gap = longest_gap = 0
    modes: dict[str, int] = {}
    desired_total = [0] * (WINDOWS - WARMUP)
    required_total = [0] * (WINDOWS - WARMUP)
    series: dict[str, list[int]] = {}
    for item in trace.workloads:
        ctx = _contexts()[item.workload_id]
        workload = ctx["workload"]
        config = _config(ctx, scenario)
        controller = make_policy(policy, ctx, config, item.points, param)
        result = simulate(trace, config, controller, missing_metric_every=scenario.missing_every)
        if isinstance(controller, FleetRightPolicy):
            for key, count in controller.forecast_modes.items():
                modes[key] = modes.get(key, 0) + count
        scored = result.windows[WARMUP:]
        series[item.workload_id] = [w.desired_replicas for w in scored]
        previous = result.windows[WARMUP - 1].desired_replicas
        run = 0
        for offset, w in enumerate(scored):
            index = WARMUP + offset
            desired_total[offset] += w.desired_replicas
            required_total[offset] += required_replicas(
                w.demand_qps_milli,
                item.points[index].concurrency,
                ctx["eff_qps"], ctx["eff_conc"], workload["min_replicas"],
                workload["availability_floor"], workload["max_replicas"], False,
            )  # fmt: skip
            kind = None
            if (
                w.error_rate_ppm > workload["error_slo_ppm"]
                or w.ready_replicas < workload["availability_floor"]
            ):
                kind = "hard"
                hard += 1
            elif w.latency_p99_ms > workload["latency_slo_ms"]:
                kind = "soft"
                soft += 1
            if kind:
                violations.append([item.workload_id, index, kind])
            run = run + 1 if kind else 0
            latencies.append(float(w.latency_p99_ms))
            arrivals = w.demand_qps_milli * trace.window_ms // 1_000
            requests += arrivals // 1_000
            failed += arrivals * w.error_rate_ppm // 1_000_000 // 1_000
            overload_windows += w.queue_growth_milli > 0
            max_queue = max(max_queue, w.queue_milli_requests)
            if w.desired_replicas != previous:
                scale_actions += 1
                replicas_added += max(0, w.desired_replicas - previous)
            previous = w.desired_replicas
            gap = w.ready_replicas < w.desired_replicas
            capacity_gap = capacity_gap + 1 if gap else 0
            longest_gap = max(longest_gap, capacity_gap)
    order = tuple(trace.workloads[i].workload_id for i in range(len(trace.workloads)))
    nodes = [
        _nodes_needed(tuple(series[w][o] for w in order), order) for o in range(WINDOWS - WARMUP)
    ]
    window_h = WINDOW_MS / 3_600_000
    shape_cpu = {w: _contexts()[w]["allocation"]["cpu_millicores"] for w in order}
    shape_mem = {w: _contexts()[w]["allocation"]["memory_mib"] for w in order}
    small = next(n for n in demo_snapshot_fields()["nodes"] if n["machine_type"] == "small")

    def idle(shape: dict[str, int], key: str) -> float:
        return statistics.mean(
            1 - sum(series[w][o] * shape[w] for w in order) / (nodes[o] * small["allocatable"][key])
            for o in range(len(nodes))
        )

    idle_cpu = idle(shape_cpu, "cpu_millicores")
    idle_mem = idle(shape_mem, "memory_mib")
    node_cost = sum(nodes) * node_price * window_h
    successes = requests - failed
    return {
        "scenario": scenario.sid,
        "seed": seed,
        "policy": policy,
        "param_ppm": param,
        "trace_sha256": trace.content_sha256,
        "node_hours": sum(nodes) * window_h,
        "node_cost_micros": round(node_cost),
        "replica_hours": sum(desired_total) * window_h,
        "required_replica_hours": sum(required_total) * window_h,
        "provisioned_over_required": sum(desired_total) / max(1, sum(required_total)),
        "idle_cpu_fraction": idle_cpu,
        "idle_memory_fraction": idle_mem,
        "requests": requests,
        "failed_requests": failed,
        "success_rate": successes / requests if requests else 1.0,
        "cost_per_million_successful_micros": round(node_cost * 1_000_000 / max(1, successes)),
        "soft_violation_windows": soft,
        "hard_violation_windows": hard,
        "scored_workload_windows": (WINDOWS - WARMUP) * len(order),
        "violation_windows": violations,
        "latency_p99_window_p50_ms": _percentile(latencies, 0.50),
        "latency_p99_window_p95_ms": _percentile(latencies, 0.95),
        "latency_p99_window_p99_ms": _percentile(latencies, 0.99),
        "latency_p99_window_max_ms": max(latencies),
        "overload_windows": overload_windows,
        "max_queue_milli_requests": max_queue,
        "scale_actions": scale_actions,
        "replicas_added": replicas_added,
        "node_release_events": sum(1 for a, b in zip(nodes, nodes[1:]) if b < a),  # noqa: B905
        "longest_capacity_gap_windows": longest_gap,
        "forecast_modes": modes,
    }


def _task(args: tuple) -> dict:
    scenario, seed, policy, param, base = args
    return run_case(scenario, seed, policy, param, base)


def _pool(tasks: list[tuple], workers: int) -> list[dict]:
    with ProcessPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(_task, tasks, chunksize=4))


def tune(workers: int) -> dict:
    """Pick each tunable policy's single knob on tuning seeds/base load; same rule for all."""
    tasks = [
        (s, seed, policy, param, TUNE_BASE_QPS_MILLI)
        for policy, grid in GRIDS.items()
        for param in grid
        for s in TUNING_SCENARIOS
        for seed in TUNE_SEEDS
    ]
    rows = _pool(tasks, workers)
    table: dict[str, dict[int, dict]] = {}
    for row in rows:
        entry = table.setdefault(row["policy"], {}).setdefault(
            row["param_ppm"], {"cost": 0.0, "soft": 0, "hard": 0, "windows": 0}
        )
        entry["cost"] += row["node_cost_micros"]
        entry["soft"] += row["soft_violation_windows"]
        entry["hard"] += row["hard_violation_windows"]
        entry["windows"] += row["scored_workload_windows"]
    chosen = {}
    for policy, grid in table.items():
        ok = [
            p for p, e in grid.items() if e["hard"] + e["soft"] <= VIOLATION_BUDGET * e["windows"]
        ]
        pick = (
            min(ok, key=lambda p: (grid[p]["cost"], p))
            if ok
            else min(grid, key=lambda p: (grid[p]["hard"] + grid[p]["soft"], grid[p]["cost"]))
        )
        chosen[policy] = {"param_ppm": pick, "rule_satisfied": bool(ok)}
    return {
        "rule": "min node cost s.t. (hard+soft) violation windows <= 3% of scored "
        "workload-windows; else min (hard+soft, cost)..",
        "tuning_seeds": list(TUNE_SEEDS),
        "tuning_base_qps_milli": TUNE_BASE_QPS_MILLI,
        "tuning_families": list(TUNE_FAMILIES),
        "grid": {p: {str(k): v for k, v in g.items()} for p, g in table.items()},
        "chosen": chosen,
    }


def bootstrap_ci(diffs: list[float], rng: random.Random, resamples: int = 10_000):
    means = sorted(statistics.fmean(rng.choices(diffs, k=len(diffs))) for _ in range(resamples))
    return means[int(0.025 * resamples)], means[int(0.975 * resamples) - 1]


def _describe(values: list[float]) -> dict:
    return {
        "n": len(values),
        "mean": statistics.fmean(values),
        "median": statistics.median(values),
        "p95": _percentile(values, 0.95),
        "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
    }


PAIRED_METRICS = (
    "node_cost_micros",
    "soft_violation_windows",
    "hard_violation_windows",
    "slo_violation_windows",
)


def _metric(row: dict, name: str) -> float:
    if name == "slo_violation_windows":
        return float(row["soft_violation_windows"] + row["hard_violation_windows"])
    return float(row[name])


def verdict(pair: dict) -> str:
    """Deterministic label from the paired 95% CIs: cost and total SLO-violation windows."""
    cost, slo = pair["node_cost_micros"]["ci95"], pair["slo_violation_windows"]["ci95"]
    cheaper, costlier = cost[1] < 0, cost[0] > 0
    safer, riskier = slo[1] < 0, slo[0] > 0
    if (cheaper and not riskier) or (safer and not costlier):
        return "FR better" if not (cheaper and safer) else "FR better (cost and SLO)"
    if (costlier and not safer) or (riskier and not cheaper):
        return "FR worse"
    if (cheaper and riskier) or (costlier and safer):
        return "trade-off"
    return "no significant difference"


def summarize(rows: list[dict]) -> dict:
    rng = random.Random(20261013)
    out: dict = {"scenarios": {}}
    by_key: dict[tuple, dict] = {(r["scenario"], r["seed"], r["policy"]): r for r in rows}
    for sid in sorted({r["scenario"] for r in rows}):
        seeds = sorted({r["seed"] for r in rows if r["scenario"] == sid})
        scenario = {"policies": {}, "paired": {}}
        for policy in POLICIES:
            recs = [by_key[(sid, s, policy)] for s in seeds]
            scenario["policies"][policy] = {
                metric: _describe([float(r[metric]) for r in recs])
                for metric in (
                    "node_cost_micros", "node_hours", "replica_hours", "idle_cpu_fraction",
                    "idle_memory_fraction", "provisioned_over_required", "success_rate",
                    "cost_per_million_successful_micros", "soft_violation_windows",
                    "hard_violation_windows", "latency_p99_window_p95_ms",
                    "latency_p99_window_max_ms", "overload_windows", "scale_actions",
                    "replicas_added", "node_release_events", "longest_capacity_gap_windows",
                )
            }  # fmt: skip
            scenario["policies"][policy]["hard_violation_seeds"] = sum(
                r["hard_violation_windows"] > 0 for r in recs
            )
        pairs = [(fr, o) for fr in FLEETRIGHT for o in POLICIES if o != fr]
        pairs.append(("fleetright_predictive_same_headroom", "fleetright_reactive"))
        for fr, other in pairs:
            pair = {}
            for metric in PAIRED_METRICS:
                mine = [_metric(by_key[(sid, x, fr)], metric) for x in seeds]
                theirs = [_metric(by_key[(sid, x, other)], metric) for x in seeds]
                diffs = [a - b for a, b in zip(mine, theirs, strict=True)]
                low, high = bootstrap_ci(diffs, rng)
                base_mean = statistics.fmean(
                    _metric(by_key[(sid, x, other)], metric) for x in seeds
                )
                pair[metric] = {
                    "mean_diff_fr_minus_other": statistics.fmean(diffs),
                    "ci95": [low, high],
                    "relative_diff": statistics.fmean(diffs) / base_mean if base_mean else None,
                    "significant": low > 0 or high < 0,
                }
            pair["verdict"] = verdict(pair)
            scenario["paired"][f"{fr}__vs__{other}"] = pair
        out["scenarios"][sid] = scenario
    return out


def _fmt(value: float, digits: int = 1) -> str:
    return f"{value:,.{digits}f}"


def render_report(summary: dict, names: dict[str, str], tuned: dict, manifest: dict) -> str:
    seeds = manifest["eval_seeds"]
    knobs = ", ".join(f"{p}={v['param_ppm']}" for p, v in tuned["chosen"].items())
    lines = [
        "# Paired policy comparison (SIMULATED)",
        "",
        f"Run `{manifest['run_id']}`, commit `{manifest['git_commit']}`. "
        f"Seeds {seeds[0]}-{seeds[-1]} "
        f"(n={len(seeds)} paired traces per scenario). Costs are {COST_MODEL_VERSION} node-micros "
        "(synthetic prices, replicas packed on 'small' nodes); latency is the declared "
        "simulator's per-window p99, not measured traffic.",
        "",
        f"Tuned knobs (chosen on separate tuning seeds/load): {knobs}",
        "",
    ]
    head = (
        "| policy | node cost | soft viol. | hard viol. (seeds) | max p99 ms "
        "| scale acts | idle CPU |"
    )
    pair_head = (
        "| FleetRight vs | cost diff [95% CI] | rel. | SLO-violation windows diff [95% CI] "
        "| hard diff [95% CI] | verdict |"
    )
    for sid, scenario in summary["scenarios"].items():
        lines += [f"## {sid} {names[sid]}", "", head, "|---|---:|---:|---:|---:|---:|---:|"]
        for policy, m in scenario["policies"].items():
            lines.append(
                f"| {policy} | {_fmt(m['node_cost_micros']['mean'], 0)} | "
                f"{_fmt(m['soft_violation_windows']['mean'])} | "
                f"{_fmt(m['hard_violation_windows']['mean'])} ({m['hard_violation_seeds']}) | "
                f"{_fmt(m['latency_p99_window_max_ms']['mean'], 0)} | "
                f"{_fmt(m['scale_actions']['mean'])} | {m['idle_cpu_fraction']['mean']:.2f} |"
            )
        lines += ["", pair_head, "|---|---:|---:|---:|---:|---|"]
        for key, pair in scenario["paired"].items():
            fr, other = key.split("__vs__")
            cells = []
            for metric in ("node_cost_micros", "slo_violation_windows", "hard_violation_windows"):
                p = pair[metric]
                low, high = p["ci95"]
                star = "*" if p["significant"] else ""
                cells.append(
                    f"{p['mean_diff_fr_minus_other']:+,.1f} [{low:+,.1f}, {high:+,.1f}]{star}"
                )
            rel = pair["node_cost_micros"]["relative_diff"]
            rel_text = "" if rel is None else f"{rel:+.1%}"
            label = fr.replace("fleetright_", "FR-")
            lines.append(
                f"| {label} vs {other} | {cells[0]} | {rel_text} | {cells[1]} | {cells[2]} "
                f"| {pair['verdict']} |"
            )
        lines.append("")
    lines.append(
        "`*` = 95% paired-bootstrap CI (10,000 resamples over seeds) excludes 0. "
        "Diff = FleetRight minus other; a negative cost/violation diff favours FleetRight."
    )
    return "\n".join(lines) + "\n"


HEADLINE = (
    ("fleetright_reactive", ("static_peak_reserve", "naive_threshold", "hpa_like")),
    ("fleetright_predictive", ("static_peak_reserve", "naive_threshold", "hpa_like")),
    ("fleetright_predictive", ("fleetright_reactive", "fleetright_predictive_same_headroom")),
    ("fleetright_predictive_same_headroom", ("fleetright_reactive",)),
)


def render_headline(summary: dict, names: dict[str, str]) -> str:
    """One row per scenario; cell = relative node-cost diff, SLO-violation-window diff, verdict."""
    lines = [
        "# Headline comparison (SIMULATED, paired by seed)",
        "",
        "Cell = `relative node-cost difference | mean SLO-violation-window difference | verdict` "
        "for FleetRight minus the column policy. Negative favours FleetRight. Verdicts use the "
        "paired 95% bootstrap CIs; see comparison.md for intervals and per-policy tables. "
        "The last table is the forecast ablation: predictive at the reactive policy's own knob.",
        "",
    ]
    for fr, others in HEADLINE:
        lines += [
            f"## {fr}",
            "",
            "| scenario | " + " | ".join(others) + " |",
            "|---|" + "---|" * len(others),
        ]
        tally = {other: {} for other in others}
        for sid, scenario in summary["scenarios"].items():
            cells = []
            for other in others:
                pair = scenario["paired"][f"{fr}__vs__{other}"]
                rel = pair["node_cost_micros"]["relative_diff"]
                slo = pair["slo_violation_windows"]["mean_diff_fr_minus_other"]
                cells.append(f"{rel:+.0%} \\| {slo:+.1f} \\| {pair['verdict']}")
                tally[other][pair["verdict"]] = tally[other].get(pair["verdict"], 0) + 1
            lines.append(f"| {sid} {names[sid]} | " + " | ".join(cells) + " |")
        lines.append(
            "| **tally** | "
            + " | ".join(
                "; ".join(f"{v}: {n}" for v, n in sorted(tally[o].items())) for o in others
            )
            + " |"
        )
        lines.append("")
    return "\n".join(lines) + "\n"


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True).stdout.strip()


def run_all(output: Path, workers: int, tuned_path: Path | None) -> Path:
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_dir = output / f"qualification-{run_id}"
    run_dir.mkdir(parents=True, exist_ok=False)
    tuned = json.loads(tuned_path.read_text()) if tuned_path else tune(workers)
    (run_dir / "tuning.json").write_text(json.dumps(tuned, indent=2, sort_keys=True) + "\n")
    params = {p: v["param_ppm"] for p, v in tuned["chosen"].items()}
    params["fleetright_predictive_same_headroom"] = params["fleetright_reactive"]
    tasks = [
        (s, seed, policy, params.get(policy, 0), EVAL_BASE_QPS_MILLI)
        for s in SCENARIOS
        for seed in EVAL_SEEDS
        for policy in POLICIES
    ]
    rows = _pool(tasks, workers)
    rows.sort(key=lambda r: (r["scenario"], r["seed"], r["policy"]))
    with (run_dir / "results.jsonl").open("w") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    summary = summarize(rows)
    names = {s.sid: s.name for s in SCENARIOS}
    manifest = {
        "run_id": run_id,
        "git_commit": _git("rev-parse", "HEAD"),
        "git_dirty": bool(_git("status", "--short", "--", "python", "cpp")),
        "python": sys.version.split()[0],
        "cost_model": COST_MODEL_VERSION,
        "eval_seeds": list(EVAL_SEEDS),
        "windows": WINDOWS,
        "warmup_windows": WARMUP,
        "window_ms": WINDOW_MS,
        "jitter_permille": JITTER_PERMILLE,
        "eval_base_qps_milli": EVAL_BASE_QPS_MILLI,
        "policies": list(POLICIES),
        "policy_params_ppm": params,
        "scenarios": [{**s.__dict__, "workloads": list(s.workloads)} for s in SCENARIOS],
        "results_sha256": hashlib.sha256((run_dir / "results.jsonl").read_bytes()).hexdigest(),
        "command": "python -m fleetright.qualify run",
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    (run_dir / "comparison.md").write_text(render_report(summary, names, tuned, manifest))
    (run_dir / "headline.md").write_text(render_headline(summary, names))
    write_csv(run_dir / "summary.csv", summary, names)
    return run_dir


def write_csv(path: Path, summary: dict, names: dict[str, str]) -> None:
    metrics = (
        "node_cost_micros", "node_hours", "replica_hours", "idle_cpu_fraction",
        "idle_memory_fraction", "provisioned_over_required", "success_rate",
        "cost_per_million_successful_micros", "soft_violation_windows", "hard_violation_windows",
        "latency_p99_window_p95_ms", "latency_p99_window_max_ms", "overload_windows",
        "scale_actions", "replicas_added", "node_release_events", "longest_capacity_gap_windows",
    )  # fmt: skip
    stats = ("mean", "median", "stdev")
    header = ["scenario", "name", "policy", "n_seeds", "hard_violation_seeds"]
    header += [f"{m}_{stat}" for m in metrics for stat in stats]
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        for sid, scenario in summary["scenarios"].items():
            for policy, m in scenario["policies"].items():
                row = [
                    sid,
                    names[sid],
                    policy,
                    m["node_cost_micros"]["n"],
                    m["hard_violation_seeds"],
                ]
                writer.writerow(row + [m[x][stat] for x in metrics for stat in stats])


def _tool_version(*command: str) -> str:
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=20)
        return (result.stdout or result.stderr).splitlines()[0].strip()
    except (OSError, IndexError, subprocess.SubprocessError):
        return "unavailable"


def environment() -> dict:
    from importlib import metadata

    packages = {}
    for name in ("fastapi", "psycopg", "pydantic", "uvicorn", "pytest", "PyYAML", "httpx", "ruff"):
        try:
            packages[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            packages[name] = "not installed"
    cpu = next(
        (
            line.split(":", 1)[1].strip()
            for line in Path("/proc/cpuinfo").read_text(errors="replace").splitlines()
            if line.startswith("model name")
        ),
        "unknown",
    )
    return {
        "git_commit": _git("rev-parse", "HEAD"),
        "python": sys.version.split()[0],
        "packages": packages,
        "compiler": _tool_version("c++", "--version"),
        "cmake": _tool_version("cmake", "--version"),
        "protoc": _tool_version("protoc", "--version"),
        "kernel": _tool_version("uname", "-sr"),
        "cpu_model": cpu,
        "logical_cpus": os.cpu_count(),
        "memory_total": _tool_version("sh", "-c", "grep MemTotal /proc/meminfo"),
        "build_type": "Release (CMAKE_BUILD_TYPE) - scripts/bootstrap.sh",
    }


def publish(run_dir: Path, destination: Path) -> None:
    """Copy the compact, reviewable artifacts; raw results are gzipped with a recorded checksum."""
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "environment.json").write_text(
        json.dumps(environment(), indent=2, sort_keys=True) + "\n"
    )
    for name in ("manifest.json", "tuning.json", "comparison.md", "headline.md", "summary.csv"):
        shutil.copy2(run_dir / name, destination / name)
    with (
        (run_dir / "results.jsonl").open("rb") as source,
        gzip.GzipFile(destination / "results.jsonl.gz", "wb", mtime=0) as target,
    ):
        shutil.copyfileobj(source, target)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("tune", "run"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--output", type=Path, default=ROOT / "results/phase3")
        cmd.add_argument("--workers", type=int, default=12)
        cmd.add_argument("--tuned", type=Path, default=None, help="reuse a tuning.json")
    pub = sub.add_parser("publish")
    pub.add_argument("run_dir", type=Path)
    pub.add_argument("destination", type=Path)
    args = parser.parse_args()
    if args.command == "publish":
        publish(args.run_dir, args.destination)
        return 0
    args.output.mkdir(parents=True, exist_ok=True)
    if args.command == "tune":
        path = args.output / "tuning.json"
        path.write_text(json.dumps(tune(args.workers), indent=2, sort_keys=True) + "\n")
        print(path)
    else:
        print(run_all(args.output, args.workers, args.tuned))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
