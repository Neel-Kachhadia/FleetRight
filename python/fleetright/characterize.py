from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
import platform
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fleetright.contracts import decode_text, encode_profile
from fleetright.profiles import RunMetrics, characterize_point
from fleetright.telemetry import Histogram
from fleetright.traces import _content_hash

ROOT = Path(__file__).resolve().parents[2]
SERVICES = {
    "cpu-api": (
        "cpu-service",
        "http://127.0.0.1:8081",
        1_000,
        512,
        120,
        "cpu-read-v1",
        "python-default",
    ),
    "memory-api": (
        "memory-service",
        "http://127.0.0.1:8082",
        1_000,
        768,
        150,
        "memory-read-v1",
        "32m-working-set",
    ),
    "queued-api": (
        "queued-service",
        "http://127.0.0.1:8083",
        1_000,
        512,
        80,
        "queue-mix-v1",
        "concurrency-32",
    ),
}
CONCURRENCY_STEPS = (1, 2, 4, 8, 16, 32, 64)


class Samples:
    def __init__(self, slo_ms: int) -> None:
        self.histogram = Histogram(
            tuple(sorted(set((1, 2, 5, 10, 20, 40, 80, slo_ms, 160, 250, 500, 1_000))))
        )
        self.errors = 0
        self.count = 0
        self.lock = threading.Lock()

    def add(self, latency_ms: float, error: bool) -> None:
        with self.lock:
            self.histogram = self.histogram.observe(latency_ms)
            self.count += 1
            self.errors += int(error)


def _request(url: str, timeout: float = 2.0) -> tuple[float, bool]:
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            response.read()
            return (time.perf_counter() - start) * 1_000, response.status >= 500
    except urllib.error.HTTPError as error:
        error.read()
        return (time.perf_counter() - start) * 1_000, True
    except (OSError, TimeoutError):
        return (time.perf_counter() - start) * 1_000, True


def _scrape(base_url: str) -> dict[str, float]:
    try:
        with urllib.request.urlopen(f"{base_url}/metrics", timeout=2) as response:
            body = response.read(2_000_000).decode("utf-8", errors="replace")
    except OSError:
        return {}
    values: dict[str, float] = {}
    for line in body.splitlines():
        if not line or line.startswith("#"):
            continue
        name, raw = line.rsplit(" ", 1)
        name = name.split("{", 1)[0]
        if name in {
            "process_resident_memory_bytes",
            "fleetright_cpu_throttled_seconds_total",
            "fleetright_queue_growth_milli_requests_per_second",
            "fleetright_queue_depth",
        }:
            try:
                values[name] = float(raw)
            except ValueError:
                pass
    return values


def _one_run(
    base_url: str, concurrency: int, hold_seconds: float, slo_ms: int, memory_limit_mib: int
) -> tuple[RunMetrics, dict[str, object]]:
    samples = Samples(slo_ms)
    before = _scrape(base_url)
    deadline = time.monotonic() + hold_seconds

    def worker() -> None:
        while time.monotonic() < deadline:
            latency, error = _request(f"{base_url}/work")
            samples.add(latency, error)

    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker) for _ in range(concurrency)]
        for future in futures:
            future.result()
    elapsed = max(time.monotonic() - started, 0.001)
    after = _scrape(base_url)
    qps_milli = math.floor(samples.count * 1_000 / elapsed)
    memory_bytes = int(after.get("process_resident_memory_bytes", 0))
    throttle_observed = (
        "fleetright_cpu_throttled_seconds_total" in before
        and "fleetright_cpu_throttled_seconds_total" in after
    )
    throttled_delta = max(
        0.0,
        after.get("fleetright_cpu_throttled_seconds_total", 0.0)
        - before.get("fleetright_cpu_throttled_seconds_total", 0.0),
    )
    throttling_ppm = (
        min(1_000_000, math.ceil(throttled_delta / elapsed * 1_000_000))
        if throttle_observed
        else 1_000_000
    )
    queue_observed = "fleetright_queue_growth_milli_requests_per_second" in after
    queue_growth = (
        int(after["fleetright_queue_growth_milli_requests_per_second"])
        if queue_observed
        else 1_000_000
    )
    metrics = RunMetrics(
        samples.histogram,
        samples.count,
        samples.errors,
        qps_milli,
        concurrency,
        queue_growth,
        min(1_000_000, math.ceil(memory_bytes / (memory_limit_mib * 1024 * 1024) * 1_000_000))
        if memory_bytes
        else 1_000_000,
        throttling_ppm,
    )
    evidence = {
        "offered_concurrency": concurrency,
        "elapsed_seconds": elapsed,
        "request_count": samples.count,
        "error_count": samples.errors,
        "throughput_qps_milli": qps_milli,
        "histogram_buckets_ms": list(samples.histogram.buckets_ms),
        "histogram_counts": list(samples.histogram.counts),
        "histogram_sum_ms": samples.histogram.sum_ms,
        "max_rss_bytes": memory_bytes,
        "cpu_throttling_ppm": throttling_ppm,
        "cpu_throttling_observable": throttle_observed,
        "queue_growth_observable": queue_observed,
        "queue_growth_upper_milli": queue_growth,
        "metrics_before": before,
        "metrics_after": after,
    }
    return metrics, evidence


def _run(command: list[str], timeout: int = 30) -> tuple[int, str]:
    try:
        result = subprocess.run(
            command, cwd=ROOT, text=True, capture_output=True, timeout=timeout, check=False
        )
        return result.returncode, (result.stdout + result.stderr).strip()
    except (OSError, subprocess.TimeoutExpired) as error:
        return 127, str(error)


def _host_metadata() -> dict[str, object]:
    memory_kib = 0
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                memory_kib = int(line.split()[1])
                break
    except (OSError, ValueError, IndexError):
        pass
    commit_code, commit = _run(["git", "rev-parse", "HEAD"])
    return {
        "kernel": platform.release(),
        "platform": platform.platform(),
        "cpu": platform.processor() or platform.machine(),
        "logical_cpu_count": os.cpu_count(),
        "memory_bytes": memory_kib * 1024,
        "git_commit": commit if commit_code == 0 else "unknown",
        "python": sys.version.split()[0],
    }


def characterize(
    output_root: Path, runs: int, hold_seconds: float, steps: tuple[int, ...] = CONCURRENCY_STEPS
) -> Path:
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ") + "-capacity"
    run_dir = output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    raw_dir = run_dir / "raw_metrics"
    profile_dir = run_dir / "capacity_profiles"
    raw_dir.mkdir()
    profile_dir.mkdir()
    started_at = datetime.now(UTC)
    manifest: dict[str, object] = {
        "schema_version": 1,
        "run_id": run_id,
        "started_at_utc": started_at.isoformat(),
        "host": _host_metadata(),
        "runs_per_load_point": runs,
        "hold_seconds_per_run": hold_seconds,
        "concurrency_steps": list(steps),
        "workload_profiles": {},
    }
    docker_code, docker_detail = _run(
        ["docker", "info", "--format", "{{.ServerVersion}}"], timeout=10
    )
    manifest["docker_daemon"] = {"available": docker_code == 0, "detail": docker_detail}
    if docker_code != 0:
        manifest["status"] = "BLOCKED"
        manifest["blocked_reason"] = (
            "Docker daemon is unavailable; no resource-limited capacity profile was promoted"
        )
        manifest["measurement_kind"] = "none"
        manifest["completed_at_utc"] = datetime.now(UTC).isoformat()
        (run_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n"
        )
        (run_dir / "report.md").write_text(
            "# Capacity characterization\n\nDocker measurements were blocked. "
            "No capacity profile was promoted.\n"
            f"\nReason: `{docker_detail}`\n"
        )
        return run_dir

    compose_up, compose_detail = _run(["docker", "compose", "up", "-d", "--build"], timeout=900)
    if compose_up != 0:
        cleanup_code, cleanup_detail = _run(["docker", "compose", "down"], timeout=120)
        manifest["status"] = "BLOCKED"
        manifest["blocked_reason"] = compose_detail
        manifest["partial_start_cleanup"] = {"exit_code": cleanup_code, "detail": cleanup_detail}
        manifest["measurement_kind"] = "none"
        manifest["completed_at_utc"] = datetime.now(UTC).isoformat()
        (run_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n"
        )
        (run_dir / "report.md").write_text(
            "# Capacity characterization\n\nDocker Compose could not start the workload "
            "containers.\n"
            f"\nReason: `{compose_detail}`\n"
        )
        return run_dir

    try:
        for workload_id, (
            service_name,
            base_url,
            cpu_millicores,
            memory_mib,
            slo_ms,
            request_mix,
            runtime_class,
        ) in SERVICES.items():
            ready_until = time.monotonic() + 60
            while time.monotonic() < ready_until:
                try:
                    with urllib.request.urlopen(f"{base_url}/ready", timeout=1) as response:
                        if response.status == 200:
                            break
                except OSError:
                    time.sleep(0.2)
            else:
                raise RuntimeError(f"{workload_id} did not become ready")

            image_code, image_id = _run(["docker", "compose", "images", "-q", service_name])
            source = (ROOT / "workloads/service.py").read_bytes() + (
                ROOT / "workloads/Dockerfile"
            ).read_bytes()
            source_digest = hashlib.sha256(source).hexdigest()
            image_digest = (
                image_id.splitlines()[0]
                if image_code == 0 and image_id.startswith("sha256:")
                else f"sha256:{source_digest}"
            )
            point_runs: dict[int, list[RunMetrics]] = {}
            evidence_points: list[dict[str, object]] = []
            safe_points: list[dict[str, int | str | bool]] = []
            for concurrency in steps:
                repeated: list[RunMetrics] = []
                raw_runs: list[dict[str, object]] = []
                for run_number in range(runs):
                    metrics, evidence = _one_run(
                        base_url, concurrency, hold_seconds, slo_ms, memory_mib
                    )
                    repeated.append(metrics)
                    raw_runs.append(evidence)
                    (raw_dir / f"{workload_id}-c{concurrency}-r{run_number}.json").write_text(
                        json.dumps(evidence, indent=2, sort_keys=True) + "\n"
                    )
                point = characterize_point(repeated, slo_ms, 1_000, haircut_ppm=100_000)
                point_runs[concurrency] = repeated
                evidence_points.append(
                    {
                        "concurrency": concurrency,
                        "statistics": point,
                        "raw_files": [
                            f"{workload_id}-c{concurrency}-r{i}.json" for i in range(runs)
                        ],
                    }
                )
                safe = (
                    int(point["latency_success_lower_ppm"]) >= 990_000
                    and int(point["error_rate_wilson_upper_ppm"]) <= 1_000
                    and int(point["p99_upper_ms"]) <= slo_ms
                    and int(point["queue_growth_upper_milli"]) <= 0
                    and int(point["memory_pressure_ppm"]) <= 900_000
                    and int(point["cpu_throttling_ppm"]) <= 50_000
                    and bool(point["stable_across_runs"])
                )
                if safe:
                    safe_points.append(
                        {
                            key: value
                            for key, value in point.items()
                            if key
                            in {
                                "safe_qps_milli",
                                "safe_concurrency",
                                "p99_upper_ms",
                                "error_rate_upper_ppm",
                                "error_rate_wilson_upper_ppm",
                                "queue_growth_upper_milli",
                                "memory_pressure_ppm",
                                "cpu_throttling_ppm",
                                "request_count",
                                "error_count",
                                "repeated_run_count",
                                "threshold_successes",
                                "threshold_trials",
                                "latency_success_lower_ppm",
                                "run_dispersion_ppm",
                                "variance_ppm",
                                "histogram_config_id",
                                "stable_across_runs",
                            }
                        }
                    )
            manifest_points = {
                "image_digest": image_digest,
                "request_mix_id": request_mix,
                "runtime_config_class": runtime_class,
                "resource_allocation": {
                    "cpu_millicores": cpu_millicores,
                    "memory_mib": memory_mib,
                    "network_mbps": 100,
                },
                "measurement_kind": "docker_runtime",
                "enforceable_resource_limits": True,
                "points": evidence_points,
            }
            manifest["workload_profiles"][workload_id] = manifest_points
            if safe_points:
                success_factor = min(
                    int(point["latency_success_lower_ppm"]) for point in safe_points
                )
                profile = {
                    "profile_id": f"{workload_id}-{cpu_millicores}m-{memory_mib}mib-v1",
                    "profile_version": "1",
                    "workload_id": workload_id,
                    "image_digest": image_digest,
                    "request_mix_id": request_mix,
                    "runtime_config_class": runtime_class,
                    "platform_class": f"{platform.machine()}-linux-docker",
                    "allocation": {
                        "cpu_millicores": cpu_millicores,
                        "memory_mib": memory_mib,
                        "network_mbps": 100,
                    },
                    "generated_at_ms": int(started_at.timestamp() * 1_000),
                    "valid_until_ms": int((started_at + timedelta(days=30)).timestamp() * 1_000),
                    "confidence_ppm": success_factor,
                    "conservative_haircut_ppm": 100_000,
                    "safe_points": safe_points,
                    "measurement_kind": "docker_runtime",
                    "enforceable_resource_limits": True,
                }
                payload, profile = encode_profile(profile)
                (profile_dir / f"{workload_id}.pb").write_bytes(payload)
                (profile_dir / f"{workload_id}.textproto").write_text(
                    "# Versioned profile generated by scripts/characterize_capacity.sh\n"
                    + decode_text(
                        "fleetright.v1.CapacityProfile", "fleetright/v1/model.proto", payload
                    )
                )
                manifest_points["profile_sha256"] = profile["profile_sha256"]
                manifest_points["status"] = "PROFILED"
            else:
                manifest_points["status"] = "NO_SAFE_POINT"
        manifest["status"] = "COMPLETE"
        manifest["measurement_kind"] = "docker_runtime"
    except Exception as error:
        manifest["status"] = "FAILED_SAFE"
        manifest["failure"] = f"{type(error).__name__}: {error}"
    finally:
        _run(["docker", "compose", "down"], timeout=120)
    manifest["completed_at_utc"] = datetime.now(UTC).isoformat()
    manifest["manifest_sha256"] = _content_hash(manifest)
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    status = str(manifest["status"])
    summary = [
        "# Capacity characterization",
        "",
        f"Status: **{status}**",
        "",
        f"Docker: `{docker_detail}`",
        "",
        "Only points passing the Wilson latency/error bounds, queue, memory, throttling, "
        "and run-dispersion gates are written to a CapacityProfile.",
        "",
        "Raw measurements and per-load histograms are in `raw_metrics/`. Profiles are in "
        "`capacity_profiles/`.",
    ]
    (run_dir / "report.md").write_text("\n".join(summary) + "\n")
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Measure workload capacity under Docker resource limits"
    )
    parser.add_argument("--output", type=Path, default=ROOT / "results/phase1")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--hold-seconds", type=float, default=5.0)
    args = parser.parse_args()
    if args.runs < 2 or args.hold_seconds <= 0:
        parser.error("--runs must be at least 2 and hold time must be positive")
    result = characterize(args.output, args.runs, args.hold_seconds)
    print(result)
    manifest = json.loads((result / "manifest.json").read_text())
    return 0 if manifest["status"] in {"COMPLETE", "BLOCKED"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
