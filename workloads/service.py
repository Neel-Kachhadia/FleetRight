from __future__ import annotations

import hashlib
import json
import os
import random
import resource
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

STARTED = time.monotonic()
MODE = os.environ.get("WORKLOAD_MODE", "cpu")
SERVICE_MS = int(os.environ.get("SERVICE_MS", "4"))
MEMORY_BYTES = int(os.environ.get("MEMORY_BYTES", str(16 * 1024 * 1024)))
MAX_CONCURRENCY = int(os.environ.get("MAX_CONCURRENCY", "32"))
QUEUE_WAIT_MS = int(os.environ.get("QUEUE_WAIT_MS", "100"))
COLD_START_MS = int(os.environ.get("COLD_START_MS", "3000"))
WARMUP_MS = int(os.environ.get("WARMUP_MS", "2000"))
FAIL_EVERY = int(os.environ.get("FAIL_EVERY", "0"))
SLO_MS = int(os.environ.get("LATENCY_SLO_MS", "120"))
BUCKETS_MS = tuple(sorted(set((1, 2, 5, 10, 20, 40, 80, SLO_MS, 160, 250, 500, 1_000))))
COUNTS = [0] * (len(BUCKETS_MS) + 1)
LOCK = threading.Lock()
GATE = threading.BoundedSemaphore(MAX_CONCURRENCY)
REQUESTS = 0
ERRORS = 0
REJECTS = 0
LATENCY_SUM_MS = 0
QUEUE_DEPTH = 0
QUEUE_GROWTH_MILLI = 0
LAST_QUEUE_TIME = time.monotonic()
LAST_QUEUE_DEPTH = 0


def _throttled_seconds() -> float | None:
    try:
        values = dict(
            line.split()
            for line in Path("/sys/fs/cgroup/cpu.stat").read_text(encoding="ascii").splitlines()
        )
        if "throttled_usec" in values:
            return int(values["throttled_usec"]) / 1_000_000
        if "throttled_time" in values:
            return int(values["throttled_time"]) / 1_000_000_000
    except (OSError, ValueError):
        return None
    return None


def _observe_latency(milliseconds: float) -> None:
    global LATENCY_SUM_MS
    sample = max(0, int(milliseconds + 0.999999))
    index = next(
        (i for i, boundary in enumerate(BUCKETS_MS) if sample <= boundary), len(BUCKETS_MS)
    )
    with LOCK:
        COUNTS[index] += 1
        LATENCY_SUM_MS += sample


def _work(request_id: int) -> None:
    rng = random.Random(request_id * 0x9E3779B1)
    delay_ms = SERVICE_MS
    if os.environ.get("SERVICE_DISTRIBUTION", "fixed") == "jittered":
        delay_ms = max(1, SERVICE_MS + rng.randint(-SERVICE_MS // 2, SERVICE_MS // 2))
    if time.monotonic() - STARTED < (COLD_START_MS + WARMUP_MS) / 1_000:
        delay_ms *= 2
    if MODE == "cpu":
        payload = str(request_id).encode()
        deadline = time.perf_counter() + delay_ms / 1_000
        while time.perf_counter() < deadline:
            payload = hashlib.sha256(payload).digest()
    elif MODE == "memory":
        working = bytearray(MEMORY_BYTES)
        for page in range(0, len(working), 4_096):
            working[page] = request_id & 0xFF
        if delay_ms:
            time.sleep(delay_ms / 1_000)
    elif MODE == "queued":
        if delay_ms:
            time.sleep(delay_ms / 1_000)
    else:
        raise RuntimeError("unknown WORKLOAD_MODE")


def _metrics() -> str:
    with LOCK:
        cumulative = 0
        lines = ["# TYPE fleetright_request_duration_seconds histogram"]
        for boundary, count in zip(BUCKETS_MS, COUNTS, strict=False):
            cumulative += count
            lines.append(
                f'fleetright_request_duration_seconds_bucket{{le="{boundary / 1000:g}"}} '
                f"{cumulative}"
            )
        cumulative += COUNTS[-1]
        lines.extend(
            (
                f'fleetright_request_duration_seconds_bucket{{le="+Inf"}} {cumulative}',
                f"fleetright_request_duration_seconds_sum {LATENCY_SUM_MS / 1000:g}",
                f"fleetright_request_duration_seconds_count {REQUESTS}",
            )
        )
        lines.extend(
            (
                "# TYPE fleetright_requests_total counter",
                f'fleetright_requests_total{{result="success"}} {REQUESTS - ERRORS - REJECTS}',
                f'fleetright_requests_total{{result="error"}} {ERRORS}',
                f'fleetright_requests_total{{result="rejected"}} {REJECTS}',
                "# TYPE fleetright_queue_depth gauge",
                f"fleetright_queue_depth {QUEUE_DEPTH}",
                "# TYPE fleetright_queue_growth_milli_requests_per_second gauge",
                f"fleetright_queue_growth_milli_requests_per_second {QUEUE_GROWTH_MILLI}",
                "# TYPE fleetright_readiness gauge",
                f"fleetright_readiness "
                f"{int(time.monotonic() - STARTED >= (COLD_START_MS + WARMUP_MS) / 1_000)}",
                "# TYPE fleetright_warmup gauge",
                f"fleetright_warmup "
                f"{int(time.monotonic() - STARTED < (COLD_START_MS + WARMUP_MS) / 1_000)}",
            )
        )
        usage = resource.getrusage(resource.RUSAGE_SELF)
        lines.extend(
            (
                "# TYPE process_cpu_seconds_total counter",
                f"process_cpu_seconds_total {usage.ru_utime + usage.ru_stime:.6f}",
                "# TYPE process_max_resident_memory_bytes gauge",
                f"process_max_resident_memory_bytes {usage.ru_maxrss * 1024}",
            )
        )
        try:
            pages = int(Path("/proc/self/statm").read_text().split()[1])
            lines.extend(
                (
                    "# TYPE process_resident_memory_bytes gauge",
                    f"process_resident_memory_bytes {pages * os.sysconf('SC_PAGE_SIZE')}",
                )
            )
        except (OSError, ValueError, IndexError):
            pass
        throttled = _throttled_seconds()
        if throttled is not None:
            lines.extend(
                (
                    "# TYPE fleetright_cpu_throttled_seconds_total counter",
                    f"fleetright_cpu_throttled_seconds_total {throttled:.6f}",
                )
            )
        else:
            lines.extend(
                (
                    "# TYPE fleetright_cpu_throttling_observable gauge",
                    "fleetright_cpu_throttling_observable 0",
                )
            )
        return "\n".join(lines) + "\n"


class Handler(BaseHTTPRequestHandler):
    server_version = "FleetRightSynthetic/1"

    def log_message(self, format: str, *args: object) -> None:
        del format, args

    def do_GET(self) -> None:
        global REQUESTS, ERRORS, REJECTS, QUEUE_DEPTH, QUEUE_GROWTH_MILLI
        global LAST_QUEUE_TIME, LAST_QUEUE_DEPTH
        path = urlparse(self.path).path
        if path == "/metrics":
            body = _metrics().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if path == "/ready":
            ready = time.monotonic() - STARTED >= (COLD_START_MS + WARMUP_MS) / 1_000
            self.send_response(200 if ready else 503)
            self.end_headers()
            return
        if path != "/work":
            self.send_error(404)
            return
        with LOCK:
            REQUESTS += 1
            request_id = REQUESTS
            QUEUE_DEPTH += 1
        queued_at = time.monotonic()
        admitted = GATE.acquire(timeout=QUEUE_WAIT_MS / 1000)
        now = time.monotonic()
        with LOCK:
            QUEUE_DEPTH = max(0, QUEUE_DEPTH - 1)
            if now > LAST_QUEUE_TIME:
                QUEUE_GROWTH_MILLI = int(
                    (QUEUE_DEPTH - LAST_QUEUE_DEPTH) * 1_000_000 / (now - LAST_QUEUE_TIME)
                )
                LAST_QUEUE_DEPTH = QUEUE_DEPTH
                LAST_QUEUE_TIME = now
        if not admitted:
            _observe_latency((now - queued_at) * 1_000)
            with LOCK:
                REJECTS += 1
            self.send_response(503)
            self.end_headers()
            return
        started = time.perf_counter()
        failed = FAIL_EVERY > 0 and request_id % FAIL_EVERY == 0
        try:
            if not failed:
                _work(request_id)
            elapsed_ms = (time.perf_counter() - started) * 1_000
            _observe_latency(elapsed_ms + (started - queued_at) * 1_000)
            if failed:
                with LOCK:
                    ERRORS += 1
                self.send_response(500)
            else:
                self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": not failed}).encode())
        finally:
            GATE.release()


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


if __name__ == "__main__":
    host = os.environ.get("LISTEN_HOST", "0.0.0.0")
    port = int(os.environ.get("LISTEN_PORT", "8080"))
    Server((host, port), Handler).serve_forever()
