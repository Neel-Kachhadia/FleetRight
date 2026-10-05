from __future__ import annotations

import io
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "workloads"))
import service


def _handler(path: str, status_codes: list[int]) -> service.Handler:
    handler = service.Handler.__new__(service.Handler)
    handler.path = path
    handler.command = "GET"
    handler.request_version = "HTTP/1.0"
    handler.requestline = f"GET {path} HTTP/1.0"
    handler.wfile = io.BytesIO()
    handler.send_response = lambda status: status_codes.append(status)
    handler.send_header = lambda name, value: None
    handler.end_headers = lambda: None
    return handler


def test_prometheus_workload_counts_rejections_and_errors(monkeypatch) -> None:
    monkeypatch.setattr(service, "REQUESTS", 0)
    monkeypatch.setattr(service, "ERRORS", 0)
    monkeypatch.setattr(service, "REJECTS", 0)
    monkeypatch.setattr(service, "COUNTS", [0] * (len(service.BUCKETS_MS) + 1))
    monkeypatch.setattr(service, "LATENCY_SUM_MS", 0)
    monkeypatch.setattr(service, "QUEUE_DEPTH", 0)
    monkeypatch.setattr(service, "LAST_QUEUE_DEPTH", 0)
    monkeypatch.setattr(service, "LAST_QUEUE_TIME", time.monotonic())
    monkeypatch.setattr(service, "GATE", threading.BoundedSemaphore(0))
    monkeypatch.setattr(service, "QUEUE_WAIT_MS", 1)
    monkeypatch.setattr(service, "FAIL_EVERY", 0)
    monkeypatch.setattr(service, "COLD_START_MS", 0)
    monkeypatch.setattr(service, "WARMUP_MS", 0)

    rejected_status: list[int] = []
    _handler("/work", rejected_status).do_GET()
    assert rejected_status == [503]
    assert service.COUNTS[-1] == 0
    metrics = service._metrics()
    assert "fleetright_request_duration_seconds_count 1" in metrics
    assert 'fleetright_requests_total{result="rejected"} 1' in metrics
    assert "fleetright_readiness 1" in metrics

    monkeypatch.setattr(service, "GATE", threading.BoundedSemaphore(1))
    monkeypatch.setattr(service, "FAIL_EVERY", 1)
    failure_status: list[int] = []
    _handler("/work", failure_status).do_GET()
    assert failure_status == [500]
    metrics = service._metrics()
    assert "fleetright_request_duration_seconds_count 2" in metrics
    assert 'fleetright_requests_total{result="error"} 1' in metrics
