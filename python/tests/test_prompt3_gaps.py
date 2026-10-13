"""Prompt 3 gap closers: execute role fences and a severed PostgreSQL connection mid-claim."""

from __future__ import annotations

import os
import time
from contextlib import contextmanager

import psycopg
from fastapi.testclient import TestClient
from test_prompt2a_control_api import (
    Database,
    _supported_scale_out_request,
    _tokens,
    clean_database,  # noqa: F401  (fixture)
    control_database,  # noqa: F401  (fixture)
    create_app,
)


class _Target:
    def observe_scale(self, _workload_id: str) -> dict[str, object]:
        return {
            "uid": "compose/cpu-service",
            "generation": 1,
            "resource_version": "1",
            "replicas": 3,
            "ready_replicas": 3,
        }


def _approved_plan(client: TestClient, raw: dict[str, str]) -> str:
    optimized = client.post(
        "/v1/optimize",
        headers={"Authorization": f"Bearer {raw['recommender']}", "Idempotency-Key": "gap-opt"},
        json=_supported_scale_out_request(time.time_ns() // 1_000_000),
    )
    assert optimized.status_code == 201, optimized.text
    plan_id = optimized.json()["plan_id"]
    approved = client.post(
        f"/v1/plans/{plan_id}/approve",
        headers={"Authorization": f"Bearer {raw['operator']}", "Idempotency-Key": "gap-approve"},
        json={},
    )
    assert approved.status_code == 200, approved.text
    return plan_id


def _app(database: Database, tokens):
    return create_app(
        database.repository,
        tokens=tokens,
        mode="EXECUTE",
        execution_target="docker",
        target_adapter=_Target(),
        health_observer=object(),
        start_execution_worker=False,
    )


def test_execute_is_forbidden_to_viewer_and_recommender(clean_database: Database) -> None:  # noqa: F811
    tokens, raw = _tokens()
    with TestClient(_app(clean_database, tokens)) as client:
        plan_id = _approved_plan(client, raw)
        for role in ("viewer", "recommender"):
            denied = client.post(
                f"/v1/plans/{plan_id}/execute",
                headers={"Authorization": f"Bearer {raw[role]}", "Idempotency-Key": f"deny-{role}"},
                json={},
            )
            assert denied.status_code == 403, (role, denied.text)
        with clean_database.repository.transaction() as conn:
            assert conn.execute("SELECT count(*) AS n FROM plan_executions").fetchone()["n"] == 0


def test_severed_postgres_connection_during_claim_leaves_no_partial_execution(
    clean_database: Database,  # noqa: F811
) -> None:
    admin_dsn = os.environ["FLEETRIGHT_TEST_DATABASE_URL"]
    tokens, raw = _tokens()
    repository = clean_database.repository
    original = repository.transaction
    armed = {"on": False, "fired": 0}

    @contextmanager
    def severing():
        with original() as conn:
            yield conn
            if armed["on"] and not armed["fired"]:
                armed["fired"] = 1  # kill this backend after the body, before COMMIT
                with psycopg.connect(admin_dsn, autocommit=True) as admin:
                    admin.execute("SELECT pg_terminate_backend(%s)", (conn.info.backend_pid,))

    with TestClient(_app(clean_database, tokens), raise_server_exceptions=False) as client:
        plan_id = _approved_plan(client, raw)
        headers = {"Authorization": f"Bearer {raw['operator']}", "Idempotency-Key": "sever-once"}
        repository.transaction = severing  # type: ignore[method-assign]
        armed["on"] = True
        first = client.post(f"/v1/plans/{plan_id}/execute", headers=headers, json={})
        repository.transaction = original  # type: ignore[method-assign]
        assert armed["fired"] == 1
        assert first.status_code >= 500, first.text  # fail closed, never a false 202
        with original() as conn:
            assert conn.execute("SELECT count(*) AS n FROM plan_executions").fetchone()["n"] == 0
        retried = client.post(f"/v1/plans/{plan_id}/execute", headers=headers, json={})
        assert retried.status_code == 202, retried.text
        with original() as conn:
            assert conn.execute("SELECT count(*) AS n FROM plan_executions").fetchone()["n"] == 1
