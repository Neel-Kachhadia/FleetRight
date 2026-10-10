from __future__ import annotations

import json
import multiprocessing
import os
import secrets
import shutil
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path

import fleetright.api as api_module
import psycopg
import pytest
from fastapi.testclient import TestClient
from fleetright.api import MAX_REQUEST_BYTES, Principal, create_app
from fleetright.control import (
    ArtifactIntegrityError,
    ControlError,
    IdempotencyConflict,
    Repository,
    transition_allowed,
)
from fleetright.execution import (
    DockerComposeTarget,
    FleetRightWorkloadTarget,
    WorkloadHealthObserver,
)
from fleetright.reconciler import ExecutionController
from fleetright.scenario import oracle_snapshot_fields
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.types.json import Jsonb


@dataclass(frozen=True)
class Database:
    repository: Repository
    dsn: str = field(repr=False)


def _run_docker_e2e_controller(
    dsn: str,
    compose_file: str,
    project: str,
    health_url: str,
    controller_id: str,
    stop_file: str,
    *,
    start_file: str | None = None,
    crash_after_scale_file: str | None = None,
    health_regression_container_id: str | None = None,
    health_regression_file: str | None = None,
    timeout_after_scale_project: str | None = None,
    timeout_shim_dir: str | None = None,
    timeout_marker: str | None = None,
    lease_ttl_ms: int = 5_000,
    readiness_timeout_seconds: int = 90,
) -> None:
    target = DockerComposeTarget(compose_file=Path(compose_file), project=project)
    if timeout_after_scale_project is not None:
        if timeout_shim_dir is None or timeout_marker is None:
            raise ValueError("Docker timeout shim directory and marker are required")
        real_docker = shutil.which("docker")
        if real_docker is None:
            raise RuntimeError("Docker CLI is unavailable")
        shim_dir = Path(timeout_shim_dir)
        shim_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        shim = shim_dir / "docker"
        # Let real Compose apply the change, then hold its response past the client timeout.
        shim.write_text(
            "#!" + sys.executable + "\n"
            "import subprocess, sys, time\n"
            f"real = {json.dumps(real_docker)}\n"
            f"project = {json.dumps(timeout_after_scale_project)}\n"
            f"marker = {json.dumps(timeout_marker)}\n"
            "args = sys.argv[1:]\n"
            "result = subprocess.run([real, *args])\n"
            "if (result.returncode == 0 and args[:1] == ['compose'] and\n"
            "    '--project-name' in args and\n"
            "    args[args.index('--project-name') + 1] == project and\n"
            "    'up' in args and '--scale' in args):\n"
            "    with open(marker, 'w', encoding='ascii') as output:\n"
            "        output.write('applied')\n"
            "    time.sleep(30)\n"
            "sys.exit(result.returncode)\n",
            encoding="utf-8",
        )
        shim.chmod(0o700)
        os.environ["PATH"] = f"{shim_dir}{os.pathsep}{os.environ['PATH']}"
        compose = target._compose

        def timeout_after_apply(*args: str, timeout: float = 15):
            return compose(*args, timeout=20 if args and args[0] == "up" else timeout)

        target._compose = timeout_after_apply  # type: ignore[method-assign]
    if crash_after_scale_file is not None or health_regression_container_id is not None:
        set_replicas = target.set_replicas

        def block_after_scale(
            workload_id: str,
            replicas: int,
            observed: dict[str, object],
            *,
            expected_replicas: int,
            fencing_token: int,
        ) -> dict[str, object]:
            result = set_replicas(
                workload_id,
                replicas,
                observed,
                expected_replicas=expected_replicas,
                fencing_token=fencing_token,
            )
            if health_regression_container_id is not None:
                if health_regression_file is None:
                    raise ValueError("health-regression marker is required")
                subprocess.run(
                    ["docker", "kill", health_regression_container_id],
                    capture_output=True,
                    check=True,
                    text=True,
                    timeout=15,
                )
                Path(health_regression_file).write_text("applied", encoding="ascii")
            if crash_after_scale_file is not None:
                Path(crash_after_scale_file).write_text("applied", encoding="ascii")
                threading.Event().wait(3_600)
            return result

        target.set_replicas = block_after_scale  # type: ignore[method-assign]

    if start_file is not None:
        while not Path(start_file).exists() and not Path(stop_file).exists():
            time.sleep(0.05)
    controller = ExecutionController(
        Repository(dsn),
        target,
        WorkloadHealthObserver({"cpu-api": health_url}),
        controller_id,
        lease_ttl_ms=lease_ttl_ms,
        max_attempts=2,
        readiness_timeout_seconds=readiness_timeout_seconds,
        poll_seconds=0.1,
    )
    while not Path(stop_file).exists():
        controller.run_once()
        time.sleep(0.1)


def _run_kind_e2e_controller(
    dsn: str,
    kubeconfig: str,
    health_url: str,
    controller_id: str,
    stop_file: str,
    *,
    start_file: str | None = None,
    block_after_ready_file: str | None = None,
    lease_ttl_ms: int = 30_000,
) -> None:
    target = FleetRightWorkloadTarget(kubeconfig=kubeconfig, namespace="fleetright-v2")
    if block_after_ready_file is not None:
        set_replicas = target.set_replicas

        def block_after_ready(
            workload_id: str,
            replicas: int,
            observed: dict[str, object],
            *,
            expected_replicas: int,
            fencing_token: int,
            plan_id: str,
            operation_id: str,
        ) -> dict[str, object]:
            result = set_replicas(
                workload_id,
                replicas,
                observed,
                expected_replicas=expected_replicas,
                fencing_token=fencing_token,
                plan_id=plan_id,
                operation_id=operation_id,
            )
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                current = target.observe_scale(workload_id)
                execution = current.get("execution")
                if (
                    current["ready_replicas"] == replicas
                    and isinstance(execution, dict)
                    and execution.get("planID") == plan_id
                    and execution.get("operationID") == operation_id
                    and execution.get("phase") == "APPLIED"
                ):
                    Path(block_after_ready_file).write_text("ready", encoding="ascii")
                    threading.Event().wait(3_600)
                    return result
                time.sleep(0.2)
            raise TimeoutError("FleetRightWorkload did not reach the requested Ready count")

        target.set_replicas = block_after_ready  # type: ignore[method-assign]

    if start_file is not None:
        while not Path(start_file).exists() and not Path(stop_file).exists():
            time.sleep(0.05)
    controller = ExecutionController(
        Repository(dsn),
        target,
        WorkloadHealthObserver({"cpu-api": health_url}),
        controller_id,
        lease_ttl_ms=lease_ttl_ms,
        max_attempts=2,
        readiness_timeout_seconds=90,
        poll_seconds=0.1,
    )
    while not Path(stop_file).exists():
        controller.run_once()
        time.sleep(0.1)


@pytest.fixture(scope="module")
def control_database() -> Iterator[Database]:
    admin_dsn = os.getenv("FLEETRIGHT_TEST_DATABASE_URL")
    if not admin_dsn:
        pytest.skip("FLEETRIGHT_TEST_DATABASE_URL is required for PostgreSQL control-plane tests")
    schema = f"prompt2a_{uuid.uuid4().hex}"
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    dsn = make_conninfo(admin_dsn, options=f"-csearch_path={schema}")
    repo = Repository(dsn)
    repo.migrate()
    assert repo.readiness() == (True, "ready")
    try:
        yield Database(repo, dsn)
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture
def docker_e2e_database() -> Iterator[Database]:
    admin_dsn = os.getenv("FLEETRIGHT_TEST_DATABASE_URL")
    if not admin_dsn:
        pytest.skip("FLEETRIGHT_TEST_DATABASE_URL is required for Docker E2E")
    if conninfo_to_dict(admin_dsn).get("dbname") != "fleetright_test":
        pytest.fail("Docker E2E requires the isolated fleetright_test database")
    schema = f"docker_e2e_{uuid.uuid4().hex}"
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    dsn = make_conninfo(admin_dsn, options=f"-csearch_path={schema}")
    repo = Repository(dsn)
    try:
        repo.migrate()
        yield Database(repo, dsn)
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture
def clean_database(control_database: Database) -> Database:
    with control_database.repository.transaction() as conn:
        conn.execute(
            "TRUNCATE control_state, plans, artifacts, idempotency_keys, conflict_leases, "
            "audit_events RESTART IDENTITY CASCADE"
        )
    return control_database


def _insert_plan(conn: psycopg.Connection, plan_id: str, state: str = "PROPOSED") -> None:
    conn.execute(
        "INSERT INTO plans (plan_id, created_at_ms, expires_at_ms, snapshot_id, "
        "snapshot_generation, "
        "policy_version, policy_hash, profile_hashes, machine_catalog_hash, forecast_metadata, "
        "planner_build_id, verifier_build_id, verification_hash, verification_accepted, "
        "admission_accepted, reason_codes, binding, state, state_version, created_by) "
        "VALUES (%s, 1, 9999999999999, 's', 1, '1', %s, %s, %s, %s, 'planner', 'verifier', %s, "
        "TRUE, TRUE, %s, %s, %s, 0, 'test')",
        (
            plan_id,
            "a" * 64,
            Jsonb({"profile": "b" * 64}),
            "c" * 64,
            Jsonb({}),
            "d" * 64,
            Jsonb([]),
            Jsonb({"source": {}}),
            state,
        ),
    )


def _claim_lease(repository: Repository, owner: str) -> int | None:
    with repository.transaction() as conn:
        return Repository.acquire_lease(conn, "workload:contended", owner, owner, 60_000)


def test_postgres_migration_state_machine_rollback_and_restart(clean_database: Database) -> None:
    repo = clean_database.repository
    assert repo.readiness() == (True, "ready")
    legal = {
        ("PROPOSED", "VERIFIED"),
        ("PROPOSED", "REJECTED"),
        ("PROPOSED", "STALE"),
        ("PROPOSED", "CANCELLED"),
        ("VERIFIED", "APPROVED"),
        ("VERIFIED", "PAUSED"),
        ("VERIFIED", "STALE"),
        ("VERIFIED", "REJECTED"),
        ("VERIFIED", "CANCELLED"),
        ("APPROVED", "PAUSED"),
        ("APPROVED", "STALE"),
        ("APPROVED", "CANCELLED"),
        ("PAUSED", "STALE"),
        ("PAUSED", "CANCELLED"),
    }
    states = {"PROPOSED", "VERIFIED", "APPROVED", "PAUSED", "STALE", "REJECTED", "CANCELLED"}
    for old_state in states:
        for new_state in states:
            assert transition_allowed(old_state, new_state) == ((old_state, new_state) in legal)

    with repo.transaction() as conn:
        for index, (old_state, new_state) in enumerate(sorted(legal)):
            plan_id = f"edge-{index}"
            _insert_plan(conn, plan_id, old_state)
            _, version = Repository.transition_plan(
                conn, plan_id, new_state, "actor", "operator", "edge", f"r-{index}", None
            )
            assert version == 1

    with repo.transaction() as conn:
        _insert_plan(conn, "state-flow")
        old, version = Repository.transition_plan(
            conn, "state-flow", "VERIFIED", "actor", "recommender", "verified", "r1", "k1"
        )
        assert (old, version) == ("PROPOSED", 1)
        Repository.transition_plan(
            conn, "state-flow", "APPROVED", "actor", "operator", "approved", "r2", "k2"
        )
    with pytest.raises(ControlError, match="illegal plan transition"):
        with repo.transaction() as conn:
            Repository.transition_plan(
                conn, "state-flow", "VERIFIED", "actor", "operator", "bad", "r3", None
            )
    with pytest.raises(RuntimeError, match="rollback probe"):
        with repo.transaction() as conn:
            Repository.transition_plan(
                conn, "state-flow", "PAUSED", "actor", "operator", "pause", "r4", None
            )
            raise RuntimeError("rollback probe")
    with repo.transaction() as conn:
        state = conn.execute(
            "SELECT state, state_version FROM plans WHERE plan_id='state-flow'"
        ).fetchone()
        events = conn.execute(
            "SELECT old_state, new_state, state_version, actor, role, request_id, idempotency_key "
            "FROM plan_transitions WHERE plan_id='state-flow' ORDER BY state_version"
        ).fetchall()
    restarted = Repository(clean_database.dsn)
    assert restarted.get_plan("state-flow")["state"] == "APPROVED"
    assert state == {"state": "APPROVED", "state_version": 2}
    assert [(event["old_state"], event["new_state"]) for event in events] == [
        ("PROPOSED", "VERIFIED"),
        ("VERIFIED", "APPROVED"),
    ]
    assert events[0]["actor"] == "actor" and events[0]["role"] == "recommender"
    assert events[0]["idempotency_key"] == "k1"


def test_broken_migration_keeps_readiness_closed(
    control_database: Database, tmp_path: Path
) -> None:
    broken = tmp_path / "999_broken.sql"
    broken.write_text("THIS IS NOT VALID SQL;", encoding="utf-8")
    repo = Repository(control_database.dsn, migrations_dir=tmp_path)
    with pytest.raises(psycopg.Error):
        repo.migrate()
    assert repo.readiness() == (False, "migrations-pending")


def test_persistent_idempotency_lease_conflict_and_artifact_integrity(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    with repo.transaction() as conn:
        assert Repository.begin_idempotent(conn, "p", "op", "key", {"x": 1}) is None
        Repository.finish_idempotent(conn, "p", "op", "key", 201, {"plan_id": "one"})
    with repo.transaction() as conn:
        assert Repository.begin_idempotent(conn, "p", "op", "key", {"x": 1}) == (
            201,
            {"plan_id": "one"},
        )
    with pytest.raises(IdempotencyConflict):
        with repo.transaction() as conn:
            Repository.begin_idempotent(conn, "p", "op", "key", {"x": 2})

    with repo.transaction() as conn:
        first = Repository.acquire_lease(conn, "workload:w", "controller-a", "a", 60_000)
        assert first is not None
    with repo.transaction() as conn:
        assert Repository.acquire_lease(conn, "workload:w", "controller-b", "b", 60_000) is None
        assert Repository.release_lease(conn, "workload:w", "controller-a", first)
        second = Repository.acquire_lease(conn, "workload:w", "controller-b", "b", 60_000)
        assert second is not None and second > first
    with ThreadPoolExecutor(max_workers=2) as executor:
        contenders = list(
            executor.map(_claim_lease, [repo, repo], ["controller-c", "controller-d"])
        )
    assert sum(token is not None for token in contenders) == 1
    with repo.transaction() as conn:
        expiring = Repository.acquire_lease(conn, "workload:expiry", "controller-e", "e", 30)
        assert expiring is not None
    time.sleep(0.06)
    with repo.transaction() as conn:
        renewed = Repository.acquire_lease(conn, "workload:expiry", "controller-f", "f", 60_000)
        assert renewed is not None and renewed > expiring

    with repo.transaction() as conn:
        _insert_plan(conn, "tamper")
        digest = Repository.store_artifact(conn, "snapshot", b"immutable")
        conn.execute(
            "INSERT INTO plan_artifacts (plan_id, role, artifact_sha256) "
            "VALUES ('tamper', 'snapshot', %s)",
            (digest,),
        )
    assert repo.verify_plan_artifacts("tamper")["snapshot"] == b"immutable"
    with repo.transaction() as conn:
        conn.execute("UPDATE artifacts SET content = %s WHERE sha256 = %s", (b"changed", digest))
    with pytest.raises(ArtifactIntegrityError, match="integrity"):
        repo.verify_plan_artifacts("tamper")


def _snapshot(
    now_ms: int, snapshot_id: str | None = None, *, predictive_scale_out: bool = False
) -> dict[str, object]:
    result = oracle_snapshot_fields()
    result.update(
        snapshot_id=snapshot_id or f"prompt2a-{uuid.uuid4().hex}",
        generation=1,
        observed_at_ms=now_ms - 1000,
        expires_at_ms=now_ms + 3_600_000,
        evaluation_time_ms=now_ms,
    )
    result["capacity_profiles"][0]["allocation"] = dict(result["workloads"][0]["current_resources"])
    for profile in result["capacity_profiles"]:
        profile["generated_at_ms"] = now_ms - 1000
        profile["valid_until_ms"] = now_ms + 3_600_000
    if predictive_scale_out:
        workload = result["workloads"][0]
        workload["current_resources"] = {
            "cpu_millicores": 500,
            "memory_mib": 512,
            "network_mbps": 50,
        }
        workload["startup_ms"] = 90_000
        workload["readiness_stabilization_ms"] = 20_000
        result["capacity_profiles"][0]["allocation"] = dict(workload["current_resources"])
        for index in range(3, 8):
            node = deepcopy(result["nodes"][0])
            node["node_id"] = f"extra-node-{index:02d}"
            node["generation"] = 1
            result["nodes"].append(node)
    return result


def _observed_scale_in_evidence(now_ms: int) -> dict[str, object]:
    return {
        "cpu-api": {
            "samples": [
                {
                    "observed_at_ms": now_ms - 300_000 + index * 30_000,
                    "qps_milli": 100_000,
                    "concurrency": 10,
                }
                for index in range(10)
            ],
            "reference_qps_milli": 1_000_000,
            "reference_concurrency": 100,
            "telemetry_coverage": 0.99,
            "required_stabilization_ms": 240_000,
            "minimum_sample_count": 5,
            "minimum_coverage": 0.95,
            "telemetry_max_age_ms": 60_000,
            "recovery_reserve_replicas": 1,
            "required_recovery_reserve": 1,
        }
    }


def _tokens() -> tuple[dict[str, Principal], dict[str, str]]:
    raw = {role: secrets.token_urlsafe(32) for role in ("viewer", "recommender", "operator")}
    return {token: Principal(role, role) for role, token in raw.items()}, raw


def _supported_scale_out_request(now_ms: int) -> dict[str, object]:
    snapshot = _snapshot(now_ms, predictive_scale_out=True)
    snapshot["nodes"] = snapshot["nodes"][:4]
    snapshot["nodes"][3]["labels"] = [("zone", "zone-3"), ("rack", "rack-3")]
    snapshot["workloads"][0]["demand_qps_milli"] = 1_400_000
    snapshot["workloads"][0]["min_topology_domains"] = 3
    snapshot["workloads"][0]["max_topology_skew"] = 0
    return {
        "snapshot": snapshot,
        "scale_in_evidence": _observed_scale_in_evidence(now_ms),
    }


def _create_verified_plan(client: TestClient, raw_tokens: dict[str, str]) -> dict[str, object]:
    now_ms = time.time_ns() // 1_000_000
    response = client.post(
        "/v1/optimize",
        headers={
            "Authorization": f"Bearer {raw_tokens['recommender']}",
            "Idempotency-Key": secrets.token_hex(16),
        },
        json=_supported_scale_out_request(now_ms),
    )
    assert response.status_code == 201, response.text
    assert response.json()["state"] == "VERIFIED", response.json()
    return response.json()


def test_authenticated_api_idempotency_approval_restart_and_fail_closed_execute(
    clean_database: Database,
) -> None:
    repo = clean_database.repository
    tokens, raw_tokens = _tokens()
    app = create_app(repo, tokens=tokens)
    now_ms = time.time_ns() // 1_000_000
    payload = _supported_scale_out_request(now_ms)
    with TestClient(app) as client:
        assert client.get("/healthz").json()["execution_enabled"] is False
        assert client.get("/readyz").status_code == 200
        denied = client.post(
            "/v1/optimize",
            headers={"Idempotency-Key": "anonymous"},
            json=payload,
        )
        assert denied.status_code == 401
        viewer_denied = client.post(
            "/v1/optimize",
            headers={
                "Authorization": f"Bearer {raw_tokens['viewer']}",
                "Idempotency-Key": "viewer-optimize",
            },
            json=payload,
        )
        assert viewer_denied.status_code == 403

        headers = {
            "Authorization": f"Bearer {raw_tokens['recommender']}",
            "Idempotency-Key": "concurrent-optimize",
        }
        with ThreadPoolExecutor(max_workers=2) as executor:
            first, second = list(
                executor.map(
                    lambda _: client.post("/v1/optimize", headers=headers, json=payload), range(2)
                )
            )
        assert first.status_code == second.status_code == 201
        assert first.json() == second.json()
        plan = first.json()
        plan_id = plan["plan_id"]
        assert plan["state"] == "VERIFIED"
        assert (
            plan["forecast_metadata"]["cpu-api"]["fallback_reason"]
            == "FORECAST_INSUFFICIENT_HISTORY"
        )
        assert "policy.json" in plan["artifact_hashes"]
        assert "profile:cpu-api-500m-512-v1.json" in plan["artifact_hashes"]
        artifacts = repo.verify_plan_artifacts(plan_id)
        assert {
            "source_snapshot.json",
            "effective_snapshot.pb",
            "plan.pb",
            "verification.pb",
        } <= set(artifacts)

        changed = deepcopy(payload)
        changed["scheduling_buffer_ms"] = 31_000
        conflict = client.post("/v1/optimize", headers=headers, json=changed)
        assert conflict.status_code == 409

        recommender_approve = client.post(
            f"/v1/plans/{plan_id}/approve",
            headers={"Authorization": headers["Authorization"], "Idempotency-Key": "rec-approve"},
            json={},
        )
        viewer_approve = client.post(
            f"/v1/plans/{plan_id}/approve",
            headers={
                "Authorization": f"Bearer {raw_tokens['viewer']}",
                "Idempotency-Key": "viewer-approve",
            },
            json={},
        )
        assert recommender_approve.status_code == 403
        assert viewer_approve.status_code == 403
        approve_headers = {
            "Authorization": f"Bearer {raw_tokens['operator']}",
            "Idempotency-Key": "operator-approve",
        }
        approved = client.post(f"/v1/plans/{plan_id}/approve", headers=approve_headers, json={})
        assert approved.status_code == 200 and approved.json()["state"] == "APPROVED"
        replayed = client.post(f"/v1/plans/{plan_id}/approve", headers=approve_headers, json={})
        assert replayed.status_code == 200 and replayed.json() == approved.json()
        assert (
            client.get(
                "/v1/state", headers={"Authorization": f"Bearer {raw_tokens['viewer']}"}
            ).status_code
            == 200
        )
        assert (
            client.get(
                "/v1/plans", headers={"Authorization": f"Bearer {raw_tokens['viewer']}"}
            ).status_code
            == 200
        )
        detail = client.get(
            f"/v1/plans/{plan_id}", headers={"Authorization": f"Bearer {raw_tokens['viewer']}"}
        )
        assert detail.status_code == 200 and detail.json()["state"] == "APPROVED"
        assert (
            client.get(
                "/v1/audit", headers={"Authorization": f"Bearer {raw_tokens['viewer']}"}
            ).status_code
            == 200
        )
        execute = client.post(
            f"/v1/plans/{plan_id}/execute",
            headers={
                "Authorization": approve_headers["Authorization"],
                "Idempotency-Key": "execute-disabled",
            },
            json={},
        )
        assert execute.status_code == 409
        assert execute.json()["detail"] == "execution-mode-not-enabled"

    restarted = Repository(clean_database.dsn)
    assert restarted.get_plan(plan_id)["state"] == "APPROVED"
    restarted_app = create_app(restarted, tokens=tokens)
    with TestClient(restarted_app) as restarted_client:
        replayed = restarted_client.post(
            f"/v1/plans/{plan_id}/approve", headers=approve_headers, json={}
        )
        assert replayed.status_code == 200
        assert replayed.json() == approved.json()
    with restarted.transaction() as conn:
        assert (
            conn.execute(
                "SELECT count(*) AS n FROM plan_transitions WHERE plan_id=%s", (plan_id,)
            ).fetchone()["n"]
            == 2
        )
        assert conn.execute("SELECT count(*) AS n FROM idempotency_keys").fetchone()["n"] >= 2


def test_execution_admission_allows_only_supported_plan_actions() -> None:
    resources = {"cpu_millicores": 250, "memory_mib": 256, "network_mbps": 100}
    source_workloads = [
        {"workload_id": "cpu-api", "current_resources": resources},
        {"workload_id": "memory-api", "current_resources": resources},
    ]
    summary = {
        "deactivated_node_ids": [],
        "evacuation_count": 0,
        "decisions": [
            {
                "workload_id": "cpu-api",
                "current_replicas": 3,
                "proposed_replicas": 4,
                "proposed_resources": resources,
            }
        ],
    }
    assert (
        api_module._execution_admission_reasons(summary, source_workloads, "kubernetes-workload")
        == []
    )

    scale_down = {
        **summary,
        "decisions": [{**summary["decisions"][0], "proposed_replicas": 2}],
    }
    assert api_module._execution_admission_reasons(
        scale_down, source_workloads, "kubernetes-workload"
    ) == ["UNSUPPORTED_EXECUTION_ACTION_SCALE_DOWN"]

    move = {**summary, "evacuation_count": 1}
    assert api_module._execution_admission_reasons(
        move, source_workloads, "kubernetes-workload"
    ) == ["UNSUPPORTED_EXECUTION_ACTION_MOVE_REPLICA"]
    deactivate = {**summary, "deactivated_node_ids": ["node-a"]}
    assert api_module._execution_admission_reasons(
        deactivate, source_workloads, "kubernetes-workload"
    ) == ["UNSUPPORTED_EXECUTION_ACTION_DEACTIVATE_NODE"]

    resize = {
        **summary,
        "decisions": [
            {
                **summary["decisions"][0],
                "proposed_resources": {**resources, "cpu_millicores": 500},
            }
        ],
    }
    assert api_module._execution_admission_reasons(
        resize, source_workloads, "kubernetes-workload"
    ) == ["UNSUPPORTED_EXECUTION_ACTION_RESIZE_WORKLOAD"]

    unsupported_workload = {
        **summary,
        "decisions": [{**summary["decisions"][0], "workload_id": "memory-api"}],
    }
    assert api_module._execution_admission_reasons(
        unsupported_workload, source_workloads, "kubernetes-workload"
    ) == ["UNSUPPORTED_EXECUTION_WORKLOAD:memory-api"]


def test_verified_v2_plan_stays_out_of_the_execution_journal(clean_database: Database) -> None:
    from test_evacuation_v2 import _v2_snapshot

    now_ms = time.time_ns() // 1_000_000
    snapshot, _, _ = _v2_snapshot(now_ms)
    tokens, raw_tokens = _tokens()
    target_calls: list[str] = []

    class UnusedTarget:
        def __getattr__(self, name: str) -> object:
            target_calls.append(name)
            raise AssertionError(f"rejected V2 plan reached target method {name}")

    app = create_app(
        clean_database.repository,
        tokens=tokens,
        mode="EXECUTE",
        execution_target="kubernetes-workload",
        target_adapter=UnusedTarget(),
        health_observer=object(),
        start_execution_worker=False,
    )
    with TestClient(app) as client:
        created = client.post(
            "/v1/optimize",
            headers={
                "Authorization": f"Bearer {raw_tokens['recommender']}",
                "Idempotency-Key": "verified-v2-remains-disabled",
            },
            json={"snapshot": snapshot},
        )
        assert created.status_code == 200
        plan = created.json()
        assert plan["state"] == "REJECTED"
        assert plan["verification_accepted"] is True
        assert plan["admission_accepted"] is False
        assert "UNSUPPORTED_EXECUTION_PROTOCOL_V2" in plan["reason_codes"]
        assert plan["candidate_actions"] == []

        app.state.execution_controller.run_once()
        execute = client.post(
            f"/v1/plans/{plan['plan_id']}/execute",
            headers={
                "Authorization": f"Bearer {raw_tokens['operator']}",
                "Idempotency-Key": "verified-v2-execute-remains-disabled",
            },
            json={},
        )
        assert execute.status_code == 409
        assert target_calls == []
        with clean_database.repository.transaction() as conn:
            assert (
                conn.execute(
                    "SELECT count(*) AS n FROM action_intents WHERE plan_id=%s",
                    (plan["plan_id"],),
                ).fetchone()["n"]
                == 0
            )
            assert (
                conn.execute(
                    "SELECT count(*) AS n FROM plan_executions WHERE plan_id=%s",
                    (plan["plan_id"],),
                ).fetchone()["n"]
                == 0
            )


@pytest.mark.parametrize("unsupported_action", ["MOVE_REPLICA", "DEACTIVATE_NODE"])
@pytest.mark.parametrize("mode", ["RECOMMEND", "EXECUTE"])
def test_approval_keeps_unsupported_actions_out_of_execution_journal(
    clean_database: Database, unsupported_action: str, mode: str
) -> None:
    tokens, raw_tokens = _tokens()

    class UnusedTarget:
        def observe_scale(self, _workload_id: str) -> dict[str, object]:
            raise AssertionError("unsupported action reached target observation")

    app = create_app(
        clean_database.repository,
        tokens=tokens,
        mode=mode,
        execution_target="docker",
        target_adapter=UnusedTarget(),
        health_observer=object(),
        start_execution_worker=False,
    )
    with TestClient(app) as client:
        plan = _create_verified_plan(client, raw_tokens)
        with clean_database.repository.transaction() as conn:
            action = conn.execute(
                "SELECT intent_id FROM action_intents WHERE plan_id=%s "
                "ORDER BY action_sequence LIMIT 1",
                (plan["plan_id"],),
            ).fetchone()
            assert action is not None
            conn.execute(
                "UPDATE action_intents SET action_type=%s WHERE plan_id=%s AND intent_id=%s",
                (unsupported_action, plan["plan_id"], action["intent_id"]),
            )

        rejected = client.post(
            f"/v1/plans/{plan['plan_id']}/approve",
            headers={
                "Authorization": f"Bearer {raw_tokens['operator']}",
                "Idempotency-Key": f"reject-{unsupported_action.lower()}",
            },
            json={},
        )
        assert rejected.status_code == 409
        assert rejected.json()["detail"] == "plan contains an unsupported execution action"
        assert clean_database.repository.get_plan(plan["plan_id"])["state"] == "VERIFIED"
        with clean_database.repository.transaction() as conn:
            assert (
                conn.execute(
                    "SELECT count(*) AS n FROM plan_executions WHERE plan_id=%s",
                    (plan["plan_id"],),
                ).fetchone()["n"]
                == 0
            )
            assert (
                conn.execute(
                    "SELECT execution_state FROM action_intents WHERE plan_id=%s AND intent_id=%s",
                    (plan["plan_id"], action["intent_id"]),
                ).fetchone()["execution_state"]
                == "NOT_STARTED"
            )


def test_execute_mode_is_authenticated_async_and_idempotent(clean_database: Database) -> None:
    tokens, raw_tokens = _tokens()

    class NoMutationTarget:
        calls = 0

        def observe_scale(self, _workload_id: str) -> dict[str, object]:
            return {
                "uid": "compose/cpu-service",
                "generation": 1,
                "resource_version": "1",
                "replicas": 3,
                "ready_replicas": 3,
            }

    app = create_app(
        clean_database.repository,
        tokens=tokens,
        mode="EXECUTE",
        execution_target="docker",
        target_adapter=NoMutationTarget(),
        health_observer=object(),
        start_execution_worker=False,
    )
    with TestClient(app) as client:
        now_ms = time.time_ns() // 1_000_000
        payload = _supported_scale_out_request(now_ms)
        optimized = client.post(
            "/v1/optimize",
            headers={
                "Authorization": f"Bearer {raw_tokens['recommender']}",
                "Idempotency-Key": "execute-plan-optimize",
            },
            json=payload,
        )
        assert optimized.status_code == 201, optimized.text
        plan = optimized.json()
        plan_id = plan["plan_id"]
        operator = f"Bearer {raw_tokens['operator']}"
        approved = client.post(
            f"/v1/plans/{plan_id}/approve",
            headers={"Authorization": operator, "Idempotency-Key": "approve-execute"},
            json={},
        )
        assert approved.status_code == 200
        unauthorized = client.post(
            f"/v1/plans/{plan_id}/execute",
            headers={"Idempotency-Key": "unauthorized-execute"},
            json={},
        )
        assert unauthorized.status_code == 401
        headers = {"Authorization": operator, "Idempotency-Key": "execute-once"}
        accepted = client.post(f"/v1/plans/{plan_id}/execute", headers=headers, json={})
        replayed = client.post(f"/v1/plans/{plan_id}/execute", headers=headers, json={})
        assert accepted.status_code == 202, accepted.text
        assert replayed.status_code == 202, replayed.text
        assert accepted.json() == replayed.json()
        assert accepted.json()["state"] == "EXECUTING"
        with clean_database.repository.transaction() as conn:
            execution = conn.execute(
                "SELECT status FROM plan_executions WHERE plan_id = %s", (plan_id,)
            ).fetchone()
            action_states = conn.execute(
                "SELECT execution_state, attempt_number FROM action_intents "
                "WHERE plan_id = %s ORDER BY action_sequence",
                (plan_id,),
            ).fetchall()
            plan_lease_count = conn.execute(
                "SELECT count(*) AS n FROM plan_leases WHERE plan_id = %s", (plan_id,)
            ).fetchone()["n"]
            audit = conn.execute(
                "SELECT actor, role, reason FROM audit_events WHERE plan_id = %s", (plan_id,)
            ).fetchall()
        assert execution["status"] == "EXECUTING"
        assert action_states
        assert all(
            row["execution_state"] == "NOT_STARTED" and row["attempt_number"] == 0
            for row in action_states
        )
        assert app.state.execution_target == "docker"
        assert plan_lease_count > 0
        serialized = repr(audit)
        assert all(token not in serialized for token in raw_tokens.values())
        detail = client.get(
            f"/v1/plans/{plan_id}",
            headers={"Authorization": f"Bearer {raw_tokens['viewer']}"},
        )
        assert detail.status_code == 200
        assert detail.json()["execution"]["status"] == "EXECUTING"
        assert detail.json()["execution"]["controller_fencing_token"] > 0
        assert all(
            action["execution_state"] == "NOT_STARTED"
            for action in detail.json()["candidate_actions"]
        )


def test_pause_cancel_release_ownership_and_prevent_resurrection(
    clean_database: Database,
) -> None:
    tokens, raw_tokens = _tokens()
    app = create_app(clean_database.repository, tokens=tokens)
    with TestClient(app) as client:
        plan = _create_verified_plan(client, raw_tokens)
        plan_id = plan["plan_id"]
        operator = f"Bearer {raw_tokens['operator']}"
        approved = client.post(
            f"/v1/plans/{plan_id}/approve",
            headers={"Authorization": operator, "Idempotency-Key": "approve-for-pause"},
            json={},
        )
        assert approved.status_code == 200 and approved.json()["state"] == "APPROVED"
        with clean_database.repository.transaction() as conn:
            assert conn.execute("SELECT count(*) AS n FROM plan_leases").fetchone()["n"] > 0
        paused = client.post(
            f"/v1/plans/{plan_id}/pause",
            headers={"Authorization": operator, "Idempotency-Key": "pause-approved"},
            json={"reason": "operator pause"},
        )
        assert paused.status_code == 200 and paused.json()["state"] == "PAUSED"
        with clean_database.repository.transaction() as conn:
            assert conn.execute("SELECT count(*) AS n FROM plan_leases").fetchone()["n"] == 0
        cancelled = client.post(
            f"/v1/plans/{plan_id}/cancel",
            headers={"Authorization": operator, "Idempotency-Key": "cancel-paused"},
            json={"reason": "operator cancelled"},
        )
        assert cancelled.status_code == 200 and cancelled.json()["state"] == "CANCELLED"
        resurrected = client.post(
            f"/v1/plans/{plan_id}/approve",
            headers={"Authorization": operator, "Idempotency-Key": "approve-cancelled"},
            json={},
        )
        assert resurrected.status_code == 409
        assert resurrected.json()["detail"] == "illegal-state-transition"


def test_predictive_upper_bound_feeds_planner_and_persists_forecast(
    clean_database: Database,
) -> None:
    tokens, raw_tokens = _tokens()
    app = create_app(clean_database.repository, tokens=tokens)
    now_ms = time.time_ns() // 1_000_000
    samples = [
        {
            "observed_at_ms": now_ms - (120 - index) * 60_000,
            "qps_milli": 2_500_000,
            "concurrency": 350,
        }
        for index in range(120)
    ]
    with TestClient(app) as client:
        response = client.post(
            "/v1/optimize",
            headers={
                "Authorization": f"Bearer {raw_tokens['recommender']}",
                "Idempotency-Key": "predictive-scale-out",
            },
            json={
                "snapshot": _snapshot(now_ms, predictive_scale_out=True),
                "history": {"cpu-api": samples},
                "model": "ewma_trend",
                "scheduling_buffer_ms": 30_000,
                "scale_in_evidence": _observed_scale_in_evidence(now_ms),
            },
        )
        assert response.status_code == 200, response.text
        plan = response.json()
        forecast = plan["forecast_metadata"]["cpu-api"]
        assert plan["state"] == "REJECTED"
        assert plan["verification_accepted"] is True and plan["admission_accepted"] is False
        assert "UNSUPPORTED_EXECUTION_ACTION_DEACTIVATE_NODE" in plan["reason_codes"]
        assert forecast["forecast_status"] == "PREDICTIVE"
        assert forecast["horizon_ms"] == 140_000
        assert len(forecast["forecast_sha256"]) == 64
        assert len(forecast["training_window_sha256"]) == 64
        assert forecast["empirical_upper_bound_coverage"] == 1
        assert "forecast:cpu-api.json" in plan["artifact_hashes"]
        assert "training:cpu-api.json" in plan["artifact_hashes"]
        assert any(
            action["action_type"] == "SET_REPLICA_COUNT"
            and action["payload"]["proposed_replicas"] > action["payload"]["current_replicas"]
            for action in plan["candidate_actions"]
        )


def test_prediction_only_scale_in_is_rejected_and_cannot_be_approved(
    clean_database: Database,
) -> None:
    tokens, raw_tokens = _tokens()
    app = create_app(clean_database.repository, tokens=tokens)
    now_ms = time.time_ns() // 1_000_000
    samples = [
        {
            "observed_at_ms": now_ms - (120 - index) * 60_000,
            "qps_milli": 100_000,
            "concurrency": 10,
        }
        for index in range(120)
    ]
    with TestClient(app) as client:
        response = client.post(
            "/v1/optimize",
            headers={
                "Authorization": f"Bearer {raw_tokens['recommender']}",
                "Idempotency-Key": "prediction-cannot-scale-in",
            },
            json={
                "snapshot": _snapshot(now_ms),
                "history": {"cpu-api": samples},
                "model": "rolling_quantile",
            },
        )
        assert response.status_code == 200
        plan = response.json()
        assert plan["forecast_metadata"]["cpu-api"]["forecast_status"] == "PREDICTIVE"
        assert "PREDICTION_SUGGESTS_LOWER_DEMAND" in plan["reason_codes"]
        assert plan["state"] == "REJECTED" and not plan["admission_accepted"]
        approval = client.post(
            f"/v1/plans/{plan['plan_id']}/approve",
            headers={
                "Authorization": f"Bearer {raw_tokens['operator']}",
                "Idempotency-Key": "prediction-only-approval",
            },
            json={},
        )
        assert approval.status_code == 409
        assert approval.json()["detail"] == "illegal-state-transition"


@pytest.mark.parametrize("changed", ["snapshot", "policy", "profile", "catalog", "workload"])
def test_approval_marks_changed_binding_stale(clean_database: Database, changed: str) -> None:
    tokens, raw_tokens = _tokens()
    app = create_app(clean_database.repository, tokens=tokens)
    with TestClient(app) as client:
        plan = _create_verified_plan(client, raw_tokens)
        with clean_database.repository.transaction() as conn:
            state = conn.execute(
                "SELECT snapshot_generation, binding FROM control_state WHERE singleton"
            ).fetchone()
            binding = deepcopy(state["binding"])
            source = binding
            if changed == "snapshot":
                source["snapshot_generation"] += 1
                conn.execute(
                    "UPDATE control_state SET snapshot_generation=snapshot_generation+1, "
                    "binding=%s WHERE singleton",
                    (Jsonb(source),),
                )
            elif changed == "policy":
                source["policy_hash"] = "e" * 64
            elif changed == "profile":
                profile_id = next(iter(source["profile_hashes"]))
                source["profile_hashes"][profile_id] = "e" * 64
            elif changed == "catalog":
                source["machine_catalog_hash"] = "e" * 64
            else:
                source["workload_hashes"]["cpu-api"] = "e" * 64
            conn.execute("UPDATE control_state SET binding=%s WHERE singleton", (Jsonb(source),))
        response = client.post(
            f"/v1/plans/{plan['plan_id']}/approve",
            headers={
                "Authorization": f"Bearer {raw_tokens['operator']}",
                "Idempotency-Key": f"stale-{changed}",
            },
            json={},
        )
        assert response.status_code == 409
        assert response.json()["state"] == "STALE"


def test_expired_plan_and_tampered_replay_artifact_are_rejected(clean_database: Database) -> None:
    tokens, raw_tokens = _tokens()
    app = create_app(clean_database.repository, tokens=tokens)
    with TestClient(app) as client:
        expired = _create_verified_plan(client, raw_tokens)
        with clean_database.repository.transaction() as conn:
            conn.execute("UPDATE plans SET expires_at_ms=1 WHERE plan_id=%s", (expired["plan_id"],))
        response = client.post(
            f"/v1/plans/{expired['plan_id']}/approve",
            headers={
                "Authorization": f"Bearer {raw_tokens['operator']}",
                "Idempotency-Key": "expired",
            },
            json={},
        )
        assert response.status_code == 409 and response.json()["state"] == "STALE"

    with clean_database.repository.transaction() as conn:
        conn.execute(
            "TRUNCATE control_state, plans, artifacts, idempotency_keys, conflict_leases, "
            "audit_events RESTART IDENTITY CASCADE"
        )
    with TestClient(app) as client:
        plan = _create_verified_plan(client, raw_tokens)
        with clean_database.repository.transaction() as conn:
            row = conn.execute(
                "SELECT artifact_sha256 FROM plan_artifacts WHERE plan_id=%s AND role='plan.pb'",
                (plan["plan_id"],),
            ).fetchone()
            conn.execute(
                "UPDATE artifacts SET content=content || decode('00', 'hex') WHERE sha256=%s",
                (row["artifact_sha256"],),
            )
        response = client.post(
            f"/v1/plans/{plan['plan_id']}/approve",
            headers={
                "Authorization": f"Bearer {raw_tokens['operator']}",
                "Idempotency-Key": "tampered",
            },
            json={},
        )
        assert response.status_code == 409
        assert response.json()["state"] == "REJECTED"
        assert response.json()["detail"] == "artifact-integrity-failure"


def test_durable_plan_expiry_is_bounded_by_profile_validity(clean_database: Database) -> None:
    tokens, raw_tokens = _tokens()
    app = create_app(clean_database.repository, tokens=tokens)
    now_ms = time.time_ns() // 1_000_000
    payload = _supported_scale_out_request(now_ms)
    snapshot = payload["snapshot"]
    profile_deadline = now_ms + 60_000
    snapshot["capacity_profiles"][0]["valid_until_ms"] = profile_deadline
    with TestClient(app) as client:
        response = client.post(
            "/v1/optimize",
            headers={
                "Authorization": f"Bearer {raw_tokens['recommender']}",
                "Idempotency-Key": "profile-expiry",
            },
            json=payload,
        )
    assert response.status_code == 201, response.text
    assert response.json()["expires_at_ms"] == profile_deadline


def test_observe_mode_never_runs_planner_and_malformed_snapshot_is_422(
    clean_database: Database,
) -> None:
    tokens, raw_tokens = _tokens()
    app = create_app(clean_database.repository, tokens=tokens, mode="OBSERVE")
    now_ms = __import__("time").time_ns() // 1_000_000
    payload = {"snapshot": _snapshot(now_ms), "mode": "RECOMMEND"}
    with TestClient(app) as client:
        response = client.post(
            "/v1/optimize",
            headers={
                "Authorization": f"Bearer {raw_tokens['recommender']}",
                "Idempotency-Key": "observe",
            },
            json=payload,
        )
        assert response.status_code == 200
        assert response.json()["planning"] == "disabled"
        assert clean_database.repository.list_plans() == []
        bad = deepcopy(payload)
        bad["snapshot"]["workloads"][0]["demand_qps_milli"] = float("nan")
        response = client.post(
            "/v1/optimize",
            headers={
                "Authorization": f"Bearer {raw_tokens['recommender']}",
                "Idempotency-Key": "bad-json-number",
                "Content-Type": "application/json",
            },
            content=json.dumps(bad, allow_nan=True).encode(),
        )
        assert response.status_code in {400, 422}


def test_database_readiness_reports_unavailable() -> None:
    class UnavailableRepository(Repository):
        @contextmanager
        def transaction(self):
            raise psycopg.OperationalError("database unavailable")
            yield

    assert UnavailableRepository("unused").readiness() == (False, "database-unavailable")


def test_api_rejects_oversized_request_body(clean_database: Database) -> None:
    tokens, _ = _tokens()
    app = create_app(clean_database.repository, tokens=tokens)
    with TestClient(app) as client:
        response = client.post("/v1/optimize", content=b"x" * (MAX_REQUEST_BYTES + 1))
    assert response.status_code == 413
    assert response.json()["detail"] == "request body exceeds 2 MiB"


@pytest.mark.skipif(
    os.getenv("FLEETRIGHT_RUN_KIND_E2E") != "1",
    reason="set FLEETRIGHT_RUN_KIND_E2E=1 for the isolated Kubernetes workload E2E",
)
def test_kubernetes_workload_api_recovers_after_ready_scale_up_crash(
    docker_e2e_database: Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    kubeconfig = os.getenv("FLEETRIGHT_EXECUTOR_KUBECONFIG")
    admin_kubeconfig = os.getenv("FLEETRIGHT_LAB_ADMIN_KUBECONFIG")
    health_url = os.getenv("FLEETRIGHT_KIND_HEALTH_URL")
    assert kubeconfig and admin_kubeconfig and health_url, "Kubeconfigs and health URL are required"
    assert Path(admin_kubeconfig).resolve() == Path(
        "/home/neel/.cache/fleetright/safety-lab.kubeconfig"
    )
    assert Path(kubeconfig).resolve() != Path(admin_kubeconfig).resolve()
    assert health_url.startswith("http://127.0.0.1:")
    assert conninfo_to_dict(docker_e2e_database.dsn).get("host") == "127.0.0.1"

    def api_server(config: str) -> str:
        return subprocess.run(
            [
                "kubectl",
                f"--kubeconfig={config}",
                "config",
                "view",
                "--minify",
                "-o",
                "jsonpath={.clusters[0].cluster.server}",
            ],
            capture_output=True,
            check=True,
            text=True,
            timeout=5,
        ).stdout.strip()

    assert api_server(kubeconfig) == api_server(admin_kubeconfig)
    assert (
        subprocess.run(
            ["kubectl", f"--kubeconfig={kubeconfig}", "config", "current-context"],
            capture_output=True,
            check=True,
            text=True,
            timeout=5,
        ).stdout.strip()
        == "fleetright-controller"
    )
    for args, expected in (
        (("patch", "fleetrightworkloads.fleetright.io/cpu-api"), "yes"),
        (("delete", "pods"), "no"),
        (("create", "pods", "--subresource=eviction"), "no"),
        (("get", "secrets"), "no"),
        (("patch", "nodes"), "no"),
    ):
        access = subprocess.run(
            [
                "kubectl",
                f"--kubeconfig={kubeconfig}",
                "auth",
                "can-i",
                *args,
                "--namespace=fleetright-v2",
            ],
            capture_output=True,
            check=False,
            text=True,
            timeout=5,
        )
        expected_returncode = 0 if expected == "yes" else 1
        assert (access.returncode, access.stdout.strip()) == (
            expected_returncode,
            expected,
        ), (args, access.returncode, access.stdout, access.stderr)
    monkeypatch.setenv("FLEETRIGHT_EXECUTION_LEASE_TTL_MS", "30000")

    target = FleetRightWorkloadTarget(kubeconfig=kubeconfig, namespace="fleetright-v2")
    baseline = target.observe_scale("cpu-api")
    assert baseline["replicas"] == baseline["ready_replicas"] == 3
    health = WorkloadHealthObserver({"cpu-api": health_url})
    traffic_stop = threading.Event()
    traffic_errors: list[str] = []

    def send_traffic() -> None:
        while not traffic_stop.is_set():
            try:
                with urllib.request.urlopen(f"{health_url}/work", timeout=2):
                    pass
            except OSError as error:
                traffic_errors.append(str(error))
            traffic_stop.wait(0.1)

    traffic_thread = threading.Thread(target=send_traffic, daemon=True)
    primary_stop = tmp_path / "kind-primary.stop"
    recovery_start = tmp_path / "kind-recovery.start"
    recovery_stop = tmp_path / "kind-recovery.stop"
    mutation_ready = tmp_path / "kind-mutation.ready"
    process_context = multiprocessing.get_context("spawn")
    primary = process_context.Process(
        target=_run_kind_e2e_controller,
        args=(
            docker_e2e_database.dsn,
            kubeconfig,
            health_url,
            "kind-e2e-controller",
            str(primary_stop),
        ),
        kwargs={"block_after_ready_file": str(mutation_ready), "lease_ttl_ms": 30_000},
    )
    recovery = process_context.Process(
        target=_run_kind_e2e_controller,
        args=(
            docker_e2e_database.dsn,
            kubeconfig,
            health_url,
            "kind-e2e-controller-recovery",
            str(recovery_stop),
        ),
        kwargs={"start_file": str(recovery_start), "lease_ttl_ms": 30_000},
    )
    tokens, raw_tokens = _tokens()
    app = create_app(
        docker_e2e_database.repository,
        tokens=tokens,
        mode="EXECUTE",
        execution_target="kubernetes-workload",
        target_adapter=target,
        health_observer=health,
        start_execution_worker=False,
    )
    try:
        traffic_thread.start()
        time.sleep(1)
        health.observe("cpu-api", p99_limit_ms=120, error_slo_ppm=1_000)
        time.sleep(0.2)
        baseline_health = health.observe("cpu-api", p99_limit_ms=120, error_slo_ppm=1_000)
        assert baseline_health["ready"] is True
        assert baseline_health["fresh_sample"] is True
        assert baseline_health["healthy"] is True
        primary.start()
        recovery.start()
        with TestClient(app) as client:
            plan = _create_verified_plan(client, raw_tokens)
            plan_id = plan["plan_id"]
            approved = client.post(
                f"/v1/plans/{plan_id}/approve",
                headers={
                    "Authorization": f"Bearer {raw_tokens['operator']}",
                    "Idempotency-Key": "kind-e2e-approve",
                },
                json={},
            )
            assert approved.status_code == 200, approved.text
            accepted = client.post(
                f"/v1/plans/{plan_id}/execute",
                headers={
                    "Authorization": f"Bearer {raw_tokens['operator']}",
                    "Idempotency-Key": "kind-e2e-execute",
                },
                json={},
            )
            replayed = client.post(
                f"/v1/plans/{plan_id}/execute",
                headers={
                    "Authorization": f"Bearer {raw_tokens['operator']}",
                    "Idempotency-Key": "kind-e2e-execute",
                },
                json={},
            )
            assert accepted.status_code == replayed.status_code == 202
            assert accepted.json() == replayed.json()

            deadline = time.monotonic() + 150
            while not mutation_ready.exists() and time.monotonic() < deadline:
                assert primary.is_alive(), f"execution controller exited: {primary.exitcode}"
                time.sleep(0.1)
            assert mutation_ready.exists(), "Kubernetes scale did not reach four Ready replicas"
            scaled = target.observe_scale("cpu-api")
            assert scaled["replicas"] == scaled["ready_replicas"] == 4
            with docker_e2e_database.repository.transaction() as conn:
                action_state = conn.execute(
                    "SELECT execution_state FROM action_intents WHERE plan_id = %s",
                    (plan_id,),
                ).fetchone()["execution_state"]
            assert action_state == "RUNNING"
            primary.kill()
            primary.join(timeout=10)
            assert not primary.is_alive()
            assert primary.exitcode != 0

            time.sleep(30.2)
            recovery_start.write_text("start", encoding="ascii")
            detail = client.get(
                f"/v1/plans/{plan_id}",
                headers={"Authorization": f"Bearer {raw_tokens['viewer']}"},
            )
            deadline = time.monotonic() + 120
            while detail.json().get("execution", {}).get("status") != "COMMITTED":
                assert detail.json().get("execution", {}).get("status") not in {
                    "FAILED_SAFE",
                    "COMPENSATING",
                    "COMPENSATED",
                    "STALE",
                }, detail.text
                assert time.monotonic() < deadline, detail.text
                time.sleep(0.2)
                detail = client.get(
                    f"/v1/plans/{plan_id}",
                    headers={"Authorization": f"Bearer {raw_tokens['viewer']}"},
                )
            assert detail.status_code == 200
            final_target = target.observe_scale("cpu-api")
            assert final_target["replicas"] == final_target["ready_replicas"] == 4
            with docker_e2e_database.repository.transaction() as conn:
                execution = conn.execute(
                    "SELECT status FROM plan_executions WHERE plan_id = %s", (plan_id,)
                ).fetchone()
                action = conn.execute(
                    "SELECT execution_state, execution_result FROM action_intents "
                    "WHERE plan_id = %s",
                    (plan_id,),
                ).fetchone()
            assert execution["status"] == "COMMITTED"
            assert action["execution_state"] == "SUCCEEDED"
            assert action["execution_result"]["unknown_resolutions"]
            dispatches = [
                item
                for item in action["execution_result"]["progress"]
                if isinstance(item, dict)
                and isinstance(item.get("result"), dict)
                and "dispatch_target_replicas" in item["result"]
            ]
            assert len(dispatches) == 1
    finally:
        primary_stop.write_text("stop", encoding="ascii")
        recovery_start.write_text("start", encoding="ascii")
        recovery_stop.write_text("stop", encoding="ascii")
        for process in (primary, recovery):
            if process.pid is not None and process.is_alive():
                process.kill()
            if process.pid is not None:
                process.join(timeout=10)
        traffic_stop.set()
        if traffic_thread.ident is not None:
            traffic_thread.join(timeout=5)
    assert not traffic_errors


@pytest.mark.skipif(
    os.getenv("FLEETRIGHT_RUN_DOCKER_E2E") != "1",
    reason="set FLEETRIGHT_RUN_DOCKER_E2E=1 to run the isolated Docker execution test",
)
@pytest.mark.parametrize("dispatch_fault", ["process-crash", "target-timeout"])
def test_docker_api_execution_changes_real_compose_state(
    docker_e2e_database: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dispatch_fault: str,
) -> None:
    tokens, raw_tokens = _tokens()
    root = Path(__file__).resolve().parents[2]
    compose_file = root / "tests/fixtures/docker-execution/compose.yml"
    project = f"fleetright-e2e-{uuid.uuid4().hex[:8]}"
    monkeypatch.setenv("FLEETRIGHT_DOCKER_FENCE_DIR", str(tmp_path / "docker-fences"))
    monkeypatch.setenv("FLEETRIGHT_READINESS_TIMEOUT_SECONDS", "90")
    monkeypatch.setenv("FLEETRIGHT_EXECUTION_LEASE_TTL_MS", "5000")
    monkeypatch.setenv("FLEETRIGHT_RECONCILE_POLL_SECONDS", "5")

    target = DockerComposeTarget(compose_file=compose_file, project=project)
    traffic_stop = threading.Event()
    traffic_errors: list[str] = []
    traffic_thread: threading.Thread | None = None
    primary_stop = tmp_path / "primary.stop"
    recovery_start = tmp_path / "recovery.start"
    recovery_stop = tmp_path / "recovery.stop"
    mutation_done = tmp_path / "mutation.done"
    timeout_marker = tmp_path / "timeout-applied"
    primary: multiprocessing.Process | None = None
    recovery: multiprocessing.Process | None = None
    try:
        target._compose(
            "up",
            "--detach",
            "--build",
            "--scale",
            "cpu-service=3",
            "cpu-service",
            timeout=900,
        )
        deadline = time.monotonic() + 120
        observed = target.observe_scale("cpu-api")
        while (
            observed["replicas"] != 3 or observed["ready_replicas"] != 3
        ) and time.monotonic() < deadline:
            time.sleep(1)
            observed = target.observe_scale("cpu-api")
        assert observed["replicas"] == observed["ready_replicas"] == 3

        inspect = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                observed["container_ids"][0],
            ],
            capture_output=True,
            check=True,
            text=True,
            timeout=10,
        )
        address = inspect.stdout.strip()
        assert address
        health_url = f"http://{address}:8080"
        health = WorkloadHealthObserver({"cpu-api": health_url})

        def send_workload_traffic() -> None:
            while not traffic_stop.is_set():
                try:
                    with urllib.request.urlopen(f"{health_url}/work", timeout=2):
                        pass
                except OSError as error:
                    traffic_errors.append(str(error))
                traffic_stop.wait(0.1)

        traffic_thread = threading.Thread(target=send_workload_traffic, daemon=True)
        traffic_thread.start()
        time.sleep(1)
        health.observe(
            "cpu-api",
            p99_limit_ms=120,
            error_slo_ppm=1_000,
            max_queue_growth_milli=0,
        )
        time.sleep(0.2)
        baseline_health = health.observe(
            "cpu-api",
            p99_limit_ms=120,
            error_slo_ppm=1_000,
            max_queue_growth_milli=0,
        )
        assert baseline_health["ready"] is True
        assert baseline_health["fresh_sample"] is True
        assert baseline_health["healthy"] is True

        process_context = multiprocessing.get_context("spawn")
        primary_kwargs = (
            {"crash_after_scale_file": str(mutation_done)}
            if dispatch_fault == "process-crash"
            else {
                "timeout_after_scale_project": project,
                "timeout_shim_dir": str(tmp_path / "docker-shim"),
                "timeout_marker": str(timeout_marker),
            }
        )
        primary = process_context.Process(
            target=_run_docker_e2e_controller,
            args=(
                docker_e2e_database.dsn,
                str(compose_file),
                project,
                health_url,
                "docker-e2e-controller",
                str(primary_stop),
            ),
            kwargs=primary_kwargs,
        )
        recovery = process_context.Process(
            target=_run_docker_e2e_controller,
            args=(
                docker_e2e_database.dsn,
                str(compose_file),
                project,
                health_url,
                "docker-e2e-controller-recovery",
                str(recovery_stop),
            ),
            kwargs={"start_file": str(recovery_start)},
        )
        primary.start()
        recovery.start()

        app = create_app(
            docker_e2e_database.repository,
            tokens=tokens,
            mode="EXECUTE",
            execution_target="docker",
            target_adapter=target,
            health_observer=health,
            start_execution_worker=False,
        )
        with TestClient(app) as client:
            now_ms = time.time_ns() // 1_000_000
            optimized = client.post(
                "/v1/optimize",
                headers={
                    "Authorization": f"Bearer {raw_tokens['recommender']}",
                    "Idempotency-Key": "docker-e2e-optimize",
                },
                json=_supported_scale_out_request(now_ms),
            )
            assert optimized.status_code == 201, optimized.text
            plan = optimized.json()
            assert plan["state"] == "VERIFIED"
            plan_id = plan["plan_id"]

            approved = client.post(
                f"/v1/plans/{plan_id}/approve",
                headers={
                    "Authorization": f"Bearer {raw_tokens['operator']}",
                    "Idempotency-Key": "docker-e2e-approve",
                },
                json={},
            )
            assert approved.status_code == 200, approved.text
            denied = client.post(
                f"/v1/plans/{plan_id}/execute",
                headers={"Idempotency-Key": "docker-e2e-anonymous"},
                json={},
            )
            assert denied.status_code == 401

            headers = {
                "Authorization": f"Bearer {raw_tokens['operator']}",
                "Idempotency-Key": "docker-e2e-execute",
            }
            accepted = client.post(f"/v1/plans/{plan_id}/execute", headers=headers, json={})
            replayed = client.post(f"/v1/plans/{plan_id}/execute", headers=headers, json={})
            assert accepted.status_code == replayed.status_code == 202
            assert accepted.json() == replayed.json()
            if dispatch_fault == "process-crash":
                deadline = time.monotonic() + 60
                while not mutation_done.exists() and time.monotonic() < deadline:
                    assert primary.is_alive(), f"execution controller exited: {primary.exitcode}"
                    time.sleep(0.1)
                assert mutation_done.exists(), "controller did not reach the post-mutation boundary"
                assert target.observe_scale("cpu-api")["replicas"] == 4
                with docker_e2e_database.repository.transaction() as conn:
                    action_state = conn.execute(
                        "SELECT execution_state FROM action_intents WHERE plan_id = %s",
                        (plan_id,),
                    ).fetchone()["execution_state"]
                assert action_state == "RUNNING"
                primary.kill()
                primary.join(timeout=10)
                assert not primary.is_alive()
                assert primary.exitcode != 0
            else:
                deadline = time.monotonic() + 120
                while not timeout_marker.exists() and time.monotonic() < deadline:
                    assert primary.is_alive(), f"execution controller exited: {primary.exitcode}"
                    time.sleep(0.1)
                assert timeout_marker.exists(), "Compose mutation did not reach the timeout shim"
                assert target.observe_scale("cpu-api")["replicas"] == 4

        if dispatch_fault == "process-crash":
            time.sleep(5.2)
            recovery_start.write_text("start", encoding="ascii")
        with TestClient(app) as client:
            detail = client.get(
                f"/v1/plans/{plan_id}",
                headers={"Authorization": f"Bearer {raw_tokens['viewer']}"},
            )
            deadline = time.monotonic() + 120
            while detail.json().get("execution", {}).get("status") != "COMMITTED":
                assert detail.json().get("execution", {}).get("status") not in {
                    "FAILED_SAFE",
                    "COMPENSATING",
                    "COMPENSATED",
                    "STALE",
                }, detail.text
                assert time.monotonic() < deadline, detail.text
                time.sleep(0.2)
                detail = client.get(
                    f"/v1/plans/{plan_id}",
                    headers={"Authorization": f"Bearer {raw_tokens['viewer']}"},
                )
            assert detail.status_code == 200
            final_target = target.observe_scale("cpu-api")
            assert final_target["replicas"] == final_target["ready_replicas"] == 4
            with docker_e2e_database.repository.transaction() as conn:
                execution = conn.execute(
                    "SELECT status FROM plan_executions WHERE plan_id = %s", (plan_id,)
                ).fetchone()
                action = conn.execute(
                    "SELECT execution_state, execution_result FROM action_intents "
                    "WHERE plan_id = %s",
                    (plan_id,),
                ).fetchone()
            assert execution["status"] == "COMMITTED"
            assert action["execution_state"] == "SUCCEEDED"
            assert action["execution_result"]["target_observation"]["replicas"] == 4
            assert action["execution_result"]["unknown_resolutions"]
            dispatches = [
                item
                for item in action["execution_result"]["progress"]
                if isinstance(item, dict)
                and isinstance(item.get("result"), dict)
                and "dispatch_target_replicas" in item["result"]
            ]
            assert len(dispatches) == 1
    finally:
        primary_stop.write_text("stop", encoding="ascii")
        recovery_start.write_text("start", encoding="ascii")
        recovery_stop.write_text("stop", encoding="ascii")
        for process in (primary, recovery):
            if process is not None and process.pid is not None and process.is_alive():
                process.kill()
            if process is not None and process.pid is not None:
                process.join(timeout=10)
        traffic_stop.set()
        if traffic_thread is not None:
            traffic_thread.join(timeout=5)
        target._compose("down", "--remove-orphans", timeout=120)
    assert not traffic_errors


@pytest.mark.skipif(
    os.getenv("FLEETRIGHT_RUN_DOCKER_E2E") != "1",
    reason="set FLEETRIGHT_RUN_DOCKER_E2E=1 to run the isolated Docker health-regression test",
)
def test_docker_api_health_regression_fails_safe_without_downscaling(
    docker_e2e_database: Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tokens, raw_tokens = _tokens()
    root = Path(__file__).resolve().parents[2]
    compose_file = root / "tests/fixtures/docker-execution/compose.yml"
    project = f"fleetright-regression-{uuid.uuid4().hex[:8]}"
    monkeypatch.setenv("FLEETRIGHT_DOCKER_FENCE_DIR", str(tmp_path / "docker-fences"))
    monkeypatch.setenv("FLEETRIGHT_EXECUTION_LEASE_TTL_MS", "30000")
    target = DockerComposeTarget(compose_file=compose_file, project=project)
    traffic_stop = threading.Event()
    traffic_errors: list[str] = []
    traffic_thread: threading.Thread | None = None
    controller_stop = tmp_path / "controller.stop"
    regression_done = tmp_path / "regression.done"
    controller: multiprocessing.Process | None = None
    try:
        target._compose(
            "up",
            "--detach",
            "--build",
            "--scale",
            "cpu-service=3",
            "cpu-service",
            timeout=900,
        )
        deadline = time.monotonic() + 120
        observed = target.observe_scale("cpu-api")
        while (
            observed["replicas"] != 3 or observed["ready_replicas"] != 3
        ) and time.monotonic() < deadline:
            time.sleep(1)
            observed = target.observe_scale("cpu-api")
        assert observed["replicas"] == observed["ready_replicas"] == 3

        inspect = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                observed["container_ids"][0],
            ],
            capture_output=True,
            check=True,
            text=True,
            timeout=10,
        )
        health_url = f"http://{inspect.stdout.strip()}:8080"
        health = WorkloadHealthObserver({"cpu-api": health_url})

        def send_traffic() -> None:
            while not traffic_stop.is_set():
                try:
                    with urllib.request.urlopen(f"{health_url}/work", timeout=2):
                        pass
                except OSError as error:
                    traffic_errors.append(str(error))
                traffic_stop.wait(0.05)

        traffic_thread = threading.Thread(target=send_traffic, daemon=True)
        traffic_thread.start()
        time.sleep(1)
        health.observe("cpu-api", p99_limit_ms=120, error_slo_ppm=1_000)
        time.sleep(0.2)
        baseline = health.observe("cpu-api", p99_limit_ms=120, error_slo_ppm=1_000)
        assert baseline["ready"] is True and baseline["fresh_sample"] is True
        assert baseline["healthy"] is True
        traffic_stop.set()
        traffic_thread.join(timeout=5)

        process_context = multiprocessing.get_context("spawn")
        controller = process_context.Process(
            target=_run_docker_e2e_controller,
            args=(
                docker_e2e_database.dsn,
                str(compose_file),
                project,
                health_url,
                "docker-e2e-regression-controller",
                str(controller_stop),
            ),
            kwargs={
                "health_regression_container_id": observed["container_ids"][0],
                "health_regression_file": str(regression_done),
                "lease_ttl_ms": 30_000,
                "readiness_timeout_seconds": 8,
            },
        )
        controller.start()
        app = create_app(
            docker_e2e_database.repository,
            tokens=tokens,
            mode="EXECUTE",
            execution_target="docker",
            target_adapter=target,
            health_observer=health,
            start_execution_worker=False,
        )
        with TestClient(app) as client:
            plan = _create_verified_plan(client, raw_tokens)
            plan_id = plan["plan_id"]
            approved = client.post(
                f"/v1/plans/{plan_id}/approve",
                headers={
                    "Authorization": f"Bearer {raw_tokens['operator']}",
                    "Idempotency-Key": "docker-regression-approve",
                },
                json={},
            )
            assert approved.status_code == 200, approved.text
            accepted = client.post(
                f"/v1/plans/{plan_id}/execute",
                headers={
                    "Authorization": f"Bearer {raw_tokens['operator']}",
                    "Idempotency-Key": "docker-regression-execute",
                },
                json={},
            )
            assert accepted.status_code == 202, accepted.text
            deadline = time.monotonic() + 60
            while not regression_done.exists() and time.monotonic() < deadline:
                assert controller.is_alive(), f"controller exited: {controller.exitcode}"
                time.sleep(0.1)
            assert regression_done.exists(), "controller did not induce the post-scale failure"
            traffic_stop.set()
            if traffic_thread is not None:
                traffic_thread.join(timeout=5)

            deadline = time.monotonic() + 60
            detail = client.get(
                f"/v1/plans/{plan_id}",
                headers={"Authorization": f"Bearer {raw_tokens['viewer']}"},
            )
            while detail.json().get("execution", {}).get("status") != "FAILED_SAFE":
                assert controller.is_alive(), f"controller exited: {controller.exitcode}"
                assert time.monotonic() < deadline, detail.text
                time.sleep(0.2)
                detail = client.get(
                    f"/v1/plans/{plan_id}",
                    headers={"Authorization": f"Bearer {raw_tokens['viewer']}"},
                )
            assert detail.status_code == 200
            final_target = target.observe_scale("cpu-api")
            assert final_target["replicas"] == 4
            assert final_target["ready_replicas"] == 3
            with docker_e2e_database.repository.transaction() as conn:
                execution = conn.execute(
                    "SELECT status FROM plan_executions WHERE plan_id = %s", (plan_id,)
                ).fetchone()
                action = conn.execute(
                    "SELECT execution_state, execution_error FROM action_intents "
                    "WHERE plan_id = %s",
                    (plan_id,),
                ).fetchone()
                compensation_count = conn.execute(
                    "SELECT count(*) AS count FROM action_intents "
                    "WHERE plan_id = %s AND compensates_intent_id IS NOT NULL",
                    (plan_id,),
                ).fetchone()["count"]
            assert execution["status"] == "FAILED_SAFE"
            assert action == {
                "execution_state": "FAILED",
                "execution_error": "replacement-readiness-timeout",
            }
            assert compensation_count == 0
    finally:
        traffic_stop.set()
        if traffic_thread is not None:
            traffic_thread.join(timeout=5)
        controller_stop.write_text("stop", encoding="ascii")
        if controller is not None and controller.pid is not None:
            if controller.is_alive():
                controller.terminate()
            controller.join(timeout=10)
        target._compose("down", "--remove-orphans", timeout=120)
    assert not traffic_errors
