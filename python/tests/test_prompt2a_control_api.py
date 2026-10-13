from __future__ import annotations

import base64
import json
import multiprocessing
import os
import secrets
import shutil
import socket
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
import yaml
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
    TargetPreconditionFailed,
    WorkloadHealthObserver,
)
from fleetright.reconciler import ExecutionController
from fleetright.scenario import oracle_snapshot_fields
from fleetright.workload_controller import REPLICA_ID_LABEL, _placement_fingerprint
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.types.json import Jsonb


@dataclass(frozen=True)
class Database:
    repository: Repository
    dsn: str = field(repr=False)


class _LiveHealthyObserver:
    def observe(self, _workload_id: str, **_kwargs: object) -> dict[str, object]:
        return {
            "ready": True,
            "fresh_sample": True,
            "healthy": True,
            "p99_within_slo": True,
            "error_rate_within_slo": True,
            "queue_growth_within_limit": True,
            "observed_at_ms": time.time_ns() // 1_000_000,
        }


def _run_live_drain_controller_until_two_done(
    dsn_file: str,
    kubeconfig: str,
    plan_id: str,
    controller_id: str,
    pending_marker: str,
    release_marker: str,
    admitted_marker: str,
    eviction_log: str,
    diagnostics_file: str,
    target_errors_file: str,
) -> None:
    repository = Repository(Path(dsn_file).read_text(encoding="utf-8").strip())
    target = FleetRightWorkloadTarget(kubeconfig=kubeconfig, namespace="fleetright-v2")
    real_kubectl = target._kubectl
    eviction_count = 0

    def trace_kubectl(*args: str, stdin: str | None = None):
        nonlocal eviction_count
        eviction = next((arg for arg in args if arg.endswith("/eviction")), None)
        if eviction is None:
            return real_kubectl(*args, stdin=stdin)
        eviction_count += 1
        if eviction_count == 3:
            Path(pending_marker).write_text("third-eviction-pending", encoding="ascii")
            deadline = time.monotonic() + 3_600
            while time.monotonic() < deadline and not Path(release_marker).exists():
                time.sleep(0.05)
        with Path(eviction_log).open("a", encoding="utf-8") as output:
            output.write(json.dumps(eviction) + "\n")
        try:
            response = real_kubectl(*args, stdin=stdin)
        except Exception as error:
            with Path(target_errors_file).open("a", encoding="utf-8") as output:
                output.write(
                    json.dumps(
                        {
                            "path": eviction,
                            "error_type": type(error).__name__,
                            "reason": str(error),
                        }
                    )
                    + "\n"
                )
            raise
        if eviction_count == 3:
            Path(admitted_marker).write_text("third-eviction-admitted", encoding="ascii")
            threading.Event().wait(3_600)
        return response

    target._kubectl = trace_kubectl  # type: ignore[method-assign]
    controller = ExecutionController(
        repository,
        target,
        _LiveHealthyObserver(),
        controller_id,
        readiness_timeout_seconds=300,
        poll_seconds=0.1,
    )
    deadline = time.monotonic() + 300
    try:
        while time.monotonic() < deadline:
            controller.run_once()
            with repository.transaction() as conn:
                execution = conn.execute(
                    "SELECT status, controller_id, controller_fencing_token "
                    "FROM plan_executions WHERE plan_id = %s",
                    (plan_id,),
                ).fetchone()
                actions = conn.execute(
                    "SELECT action_sequence, action_type, execution_state, execution_error, "
                    "execution_result FROM action_intents WHERE plan_id = %s "
                    "ORDER BY action_sequence",
                    (plan_id,),
                ).fetchall()
            action_diagnostics = []
            for action in actions:
                result = action["execution_result"]
                progress = result.get("progress", []) if isinstance(result, dict) else []
                action_diagnostics.append(
                    {
                        "sequence": action["action_sequence"],
                        "type": action["action_type"],
                        "state": action["execution_state"],
                        "error": action["execution_error"],
                        "stages": [
                            item.get("result", {}).get("stage")
                            for item in progress
                            if isinstance(item, dict) and isinstance(item.get("result"), dict)
                        ][-8:],
                    }
                )
            Path(diagnostics_file).write_text(
                json.dumps(
                    {
                        "execution": {
                            "status": execution["status"],
                            "controller_id": execution["controller_id"],
                            "fencing_token": execution["controller_fencing_token"],
                        },
                        "actions": action_diagnostics,
                    }
                ),
                encoding="utf-8",
            )
            status = execution["status"]
            if status in {"FAILED_SAFE", "COMPENSATING", "COMMITTED"}:
                Path(pending_marker).write_text(f"stopped:{status}", encoding="ascii")
                return
            time.sleep(0.1)
        Path(pending_marker).write_text("timeout", encoding="ascii")
    except BaseException as error:
        Path(pending_marker).write_text(f"error:{type(error).__name__}", encoding="ascii")
        raise


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


def _supported_scale_out_request(
    now_ms: int,
    *,
    current_replicas: int = 3,
    demand_qps_milli: int = 1_400_000,
) -> dict[str, object]:
    snapshot = _snapshot(now_ms, predictive_scale_out=True)
    snapshot["nodes"] = snapshot["nodes"][: max(4, current_replicas + 1)]
    for index in range(3, len(snapshot["nodes"])):
        snapshot["nodes"][index]["labels"] = [
            ("zone", f"zone-{index}"),
            ("rack", f"rack-{index}"),
        ]
    placements = snapshot["current_placements"]
    while len(placements) < current_replicas:
        index = len(placements)
        placements.append(
            {
                "workload_id": "cpu-api",
                "replica_id": f"cpu-api-{index}",
                "node_id": snapshot["nodes"][index]["node_id"],
            }
        )
    snapshot["workloads"][0]["demand_qps_milli"] = demand_qps_milli
    snapshot["workloads"][0]["min_topology_domains"] = 3
    snapshot["workloads"][0]["max_topology_skew"] = 0
    return {
        "snapshot": snapshot,
        "scale_in_evidence": _observed_scale_in_evidence(now_ms),
    }


def test_kubernetes_execution_allowlist_can_enroll_a_new_managed_workload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FLEETRIGHT_KUBERNETES_WORKLOADS", raising=False)
    assert api_module._supported_execution_workloads("kubernetes-workload") == {"cpu-api"}
    monkeypatch.setenv("FLEETRIGHT_KUBERNETES_WORKLOADS", "cpu-api, p2b-live-test")
    assert api_module._supported_execution_workloads("kubernetes-workload") == {
        "cpu-api",
        "p2b-live-test",
    }
    monkeypatch.setenv("FLEETRIGHT_KUBERNETES_WORKLOADS", "cpu-api,cpu-api")
    with pytest.raises(RuntimeError, match="unique DNS labels"):
        api_module._supported_execution_workloads("kubernetes-workload")


def _create_verified_plan(
    client: TestClient,
    raw_tokens: dict[str, str],
    *,
    current_replicas: int = 3,
    demand_qps_milli: int = 1_400_000,
) -> dict[str, object]:
    now_ms = time.time_ns() // 1_000_000
    response = client.post(
        "/v1/optimize",
        headers={
            "Authorization": f"Bearer {raw_tokens['recommender']}",
            "Idempotency-Key": secrets.token_hex(16),
        },
        json=_supported_scale_out_request(
            now_ms,
            current_replicas=current_replicas,
            demand_qps_milli=demand_qps_milli,
        ),
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


def test_v2_plan_publishes_journaled_replacement_intent(
    clean_database: Database,
) -> None:
    from test_evacuation_v2 import _v2_snapshot

    now_ms = time.time_ns() // 1_000_000
    snapshot, _, _ = _v2_snapshot(now_ms)
    tokens, raw_tokens = _tokens()

    class RetirementTarget(FleetRightWorkloadTarget):
        def __init__(self) -> None:
            super().__init__(namespace="default")
            self.calls = 0
            self.source: dict[str, object] | None = None
            self.workload = {
                "metadata": {
                    "name": "cpu-api",
                    "uid": "workload-uid",
                    "generation": 1,
                    "resourceVersion": "1",
                },
                "spec": {
                    "workloadID": "cpu-api",
                    "executionProtocol": "monotonic-scale-up-v1",
                    "podCreateProtocol": "reserved-create-v1",
                    "podCreateAdmissionProtocol": "claimed-create-v1",
                    "replicas": 3,
                    "availabilityFloor": 1,
                    "surgeBudget": 2,
                    "placement": {
                        "requirePodAntiAffinity": True,
                        "allowedNodes": [
                            {
                                "name": item["node_id"],
                                "uid": item["node_uid"],
                            }
                            for item in snapshot["evacuation_v2"]["nodes"]
                        ],
                    },
                },
                "status": {
                    "observedGeneration": 1,
                    "replicas": 3,
                    "readyReplicas": 3,
                    "availableReplicas": 3,
                    "conditions": [{"type": "Reconciled", "status": "True"}],
                },
            }

        def observe_scale(self, _workload_id: str) -> dict[str, object]:
            return self._observe(self.workload, "cpu-api")

        def _require_fencing_schema(self) -> None:
            return None

        def _json(self, *args: str) -> dict[str, object]:
            if self.source is None:
                raise AssertionError("retirement target attempted an unsupported Kubernetes call")
            if args[0] == "patch" and args[1] == "fleetrightworkload":
                patch = json.loads(args[args.index("-p") + 1])
                metadata = self.workload["metadata"]
                if patch["metadata"]["resourceVersion"] != metadata["resourceVersion"]:
                    raise TargetPreconditionFailed("workload resourceVersion changed")
                self.workload["spec"].update(patch["spec"])
                metadata["generation"] += 1
                metadata["resourceVersion"] = str(int(metadata["resourceVersion"]) + 1)
                return deepcopy(self.workload)
            if args[0] != "get":
                raise AssertionError("retirement target attempted an unsupported Kubernetes call")
            if args[1] == "pod":
                return {
                    "metadata": {
                        "name": self.source["source_pod_name"],
                        "uid": self.source["source_pod_uid"],
                        "labels": {
                            "fleetright.io/workload-uid": "workload-uid",
                            "fleetright.io/replica-id": self.source["source_logical_slot_id"],
                        },
                        "ownerReferences": [
                            {
                                "kind": "FleetRightWorkload",
                                "name": "cpu-api",
                                "uid": "workload-uid",
                                "controller": True,
                            }
                        ],
                    },
                    "spec": {"nodeName": self.source["source_node_id"]},
                }
            if args[1] == "node":
                return {
                    "metadata": {
                        "name": self.source["source_node_id"],
                        "uid": self.source["source_node_uid"],
                    }
                }
            raise AssertionError("retirement target read an unexpected Kubernetes resource")

        def prepare_source_retirement(
            self,
            _workload_id: str,
            _plan_id: str,
            _payload: dict[str, object],
            *,
            fencing_token: int | None = None,
        ) -> dict[str, object]:
            self.calls += 1
            self.source = _payload
            return super().prepare_source_retirement(
                _workload_id,
                _plan_id,
                _payload,
                fencing_token=fencing_token,
            )

    target = RetirementTarget()

    app = create_app(
        clean_database.repository,
        tokens=tokens,
        mode="EXECUTE",
        execution_target="kubernetes-workload",
        target_adapter=target,
        health_observer=object(),
        start_execution_worker=False,
    )
    with TestClient(app) as client:
        created = client.post(
            "/v1/optimize",
            headers={
                "Authorization": f"Bearer {raw_tokens['recommender']}",
                "Idempotency-Key": "verified-v2-retirement-preparation",
            },
            json={"snapshot": snapshot},
        )
        assert created.status_code == 201, created.text
        plan = created.json()
        assert plan["state"] == "VERIFIED"
        assert plan["verification_accepted"] is True and plan["admission_accepted"] is True
        action = plan["candidate_actions"][0]
        assert action["action_type"] == "PREPARE_SOURCE_RETIREMENT"
        assert action["payload"]["source_pod_uid"]
        assert action["payload"]["source_node_uid"]
        expected_nodes = {
            node["node_id"]: node["node_uid"] for node in snapshot["evacuation_v2"]["nodes"]
        }
        assert action["payload"]["destination_nodes"] == [
            {"name": name, "uid": expected_nodes[name]}
            for name in action["payload"]["destination_node_ids"]
        ]
        target.source = action["payload"]
        with clean_database.repository.transaction() as conn:
            artifacts = Repository.load_artifacts(conn, plan["plan_id"])
            journal_actions = conn.execute(
                "SELECT intent_id, action_type, workload_id, payload FROM action_intents "
                "WHERE plan_id = %s ORDER BY action_sequence",
                (plan["plan_id"],),
            ).fetchall()
        decoded_plan = api_module.decode_text(
            "fleetright.v2.EvacuationPlan",
            "fleetright/v2/evacuation.proto",
            artifacts["plan.pb"],
        )
        plan_summary = api_module._evacuation_plan_summary(decoded_plan)
        assert plan_summary["mode"] == 0
        assert plan_summary["evacuations"][0]["sequence"] == 0
        assert action["payload"]["sequence"] == 0
        api_module._verify_source_retirement_actions(artifacts, plan["plan_id"], 2, journal_actions)
        forged_actions = deepcopy(journal_actions)
        forged_actions[0]["payload"]["source_pod_uid"] = "different-source"
        with pytest.raises(ControlError, match="does not match verified V2 operation"):
            api_module._verify_source_retirement_actions(
                artifacts, plan["plan_id"], 2, forged_actions
            )
        forged_actions = deepcopy(journal_actions)
        forged_actions[0]["payload"]["destination_nodes"][0]["uid"] = "different-node-uid"
        with pytest.raises(ControlError, match="does not match verified V2 operation"):
            api_module._verify_source_retirement_actions(
                artifacts, plan["plan_id"], 2, forged_actions
            )
        forged_actions = deepcopy(journal_actions)
        forged_actions.append(
            {
                **journal_actions[0],
                "intent_id": "unverified-scale-action",
                "action_type": "SET_REPLICA_COUNT",
            }
        )
        with pytest.raises(ControlError, match="does not match verified V2 operations"):
            api_module._verify_source_retirement_actions(
                artifacts, plan["plan_id"], 2, forged_actions
            )

        operator = f"Bearer {raw_tokens['operator']}"
        approved = client.post(
            f"/v1/plans/{plan['plan_id']}/approve",
            headers={"Authorization": operator, "Idempotency-Key": "approve-v2-retirement"},
            json={},
        )
        assert approved.status_code == 200 and approved.json()["state"] == "APPROVED", approved.text
        assert clean_database.repository.get_plan(plan["plan_id"])["state"] == "APPROVED"
        execute = client.post(
            f"/v1/plans/{plan['plan_id']}/execute",
            headers={"Authorization": operator, "Idempotency-Key": "execute-v2-retirement"},
            json={},
        )
        assert execute.status_code == 202, execute.text
        execute_replay = client.post(
            f"/v1/plans/{plan['plan_id']}/execute",
            headers={"Authorization": operator, "Idempotency-Key": "execute-v2-retirement"},
            json={},
        )
        assert execute_replay.status_code == 202
        app.state.execution_controller.run_once()

        with clean_database.repository.transaction() as conn:
            stored_action = conn.execute(
                "SELECT action_type, execution_state, execution_error, execution_result "
                "FROM action_intents "
                "WHERE plan_id = %s",
                (plan["plan_id"],),
            ).fetchone()
            execution_state = conn.execute(
                "SELECT status FROM plan_executions WHERE plan_id = %s",
                (plan["plan_id"],),
            ).fetchone()["status"]
        assert stored_action["action_type"] == "PREPARE_SOURCE_RETIREMENT"
        assert stored_action["execution_state"] == "RUNNING", stored_action
        assert stored_action["execution_error"] is None
        assert stored_action["execution_result"]["progress"][-1]["result"]["stage"] == (
            "REPLACEMENT_REQUESTED"
        )
        assert target.workload["spec"]["executionRequest"]["purpose"] == ("staged-replacement-v1")
        assert execution_state == "EXECUTING"
        assert target.calls == 1


@pytest.mark.skipif(
    os.getenv("FLEETRIGHT_RUN_KIND_E2E") != "1",
    reason="set FLEETRIGHT_RUN_KIND_E2E=1 for live API-to-workload execution",
)
@pytest.mark.parametrize(
    ("takeover_after_eviction_acceptance", "health_regression", "unplaceable_recovery"),
    [(False, False, False), (True, False, False), (False, True, False), (False, True, True)],
    ids=[
        "api-evacuation",
        "takeover-held-response",
        "health-compensation",
        "health-compensation-unplaceable",
    ],
)
def test_live_v2_api_journal_controller_prepares_replacement(
    clean_database: Database,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    takeover_after_eviction_acceptance: bool,
    health_regression: bool,
    unplaceable_recovery: bool,
) -> None:
    from test_evacuation_v2 import _v2_snapshot

    kubeconfig = os.getenv(
        "FLEETRIGHT_LAB_ADMIN_KUBECONFIG",
        os.path.expanduser("~/.cache/fleetright/safety-lab.kubeconfig"),
    )
    context = os.getenv("FLEETRIGHT_LAB_CONTEXT", "kind-fleetright-safety-lab")

    def kubectl(
        *args: str, check: bool = True, **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            ["kubectl", f"--kubeconfig={kubeconfig}", f"--context={context}", *args],
            capture_output=True,
            check=False,
            text=True,
            timeout=kwargs.pop("timeout", 30),  # type: ignore[arg-type]
            **kwargs,
        )
        if check and result.returncode:
            pytest.fail(f"kubectl {' '.join(args)} failed: {result.stderr.strip()}")
        return result

    assert kubectl("config", "current-context").stdout.strip() == context
    nodes = json.loads(kubectl("get", "nodes", "-o", "json").stdout)["items"]
    ready_nodes = [
        node
        for node in nodes
        if any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in node.get("status", {}).get("conditions", [])
        )
    ]
    if len(ready_nodes) < 5:
        pytest.skip("verified V2 evacuation fixture requires five Ready lab Nodes")
    workers = [
        node
        for node in ready_nodes
        if not any(key.endswith("/control-plane") for key in node["metadata"].get("labels", {}))
        and not node.get("spec", {}).get("unschedulable", False)
        and not any(
            taint.get("effect") in {"NoSchedule", "NoExecute"}
            for taint in node.get("spec", {}).get("taints", [])
        )
    ]
    assert len(workers) >= 4
    if health_regression and unplaceable_recovery and len(workers) > 4:
        pytest.skip("the capacity-exhaustion case requires exactly the four-worker lab")
    if health_regression and not unplaceable_recovery and len(workers) < 5:
        pytest.skip("successful compensation requires a free destination (five workers)")
    schedulable_workers = workers[:5]
    placement_nodes = list(schedulable_workers)
    # Keep two real destination domains available in the four-worker lab.
    workload_id = "cpu-api"
    plan_key = f"p2b-api-plan-{uuid.uuid4().hex[:12]}"
    workload = {
        "apiVersion": "fleetright.io/v1alpha1",
        "kind": "FleetRightWorkload",
        "metadata": {"name": workload_id, "namespace": "fleetright-v2"},
        "spec": {
            "workloadID": workload_id,
            "executionProtocol": "monotonic-scale-up-v1",
            "podCreateProtocol": "reserved-create-v1",
            "podCreateAdmissionProtocol": "claimed-create-v1",
            "replicas": 2,
            "availabilityFloor": 1,
            "surgeBudget": 2,
            "networkMbps": 0,
            "disruption": {"maxConcurrentDisruptions": 1},
            "placement": {
                "allowedNodes": [
                    {"name": node["metadata"]["name"], "uid": node["metadata"]["uid"]}
                    for node in placement_nodes
                ],
                "topology": {
                    "key": "kubernetes.io/hostname",
                    "maxSkew": 1,
                    "minDomains": 2,
                },
                "requirePodAntiAffinity": True,
            },
            "podTemplate": {
                "containers": [
                    {
                        "name": "app",
                        "image": "fleetright-workload:local",
                        "imagePullPolicy": "IfNotPresent",
                        "ports": [{"containerPort": 8080, "protocol": "TCP"}],
                        "readinessProbe": {"httpGet": {"path": "/ready", "port": 8080}},
                        "resources": {"requests": {"cpu": "100m", "memory": "64Mi"}},
                    }
                ],
            },
        },
    }
    kubectl("apply", "-f", "-", input=json.dumps(workload))
    token = kubectl(
        "create",
        "token",
        "fleetright-controller",
        "-n",
        "fleetright",
        "--duration=30m",
    ).stdout.strip()
    admin_config = yaml.safe_load(Path(kubeconfig).read_text(encoding="utf-8"))
    context_entry = next(item for item in admin_config["contexts"] if item["name"] == context)
    cluster_name = context_entry["context"]["cluster"]
    cluster = next(
        item["cluster"] for item in admin_config["clusters"] if item["name"] == cluster_name
    )
    certificate = cluster.get("certificate-authority-data")
    if certificate is None:
        certificate = base64.b64encode(Path(cluster["certificate-authority"]).read_bytes()).decode(
            "ascii"
        )
    executor_config = tmp_path / "fleetright-executor.kubeconfig"
    executor_config.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "clusters": [
                    {
                        "name": "fleetright-lab",
                        "cluster": {
                            "server": cluster["server"],
                            "certificate-authority-data": certificate,
                        },
                    }
                ],
                "users": [{"name": "fleetright-controller", "user": {"token": token}}],
                "contexts": [
                    {
                        "name": "fleetright-executor",
                        "context": {
                            "cluster": "fleetright-lab",
                            "user": "fleetright-controller",
                            "namespace": "fleetright-v2",
                        },
                    }
                ],
                "current-context": "fleetright-executor",
            }
        ),
        encoding="utf-8",
    )
    executor_config.chmod(0o600)
    target = FleetRightWorkloadTarget(kubeconfig=str(executor_config), namespace="fleetright-v2")
    target_commands: list[tuple[tuple[str, ...], str]] = []
    real_target_kubectl = target._kubectl
    held_source_name: str | None = None
    eviction_response_ready = threading.Event()
    release_eviction_response = threading.Event()
    eviction_attempts: list[tuple[str, ...]] = []
    controller_thread: threading.Thread | None = None
    takeover_controller: ExecutionController | None = None
    controller_errors: list[Exception] = []
    final_retirement_checks: list[dict[str, object]] = []
    real_final_retirement_check = target._verify_final_retirement_pods

    def trace_final_retirement_check(
        current: dict[str, object], payload: dict[str, object], final_replicas: int
    ) -> bool:
        result = real_final_retirement_check(current, payload, final_replicas)
        if not result:
            pods = target._json(
                "get",
                "pods",
                "-n",
                "fleetright-v2",
                "-l",
                f"fleetright.io/workload-uid={current['uid']}",
                "-o",
                "json",
            ).get("items", [])
            final_retirement_checks.append(
                {
                    "expected_replicas": final_replicas,
                    "pods": [
                        {
                            "name": pod.get("metadata", {}).get("name"),
                            "uid": pod.get("metadata", {}).get("uid"),
                            "slot": pod.get("metadata", {}).get("labels", {}).get(REPLICA_ID_LABEL),
                            "node": pod.get("spec", {}).get("nodeName"),
                            "phase": pod.get("status", {}).get("phase"),
                            "ready": any(
                                condition.get("type") == "Ready"
                                and condition.get("status") == "True"
                                for condition in pod.get("status", {}).get("conditions", [])
                                if isinstance(condition, dict)
                            ),
                        }
                        for pod in pods
                        if isinstance(pod, dict)
                    ],
                }
            )
        return result

    target._verify_final_retirement_pods = trace_final_retirement_check  # type: ignore[method-assign]

    def trace_target_kubectl(*args: str, stdin: str | None = None):
        command = tuple(args)
        try:
            result = real_target_kubectl(*args, stdin=stdin)
        except Exception as error:
            target_commands.append(
                (command, f"{type(error).__name__}:{getattr(error, 'reason', str(error))}")
            )
            raise
        if any(arg.endswith("/eviction") for arg in command):
            eviction_attempts.append(command)
            if takeover_after_eviction_acceptance and len(eviction_attempts) == 1:
                eviction_response_ready.set()
                if not release_eviction_response.wait(60):
                    raise TimeoutError("accepted Eviction response was not released")
        target_commands.append((command, "ok"))
        return result

    target._kubectl = trace_target_kubectl  # type: ignore[method-assign]
    try:
        deadline = time.monotonic() + 120
        observed = target.observe_scale(workload_id)
        while observed["ready_replicas"] != 2 or observed["available_replicas"] != 2:
            assert time.monotonic() < deadline, observed
            time.sleep(1)
            observed = target.observe_scale(workload_id)
        pod_list = json.loads(
            kubectl(
                "get",
                "pods",
                "-n",
                "fleetright-v2",
                "-l",
                f"fleetright.io/workload-uid={observed['uid']}",
                "-o",
                "json",
            ).stdout
        )["items"]
        pod_by_slot = {pod["metadata"]["labels"][REPLICA_ID_LABEL]: pod for pod in pod_list}
        assert len(pod_by_slot) == 2
        now_ms = time.time_ns() // 1_000_000
        source, _, _ = _v2_snapshot(now_ms)
        source["snapshot_id"] = f"{workload_id}-snapshot"
        source["workloads"][0]["topology_key"] = "kubernetes.io/hostname"
        source["workloads"][0]["min_topology_domains"] = 2
        source["workloads"][0]["max_topology_skew"] = 1
        source["workloads"][0]["availability_floor"] = 1
        source["capacity_profiles"][0]["workload_id"] = workload_id
        source["nodes"] = source["nodes"][: len(placement_nodes)]
        model_nodes = [node["metadata"]["name"] for node in placement_nodes]
        model_by_id = {node["node_id"]: node for node in source["nodes"]}
        for index, node_name in enumerate(model_nodes):
            model = list(model_by_id.values())[index]
            model["node_id"] = node_name
            model["active"] = True
            model["health"] = "NODE_HEALTH_HEALTHY"
            model["labels"] = [("kubernetes.io/hostname", node_name)]
        ordered_pods = [pod_by_slot[f"{workload_id}-{ordinal}"] for ordinal in range(2)]
        source["current_placements"] = [
            {
                "workload_id": workload_id,
                "replica_id": f"{workload_id}-{ordinal}",
                "node_id": pod["spec"]["nodeName"],
            }
            for ordinal, pod in enumerate(ordered_pods)
        ]
        pdb = json.loads(
            kubectl("get", "pdb", workload_id, "-n", "fleetright-v2", "-o", "json").stdout
        )
        inventory_nodes = [
            {
                "node_id": node["metadata"]["name"],
                "node_uid": node["metadata"]["uid"],
                "resource_version": node["metadata"]["resourceVersion"],
            }
            for node in ready_nodes
            if node["metadata"]["name"] in model_nodes
        ]
        inventory_pods = [
            {
                "workload_id": workload_id,
                "replica_id": pod["metadata"]["labels"][REPLICA_ID_LABEL],
                "node_id": pod["spec"]["nodeName"],
                "pod_name": pod["metadata"]["name"],
                "pod_uid": pod["metadata"]["uid"],
                "owner_kind": "FleetRightWorkload",
                "owner_name": workload_id,
                "owner_uid": observed["uid"],
                "logical_slot_id": pod["metadata"]["labels"][REPLICA_ID_LABEL],
                "ready": True,
                "terminating": False,
            }
            for pod in ordered_pods
        ]
        inventory_controller = {
            "workload_id": workload_id,
            "namespace": "fleetright-v2",
            "owner_kind": "FleetRightWorkload",
            "owner_name": workload_id,
            "owner_uid": observed["uid"],
            "owner_resource_version": observed["resource_version"],
            "owner_generation": observed["generation"],
            "controller_type": "fleetright-workload-controller/v1",
            "execution_protocol": "monotonic-scale-up-v1",
            "pod_template_sha256": _placement_fingerprint(
                workload["spec"], workload["spec"]["podTemplate"]
            ),
            "desired_replicas": 2,
            "pdb_allowed_disruptions": pdb["status"]["disruptionsAllowed"],
            "pdb_resource_version": pdb["metadata"]["resourceVersion"],
            "observed_at_ms": now_ms,
        }
        source["evacuation_v2"] = {
            **source["evacuation_v2"],
            "cluster_uid": json.loads(
                kubectl("get", "namespace", "kube-system", "-o", "json").stdout
            )["metadata"]["uid"],
            "namespace": "fleetright-v2",
            "inventory_observed_at_ms": now_ms,
            "controllers": [inventory_controller],
            "pods": inventory_pods,
            "nodes": inventory_nodes,
        }
        snapshot = {**source, "plan_schema_version": 2}
        monkeypatch.setenv("FLEETRIGHT_KUBERNETES_WORKLOADS", workload_id)
        tokens, raw_tokens = _tokens()

        class HealthyObserver:
            source_pod_name: str | None = None
            plan_id: str | None = None
            intent_id: str | None = None
            regression_sent = False

            def observe(self, _workload_id: str, **_kwargs: object) -> dict[str, object]:
                retirement_verified = False
                if health_regression and self.plan_id and self.intent_id:
                    with clean_database.repository.transaction() as conn:
                        state = conn.execute(
                            "SELECT e.status, a.execution_state FROM plan_executions e "
                            "JOIN action_intents a USING (plan_id) "
                            "WHERE e.plan_id = %s AND a.intent_id = %s",
                            (self.plan_id, self.intent_id),
                        ).fetchone()
                    retirement_verified = state is not None and (
                        state["status"],
                        state["execution_state"],
                    ) == ("VERIFYING", "SUCCEEDED")
                if (
                    health_regression
                    and self.source_pod_name
                    and retirement_verified
                    and not self.regression_sent
                    and target.get_pod(self.source_pod_name) is None
                ):
                    self.regression_sent = True
                    return {
                        "ready": True,
                        "fresh_sample": True,
                        "healthy": False,
                        "p99_within_slo": False,
                        "error_rate_within_slo": True,
                        "queue_growth_within_limit": True,
                        "observed_at_ms": time.time_ns() // 1_000_000,
                    }
                return {
                    "ready": True,
                    "fresh_sample": True,
                    "healthy": True,
                    "p99_within_slo": True,
                    "error_rate_within_slo": True,
                    "queue_growth_within_limit": True,
                    "observed_at_ms": time.time_ns() // 1_000_000,
                }

        health_observer = HealthyObserver()
        readiness_timeout = "60" if unplaceable_recovery else "300"
        monkeypatch.setenv("FLEETRIGHT_READINESS_TIMEOUT_SECONDS", readiness_timeout)
        app = create_app(
            clean_database.repository,
            tokens=tokens,
            mode="EXECUTE",
            execution_target="kubernetes-workload",
            target_adapter=target,
            health_observer=health_observer,
            start_execution_worker=False,
        )
        assert app.state.execution_controller.readiness_timeout_seconds == int(readiness_timeout)
        with TestClient(app) as client:
            optimized = client.post(
                "/v1/optimize",
                headers={
                    "Authorization": f"Bearer {raw_tokens['recommender']}",
                    "Idempotency-Key": plan_key,
                },
                json={"snapshot": snapshot},
            )
            assert optimized.status_code == 201, optimized.text
            plan = optimized.json()
            assert plan["state"] == "VERIFIED", plan
            assert len(plan["candidate_actions"]) == 1, plan["candidate_actions"]
            assert plan["candidate_actions"][0]["action_type"] == "PREPARE_SOURCE_RETIREMENT"
            health_observer.plan_id = plan["plan_id"]
            health_observer.intent_id = plan["candidate_actions"][0]["intent_id"]
            health_observer.source_pod_name = plan["candidate_actions"][0]["payload"][
                "source_pod_name"
            ]
            operator = f"Bearer {raw_tokens['operator']}"
            approved = client.post(
                f"/v1/plans/{plan['plan_id']}/approve",
                headers={"Authorization": operator, "Idempotency-Key": f"approve-{plan_key}"},
                json={},
            )
            assert approved.status_code == 200, approved.text
            if takeover_after_eviction_acceptance:
                held_source_name = plan["candidate_actions"][0]["payload"]["source_pod_name"]
                kubectl(
                    "patch",
                    "pod",
                    held_source_name,
                    "-n",
                    "fleetright-v2",
                    "--type=merge",
                    "-p",
                    json.dumps({"metadata": {"finalizers": ["fleetright.io/prompt2b-test-hold"]}}),
                )
            accepted = client.post(
                f"/v1/plans/{plan['plan_id']}/execute",
                headers={"Authorization": operator, "Idempotency-Key": f"execute-{plan_key}"},
                json={},
            )
            assert accepted.status_code == 202, accepted.text
            if takeover_after_eviction_acceptance:

                def run_old_controller() -> None:
                    try:
                        deadline = time.monotonic() + 180
                        while not eviction_response_ready.is_set() and time.monotonic() < deadline:
                            app.state.execution_controller.run_once()
                            eviction_response_ready.wait(0.25)
                    except Exception as error:
                        controller_errors.append(error)

                controller_thread = threading.Thread(target=run_old_controller, daemon=True)
                controller_thread.start()
                assert eviction_response_ready.wait(180), (
                    "Kubernetes did not accept Eviction; "
                    f"controller_errors={controller_errors}, "
                    f"target_commands={target_commands[-12:]}"
                )
                source_in_progress = target.get_pod(held_source_name)
                assert source_in_progress is not None
                assert source_in_progress["metadata"].get("deletionTimestamp") is not None
                assert len(eviction_attempts) == 1
                with clean_database.repository.transaction() as conn:
                    conn.execute(
                        "UPDATE plan_executions SET controller_lease_expires_at = "
                        "clock_timestamp() WHERE plan_id = %s",
                        (plan["plan_id"],),
                    )
                takeover_controller = ExecutionController(
                    clean_database.repository,
                    target,
                    health_observer,
                    f"kind-eviction-takeover-{uuid.uuid4().hex[:8]}",
                    readiness_timeout_seconds=300,
                )
                takeover_controller.run_once()
                held_detail = client.get(
                    f"/v1/plans/{plan['plan_id']}",
                    headers={"Authorization": f"Bearer {raw_tokens['viewer']}"},
                ).json()
                held_action = held_detail["candidate_actions"][0]
                held_stages = [
                    item.get("result", {}).get("stage")
                    for item in held_action["execution_result"].get("progress", [])
                    if isinstance(item, dict) and isinstance(item.get("result"), dict)
                ]
                assert held_detail["execution"]["status"] == "EXECUTING"
                assert held_action["execution_state"] == "RUNNING"
                assert "EVICTION_IN_PROGRESS" in held_stages
                assert len(eviction_attempts) == 1

                release_eviction_response.set()
                controller_thread.join(timeout=30)
                assert not controller_thread.is_alive(), (
                    "old controller did not release its response"
                )
                assert len(controller_errors) == 1
                assert isinstance(controller_errors[0], ControlError), controller_errors[0]
                assert len(eviction_attempts) == 1
                stale_detail = client.get(
                    f"/v1/plans/{plan['plan_id']}",
                    headers={"Authorization": f"Bearer {raw_tokens['viewer']}"},
                ).json()
                stale_progress = stale_detail["candidate_actions"][0]["execution_result"].get(
                    "progress", []
                )
                assert not any(
                    isinstance(item, dict)
                    and isinstance(item.get("result"), dict)
                    and item["result"].get("stage") == "EVICTION_ACCEPTED"
                    for item in stale_progress
                )
                kubectl(
                    "patch",
                    "pod",
                    held_source_name,
                    "-n",
                    "fleetright-v2",
                    "--type=merge",
                    "-p",
                    json.dumps({"metadata": {"finalizers": []}}),
                    check=False,
                )
            expected_status = (
                "FAILED_SAFE"
                if unplaceable_recovery
                else "COMPENSATED"
                if health_regression
                else "COMMITTED"
            )
            deadline = time.monotonic() + 300
            while time.monotonic() < deadline:
                (takeover_controller or app.state.execution_controller).run_once()
                detail = client.get(
                    f"/v1/plans/{plan['plan_id']}",
                    headers={"Authorization": f"Bearer {raw_tokens['viewer']}"},
                ).json()
                action = detail["candidate_actions"][0]
                progress = action.get("execution_result", {}).get("progress", [])
                latest = progress[-1].get("result", {}) if progress else {}
                if detail["execution"]["status"] == expected_status:
                    break
                if (
                    detail["execution"]["status"] == "FAILED_SAFE"
                    and expected_status != "FAILED_SAFE"
                ) or (detail["execution"]["status"] == "COMPENSATING" and not health_regression):
                    progress_stages = [
                        item["result"].get("stage")
                        for item in progress[-10:]
                        if isinstance(item, dict) and isinstance(item.get("result"), dict)
                    ]
                    pytest.fail(
                        f"execution stopped: {detail['execution']}, "
                        f"error={action.get('execution_error')}, latest={latest}, "
                        f"stages={progress_stages}, "
                        f"started_at={action.get('execution_started_at')}, "
                        f"timeout={app.state.execution_controller.readiness_timeout_seconds}, "
                        f"final_checks={final_retirement_checks[-5:]}, "
                        f"commands={target_commands[-20:]}"
                    )
                time.sleep(0.5)
            if detail["execution"]["status"] != expected_status:
                stages = [
                    item["result"].get("stage")
                    for item in progress
                    if isinstance(item, dict) and isinstance(item.get("result"), dict)
                ]
                eviction_commands = [
                    item
                    for item in target_commands
                    if len(item[0]) > 2
                    and item[0][0] == "create"
                    and item[0][1] == "--raw"
                    and item[0][2].endswith("/eviction")
                ]
                current = target.observe_scale(workload_id)
                target_state = {
                    key: current.get(key)
                    for key in (
                        "uid",
                        "generation",
                        "observed_generation",
                        "replicas",
                        "observed_replicas",
                        "ready_replicas",
                        "available_replicas",
                        "reconciled",
                        "fencing_token",
                        "execution_request",
                        "execution",
                        "pending_pod_create",
                        "pod_create_history_uncertain",
                        "unverified_pod_creates",
                        "retired_replica_slots",
                        "authorized_pod_creates",
                    )
                }
                pytest.fail(
                    f"retirement did not complete: plan={detail.get('state')}, "
                    f"execution={detail.get('execution', {}).get('status')}, "
                    f"action={action.get('execution_state')}, "
                    f"error={action.get('execution_error')}, "
                    f"stages={stages[-5:]}, latest={latest.get('stage')}:{latest.get('reason')}, "
                    f"started_at={action.get('execution_started_at')}, "
                    f"timeout={app.state.execution_controller.readiness_timeout_seconds}, "
                    f"final_checks={final_retirement_checks[-5:]}, "
                    f"eviction_commands={eviction_commands}, "
                    f"target={target_state}, commands={target_commands[-15:]}"
                )
            assert detail["execution"]["status"] == expected_status, detail
            if unplaceable_recovery:
                _assert_unplaceable_recovery_failed_safe(
                    clean_database,
                    target,
                    kubectl,
                    client,
                    raw_tokens,
                    health_observer,
                    plan["plan_id"],
                    workload_id,
                    action["intent_id"],
                    len(eviction_attempts),
                )
                return
            if not health_regression:
                assert action.get("execution_state") == "SUCCEEDED"
            stages = [
                item["result"].get("stage")
                for item in progress
                if isinstance(item, dict) and isinstance(item.get("result"), dict)
            ]
            assert "READY_FOR_SAFE_EVICTION" in stages
            assert "EVICTION_INTENT" in stages
            if takeover_after_eviction_acceptance:
                assert "EVICTION_IN_PROGRESS" in stages
                assert "EVICTION_ACCEPTED" not in stages
            else:
                assert "EVICTION_ACCEPTED" in stages
            source_payload = plan["candidate_actions"][0]["payload"]
            completion = action["execution_result"]["result"]
            assert completion["stage"] == "RETIREMENT_COMPLETE"
            final_observation = completion["observation"]
            assert final_observation["ready_replicas"] == source_payload["final_desired_replicas"]
            assert (
                final_observation["available_replicas"] == source_payload["final_desired_replicas"]
            )
            assert target.get_pod(source_payload["source_pod_name"]) is None
            replacement_uid = completion.get("replacement_pod_uid")
            assert isinstance(replacement_uid, str) and replacement_uid
            replacement = target.get_pod(completion["replacement_pod_name"])
            assert replacement is not None
            assert replacement["metadata"]["uid"] == replacement_uid
            assert replacement["spec"]["nodeName"] in source_payload["destination_node_ids"]
            assert replacement["spec"]["nodeName"] != source_payload["source_node_id"]
            assert {
                "topologyKey": "kubernetes.io/hostname",
                "labelSelector": {"matchLabels": {"fleetright.io/workload-uid": observed["uid"]}},
            } in replacement["spec"].get("affinity", {}).get("podAntiAffinity", {}).get(
                "requiredDuringSchedulingIgnoredDuringExecution", []
            )
            if takeover_after_eviction_acceptance:
                assert len(eviction_attempts) == 1
                assert detail["execution"]["controller_id"] == takeover_controller.controller_id
            assert health_observer.regression_sent is health_regression
            if health_regression:
                compensation_actions = [
                    item
                    for item in detail["candidate_actions"]
                    if item["compensates_intent_id"] == action["intent_id"]
                ]
                assert len(compensation_actions) == 1
                compensation = compensation_actions[0]
                assert compensation["action_type"] == "SET_REPLICA_COUNT"
                assert compensation["execution_state"] == "SUCCEEDED"
                assert compensation["payload"]["proposed_replicas"] == 4
                assert compensation["execution_result"]["target_observation"]["ready_replicas"] == 4
                recovered = target.observe_scale(workload_id)
                assert recovered["replicas"] == recovered["observed_replicas"] == 4
                assert recovered["ready_replicas"] == recovered["available_replicas"] == 4
                assert recovered["reconciled"] is True
                recovered_pods = kubectl(
                    "get",
                    "pods",
                    "-n",
                    "fleetright-v2",
                    "-l",
                    f"fleetright.io/workload-uid={recovered['uid']}",
                    "-o",
                    "json",
                )
                live_pods = json.loads(recovered_pods.stdout)["items"]
                live_nodes = {pod["spec"]["nodeName"] for pod in live_pods}
                approved_nodes = {item["name"]: item["uid"] for item in recovered["approved_nodes"]}
                assert len(live_pods) == len(live_nodes) == 4
                assert live_nodes <= approved_nodes.keys()
                compensation_creates = [
                    item
                    for item in recovered["authorized_pod_creates"]
                    if item["planID"] == plan["plan_id"]
                    and item["operationID"] == compensation["intent_id"]
                ]
                compensation_destinations = {
                    item["name"] for item in compensation["payload"]["destination_nodes"]
                }
                assert len(compensation_creates) == 1
                compensation_pod = next(
                    pod
                    for pod in live_pods
                    if pod["metadata"]["uid"] == compensation_creates[0]["podUID"]
                )
                assert compensation_pod["spec"]["nodeName"] in compensation_destinations
                assert all(
                    {
                        "topologyKey": "kubernetes.io/hostname",
                        "labelSelector": {
                            "matchLabels": {"fleetright.io/workload-uid": recovered["uid"]}
                        },
                    }
                    in pod["spec"]
                    .get("affinity", {})
                    .get("podAntiAffinity", {})
                    .get("requiredDuringSchedulingIgnoredDuringExecution", [])
                    for pod in live_pods
                )
                actual_node_uids = {
                    item["metadata"]["name"]: item["metadata"]["uid"]
                    for item in json.loads(kubectl("get", "nodes", "-o", "json").stdout)["items"]
                }
                assert all(approved_nodes[name] == actual_node_uids[name] for name in live_nodes)
                assert health_observer.observe(workload_id)["healthy"] is True
                assert detail["state"] == "COMPENSATED"
            assert clean_database.repository.get_plan(plan["plan_id"])["state"] == expected_status
    finally:
        release_eviction_response.set()
        if controller_thread is not None:
            controller_thread.join(timeout=5)
        if held_source_name is not None:
            kubectl(
                "patch",
                "pod",
                held_source_name,
                "-n",
                "fleetright-v2",
                "--type=merge",
                "-p",
                json.dumps({"metadata": {"finalizers": []}}),
                check=False,
                timeout=10,
            )
        kubectl(
            "delete",
            "fleetrightworkload",
            workload_id,
            "-n",
            "fleetright-v2",
            "--wait=true",
            "--ignore-not-found=true",
            check=False,
            timeout=60,
        )


def _assert_unplaceable_recovery_failed_safe(
    database: Database,
    target: FleetRightWorkloadTarget,
    kubectl: object,
    client: TestClient,
    raw_tokens: dict[str, str],
    health_observer: object,
    plan_id: str,
    workload_id: str,
    retirement_intent: str,
    evictions_before: int,
) -> None:
    """Live: with no free destination the recovery Pod stays Pending and the plan fails safe."""

    def snapshot() -> dict[str, object]:
        uid = target.observe_scale(workload_id)["uid"]
        pods = json.loads(
            kubectl(  # type: ignore[operator]
                "get", "pods", "-n", "fleetright-v2", "-l", f"fleetright.io/workload-uid={uid}",
                "-o", "json",
            ).stdout
        )["items"]  # fmt: skip
        with database.repository.transaction() as conn:
            execution = conn.execute(
                "SELECT status, outcome FROM plan_executions WHERE plan_id = %s", (plan_id,)
            ).fetchone()
            plan_state = conn.execute(
                "SELECT state FROM plans WHERE plan_id = %s", (plan_id,)
            ).fetchone()["state"]
            recovery = conn.execute(
                "SELECT intent_id, execution_state, execution_error FROM action_intents "
                "WHERE plan_id = %s AND compensates_intent_id = %s",
                (plan_id, retirement_intent),
            ).fetchall()
        return {
            "uids": sorted(p["metadata"]["uid"] for p in pods),
            "pending": [p for p in pods if p["status"].get("phase") == "Pending"],
            "ready": sum(
                any(
                    c["type"] == "Ready" and c["status"] == "True"
                    for c in p["status"]["conditions"]
                )
                for p in pods
                if p["status"].get("phase") == "Running"
            ),
            "execution": execution,
            "plan_state": plan_state,
            "recovery": recovery,
        }

    before = snapshot()
    assert before["execution"]["status"] == "FAILED_SAFE"  # type: ignore[index]
    assert before["plan_state"] == "FAILED_SAFE"
    # Durable, specific reason; never a false COMPENSATED.
    outcome = before["execution"]["outcome"]  # type: ignore[index]
    assert outcome["COMPENSATING"] == {"observed_health": "regressed"}, outcome
    assert outcome["FAILED_SAFE"]["reason"] in {
        "readiness-timeout",
        "compensation-readiness-timeout",
    }
    assert health_observer.regression_sent is True  # type: ignore[attr-defined]
    assert len(before["recovery"]) == 1 and before["recovery"][0]["execution_state"] == "FAILED"  # type: ignore[arg-type]
    assert before["recovery"][0]["execution_error"] == "readiness-timeout"  # type: ignore[index]
    # Exactly one recovery Pod was requested; it could not be scheduled; 3 serving Pods remain.
    assert len(before["uids"]) == 4 and before["ready"] == 3  # type: ignore[arg-type]
    pending = before["pending"]
    assert len(pending) == 1 and not pending[0]["spec"].get("nodeName")  # type: ignore[index,arg-type]
    assert any(
        c.get("reason") == "Unschedulable"
        for c in pending[0]["status"]["conditions"]  # type: ignore[index]
    )
    assert evictions_before == 1  # no additional eviction was ever dispatched
    # A replacement controller finds nothing to do: no new Pods, no retries, no eviction.
    restart = ExecutionController(
        database.repository,
        target,
        health_observer,
        f"unplaceable-restart-{uuid.uuid4().hex[:8]}",
        readiness_timeout_seconds=60,
    )
    for _ in range(6):
        restart.run_once()
        time.sleep(0.5)
    after = snapshot()
    assert after["uids"] == before["uids"]
    assert after["execution"]["status"] == "FAILED_SAFE"  # type: ignore[index]
    assert after["recovery"] == before["recovery"]
    detail = client.get(
        f"/v1/plans/{plan_id}", headers={"Authorization": f"Bearer {raw_tokens['viewer']}"}
    ).json()
    assert detail["execution"]["status"] == "FAILED_SAFE"


@pytest.mark.skipif(
    os.getenv("FLEETRIGHT_RUN_KIND_E2E") != "1",
    reason="set FLEETRIGHT_RUN_KIND_E2E=1 for live API-to-workload execution",
)
def test_live_v2_authenticated_node_drain_partial_crash_recovery(
    clean_database: Database,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from test_evacuation_v2 import _v2_snapshot

    kubeconfig = os.getenv(
        "FLEETRIGHT_LAB_ADMIN_KUBECONFIG", "/tmp/fleetright-prompt2b-lab.kubeconfig"
    )
    context = os.getenv("FLEETRIGHT_LAB_CONTEXT", "kind-fleetright-prompt2b-lab")
    namespace = "fleetright-v2"

    def kubectl(
        *args: str, check: bool = True, **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            ["kubectl", f"--kubeconfig={kubeconfig}", f"--context={context}", *args],
            capture_output=True,
            check=False,
            text=True,
            timeout=kwargs.pop("timeout", 30),  # type: ignore[arg-type]
            **kwargs,
        )
        if check and result.returncode:
            pytest.fail(f"kubectl {' '.join(args)} failed: {result.stderr.strip()}")
        return result

    assert kubectl("config", "current-context").stdout.strip() == context
    namespace_object = json.loads(kubectl("get", "namespace", namespace, "-o", "json").stdout)
    assert namespace_object["metadata"]["uid"] == "df488d28-0a9a-4b1c-ba5d-ff4632be9b29"
    labels = namespace_object["metadata"].get("labels", {})
    assert labels.get("fleetright.io/pod-create-admission") == "enabled"
    assert labels.get("pod-security.kubernetes.io/enforce") == "restricted"

    nodes = json.loads(kubectl("get", "nodes", "-o", "json").stdout)["items"]
    ready_workers = [
        node
        for node in nodes
        if any(
            item.get("type") == "Ready" and item.get("status") == "True"
            for item in node.get("status", {}).get("conditions", [])
        )
        and not any(key.endswith("/control-plane") for key in node["metadata"].get("labels", {}))
        and not node.get("spec", {}).get("unschedulable", False)
        and not any(
            taint.get("effect") in {"NoSchedule", "NoExecute"}
            for taint in node.get("spec", {}).get("taints", [])
        )
    ]
    ready_workers.sort(key=lambda node: node["metadata"]["name"])
    worker_names = [node["metadata"]["name"] for node in ready_workers]
    assert len(ready_workers) == 4
    quiet_deadline = time.monotonic() + 180
    while any(  # Pods of earlier tests may still be terminating and hide drainable Nodes
        pod["metadata"]["name"].startswith("frw-")
        for pod in json.loads(kubectl("get", "pods", "-n", namespace, "-o", "json").stdout)["items"]
    ):
        assert time.monotonic() < quiet_deadline, "previous test Pods are still terminating"
        time.sleep(2)
    existing_pods = json.loads(kubectl("get", "pods", "-A", "-o", "json").stdout)["items"]

    def can_be_drained(node_name: str) -> bool:
        for pod in existing_pods:
            if pod.get("spec", {}).get("nodeName") != node_name:
                continue
            metadata = pod.get("metadata", {})
            if metadata.get("annotations", {}).get("kubernetes.io/config.mirror"):
                continue
            if any(
                owner.get("kind") == "DaemonSet" and owner.get("controller") is True
                for owner in metadata.get("ownerReferences", [])
            ):
                continue
            return False
        return True

    safe_initial_nodes = [
        node for node in ready_workers if can_be_drained(node["metadata"]["name"])
    ]
    assert len(safe_initial_nodes) >= 2
    initial_nodes = safe_initial_nodes[:2]
    initial_node_names = {node["metadata"]["name"] for node in initial_nodes}
    test_taint = "fleetright.io/p2b-drain-test"
    tainted_nodes = [
        node["metadata"]["name"]
        for node in ready_workers
        if node["metadata"]["name"] not in initial_node_names
    ]
    assert all(
        not any(taint.get("key") == test_taint for taint in node.get("spec", {}).get("taints", []))
        for node in ready_workers
    )
    workload_ids = [f"p2b-drain-{uuid.uuid4().hex[:8]}-{index}" for index in range(4)]
    test_workloads: dict[str, str] = {}
    source_node_id: str | None = None
    source_node_uid: str | None = None
    cordon_operation_id: str | None = None
    process: multiprocessing.Process | None = None
    plan_id: str | None = None
    marker = tmp_path / "third-eviction-pending"
    release_marker = tmp_path / "release-third-eviction"
    admitted_marker = tmp_path / "third-eviction-admitted"
    held_finalizer = "fleetright.io/p2b-drain-test"
    eviction_log = tmp_path / "evictions.jsonl"
    dsn_file = tmp_path / "test-database.dsn"
    diagnostics_file = tmp_path / "drain-diagnostics.json"
    target_errors_file = tmp_path / "target-errors.jsonl"
    dsn_file.write_text(clean_database.dsn, encoding="utf-8")
    dsn_file.chmod(0o600)

    try:
        for node_name in tainted_nodes:
            kubectl("taint", "node", node_name, f"{test_taint}=true:NoSchedule")
        for workload_id in workload_ids:
            workload = {
                "apiVersion": "fleetright.io/v1alpha1",
                "kind": "FleetRightWorkload",
                "metadata": {"name": workload_id, "namespace": namespace},
                "spec": {
                    "workloadID": workload_id,
                    "executionProtocol": "monotonic-scale-up-v1",
                    "podCreateProtocol": "reserved-create-v1",
                    "podCreateAdmissionProtocol": "claimed-create-v1",
                    "replicas": 2,
                    "availabilityFloor": 1,
                    "surgeBudget": 2,
                    "networkMbps": 0,
                    "disruption": {"maxConcurrentDisruptions": 1},
                    "placement": {
                        "allowedNodes": [
                            {"name": node["metadata"]["name"], "uid": node["metadata"]["uid"]}
                            for node in ready_workers
                        ],
                        "topology": {
                            "key": "kubernetes.io/hostname",
                            "maxSkew": 1,
                            "minDomains": 2,
                        },
                        "requirePodAntiAffinity": True,
                    },
                    "podTemplate": {
                        "containers": [
                            {
                                "name": "app",
                                "image": "fleetright-workload:local",
                                "imagePullPolicy": "IfNotPresent",
                                "ports": [{"containerPort": 8080, "protocol": "TCP"}],
                                "readinessProbe": {"httpGet": {"path": "/ready", "port": 8080}},
                                "resources": {"requests": {"cpu": "100m", "memory": "64Mi"}},
                            }
                        ],
                    },
                },
            }
            kubectl("apply", "-f", "-", input=json.dumps(workload))
            created = json.loads(
                kubectl(
                    "get", "fleetrightworkload", workload_id, "-n", namespace, "-o", "json"
                ).stdout
            )
            test_workloads[workload_id] = created["metadata"]["uid"]

        deadline = time.monotonic() + 180
        workload_objects: dict[str, dict[str, object]] = {}
        live_pods: dict[str, list[dict[str, object]]] = {}
        while time.monotonic() < deadline:
            ready = True
            for workload_id in workload_ids:
                workload_object = json.loads(
                    kubectl(
                        "get",
                        "fleetrightworkload",
                        workload_id,
                        "-n",
                        namespace,
                        "-o",
                        "json",
                    ).stdout
                )
                uid = workload_object["metadata"]["uid"]
                pods = json.loads(
                    kubectl(
                        "get",
                        "pods",
                        "-n",
                        namespace,
                        "-l",
                        f"fleetright.io/workload-uid={uid}",
                        "-o",
                        "json",
                    ).stdout
                )["items"]
                ready_pods = [
                    pod
                    for pod in pods
                    if any(
                        item.get("type") == "Ready" and item.get("status") == "True"
                        for item in pod.get("status", {}).get("conditions", [])
                    )
                    and pod.get("metadata", {}).get("deletionTimestamp") is None
                ]
                if len(ready_pods) != 2 or {pod["spec"]["nodeName"] for pod in ready_pods} != {
                    node["metadata"]["name"] for node in initial_nodes
                }:
                    ready = False
                    break
                workload_objects[workload_id] = workload_object
                live_pods[workload_id] = ready_pods
            if ready:
                break
            time.sleep(1)
        else:
            pytest.fail(f"four initial FleetRightWorkloads did not become Ready: {workload_ids}")

        for node_name in tainted_nodes:
            kubectl("taint", "node", node_name, f"{test_taint}-")
        refreshed_nodes = json.loads(kubectl("get", "nodes", "-o", "json").stdout)["items"]
        nodes_by_name = {node["metadata"]["name"]: node for node in refreshed_nodes}
        ready_workers = [nodes_by_name[name] for name in worker_names]

        now_ms = time.time_ns() // 1_000_000
        source, _, _ = _v2_snapshot(now_ms)
        source["snapshot_id"] = f"node-drain-{uuid.uuid4().hex}"
        model_workloads: list[dict[str, object]] = []
        model_profiles: list[dict[str, object]] = []
        inventory_controllers: list[dict[str, object]] = []
        inventory_pods: list[dict[str, object]] = []
        placements: list[dict[str, str]] = []
        model_nodes = source["nodes"][: len(ready_workers)]
        source["nodes"] = model_nodes
        for model_node, node in zip(model_nodes, ready_workers, strict=True):
            node_name = node["metadata"]["name"]
            model_node.update(
                node_id=node_name,
                active=True,
                health="NODE_HEALTH_HEALTHY",
                labels=[("kubernetes.io/hostname", node_name)],
            )
        for workload_id in workload_ids:
            workload_object = workload_objects[workload_id]
            uid = workload_object["metadata"]["uid"]
            workload_spec = workload_object["spec"]
            model_workload = deepcopy(source["workloads"][0])
            model_workload.update(
                workload_id=workload_id,
                current_resources={
                    "cpu_millicores": 100,
                    "memory_mib": 64,
                    "network_mbps": 0,
                },
                topology_key="kubernetes.io/hostname",
                min_topology_domains=2,
                max_topology_skew=1,
                availability_floor=1,
            )
            model_workloads.append(model_workload)
            profile = deepcopy(source["capacity_profiles"][0])
            profile.update(
                workload_id=workload_id,
                profile_id=f"{workload_id}-profile",
                allocation=deepcopy(model_workload["current_resources"]),
            )
            model_profiles.append(profile)
            pdb = json.loads(
                kubectl("get", "pdb", workload_id, "-n", namespace, "-o", "json").stdout
            )
            inventory_controllers.append(
                {
                    "workload_id": workload_id,
                    "namespace": namespace,
                    "owner_kind": "FleetRightWorkload",
                    "owner_name": workload_id,
                    "owner_uid": uid,
                    "owner_resource_version": workload_object["metadata"]["resourceVersion"],
                    "owner_generation": workload_object["metadata"]["generation"],
                    "controller_type": "fleetright-workload-controller/v1",
                    "execution_protocol": "monotonic-scale-up-v1",
                    "pod_template_sha256": _placement_fingerprint(
                        workload_spec, workload_spec["podTemplate"]
                    ),
                    "desired_replicas": 2,
                    "pdb_allowed_disruptions": pdb["status"]["disruptionsAllowed"],
                    "pdb_resource_version": pdb["metadata"]["resourceVersion"],
                    "observed_at_ms": now_ms,
                }
            )
            for pod in live_pods[workload_id]:
                replica_id = pod["metadata"]["labels"][REPLICA_ID_LABEL]
                node_id = pod["spec"]["nodeName"]
                placements.append(
                    {"workload_id": workload_id, "replica_id": replica_id, "node_id": node_id}
                )
                inventory_pods.append(
                    {
                        **placements[-1],
                        "pod_name": pod["metadata"]["name"],
                        "pod_uid": pod["metadata"]["uid"],
                        "owner_kind": "FleetRightWorkload",
                        "owner_name": workload_id,
                        "owner_uid": uid,
                        "logical_slot_id": replica_id,
                        "ready": True,
                        "terminating": False,
                    }
                )
        source["workloads"] = model_workloads
        source["capacity_profiles"] = model_profiles
        source["current_placements"] = placements
        source["evacuation_v2"] = {
            **source["evacuation_v2"],
            "cluster_uid": json.loads(
                kubectl("get", "namespace", "kube-system", "-o", "json").stdout
            )["metadata"]["uid"],
            "namespace": namespace,
            "inventory_observed_at_ms": now_ms,
            "controllers": inventory_controllers,
            "pods": inventory_pods,
            "nodes": [
                {
                    "node_id": node["metadata"]["name"],
                    "node_uid": node["metadata"]["uid"],
                    "resource_version": node["metadata"]["resourceVersion"],
                }
                for node in ready_workers
            ],
        }
        monkeypatch.setenv("FLEETRIGHT_KUBERNETES_WORKLOADS", ",".join(workload_ids))

        token = kubectl(
            "create",
            "token",
            "fleetright-controller",
            "-n",
            "fleetright",
            "--duration=30m",
        ).stdout.strip()
        admin_config = yaml.safe_load(Path(kubeconfig).read_text(encoding="utf-8"))
        context_entry = next(item for item in admin_config["contexts"] if item["name"] == context)
        cluster_name = context_entry["context"]["cluster"]
        cluster = next(
            item["cluster"] for item in admin_config["clusters"] if item["name"] == cluster_name
        )
        certificate = cluster.get("certificate-authority-data")
        if certificate is None:
            certificate = base64.b64encode(
                Path(cluster["certificate-authority"]).read_bytes()
            ).decode("ascii")
        executor_config = tmp_path / "fleetright-drain-executor.kubeconfig"
        executor_config.write_text(
            yaml.safe_dump(
                {
                    "apiVersion": "v1",
                    "kind": "Config",
                    "clusters": [
                        {
                            "name": "fleetright-lab",
                            "cluster": {
                                "server": cluster["server"],
                                "certificate-authority-data": certificate,
                            },
                        }
                    ],
                    "users": [{"name": "fleetright-controller", "user": {"token": token}}],
                    "contexts": [
                        {
                            "name": "fleetright-executor",
                            "context": {
                                "cluster": "fleetright-lab",
                                "user": "fleetright-controller",
                                "namespace": namespace,
                            },
                        }
                    ],
                    "current-context": "fleetright-executor",
                }
            ),
            encoding="utf-8",
        )
        executor_config.chmod(0o600)
        target = FleetRightWorkloadTarget(kubeconfig=str(executor_config), namespace=namespace)
        tokens, raw_tokens = _tokens()
        monkeypatch.setenv("FLEETRIGHT_READINESS_TIMEOUT_SECONDS", "300")
        app = create_app(
            clean_database.repository,
            tokens=tokens,
            mode="EXECUTE",
            execution_target="kubernetes-workload",
            target_adapter=target,
            health_observer=_LiveHealthyObserver(),
            start_execution_worker=False,
        )
        eviction_log.touch(mode=0o600, exist_ok=True)
        with TestClient(app) as client:
            optimized = client.post(
                "/v1/optimize",
                headers={
                    "Authorization": f"Bearer {raw_tokens['recommender']}",
                    "Idempotency-Key": f"drain-plan-{uuid.uuid4().hex}",
                },
                json={"snapshot": {**source, "plan_schema_version": 2}},
            )
            assert optimized.status_code == 201, optimized.text
            plan = optimized.json()
            plan_id = plan["plan_id"]
            cordon_operation_id = next(
                action["payload"]["operation_id"]
                for action in plan["candidate_actions"]
                if action["action_type"] == "CORDON_NODE"
            )
            assert plan["verification_accepted"] is True and plan["admission_accepted"] is True
            assert len(plan["candidate_actions"]) == 6
            action_types = [action["action_type"] for action in plan["candidate_actions"]]
            assert action_types.count("CORDON_NODE") == 1
            assert action_types.count("PREPARE_SOURCE_RETIREMENT") == 4
            assert action_types.count("VERIFY_NODE_DRAIN") == 1
            with clean_database.repository.transaction() as conn:
                persisted_order = conn.execute(
                    "SELECT intent_id, action_type FROM action_intents WHERE plan_id = %s "
                    "ORDER BY action_sequence",
                    (plan_id,),
                ).fetchall()
            assert [action["action_type"] for action in persisted_order] == [
                "CORDON_NODE",
                "PREPARE_SOURCE_RETIREMENT",
                "PREPARE_SOURCE_RETIREMENT",
                "PREPARE_SOURCE_RETIREMENT",
                "PREPARE_SOURCE_RETIREMENT",
                "VERIFY_NODE_DRAIN",
            ]
            retirement_actions = [
                action
                for action in plan["candidate_actions"]
                if action["action_type"] == "PREPARE_SOURCE_RETIREMENT"
            ]
            retirements_by_id = {action["intent_id"]: action for action in retirement_actions}
            retirement_order = [
                action["intent_id"]
                for action in persisted_order
                if action["action_type"] == "PREPARE_SOURCE_RETIREMENT"
            ]
            assert set(retirement_order) == set(retirements_by_id)
            retirement_actions = [retirements_by_id[intent_id] for intent_id in retirement_order]
            source_node_ids = {
                action["payload"]["source_node_id"]
                for action in plan["candidate_actions"]
                if action["action_type"] == "PREPARE_SOURCE_RETIREMENT"
            }
            assert len(source_node_ids) == 1
            source_node_id = next(iter(source_node_ids))
            source_node = next(
                node for node in ready_workers if node["metadata"]["name"] == source_node_id
            )
            source_node_uid = source_node["metadata"]["uid"]
            assert source_node_id in {node["metadata"]["name"] for node in safe_initial_nodes}

            operator = f"Bearer {raw_tokens['operator']}"
            approved = client.post(
                f"/v1/plans/{plan_id}/approve",
                headers={"Authorization": operator, "Idempotency-Key": f"drain-approve-{plan_id}"},
                json={},
            )
            assert approved.status_code == 200, approved.text
            late_action = retirement_actions[2]
            held_source = target.get_pod(late_action["payload"]["source_pod_name"])
            assert held_source is not None
            held_metadata = held_source["metadata"]
            assert held_metadata["uid"] == late_action["payload"]["source_pod_uid"]
            assert (
                held_metadata["labels"]["fleetright.io/workload-uid"]
                == late_action["payload"]["source_owner_uid"]
            )
            held_finalizers = list(held_metadata.get("finalizers", []))
            assert held_finalizer not in held_finalizers
            hold_patch = [
                {"op": "test", "path": "/metadata/uid", "value": held_metadata["uid"]},
                {
                    "op": "test",
                    "path": "/metadata/resourceVersion",
                    "value": held_metadata["resourceVersion"],
                },
                {
                    "op": "test",
                    "path": "/metadata/labels/fleetright.io~1workload-uid",
                    "value": late_action["payload"]["source_owner_uid"],
                },
                {
                    "op": "add",
                    "path": "/metadata/finalizers",
                    "value": [*held_finalizers, held_finalizer],
                },
            ]
            kubectl(
                "patch",
                "pod",
                late_action["payload"]["source_pod_name"],
                "-n",
                namespace,
                "--type=json",
                "-p",
                json.dumps(hold_patch),
            )
            started = client.post(
                f"/v1/plans/{plan_id}/execute",
                headers={"Authorization": operator, "Idempotency-Key": f"drain-execute-{plan_id}"},
                json={},
            )
            assert started.status_code == 202, started.text
            dsn_file.write_text(clean_database.dsn, encoding="utf-8")
            with clean_database.repository.transaction() as conn:
                conn.execute(
                    "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
                    "WHERE plan_id = %s",
                    (plan_id,),
                )
            context_mp = multiprocessing.get_context("spawn")
            old_controller_id = f"drain-before-crash-{uuid.uuid4().hex[:8]}"
            process = context_mp.Process(
                target=_run_live_drain_controller_until_two_done,
                args=(
                    str(dsn_file),
                    str(executor_config),
                    plan_id,
                    old_controller_id,
                    str(marker),
                    str(release_marker),
                    str(admitted_marker),
                    str(eviction_log),
                    str(diagnostics_file),
                    str(target_errors_file),
                ),
                daemon=True,
            )
            process.start()
            deadline = time.monotonic() + 360
            while time.monotonic() < deadline and process.is_alive() and not marker.exists():
                time.sleep(0.2)
            marker_value = marker.read_text(encoding="ascii") if marker.exists() else "missing"
            journal_diagnostics = (
                diagnostics_file.read_text(encoding="utf-8")
                if diagnostics_file.exists()
                else "missing"
            )
            target_errors = (
                target_errors_file.read_text(encoding="utf-8")
                if target_errors_file.exists()
                else "none"
            )
            assert marker_value == "third-eviction-pending", (
                f"marker={marker_value}, journal="
                f"{journal_diagnostics}, target_errors={target_errors}"
            )
            before_takeover_evictions = [
                json.loads(line) for line in eviction_log.read_text(encoding="utf-8").splitlines()
            ]
            assert len(before_takeover_evictions) == 2
            with clean_database.repository.transaction() as conn:
                states = conn.execute(
                    "SELECT action_type, execution_state, payload, execution_result "
                    "FROM action_intents WHERE plan_id = %s "
                    "ORDER BY action_sequence",
                    (plan_id,),
                ).fetchall()
                retirements_before = [
                    row for row in states if row["action_type"] == "PREPARE_SOURCE_RETIREMENT"
                ]
                completed_before = sum(
                    row["action_type"] == "PREPARE_SOURCE_RETIREMENT"
                    and row["execution_state"] == "SUCCEEDED"
                    for row in states
                )
                assert completed_before == 2
                uncertain = [
                    row
                    for row in retirements_before
                    if row["execution_state"] in {"RUNNING", "OUTCOME_UNKNOWN"}
                ]
                assert len(uncertain) == 1
                progress = uncertain[0]["execution_result"].get("progress", [])
                assert any(
                    item.get("result", {}).get("stage") == "EVICTION_INTENT"
                    for item in progress
                    if isinstance(item, dict) and isinstance(item.get("result"), dict)
                )
                in_flight_source = target.get_pod(uncertain[0]["payload"]["source_pod_name"])
                assert in_flight_source is not None
                assert (
                    in_flight_source["metadata"].get("uid")
                    == uncertain[0]["payload"]["source_pod_uid"]
                )
                assert in_flight_source["metadata"].get("deletionTimestamp") is None
                assert held_finalizer in in_flight_source["metadata"].get("finalizers", [])
                assert (
                    uncertain[0]["payload"]["operation_id"]
                    == late_action["payload"]["operation_id"]
                )

            old_detail = client.get(
                f"/v1/plans/{plan_id}",
                headers={"Authorization": f"Bearer {raw_tokens['viewer']}"},
            ).json()
            old_execution = old_detail["execution"]
            assert old_execution["controller_id"] == old_controller_id
            old_fencing_token = old_execution["controller_fencing_token"]
            release_marker.write_text("release", encoding="ascii")
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and not admitted_marker.exists():
                time.sleep(0.1)
            assert admitted_marker.exists()
            admitted_source = target.get_pod(uncertain[0]["payload"]["source_pod_name"])
            assert admitted_source is not None
            assert (
                admitted_source["metadata"].get("uid") == uncertain[0]["payload"]["source_pod_uid"]
            )
            assert admitted_source["metadata"].get("deletionTimestamp") is not None
            assert held_finalizer in admitted_source["metadata"].get("finalizers", [])

            with clean_database.repository.transaction() as conn:
                conn.execute(
                    "UPDATE plan_executions SET controller_lease_expires_at = clock_timestamp() "
                    "WHERE plan_id = %s",
                    (plan_id,),
                )

            real_kubectl = target._kubectl

            def trace_takeover_kubectl(*args: str, stdin: str | None = None):
                eviction = next((arg for arg in args if arg.endswith("/eviction")), None)
                if eviction is not None:
                    with eviction_log.open("a", encoding="utf-8") as output:
                        output.write(json.dumps(eviction) + "\n")
                return real_kubectl(*args, stdin=stdin)

            target._kubectl = trace_takeover_kubectl  # type: ignore[method-assign]
            takeover = ExecutionController(
                clean_database.repository,
                target,
                _LiveHealthyObserver(),
                f"drain-after-crash-{uuid.uuid4().hex[:8]}",
                readiness_timeout_seconds=300,
                poll_seconds=0.1,
            )
            takeover.run_once()
            assert len(eviction_log.read_text(encoding="utf-8").splitlines()) == 3
            detail = client.get(
                f"/v1/plans/{plan_id}",
                headers={"Authorization": f"Bearer {raw_tokens['viewer']}"},
            ).json()
            assert detail["execution"]["status"] == "EXECUTING", detail
            assert detail["execution"]["controller_id"] == takeover.controller_id
            takeover_fencing_token = detail["execution"]["controller_fencing_token"]
            assert takeover_fencing_token > old_fencing_token
            current_source_state = target.observe_scale(uncertain[0]["payload"]["workload_id"])
            assert current_source_state["fencing_token"] == takeover_fencing_token
            assert current_source_state["execution_fence"]["generation"] == takeover_fencing_token
            late_source = target.get_pod(uncertain[0]["payload"]["source_pod_name"])
            assert late_source is not None
            assert late_source["metadata"].get("uid") == uncertain[0]["payload"]["source_pod_uid"]
            assert late_source["metadata"].get("deletionTimestamp") is not None
            process.terminate()
            process.join(timeout=10)
            assert process.exitcode is not None and process.exitcode < 0
            process = None

            release_source = target.get_pod(uncertain[0]["payload"]["source_pod_name"])
            assert release_source is not None
            release_metadata = release_source["metadata"]
            remaining_finalizers = [
                item for item in release_metadata.get("finalizers", []) if item != held_finalizer
            ]
            release_patch = [
                {
                    "op": "test",
                    "path": "/metadata/uid",
                    "value": uncertain[0]["payload"]["source_pod_uid"],
                },
                {
                    "op": "test",
                    "path": "/metadata/resourceVersion",
                    "value": release_metadata["resourceVersion"],
                },
            ]
            if remaining_finalizers:
                release_patch.append(
                    {"op": "replace", "path": "/metadata/finalizers", "value": remaining_finalizers}
                )
            else:
                release_patch.append({"op": "remove", "path": "/metadata/finalizers"})
            kubectl(
                "patch",
                "pod",
                uncertain[0]["payload"]["source_pod_name"],
                "-n",
                namespace,
                "--type=json",
                "-p",
                json.dumps(release_patch),
            )
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if target.get_pod(uncertain[0]["payload"]["source_pod_name"]) is None:
                    break
                time.sleep(0.1)
            else:
                pytest.fail(
                    "accepted source Eviction did not finish after releasing the test finalizer"
                )
            assert len(eviction_log.read_text(encoding="utf-8").splitlines()) == 3
            deadline = time.monotonic() + 360
            while time.monotonic() < deadline:
                takeover.run_once()
                detail = client.get(
                    f"/v1/plans/{plan_id}",
                    headers={"Authorization": f"Bearer {raw_tokens['viewer']}"},
                ).json()
                status = detail["execution"]["status"]
                if status == "COMMITTED":
                    break
                assert status not in {"FAILED_SAFE", "COMPENSATING"}, detail
                time.sleep(0.5)
            assert detail["execution"]["status"] == "COMMITTED", [
                (
                    action["action_type"],
                    action["execution_state"],
                    [
                        (item.get("result") or {}).get("stage")
                        for item in (action.get("execution_result") or {}).get("progress", [])
                        if isinstance(item, dict)
                    ][-3:],
                    str((action.get("execution_result") or {}).get("error"))[:300],
                )
                for action in detail["candidate_actions"]
            ]
            assert all(
                action["execution_state"] == "SUCCEEDED" for action in detail["candidate_actions"]
            )
            eviction_paths = [
                json.loads(line) for line in eviction_log.read_text(encoding="utf-8").splitlines()
            ]
            assert len(eviction_paths) == 4
            expected_sources = {
                action["payload"]["source_pod_name"]
                for action in detail["candidate_actions"]
                if action["action_type"] == "PREPARE_SOURCE_RETIREMENT"
            }
            actual_sources = {
                path.split("/pods/", 1)[1].removesuffix("/eviction") for path in eviction_paths
            }
            assert len(actual_sources) == 4
            assert actual_sources == expected_sources
            for action in detail["candidate_actions"]:
                if action["action_type"] != "PREPARE_SOURCE_RETIREMENT":
                    continue
                progress = action["execution_result"].get("progress", [])
                stages = [
                    item.get("result", {}).get("stage")
                    for item in progress
                    if isinstance(item, dict) and isinstance(item.get("result"), dict)
                ]
                assert stages.index("READY_FOR_SAFE_EVICTION") < stages.index("EVICTION_INTENT")
                assert stages.count("EVICTION_INTENT") == 1
                result = action["execution_result"]["result"]
                assert result["stage"] == "RETIREMENT_COMPLETE"
                assert target.get_pod(action["payload"]["source_pod_name"]) is None
                replacement = target.get_pod(result["replacement_pod_name"])
                assert replacement is not None
                assert replacement["metadata"]["uid"] == result["replacement_pod_uid"]
                final = target.observe_scale(action["workload_id"])
                payload = action["payload"]
                desired = payload["final_desired_replicas"]
                assert desired == payload["baseline_replicas"] + 1
                assert final["replicas"] == final["observed_replicas"] == desired
                assert final["ready_replicas"] == desired
                assert final["available_replicas"] >= max(
                    final["availability_floor"], payload["profile_required_replicas"]
                )
                assert final["reconciled"] is True
                assert final["fencing_token"] == detail["execution"]["controller_fencing_token"]
                assert (
                    final["execution_fence"]["generation"]
                    == detail["execution"]["controller_fencing_token"]
                )
                assert final["execution_fence"]["planID"] == plan_id
                assert final["execution_fence"]["operationID"] == payload["operation_id"]
                pods = target._json(
                    "get",
                    "pods",
                    "-n",
                    namespace,
                    "-l",
                    f"fleetright.io/workload-uid={final['uid']}",
                    "-o",
                    "json",
                )["items"]
                assert len(pods) == desired
                hard_anti_affinity = {
                    "topologyKey": payload["topology_key"],
                    "labelSelector": {"matchLabels": {"fleetright.io/workload-uid": final["uid"]}},
                }
                assert all(
                    hard_anti_affinity
                    in pod.get("spec", {})
                    .get("affinity", {})
                    .get("podAntiAffinity", {})
                    .get("requiredDuringSchedulingIgnoredDuringExecution", [])
                    for pod in pods
                )
                pod_nodes = {pod["spec"]["nodeName"] for pod in pods}
                assert len(pod_nodes) == desired
                assert all(pod["spec"]["nodeName"] != source_node_id for pod in pods)
                approved_destinations = {
                    node["name"]: node["uid"] for node in payload["destination_nodes"]
                }
                assert result["replacement_node_id"] in approved_destinations
                assert (
                    approved_destinations[result["replacement_node_id"]]
                    == result["replacement_node_uid"]
                )
                allowed_nodes = {node["name"]: node["uid"] for node in final["approved_nodes"]}
                assert pod_nodes <= allowed_nodes.keys()
                current_nodes = {
                    node["metadata"]["name"]: node["metadata"]["uid"]
                    for node in json.loads(kubectl("get", "nodes", "-o", "json").stdout)["items"]
                }
                assert all(allowed_nodes[name] == current_nodes[name] for name in pod_nodes)
                assert (
                    current_nodes[result["replacement_node_id"]] == result["replacement_node_uid"]
                )
            final_node = json.loads(kubectl("get", "node", source_node_id, "-o", "json").stdout)
            assert final_node["metadata"]["uid"] == source_node_uid
            assert final_node["spec"].get("unschedulable") is True
            # Ownership is durable in the journal; transient Node metadata is gone, cordon stays.
            cordon = next(
                x for x in detail["candidate_actions"] if x["action_type"] == "CORDON_NODE"
            )
            assert cordon["execution_result"]["pre_observation"]["unschedulable_before"] is False
            assert cordon["execution_result"]["target_observation"]["cordon_owned"] is True
            assert [
                key
                for key in final_node["metadata"].get("annotations", {})
                if key.startswith("fleetright.io/")
            ] == ["fleetright.io/fencing-token"]  # only the monotonic high-water mark remains
            remaining_names = [
                pod["metadata"]["name"]
                for pod in json.loads(kubectl("get", "pods", "-A", "-o", "json").stdout)["items"]
                if pod.get("spec", {}).get("nodeName") == source_node_id
                and pod.get("metadata", {}).get("namespace") == namespace
                and pod.get("metadata", {}).get("labels", {}).get("fleetright.io/workload")
                in workload_ids
            ]
            assert remaining_names == []
    finally:
        if process is not None and process.is_alive():
            process.terminate()
            process.join(timeout=10)
        cleanup_complete = True
        proxy_process: subprocess.Popen[bytes] | None = None
        proxy_url: str | None = None
        if test_workloads:
            with socket.socket() as reserved_port:
                reserved_port.bind(("127.0.0.1", 0))
                proxy_port = reserved_port.getsockname()[1]
            try:
                proxy_process = subprocess.Popen(
                    [
                        "kubectl",
                        f"--kubeconfig={kubeconfig}",
                        f"--context={context}",
                        "proxy",
                        "--address=127.0.0.1",
                        f"--port={proxy_port}",
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                proxy_url = f"http://127.0.0.1:{proxy_port}"
                proxy_ready = False
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    if proxy_process.poll() is not None:
                        break
                    try:
                        with urllib.request.urlopen(f"{proxy_url}/healthz", timeout=1):
                            proxy_ready = True
                            break
                    except OSError:
                        time.sleep(0.05)
                if not proxy_ready:
                    proxy_url = None
            except OSError:
                proxy_url = None
            if proxy_url is None:
                cleanup_complete = False
        try:
            for workload_id, workload_uid in test_workloads.items():
                current = kubectl(
                    "get",
                    "fleetrightworkload",
                    workload_id,
                    "-n",
                    namespace,
                    "-o",
                    "json",
                    check=False,
                )
                if current.returncode == 0:
                    current_workload = json.loads(current.stdout)
                    if current_workload.get("metadata", {}).get("uid") != workload_uid:
                        cleanup_complete = False
                    else:
                        workload_cleanup_safe = True
                        owned_pods = kubectl(
                            "get",
                            "pods",
                            "-n",
                            namespace,
                            "-l",
                            f"fleetright.io/workload-uid={workload_uid}",
                            "-o",
                            "json",
                            check=False,
                        )
                        if owned_pods.returncode != 0:
                            cleanup_complete = False
                            workload_cleanup_safe = False
                        else:
                            for pod in json.loads(owned_pods.stdout).get("items", []):
                                metadata = pod.get("metadata", {})
                                finalizers = metadata.get("finalizers", [])
                                if held_finalizer not in finalizers:
                                    continue
                                if (
                                    metadata.get("labels", {}).get("fleetright.io/workload-uid")
                                    != workload_uid
                                ):
                                    cleanup_complete = False
                                    workload_cleanup_safe = False
                                    continue
                                remaining = [item for item in finalizers if item != held_finalizer]
                                release = [
                                    {
                                        "op": "test",
                                        "path": "/metadata/uid",
                                        "value": metadata["uid"],
                                    },
                                    {
                                        "op": "test",
                                        "path": "/metadata/resourceVersion",
                                        "value": metadata["resourceVersion"],
                                    },
                                ]
                                if remaining:
                                    release.append(
                                        {
                                            "op": "replace",
                                            "path": "/metadata/finalizers",
                                            "value": remaining,
                                        }
                                    )
                                else:
                                    release.append({"op": "remove", "path": "/metadata/finalizers"})
                                result = kubectl(
                                    "patch",
                                    "pod",
                                    metadata["name"],
                                    "-n",
                                    namespace,
                                    "--type=json",
                                    "-p",
                                    json.dumps(release),
                                    check=False,
                                )
                                if result.returncode != 0:
                                    cleanup_complete = False
                                    workload_cleanup_safe = False
                        if proxy_url is not None and workload_cleanup_safe:
                            delete = urllib.request.Request(
                                f"{proxy_url}/apis/fleetright.io/v1alpha1/namespaces/"
                                f"{namespace}/fleetrightworkloads/{workload_id}",
                                data=json.dumps(
                                    {
                                        "apiVersion": "v1",
                                        "kind": "DeleteOptions",
                                        "preconditions": {"uid": workload_uid},
                                    }
                                ).encode(),
                                headers={"Content-Type": "application/json"},
                                method="DELETE",
                            )
                            try:
                                with urllib.request.urlopen(delete, timeout=30):
                                    pass
                            except OSError:
                                cleanup_complete = False
                elif "NotFound" not in current.stderr:
                    cleanup_complete = False
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    pods = kubectl(
                        "get",
                        "pods",
                        "-n",
                        namespace,
                        "-l",
                        f"fleetright.io/workload-uid={workload_uid}",
                        "-o",
                        "json",
                        check=False,
                    )
                    if pods.returncode != 0:
                        cleanup_complete = False
                        break
                    if not json.loads(pods.stdout).get("items"):
                        break
                    time.sleep(0.5)
                else:
                    cleanup_complete = False
        finally:
            if proxy_process is not None:
                proxy_process.terminate()
                try:
                    proxy_process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proxy_process.kill()
                    proxy_process.wait(timeout=10)
        for node_name in tainted_nodes:
            kubectl("taint", "node", node_name, f"{test_taint}-", check=False)
        if (
            cleanup_complete
            and source_node_id is not None
            and source_node_uid is not None
            and plan_id is not None
            and cordon_operation_id is not None
        ):
            current_node = kubectl("get", "node", source_node_id, "-o", "json", check=False)
            if current_node.returncode == 0:
                current = json.loads(current_node.stdout)
                metadata = current.get("metadata", {})
                annotations = metadata.get("annotations", {})
                token = annotations.get("fleetright.io/fencing-token")
                if (
                    metadata.get("uid") == source_node_uid
                    and isinstance(metadata.get("resourceVersion"), str)
                    and annotations.get("fleetright.io/plan-id") == plan_id
                    and annotations.get("fleetright.io/operation-id") == cordon_operation_id
                    and isinstance(token, str)
                    and token.isdecimal()
                    and current.get("spec", {}).get("unschedulable") is True
                ):
                    patch = [
                        {"op": "test", "path": "/metadata/uid", "value": source_node_uid},
                        {
                            "op": "test",
                            "path": "/metadata/resourceVersion",
                            "value": metadata["resourceVersion"],
                        },
                        {
                            "op": "test",
                            "path": "/metadata/annotations/fleetright.io~1plan-id",
                            "value": plan_id,
                        },
                        {
                            "op": "test",
                            "path": "/metadata/annotations/fleetright.io~1operation-id",
                            "value": cordon_operation_id,
                        },
                        {
                            "op": "test",
                            "path": "/metadata/annotations/fleetright.io~1fencing-token",
                            "value": token,
                        },
                        {
                            "op": "test",
                            "path": "/spec/unschedulable",
                            "value": True,
                        },
                        {
                            "op": "remove",
                            "path": "/metadata/annotations/fleetright.io~1plan-id",
                        },
                        {
                            "op": "remove",
                            "path": "/metadata/annotations/fleetright.io~1operation-id",
                        },
                        {
                            "op": "remove",
                            "path": "/metadata/annotations/fleetright.io~1fencing-token",
                        },
                        {"op": "replace", "path": "/spec/unschedulable", "value": False},
                    ]
                    uncordoned = kubectl(
                        "patch",
                        "node",
                        source_node_id,
                        "--type=json",
                        "-p",
                        json.dumps(patch),
                        check=False,
                    )
                    cleanup_complete &= uncordoned.returncode == 0
                    if uncordoned.returncode == 0:
                        after = kubectl("get", "node", source_node_id, "-o", "json", check=False)
                        if after.returncode == 0:
                            value = json.loads(after.stdout)
                            cleanup_complete &= (
                                value.get("metadata", {}).get("uid") == source_node_uid
                                and value.get("spec", {}).get("unschedulable") is not True
                                and not any(
                                    key in value.get("metadata", {}).get("annotations", {})
                                    for key in (
                                        "fleetright.io/plan-id",
                                        "fleetright.io/operation-id",
                                        "fleetright.io/fencing-token",
                                    )
                                )
                            )
                        else:
                            cleanup_complete = False
        if not cleanup_complete:
            pytest.fail("test FleetRightWorkloads or Pods did not finish UID-scoped cleanup")


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


def test_supported_scale_out_request_can_plan_four_to_five(
    clean_database: Database,
) -> None:
    tokens, raw_tokens = _tokens()
    app = create_app(clean_database.repository, tokens=tokens)
    with TestClient(app) as client:
        plan = _create_verified_plan(
            client, raw_tokens, current_replicas=4, demand_qps_milli=2_200_000
        )
    scale = next(
        action
        for action in plan["candidate_actions"]
        if action["action_type"] == "SET_REPLICA_COUNT"
    )
    assert scale["payload"]["current_replicas"] == 4
    assert scale["payload"]["proposed_replicas"] == 5


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
        os.path.expanduser("~/.cache/fleetright/safety-lab.kubeconfig")
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
        # Cluster-wide patch is granted for cordon; the Node admission policy confines it.
        (("patch", "nodes"), "yes"),
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
    assert baseline["replicas"] == baseline["ready_replicas"]
    with docker_e2e_database.repository.transaction() as conn:
        conn.execute(
            "SELECT setval('execution_controller_fence_seq', GREATEST(%s, 1), true)",
            (baseline["fencing_token"],),
        )
    expected_replicas = baseline["replicas"] + 1
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
            plan = _create_verified_plan(
                client,
                raw_tokens,
                current_replicas=baseline["replicas"],
                demand_qps_milli=500_000 * baseline["replicas"] + 200_000,
            )
            plan_id = plan["plan_id"]
            scale = next(
                action
                for action in plan["candidate_actions"]
                if action["action_type"] == "SET_REPLICA_COUNT"
            )
            assert scale["payload"]["proposed_replicas"] == expected_replicas
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
            if not mutation_ready.exists():
                detail = client.get(
                    f"/v1/plans/{plan_id}",
                    headers={"Authorization": f"Bearer {raw_tokens['viewer']}"},
                )
                pytest.fail(
                    "Kubernetes scale did not reach the approved Ready count; "
                    f"execution={detail.json().get('execution')}; "
                    f"target={target.observe_scale('cpu-api')}"
                )
            scaled = target.observe_scale("cpu-api")
            assert scaled["replicas"] == scaled["ready_replicas"] == expected_replicas
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
            assert final_target["replicas"] == final_target["ready_replicas"] == expected_replicas
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
