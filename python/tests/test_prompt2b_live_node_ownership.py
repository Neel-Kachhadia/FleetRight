"""Live Node cordon ownership against the real API server and Node admission policy."""

from __future__ import annotations

import base64
import json
import os
import subprocess
import uuid
from pathlib import Path

import pytest
import yaml
from fleetright.execution import ActionBlocked, FleetRightWorkloadTarget, TargetPreconditionFailed

pytestmark = pytest.mark.skipif(
    os.getenv("FLEETRIGHT_RUN_KIND_E2E") != "1",
    reason="set FLEETRIGHT_RUN_KIND_E2E=1 for live Node cordon ownership tests",
)

TRANSIENT = (
    "fleetright.io/plan-id",
    "fleetright.io/operation-id",
    "fleetright.io/fencing-token",
    "fleetright.io/cordon-owned",
)


def test_live_cordon_ownership_release_and_external_uncordon(tmp_path: Path) -> None:
    kubeconfig = os.getenv(
        "FLEETRIGHT_LAB_ADMIN_KUBECONFIG", "/tmp/fleetright-prompt2b-lab.kubeconfig"
    )
    context = os.getenv("FLEETRIGHT_LAB_CONTEXT", "kind-fleetright-prompt2b-lab")

    def kubectl(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["kubectl", f"--kubeconfig={kubeconfig}", f"--context={context}", *args],
            capture_output=True,
            text=True,
            check=check,
            timeout=30,
        )

    cluster = json.loads(kubectl("config", "view", "--minify", "--raw", "-o", "json").stdout)[
        "clusters"
    ][0]["cluster"]
    executor_config = tmp_path / "executor.kubeconfig"
    executor_config.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "clusters": [
                    {
                        "name": "lab",
                        "cluster": {
                            "server": cluster["server"],
                            "certificate-authority-data": cluster["certificate-authority-data"],
                        },
                    }
                ],
                "users": [
                    {
                        "name": "executor",
                        "user": {
                            "token": kubectl(
                                "create",
                                "token",
                                "fleetright-controller",
                                "-n",
                                "fleetright",
                                "--duration=15m",
                            ).stdout.strip()  # fmt: skip
                        },
                    }
                ],
                "contexts": [
                    {"name": "executor", "context": {"cluster": "lab", "user": "executor"}}
                ],
                "current-context": "executor",
            }
        )
    )
    executor_config.chmod(0o600)
    assert base64.b64decode(cluster["certificate-authority-data"])
    target = FleetRightWorkloadTarget(kubeconfig=str(executor_config), namespace="fleetright-v2")

    nodes = json.loads(kubectl("get", "nodes", "-o", "json").stdout)["items"]
    workers = sorted(
        node["metadata"]["name"]
        for node in nodes
        if not any(key.endswith("/control-plane") for key in node["metadata"].get("labels", {}))
        and not node.get("spec", {}).get("unschedulable")
    )
    assert len(workers) >= 3
    plan = f"own-{uuid.uuid4().hex[:8]}"

    def get_node(name: str) -> dict:
        return json.loads(kubectl("get", "node", name, "-o", "json").stdout)

    def transient(name: str) -> dict[str, str]:
        return {
            key: value
            for key, value in get_node(name)["metadata"].get("annotations", {}).items()
            if key in TRANSIENT
        }

    def payload(name: str) -> dict[str, str]:
        return {
            "operation_id": f"{plan}-cordon",
            "source_node_id": name,
            "source_node_uid": get_node(name)["metadata"]["uid"],
        }

    try:
        # --- initially schedulable Node: FleetRight owns the cordon ---
        fresh = workers[0]
        result = target.cordon_source_node(plan, payload(fresh), 7)
        assert result["cordon_owned"] is True and result["already_applied"] is False
        assert get_node(fresh)["spec"]["unschedulable"] is True
        assert transient(fresh)["fleetright.io/cordon-owned"] == "true"
        version = get_node(fresh)["metadata"]["resourceVersion"]
        # duplicate reconciliation of a completed cordon writes nothing
        assert target.cordon_source_node(plan, payload(fresh), 7)["already_applied"] is True
        assert get_node(fresh)["metadata"]["resourceVersion"] == version
        # takeover adopts the fence monotonically and keeps ownership; stale fences are refused
        assert target.cordon_source_node(plan, payload(fresh), 8)["cordon_owned"] is True
        with pytest.raises(TargetPreconditionFailed, match="newer controller"):
            target.cordon_source_node(plan, payload(fresh), 7)
        with pytest.raises(ActionBlocked, match="OPERATION_CONFLICT"):
            target.cordon_source_node("another-plan", payload(fresh), 9)
        # the executor still cannot do anything beyond cordon + fleetright.io annotations
        taint = [
            "kubectl",
            f"--kubeconfig={executor_config}",
            "taint",
            "node",
            fresh,
            "x=y:NoSchedule",
        ]
        denied = subprocess.run(taint, capture_output=True, text=True, timeout=30, check=False)
        assert denied.returncode != 0 and "may only cordon" in denied.stderr, denied.stderr
        # external uncordon mid-operation is never overridden
        kubectl("uncordon", fresh)
        with pytest.raises(ActionBlocked, match="UNCORDONED_EXTERNALLY"):
            target.cordon_source_node(plan, payload(fresh), 8)
        assert not get_node(fresh)["spec"].get("unschedulable")
        kubectl("cordon", fresh)
        # terminal: only FleetRight's transient metadata goes; the cordon remains
        assert target.release_source_node(plan, payload(fresh), 8)["released"] is True
        assert transient(fresh) == {} and get_node(fresh)["spec"]["unschedulable"] is True
        assert target.release_source_node(plan, payload(fresh), 8) == {"released": False}

        # --- administrator-cordoned Node: recorded as not owned and never released ---
        admin = workers[1]
        kubectl("cordon", admin)
        result = target.cordon_source_node(plan, payload(admin), 11)
        assert result["cordon_owned"] is False
        assert transient(admin)["fleetright.io/cordon-owned"] == "false"
        assert target.release_source_node(plan, payload(admin), 11)["cordon_owned"] is False
        assert transient(admin) == {}
        assert get_node(admin)["spec"]["unschedulable"] is True  # administrator's cordon intact

        # --- stale/forged Node identity is refused before any write ---
        wrong = {**payload(workers[2]), "source_node_uid": get_node(workers[0])["metadata"]["uid"]}
        before = get_node(workers[2])["metadata"]["resourceVersion"]
        with pytest.raises(ActionBlocked, match="UID_CHANGED"):
            target.cordon_source_node(plan, wrong, 12)
        assert get_node(workers[2])["metadata"]["resourceVersion"] == before
    finally:
        for name in workers[:3]:
            kubectl("uncordon", name, check=False)
            kubectl("annotate", "node", name, *(f"{key}-" for key in TRANSIENT), check=False)
