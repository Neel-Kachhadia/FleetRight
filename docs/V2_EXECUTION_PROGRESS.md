# V2 Execution Progress

## 2026-10-08 — Phase 1, session 1

**Current phase:** Phase 1, supported workload ownership and scheduling.

**Branch/start HEAD:** `phase4/safe-execution-reconciliation` / `9116b8a2bfbed015715073e6372a476f57944247`.

**Session status:** implementation and local checks complete; final live run must be repeated from the committed code before Phase 1 closes.

### Contract and implementation

Added a namespaced `fleetright.io/v1alpha1` `FleetRightWorkload` CRD with scale
and status subresources, immutable workload identity/template/placement, desired
replicas, resource requests, availability floor, disruption limits, bounded
allowed `{name, uid}` Nodes, topology requirements, surge cap, and operation
references. The Python controller creates deterministic Pods with CR owner
references, reconciles status and an owner-matched PDB, and rebuilds state from
the Kubernetes API after restart.

Required node affinity uses one `metadata.name In [node]` term per approved
Node, so the scheduler can choose only from the allowed name set. Each pass
checks the approved Node UIDs and scheduled Pod Node against a single current
Node inventory. Native affinity cannot match Node UID; a same-name Node
replacement between inventory read and binding remains a race. Topology spread
uses `DoNotSchedule`, and requested anti-affinity is required. CPU and memory
requests are supported; nonzero `networkMbps` is rejected. Phase 1 never uses
`surgeBudget` to create over-desired Pods.

The controller runs in isolated namespace `fleetright-v2`. Its Role manages
Pods and PDBs only in that namespace; a separate ClusterRole can only get/list
Nodes. It has no Deployment, eviction, Node mutation, or Secret permission.
Namespace Pod Security Admission is set to restricted enforce/audit/warn.
Generated Pods set nonroot UID/GID defaults, `RuntimeDefault` seccomp,
`allowPrivilegeEscalation: false`, and drop all capabilities. `Ready` means the
Kubernetes Pod Ready condition, including configured probes; it is not a
separate application-health measure.

Ordinary scale reconciliation counts owned, nonterminal Pods. It does not
delete an extra ordinal when the desired count is already met, deletes at most
one eligible Pod per pass when scaling down, preserves the Ready availability
floor, and waits for deletion completion before another step or logical
replica reuse. CRs with `deletionTimestamp` are left to owner-reference garbage
collection. Env list order remains immutable because Kubernetes environment
expansion can depend on earlier entries.

Ordinary Deployments remain on their existing scale-only path. The independent
C++ verifier, V2 execution rejection, and ADR-008 gate are unchanged. No V2
execution, source eviction, Deployment takeover, exact Node UID placement, or
node drain is enabled.

### Verification

Local checks on the final uncommitted code:

```text
PYTHONPATH=python .venv/bin/python -m pytest -q python/tests/test_workload_controller.py
  22 passed
env -u FLEETRIGHT_TEST_DATABASE_URL PYTHONPATH=python .venv/bin/python -m pytest -q
  74 passed, 47 skipped (PostgreSQL integration not configured)
ctest --test-dir build --output-on-failure
  36/36 passed
ruff check python/fleetright/workload_controller.py python/tests/test_workload_controller.py
  All checks passed
py_compile, YAML safe_load_all, and git diff --check
  passed
```

The focused suite covers owned Pods, hard affinity/topology, restart
reconstruction, Node UID change, identity conflict/status accounting, PDB
construction, env ordering, terminal and terminating Pod identity, count-based
scale-down, availability-floor candidate selection, deletion waits, and
sanitized/transient API errors.

On the dedicated `kind-fleetright-safety-lab` Kubernetes v1.37.0 API server,
the current controller manifest and generated restricted Pod passed
server-side admission. A generated Pod with UID 0 was rejected by restricted
PSA; a CR setting `runAsUser: 0` was rejected by schema validation. The env
reorder dry-run was rejected as immutable. These admission checks create no
workload objects. The earlier live scheduling/PDB/restart run predates the
latest hardening and is not counted as final Phase 1 evidence; repeat it against
the committed image and record the commit/image identity before closure.

### Limits and remaining work

- The kind workers have pre-existing `kube-proxy` errors (`fsnotify watcher init: too many open files`); controller requests through the Service IP timed out from workers. The prior lab run temporarily scheduled the controller on the healthy control plane; workload Pods remained affinity-limited to workers. No host setting was changed.
- Node name affinity cannot close the documented same-name Node UID race.
- Network-bandwidth scheduling is unsupported.
- PostgreSQL tests were skipped and the isolated database was not accessed. A previously surfaced local test-DB credential remains unrotated; it is not used by this phase.
- Implementation files and documentation are staged for the Phase 1 implementation commit; the committed-image live run and its evidence update follow that commit.

**First next action:** finalize the Phase 1 code commit, build the controller image from that SHA, and rerun the scheduler, scale/PDB, restart, RBAC, topology, and capacity checks against the explicit safety-lab context. Then record the exact evidence and close Phase 1.
