# V2 Execution Progress

## 2026-10-08 checkpoint

**Current phase:** Phase 1 complete; Phase 2 is the next incomplete phase.

**Session:** 1; approximately 75 minutes.

**Branch:** `phase4/safe-execution-reconciliation`.

**Starting HEAD:** `9116b8a2bfbed015715073e6372a476f57944247`.

**Implementation commit:** `ee29b96ec8975b3bb7703ab258a5e8ffcd81e59b` (`feat: add FleetRight-managed workload controller`).

**Verified source HEAD:** `ee29b96ec8975b3bb7703ab258a5e8ffcd81e59b`.

**Final live evidence:** [phase1-kind-2026-10-08.json](evidence/phase1-kind-2026-10-08.json).

### Completed contract

Added namespaced `fleetright.io/v1alpha1` `FleetRightWorkload` with a CRD schema,
scale/status subresources, desired replica count, immutable workload ID/template/
placement, CPU/memory requests, availability floor, disruption limit, surge cap,
allowed Node `{name, uid}` records, topology requirements, and active operation
references. The controller creates deterministic logical replicas with
`FleetRightWorkload` owner references and reconstructs state from the Kubernetes
API after restart. It also maintains an owner-matched PDB whose selector agrees
with the serialized scale selector.

Required node affinity uses one `metadata.name In [name]` term per allowed Node.
The scheduler can choose only within the approved name set; the controller checks
approved Node UIDs and each scheduled Pod's Node against one fresh Node inventory
per reconcile. Topology spread uses `DoNotSchedule`; requested Pod anti-affinity
is required. No soft affinity or custom scheduler is used.

Ordinary scaling counts owned, nonterminal Pods. It preserves missing logical
ordinal gaps when desired count is already met, deletes at most one eligible Pod
per pass, preserves `availabilityFloor`, and waits while a Pod deletion or
logical identity is still present. A deleting CR is left to owner-reference
garbage collection. Phase 1 does not consume `surgeBudget` to create
over-desired Pods. Env order is immutable because Kubernetes expansion can
depend on earlier entries.

The controller is isolated in `fleetright-v2`; its namespaced Role manages Pods
and PDBs only there, and its ClusterRole only reads Nodes. It cannot mutate
Deployments, evict Pods, mutate Nodes, or read Secrets. The namespace enforces,
audits, and warns at restricted Pod Security. Generated Pods use nonroot UID/GID,
RuntimeDefault seccomp, no privilege escalation, and drop all capabilities.
`Ready` means Kubernetes Pod readiness and configured probes; it is not an
application-health signal.

Ordinary Deployments remain on their existing scale-only path. The independent
C++ verifier, V2 execution-admission rejection, and ADR-008 gate are unchanged.
No V2 plan execution, eviction, Deployment takeover, exact Node UID placement,
or node drain is enabled.

### Verification results

| Gate | Result | Evidence |
|---|---|---|
| Focused workload-controller tests | PASS, 22 | owned Pod identity, hard affinity/topology, restart, conflicts, status, env order, scale down, API failures |
| Full Python suite | PASS, 74; 47 PostgreSQL tests skipped | `FLEETRIGHT_TEST_DATABASE_URL` unset; no database access |
| C++ regression | PASS, 36/36 | `ctest --test-dir build --output-on-failure` |
| Ruff, Python compilation, YAML parse, diff check | PASS | final implementation commit |
| Kubernetes CRD/scale API | PASS | Kubernetes server v1.37.0; `/scale` returned desired=2, observed=2, matching selector |
| Owned workload and hard placement | PASS | two Ready Pods with exact owner UID on the two allowed workers; no control-plane placement |
| Topology and capacity blocks | PASS | real Pending Pods and scheduler conditions; 64 CPU request vs 32 CPU allocatable per worker |
| PDB custom-scale integration | PASS | expectedPods followed scale 2→3; observed disruptionsAllowed 1 at floor 1 and 0 at floor 2 |
| Restart reconstruction | PASS | controller rollout completed; main workload Pod names and UIDs were unchanged, with no duplicate logical replica |
| RBAC positive/negative | PASS | managed-namespace Pod create and Node read allowed; unrelated namespace Pod create, eviction, Secret read, Deployment/Node/workload patch denied |
| Restricted Pod Security and schema | PASS | generated Pod admitted; privileged Pod and UID 0 CR rejected |
| Env-order immutability | PASS | server-side dry-run rejected reversing dependent env entries |
| Cleanup | PASS | test namespace, CRD, ClusterRole/Binding, and workload-labeled Pods absent |

The committed image was built from `ee29b96` and loaded into `kind-fleetright-safety-lab`.
Tool versions were kind v0.33.0, Docker 29.1.3, Kubernetes client v1.37.1, and
server v1.37.0. The evidence JSON records exact Node/Pod UIDs, scheduler
messages, image IDs, PDB status, RBAC decisions, restart comparison, and cleanup.

### Review and session record

GPT-6 Luna Max executed Phase 1. GPT-6.1 Sol High reviewed scheduling/API
semantics and later confirmed the downscale candidate-selection fix closed its
convergence finding. Claude Sonnet 5.5 High was called through first-party
Claude Code; the provider call succeeded, but the captured review output was
truncated. Visible findings were addressed and rechecked; the uncertain call
was not replayed.

The live controller manifest was copied to `/tmp` with the committed image and
a temporary control-plane nodeSelector/toleration. The checked-in manifest and
host configuration were not changed. Both worker kube-proxy Pods remain Error;
their previous logs report `fsnotify watcher init: too many open files`, while
the control-plane kube-proxy is Running. Workload Pods remained hard-limited to
the workers. The node-name affinity versus Node-UID race between inventory read
and scheduler binding remains an explicit limitation; nonzero network bandwidth
requests are unsupported.

PostgreSQL integration was not run. The isolated DB was not accessed; a
previously surfaced local test-DB credential remains unrotated and must not be
used until rotated. No Phase 4 milestone tag was created and nothing was pushed.

Phase 1 has no remaining blockers under its allowed-Node-name scheduling
contract. Phase 2 target-side fencing, Phase 3 eviction, Phase 4 drain, and
Phase 5 adversarial qualification are not started; V2 destructive execution
remains disabled.

### Next action

Start Phase 2 target-side execution authority and fencing analysis. Keep V2
destructive execution disabled until the stale-request takeover race has an
implementation-backed, adversarially tested safety boundary.
