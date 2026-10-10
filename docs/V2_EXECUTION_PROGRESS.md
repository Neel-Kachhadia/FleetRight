# V2 Execution Progress

## 2026-10-10 Prompt 2B continuation — current session audit

**Current top-level prompt:** Prompt 2B of the original three-prompt plan.
Prompt 3 has not started. No new milestone or release tag was created.

**Session start:** 2026-10-10 13:07:50 UTC; nine-hour hard stop is
2026-10-10 22:07:50 UTC, with target finish by 21:37:50 UTC.

**Branch / starting HEAD:** `phase4/safe-execution-reconciliation` /
`d70c276` (`fix: bound Kubernetes execution to workload authority`). The
worktree was clean at start.

**Local implementation checkpoint:** `10292a2`
(`fix: fence workload authority across takeovers`). No tag or push was made.

**Checkpoint recorded:** 2026-10-10 16:02 UTC (about 2 hours 55 minutes elapsed;
about 6 hours 5 minutes remains to the hard stop).

**Current committed HEAD:** `27df715`
(`test: cover docker controller process crash recovery`). The worktree was
clean after this commit; current checkpoint edits are documentation only.

**Current gate:** Prompt 2B remains PARTIAL. Current Kubernetes execution
admits only monotonic scale-up on a newly created, explicitly marked
`FleetRightWorkload`. V2 movement, Pod eviction, scale-down, terminal-Pod
replacement, and node drain remain disabled. The saved Kubernetes 1.37.0
negative control shows that an unready-Pod Eviction can replace the caller's
resourceVersion precondition; no safe final-mutation fence has been established.

### Current-session evidence

| Check | Result | Evidence |
|---|---|---|
| Initial repository audit | PASS | Expected root and branch; clean worktree; `git diff --check` clean; HEAD `d70c276`; no milestone-4 or release tag |
| Python and PostgreSQL suite | PASS, 163 passed, 1 skipped, 1 warning (7.80s) | `.venv/bin/python -m pytest -q python/tests`; fresh no-volume PostgreSQL 16 `fleetright_test`, trust auth bound to `127.0.0.1:32976`; temporary container stopped and removed. The opt-in Docker E2E is the one skip. |
| Targeted takeover replay regression | PASS | Included in the 163-passed Python suite; injects response loss after fake target authorization has published the request, then checks takeover replay and one authorization. |
| Docker API E2E | PASS, 1 passed (21.31s) | `python/tests/test_prompt2a_control_api.py::test_docker_api_execution_changes_real_compose_state`; authenticated optimize/verify/approve/execute, duplicate request, real Compose 3→4, healthcheck readiness for all four replicas, fresh SLO metrics, then kill a spawned controller process after mutation while the durable action remains `RUNNING`. After lease expiry, a second controller process observes the target and commits with exactly one recorded dispatch. Fresh no-volume `fleetright_test` on `127.0.0.1:32976`; isolated random project removed. |
| CRD preflight unit and manifest checks | PASS | 5 focused tests; all 25 required runtime execution rules and required request/fence fields match the checked-in CRD. Live API-server validation remains blocked until the cluster cert is valid. |
| C++ regression | PASS, 36/36 | `ctest --test-dir build --output-on-failure` |
| Changed Python quality checks | PASS | Ruff 0.15.20 and `compileall` on all four changed Python files |
| Changed YAML and CRD contract | PASS | PyYAML parsed the CRD, all 7 RBAC documents, and the portless Docker fixture; direct check confirmed all 25 execution CEL rules, required fields, and scale-up protocol enum |
| Supplied DB endpoint | BLOCKED | Credential file parsed without displaying secrets; configured database is `fleetright_test`, but `127.0.0.1:32768` refused connections and `fleetright-isolated-test-db` was absent |
| Safety-lab live access | BLOCKED | Explicit kubeconfig/context used. Sandbox first denied localhost socket access; one elevated read reached Kubernetes client v1.37.1 but API TLS certificate is not valid until `2026-10-10 18:29:54 UTC`. No TLS bypass or cluster mutation was attempted |
| Current V2 destructive execution | DISABLED | Source, verifier, current RBAC, ADR-008, and saved API-server race evidence reviewed; no eviction or cordon mutator exists at committed HEAD |
| Docker failure coverage | PARTIAL | Lost response, actual process kill at the post-mutation/pre-response boundary, and process takeover from observed state pass. Target timeout, health-regression compensation, and compensation recovery remain untested. |

The full-suite DB run used a fresh no-volume container bound only to
`127.0.0.1:32976`, confirmed database name `fleetright_test`, ran the suite,
then stopped and removed the container. The separate Docker E2E used a fresh
container on the same port, then also stopped and removed it. The existing
`fleetright_fleetright-postgres` volume was not attached or changed.

### Current-session fence changes and review

- The FleetRightWorkload target preflight now requires the immutable and
  monotonic execution CEL rules, request/fence fields, and protocol enum before
  it patches the target. This closes the defense-in-depth gap where an
  incomplete CRD could pass the fence-only preflight. Target observations also
  fail closed unless `fencedGeneration` equals `metadata.generation`; CEL does
  not directly compare those metadata fields.
- Takeover receipt validation checks the latest dispatch against the live
  target fence. If a delayed old authorization wins before the takeover fence
  is written, the controller re-observes and adopts that exact published
  request before recording the new fence; the old request is not rejected in
  that ordering. An authorization arriving after a newer fence is rejected,
  and an old receipt cannot close a later dispatch. Repeated takeover replay
  reconstructs the prior fenced generation.
- Added direct target and PostgreSQL journal race coverage for stale
  preconditions, same-token replay, repeated takeover, delayed old authorization,
  and an old receipt arriving after a newer dispatch.
- GPT-6.1 Sol High and Sol Max re-reviewed the final scale-up fencing changes
  and reported no remaining production blockers in the reviewed path. This
  review is not runtime proof and does not extend the destructive-action
  boundary. A read-only Claude Sonnet request through the signed-in Claude Code
  subscription failed before inference because DNS could not reach its API
  (`EAI_AGAIN`); `ANTHROPIC_API_KEY` was absent, and no pay-as-you-go API was
  used.
- Claude Opus 5.5 Medium completed a read-only principal review through the
  first-party Pro subscription. It confirmed the destructive path must remain
  disabled and found additional contract mismatches: V2 binds Deployment and
  ReplicaSet identities, not FleetRightWorkload-owned Pods; the managed FRW
  PDB protects `availabilityFloor`, not the profile-required replica count and
  N-1 reserve. Review conclusions are advisory; source checks and tests are
  recorded separately. The wrapper reported the two `--conclusion` arguments
  as excluded file paths, but the Sol findings were present in the supplied
  evidence file.
- The scale-up review's possible fake-ordering finding was withdrawn after
  confirming the current fake already handles same-fence replay before the
  changed-precondition rejection. A regression test now covers a published
  workload request, takeover fence applied before receipt commit, replay under
  the same pending token, and exactly one authorization; the full Python suite
  passes.

### Independent destructive-action race review

GPT-6.1 Sol High and Sol Max reviewed whether an irrevocable authorization to
evict one exact Pod UID could make a delayed Eviction safe. They found this is
viable in principle only if the intent stays irrevocable while that UID exists,
every request targets that UID, and every state reachable during
`OUTCOME_UNKNOWN` preserves the availability floor, capacity, topology, and
allowed failure reserve. The current V2 runtime does not establish those
conditions, and that narrow retirement promise still does not meet V2's
source-excluding placement or node-drain postconditions.

The FleetRightWorkload controller can have a stale Pod CREATE in flight after
it observed a logical ordinal absent. Another reconcile can create UID `U`, a
staged operation can replace and evict `U`, and the old CREATE can then be
accepted under the same deterministic Pod name, producing UID `U2` from the
old workload placement. `reconcile_workload` separates GET and CREATE at
`python/fleetright/workload_controller.py:984-1020`; the existing
`test_delayed_old_create_after_takeover_remains_a_valid_deterministic_slot`
demonstrates a delayed CREATE crossing a takeover, and
`test_terminal_pod_replacement_uses_same_deterministic_slot_after_removal`
demonstrates same-name reuse after removal. `U2` may be placed back on the
source node, so source absence observed once is not a durable drain proof.

The current controller's independent Pod CREATE path has no cross-resource
atomic fence with workload authority, so it cannot prove source exclusion or
durable drain against this ordering. Keep Pod eviction, staged replacement,
and node drain disabled until a target-side protocol proves the complete
permitted outcome set safe. This is a material Prompt 2B blocker; no milestone
tag or Prompt 3 qualification is authorized.

The normal reconciliation path recreates the same logical ordinal after it is
evicted while that ordinal remains below the immutable desired count. The
deterministic replacement can use the old allowed-node set and may return to
the source node even without a stale request. V2's independent verifier also
requires each source Pod's controller UID to equal the active ReplicaSet UID;
FleetRightWorkload Pods are instead owned by the FleetRightWorkload UID. The
current V2 contract therefore cannot bind to this execution target. Its
managed PDB computes `maxUnavailable` from `availabilityFloor` and does not
encode the verifier's profile-required replica floor or N-1 reserve. These
are additional Prompt 2B blockers, not gates that can be closed by the current
scale-up path.

The review also identified two Eviction API subtleties in pinned Kubernetes
v1.37.0 source. An accepted Eviction can delete its source after a later
replacement-readiness regression; a PDB check at admission does not reserve
future readiness. In the healthy-Pod path, a stale same-name request may
decrement disruption allowance and record `disruptedPods` before the final UID
mismatch prevents Pod mutation. In the unready-Pod path, the handler may
replace the caller's resourceVersion and skip that PDB bookkeeping entirely.
Thus neither UID mismatch, resourceVersion, nor PDB status alone proves that a
stale request had no side effects or that no request remains in flight.
Finally, refusing every resend can remain `OUTCOME_UNKNOWN` forever after a
crash between durable dispatch recording and HTTP transmission. No current
target protocol resolves these cases.

### Source findings and remaining Prompt 2B gates

- `FleetRightWorkload` `APPLIED` means all deterministic desired Pod ordinals
  exist and pass identity/template/placement checks. It does not mean Ready.
  The API reconciler separately waits for matching controller status, observed
  replicas, readiness, availability, and a healthy metrics window before it
  completes an action.
- PostgreSQL leases and the workload `resourceVersion` CAS protect
  authorization writes only; they do not fence a separate Eviction request.
  A durable immutable retirement intent might be a different protocol, but it
  must also prevent stale controller CREATEs from recreating retired ordinals,
  bind the exact operation/source, preserve cancellation semantics, and define
  mixed-version behavior. No such protocol is implemented or proven.
- The complete authenticated API approval/journal/takeover path against the
  live `FleetRightWorkload` target, including process crash and lost response,
  remains NOT RUN in this session because the cluster certificate is not yet
  valid. Recheck only after its `NotBefore` time, with the explicit context.
- The positive Docker API path now passes against a portless isolated Compose
  fixture: verified 3→4 scaling, authenticated approval/execution, duplicate
  execute replay, per-container healthchecks, and a fresh SLO window. A spawned
  controller process is killed after the actual Compose scale but before the
  target response returns; the journal remains `RUNNING`. After lease expiry,
  a second process resolves the operation from observed state and commits with
  one durable dispatch. This proves one scale-up crash boundary, not every
  interruption point.
- Docker target timeout, health regression, successful compensation, and
  compensation failure remain NOT RUN. The checked-in
  Compose file still publishes fixed ports and remains single-replica; the
  adapter permits multiple replicas only after Compose confirms no published
  host ports. Docker's lock is host-local, not a fence against another host.
- PDB-safe replacement/eviction, bounded drain, partial-drain recovery, and
  V2 evacuation compensation remain unimplemented. The existing scale-up
  compensation foundation is not live-qualified. Prompt 3 must not start
  unless every mandatory Prompt 2B gate passes.

**Next executable action:** after `2026-10-10 18:29:54 UTC`, retry only the
explicitly scoped `kind-fleetright-safety-lab` API access. If TLS and RBAC
permit, perform bounded API-server validation of the updated workload CRD and
the currently supported monotonic scale-up path in an isolated namespace. This
cannot qualify destructive execution. Keep the V2 mutation path disabled; do
not create milestone 4 or start Prompt 3 while any mandatory gate remains
incomplete.

## 2026-10-08 Prompt 2B continuation — Checkpoint A result (historical)

**Current top-level prompt:** Prompt 2B of the original three-prompt plan.
Prompt 3 has not started. Older “Phase” labels below are historical checkpoint
notes, not new project phases.

**Checkpoint:** A — target-side execution authority and fencing.

**Checkpoint status:** PARTIAL. Only the fresh-object
`monotonic-scale-up-v1` FleetRightWorkload operation is admitted. Its durable
authorization and deterministic Pod creation were tested on the supplied
Kubernetes v1.37.0 safety-lab. `MOVE_REPLICA`, `DEACTIVATE_NODE`, Pod
Eviction, scale-down, terminal-Pod replacement, and node draining remain
disabled because their delayed effects cannot be shown safe under every
permitted ordering.

**Session start:** 2026-10-08 13:10:02 UTC.

**Branch:** `phase4/safe-execution-reconciliation`.

**Starting HEAD:** `2aabc871fb6aee31a5ea974089a2b14f6cd772d2`.
The verified continuation is recorded in the local commit; no push or
milestone tag was made.

### Implemented authority model

- The API execution target is `kubernetes-workload`, namespace
  `fleetright-v2`, and the explicitly named `cpu-api` FleetRightWorkload.
  Direct Kubernetes Deployment execution is rejected by application startup.
  The API executor Role grants only GET/PATCH on that named custom resource;
  it has no Pod, Eviction, Deployment, PDB, or Node mutation access.
- The existing FleetRightWorkload controller remains the only Pod creator.
  Its separate Role can get/list/create Pods and manage the matching PDB, but
  cannot delete Pods, create Evictions, patch the workload spec, or mutate
  Nodes. The Deployment uses `Recreate`; the supported protocol also tolerates
  concurrent old/new CREATEs through deterministic names.
- `executionProtocol: monotonic-scale-up-v1` must be present when a workload
  is created. CEL rejects in-place promotion of legacy objects, replica
  decreases, unbound replica increases, and changes to workload ID, template,
  placement, availability floor, disruption policy, or surge budget.
- One Kubernetes Merge Patch, guarded by the object's current
  `metadata.resourceVersion`, writes the durable plan/action identity,
  execution generation, expected/target replica counts, and raises
  `spec.replicas` in the same update. That value is the monotonic high-water
  value; no separate high-water field exists. A losing concurrent writer
  receives a Kubernetes 409. The
  generation uses the durable SQL execution fencing token. A new action is
  blocked until the previous action is `APPLIED`, the current generation is
  observed with `Reconciled=True`, and its replica count matches the journal.
- Before each create, the controller checks FleetRightWorkload UID and logical
  identity, deterministic ordinal, template fingerprint, owner UID, allowed
  node name and UID, and any existing Pod's identity/placement. It persists
  `RECONCILING` before Pod creation and `APPLIED` once every desired ordinal
  exists and is validated; this status does not assert readiness. The API
  reconciler separately gates action completion on Ready/Available observations
  and the configured workload health window. Ambiguous API outcomes remain
  `OUTCOME_UNKNOWN` in the durable execution journal; retries observe or adopt
  the deterministic slot before attempting a duplicate.

### Exact safety guarantee and limit

For a FleetRightWorkload created with this protocol and kept at one UID, every
accepted spec update preserves or raises the replica high-water mark and keeps
the execution identity and target in the same CAS-protected object update. The
workload shape and placement are immutable. A delayed Pod CREATE from an older
reconcile may still be accepted; it can only create a deterministic ordinal
that was within a previously authorized replica count. A newer reconcile uses
the same Pod name for that ordinal, and Kubernetes name uniqueness plus owner,
template, and placement validation prevents a second incompatible slot from
being adopted. New operations wait for `APPLIED`. This protocol makes those
CREATE orderings compatible; it does not atomically fence a separate Pod POST
or prove that only one controller process exists.

Kubernetes resourceVersion CAS serializes FleetRightWorkload authorization
writes only. Leases, Pod UIDs, PDB reads, and admission-webhook checks are not
claimed to fence separate Eviction requests. Kubernetes v1.37.0 accepted a
delayed same-UID unready-Pod Eviction under `IfHealthyBudget` after replacing
the caller's stale resourceVersion; the live negative control is preserved in
[execution-authority-race-2026-10-08.txt](evidence/execution-authority-race-2026-10-08.txt).
No Eviction or node-drain mutation is enabled. Scale-down remains prohibited,
so supported execution never needs to retire a Pod.

### Verification this continuation

| Gate | Result | Evidence |
|---|---|---|
| Full Python and PostgreSQL suite | PASS, 142 | `.venv/bin/python -m pytest -q python/tests`; isolated DB supplied by user |
| C++ regression | PASS, 36/36 | `ctest --test-dir build --output-on-failure` |
| Focused authority/controller tests after final preconditions | PASS, 55 | `python/tests/test_prompt2b_execution_targets.py python/tests/test_workload_controller.py` |
| PostgreSQL unknown-outcome cancellation | PASS | Journal remains `OUTCOME_UNKNOWN` until matching FleetRightWorkload `APPLIED` status, exact observed generation/count, and readiness |
| Ruff and `git diff --check` | PASS | Changed Python files; repeated after final edits |
| CRD schema/admission | PASS | Applied to Kubernetes v1.37.0; downscale, replica edit without request, and legacy protocol promotion rejected |
| Live scale-up/controller | PASS | Actual target client authorized 1→2 and 2→3; controller reported APPLIED and all 3 Pods Ready |
| Concurrent takeover | PASS | Two synchronized resourceVersion writes: one accepted, one 409 conflict; only winner persisted |
| Delayed stale authorization | PASS | Old Merge Patch sent after newer generation; v1.37 returned 409 and preserved newer token/count |
| Owner/placement/readiness/PDB | PASS | Three deterministic Pod IDs; each owner UID and approved Node UID verified; PDB owner/selector matched and `maxUnavailable=0` |
| Live RBAC | PASS | Executor may get/patch named `cpu-api` CR only; controller may create but not delete/evict Pods; status/PDB permissions as specified |
| Kind cleanup | PASS | Test namespaces, CRD, RBAC, Pods, and temporary image tags removed; prior image preserved |
| Secret scan | PASS, zero findings | `detect-secrets scan --no-verify --slim` over all changed files; `gitleaks` unavailable |

The controller initially timed out reaching the API Service from a worker,
consistent with prior evidence that worker kube-proxy Pods are unhealthy. For
the live test only, its Pod was pinned to the healthy control-plane Node with a
temporary cluster-side patch. This did not change the checked-in manifest or
host security settings; the temporary patch will be removed with test cleanup.

### Remaining Prompt 2B gates

- Keep Eviction, replacement, scale-down, and node drain disabled until a
  target operation with incompatible effects can be atomically fenced or a
  narrower protocol proves every request ordering safe. The v1.37 unhealthy-Pod
  behavior rules out the current delayed-Eviction approach.
- Qualify PDB migration and mixed-controller-version takeover before any
  PDB-aware retirement. An unmarked existing managed PDB fails closed; a
  trusted migration is not implemented. The monotonic scale-up guarantee does
  not claim a PDB eviction guarantee.
- Test the complete API approval/journal/takeover flow against the real
  workload target under controller crash and lost response, beyond the real
  target/CAS and existing PostgreSQL journal tests.
- Existing objects cannot be promoted. Provision new marked FleetRightWorkload
  objects and apply the restrictive RBAC before configuring
  `FLEETRIGHT_EXECUTION_TARGET=kubernetes-workload`.

### Next action

The live test namespace, CRD, controller RBAC, and temporary image have been
removed; prior images and host security settings were preserved. Changed-file
checks and evidence updates are complete, and this checkpoint is committed on
the current branch. Next action at that checkpoint: run the full API
approval/journal/takeover flow against
the real `FleetRightWorkload` target through a controller crash and lost
response. Keep Eviction, replacement, scale-down, and node drain disabled until
their request-order safety is proven. Do not start Prompt 3.

## Earlier checkpoint A record — before authority implementation (historical)

**Current top-level prompt:** Prompt 2B of the original three-prompt plan.
The older “Phase” labels below are historical checkpoint notes, not new project
phases. Prompt 3 has not started.

**Checkpoint:** A — target-side execution authority and fencing.

**Checkpoint status:** BLOCKED. The Deployment same-object replica-scaling
fence has real API-server race evidence, but the supported V2 Pod Eviction path
still has no final-mutation fence. V2 `MOVE_REPLICA`, `DEACTIVATE_NODE`,
`evict_one`, and `cordon` remain unwired or rejected. No milestone tag was
created.

**Session start:** 2026-10-08 13:10:02 UTC.

**Branch:** `phase4/safe-execution-reconciliation`.

**Starting HEAD / current HEAD:** `2aabc871fb6aee31a5ea974089a2b14f6cd772d2`.
Session changes are uncommitted.

**Checkpoint recorded:** 2026-10-08 13:55 UTC. No commit or new milestone tag
was created.

### Work and evidence

- Added a deterministic adapter race test: a delayed old Deployment scale JSON
  Patch loses to a newer fencing-token patch through resourceVersion CAS.
- Reproduced the race against the actual Kubernetes v1.37.0 API server in the
  dedicated safety-lab cluster. Token 101 remained on the test Deployment,
  replicas stayed 0, and the delayed token-100 request was rejected.
- In a separate isolated namespace, confirmed that a stale resourceVersion on a
  healthy Pod Eviction is rejected, while an unready Pod Eviction under
  `IfHealthyBudget` is accepted after that same Pod UID's resourceVersion
  changes. Exact UIDs, PDB status, and cleanup are in
  [execution-authority-race-2026-10-08.txt](evidence/execution-authority-race-2026-10-08.txt).
- Changed FleetRightWorkload reconciliation to preserve nonterminal Pods when
  live count exceeds desired replicas and report
  `ScaleDownRequiresStagedExecution`. `Ready` and `Available` reflect observed
  readiness while the new `Reconciled=False` condition and kubectl column show
  the blocked controller state. This closes its direct nonterminal Pod DELETE
  path until staged PDB-aware execution exists. Terminal cleanup remains
  UID-preconditioned and has a direct API request test. This supersedes the
  earlier ordinary nonterminal scale-down behavior described below.
- Added a workload-generation high-water annotation to newly created managed
  PDBs. Current controller instances skip lower-generation snapshots. An
  unmarked existing PDB now fails closed with `PDBGenerationUnknown`: its
  generation cannot be inferred from a possibly stale workload snapshot.
  Updating it requires an explicit trusted migration; mixed-version controller
  rollout is still unproven.
- Added a deterministic stale-read race test: a PDB update carrying an old
  resourceVersion conflicts after a newer generation has written the PDB.
- Updated ADR-008 with the live Eviction result. The existing Kubernetes target
  docstring now says eviction and cordon helpers are not execution-admitted.

### Verification

| Gate | Result | Evidence |
|---|---|---|
| Focused controller/execution/planner regressions | PASS, 46 | `.venv/bin/python -m pytest -q python/tests/test_workload_controller.py python/tests/test_prompt2b_execution_targets.py python/tests/test_evacuation_v2.py` |
| Ruff check and format | PASS | Four changed Python files |
| Python compileall | PASS | Four changed Python files |
| CRD YAML and printer column | PASS | Parsed; `Reconciled` column present |
| Live Deployment fencing race | PASS for `SET_REPLICA_COUNT` only | API server v1.37.0; no V2 claim |
| Live Eviction stale-resourceVersion control | PASS as a negative safety control | Healthy stale request rejected; unready stale request accepted and source UID absent |
| Temporary namespace cleanup | PASS | All four labeled test namespaces confirmed absent |
| Final read-only cluster recheck | BLOCKED | Local sandbox denied the safety-lab API socket; no further cluster mutations were attempted |
| PostgreSQL/Docker | NOT RUN | Isolated DB credential remains unrotated; Docker socket unavailable |
| Durable V2 Pod eviction, node drain, compensation | NOT RUN / disabled | No V2 execution authority exists |

### Remaining safety gaps

- No single target-side authority currently owns V2 Pod mutations. A Kubernetes
  claim object or PostgreSQL lease alone cannot fence the Eviction request.
- Kubernetes v1.37.0's unready-Pod Eviction branch can replace a caller's
  resourceVersion; UID protects against a different Pod under the same name,
  not against a delayed request for the same Pod UID. That branch also bypasses
  the PDB `disruptedPods` bookkeeping, so takeover cannot use it to enumerate
  accepted in-flight requests.
- The PDB generation guard is not understood by older controller binaries.
  An old binary overlapping rollout can read a fresh PDB resourceVersion and
  still patch a stale budget and retains the prior nonterminal Pod deletion
  path. Mixed-version takeover is unproven and must be resolved before enabling
  drains or staged eviction.
- Existing PDBs without the generation marker block workload reconciliation.
  Stamping one from the current snapshot was rejected after review because
  that snapshot may be stale; a trusted migration procedure is not implemented.
- A lower-generation snapshot skips only the PDB write; normal reconciliation
  can still create Pods from its stale desired replica count. This controller
  has no target-side admission fence for Pod creation, so V2 execution remains
  disabled.
- No durable V2 replacement identity/state machine, crash recovery, partial
  drain, or live compensation is implemented here.

### Next executable action

Design and test a target-side mutation protocol whose admitted effects remain
safe if an Eviction request is delayed across takeover. Keep V2 execution
rejected while the same-UID unready-Pod path is not safely bounded. Do not add an
authority claim record unless it is tied to the actual mutation guarantee.

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
