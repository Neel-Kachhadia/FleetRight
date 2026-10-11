# V2 Execution Progress

## 2026-10-11 Prompt 2B — replacement and eviction safety checkpoint (incomplete)

**Execution window:** began approximately 11:21 UTC; six-hour hard stop is
approximately 17:21 UTC. Checkpoint recorded around 14:55 UTC; engineering is
continuing. **Branch:** `phase4/safe-execution-reconciliation`. **Starting
SHA:** `b2345a05bbffa6979ebaacc0594ea17c8c9c88a0`. Prompt 3 has not started.
Milestone 4 is not qualified; no tag or push was made.

### 15:16 UTC follow-up — compensation recovery verification

The recovery completion barrier now binds compensation to the journaled
workload UID and operation, current target fence, exact authorized Pod CREATE
records, live Pod owner/labels/annotations/readiness, recovery destination
Node UIDs/readiness, and the plan's topology rule. Empty eligible topology
domains count toward skew. A stale workload or Node identity moves the plan to
`FAILED_SAFE`; a transient target API error leaves reconciliation pending.
Approved compensation capacity remains bounded by the V2 live-replica and
temporary-surge limits.

Focused evidence: target tests **70 passed**; PostgreSQL journal tests **42
passed**; the journal compensation test covers both verified completion and
`TargetPreconditionFailed` settling to `FAILED_SAFE`. Ruff, formatting,
compilation, and `git diff --check` passed. The live Kind replacement/PDB suite
passed **2/2 in 178.38s** after adding an actual controller-authorized recovery
scale-up and target-side observation check. This live addition calls the
Kubernetes target directly; it does not claim authenticated API-to-PostgreSQL
compensation E2E or a real Python controller takeover.

### Implemented and connected

- The V2 `PREPARE_SOURCE_RETIREMENT` action stays on the authenticated API,
  PostgreSQL journal, `ExecutionController`, and `FleetRightWorkloadTarget`
  path. The target publishes durable staged replacement intent, waits for a
  controller-authorized Pod, checks its observed Pod UID, Node UID, Ready
  state, warm-up, workload readiness, topology and PDB, then writes the
  source-UID tombstone before reporting readiness for eviction.
- Pod CREATE admission now claims the immutable pending-create request UID in
  FleetRightWorkload status before allowing that CREATE. The validating
  webhook is installed fail closed in the dedicated lab. Unresolved creates
  and legacy owner history remain retirement blockers.
- Policy/v1 Eviction admission validates executor identity, current workload
  request/fence, durable source/replacement identity, exact source Pod UID and
  resourceVersion, Node UIDs, replacement readiness, approved placement and
  the controller-owned PDB. The target does not retry an uncertain eviction
  until observation determines the prior request is safe to retry.
- A journal retry bug is fixed: `PDB_BLOCKED` and `PDB_STATUS_PENDING` results
  without a persisted retirement tombstone re-enter replacement preparation;
  the same stages with a tombstone reconcile as possible eviction outcomes.

### Verification

| Check | Result | Evidence / limit |
|---|---|---|
| Python and PostgreSQL suite | PASS, 364 passed / 6 skipped | Full `python/tests`; isolated `fleetright_test` DB, one existing Starlette TestClient deprecation warning. The six skips are optional environment-gated Docker/Kind cases; the dedicated live test below was run separately. |
| C++ / CTest | PASS, 38/38 | `ctest --test-dir build --output-on-failure -j2`. |
| Ruff, formatting, compilation, shell syntax, diff | PASS | `ruff check python`, changed-file format check, `compileall`, installer `bash -n`, and `git diff --check`. |
| Live replacement and PDB cases | PASS, 2/2 in 178.38s | Kubernetes 1.37.0 created a real controller-owned replacement with observed Pod/Node UIDs. The permitted case completed policy/v1 Eviction; the blocked case returned `PDB_BLOCKED` with no tombstone or source deletion. A lost Eviction response was reconciled to source removal and two Ready replicas. The permitted case also exercised a bounded recovery scale-up and actual Pod/Node/topology verification. |
| Live PDB-blocked replacement | PASS, target-level | Managed PDB with `maxUnavailable: 0` returned `PDB_BLOCKED`; no source tombstone or deletion was observed. |
| Delayed stale Eviction dispatch after fence takeover | PASS, target-level | Controller A paused after target prechecks and before sending its Eviction. A target using fence 14 took over; the live validating webhook rejected A's request carrying fence 13. The current target then evicted that same source. This is not a real two-process `ExecutionController` takeover test. |
| Workload-controller interruption/restart | PASS, limited live recovery | After replacement readiness and source tombstone, the dedicated controller Pod was terminated and replaced by a new Ready Pod UID. The observed replacement UID/create record remained unique and authorized, no CREATE stayed pending, and workload readiness recovered. Repeated after Eviction acceptance with its response lost. This does not test Python `ExecutionController` process takeover. |
| Admission webhook installation | PASS | Installer completed against only `kind-fleetright-safety-lab`; fail-closed webhook rejected a dry-run Pod probe. |
| Secret scan | PASS with one reviewed false positive | `detect-secrets` found only the Kubernetes `secretName: fleetright-pod-admission-tls` reference; the manifest contains no credential value. |

### Remaining qualification gates

| Requirement | Result | Evidence / next work |
|---|---|---|
| Live API -> PostgreSQL -> controller -> Kubernetes evacuation | BLOCKED / NOT RUN | API/journal path passes deterministic integration tests, and live target path passes separately. The only pre-existing `cpu-api` in the lab is a legacy FWW; it was left untouched. No end-to-end live plan was fabricated. |
| Delayed CREATE after takeover on live API server | NOT RUN | Unit adversarial tests still prove pending CREATE blocks retirement and late legacy Pods remain unverified. Need a live held CREATE/admission response test. |
| Already-admitted Eviction response delayed across takeover | NOT RUN | Live coverage proves a stale request not yet admitted is fenced and separately reconciles a lost response after accepted Eviction. It does not prove an already-admitted request held across a real process takeover. |
| Real process crash and controller takeover | PASS, limited / BLOCKED | Live Kubernetes workload-controller Pod interruption and restart pass after replacement preparation and after an uncertain Eviction. PostgreSQL journal takeover tests pass. A real two-process Python `ExecutionController` takeover against PostgreSQL/Kubernetes remains NOT RUN. |
| Multi-replica or node drain, partial-drain recovery | DISABLED / NOT IMPLEMENTED | V2 deactivation and multi-action drain remain rejected. No Pod deletion or Node termination path was added. |
| Kubernetes health-regression compensation | BLOCKED / NOT QUALIFIED | Bounded target-side capacity recovery and final Pod/Node/topology verification now pass live. The real health-regression trigger and compensation commit through authenticated API/PostgreSQL journal remain NOT RUN. |
| Kubernetes legacy owner policy | PASS, fail closed | Legacy CREATE history remains uncertain and target rejects it before mutation; new workload UIDs require the reserved-create and admission protocols. |
| Milestone 4 | INCOMPLETE | Live API/journal E2E, delayed CREATE, admitted-request takeover/crash recovery, bounded drain and Kubernetes compensation remain unqualified. No milestone tag is created. |

The live safety test created and deleted only uniquely named temporary objects
in `fleetright-v2`. The existing legacy `cpu-api`, other namespaces, clusters,
and database objects were not modified. Continue work before the hard deadline;
keep DRAIN_NODE, DEACTIVATE_NODE, scale-down and Kubernetes compensation
disabled unless their required evidence passes.

### 15:30 UTC follow-up — new workload admission and live-plan constraint

`FLEETRIGHT_KUBERNETES_WORKLOADS` now enrolls additional DNS-label workload IDs
for Kubernetes execution; the default remains `cpu-api`, duplicate/invalid
names are rejected, and new IDs use a conservative 120 ms / 1,000 ppm health
ceiling. Unit and PostgreSQL API tests cover the allowlist and bounded policy.

The API-to-Kubernetes live preparation test now requires five Ready Nodes and
reports an explicit skip when the lab is smaller. On the current three-Node
lab, the attempted plan was rejected before execution: the independent V2
planner reported `fewer than two active healthy destination nodes`. The
test-created unique FleetRightWorkload was removed during cleanup; the legacy
`cpu-api` object was untouched. This live API/journal/controller gate remains
**BLOCKED**, and the skip is not qualification evidence. A five-Node run is
still needed to bind the new-workload allowlist to an observed replacement UID
through the production path.

After this follow-up, changed-file Ruff check/format, Python compilation, and
`git diff --check` passed. The explicit live test result was `1 skipped` on
this lab. Existing direct-target live replacement/PDB evidence remains as
recorded above; this does not establish API-to-Kubernetes completion.

## 2026-10-11 Prompt 2B — journaled source-retirement preparation gate

**Session start:** 10:35:07 UTC. **Starting SHA:**
`a484abf22cdfcd36ddb609b8cd7be0922443c074` on
`phase4/safe-execution-reconciliation`. Prompt 3 and milestone 4 were not
started. No push or destructive Kubernetes action was performed. Existing
untracked `graphify-out/` was preserved.

### Production path added

- A verified V2 plan with exactly one supported `FleetRightWorkload`
  evacuation now produces one `PREPARE_SOURCE_RETIREMENT` journal intent.
  V2 deactivation and eviction actions are not emitted. Approval and execution
  re-verify the plan; approval binds the live workload UID and generation to
  the verified operation.
- `/v1/plans/{plan_id}/execute` -> `ExecutionController._start_action` ->
  `FleetRightWorkloadTarget.prepare_source_retirement` is the production call
  path. The controller records `RETIREMENT_REQUESTED` in the PostgreSQL action
  journal before the target call. Approval and execution bind the journal
  payload field-for-field to the independently reverified V2 operation.
- The target checks workload UID/generation, CREATE protocol and reservations,
  tombstones, and the live source Pod UID/owner/slot plus Node UID. The current
  V2 action has no observed replacement Pod UID, so it blocks at
  `VALIDATE_REPLACEMENT` with `REPLACEMENT_POD_IDENTITY_UNAVAILABLE`. Even with
  a replacement identity, the target retains the separate
  `POD_CREATE_PERSISTENCE_FENCE_UNPROVEN` block before any tombstone write.
- A target regression supplies a stable replacement Pod UID and confirms the
  production adapter still blocks at `POD_CREATE_PERSISTENCE_FENCE_UNPROVEN`.
- Read-only retirement actions recover after takeover: the new controller
  records a `no_target_mutation` receipt, rechecks the workload, and resumes the
  gate. Mutating target adapters are not takeover-qualified. Pause/cancel
  settles a read-only pending action before entering the requested state.
- The gate runs before any Kubernetes write. This path does not call
  `_write_retired_slot_tombstone`, does not report `READY_FOR_SAFE_EVICTION`,
  and cannot make an ordinary controller stop healing a desired slot.
  Tombstone integration remains BLOCKED; no retirement intent was persisted in
  the workload object.

### Delayed CREATE evidence and result

- The preserved executable counterexample remains
  `test_pre_upgrade_inflight_create_can_recreate_a_retired_slot`. It holds an
  unreserved Pod CREATE after admission, writes the tombstone, removes the
  source, and then allows the old request to create a new Pod UID. Reconcile
  marks the Pod unverified and `BLOCKED`; it does not count it as ready
  capacity. A workload status resourceVersion CAS cannot prove this request
  has finished.
- `test_retirement_and_create_reservation_race_uses_status_cas` still tests
  both winners for participating reservation/tombstone writes. That is not a
  persistence fence and does not override the delayed-request counterexample.
- The new-protocol barrier test holds a CREATE after its durable reservation,
  has the takeover attempt retirement, and proves retirement is rejected
  while the reservation is pending. After release, the Pod authorization stays
  bound to the original scale-up operation; no retirement tombstone or
  eviction occurs. The separate legacy regression still proves a late Pod is
  unverified after an unsafe tombstone/source-removal sequence.
- The real current `fleetright-v2/cpu-api` is legacy: UID
  `48fdf658-8ed9-4c42-9016-e345bfe37ef5`, generation 5, no
  `podCreateProtocol`. The live target rejected it with
  `FleetRightWorkload Pod CREATE history is uncertain` before any mutation.
- Read-only lab inventory captured five `cpu-api` Pods, owned by that legacy
  workload UID: `r0` `f3db00c3-5ec2-4ba0-b4d7-03e8776c915d`, `r1`
  `d1cc449e-3249-4933-b500-b1e3bf6d1824`, `r2`
  `7f58a2d5-b3ca-46b1-acb3-8f96d9802e4f`, `r3`
  `3a9a9932-6da3-420e-92cf-8252751f9cee`, `r4`
  `5a5f847a-c8fd-417b-8b78-e3a94e12c1f0`. They remained on workers 1/2;
  no live writes were made. The new read-only Pod/Node RBAC rules were not
  applied to the lab.
- No supported live workload reached a successful preparation state. The
  end-to-end API/PostgreSQL/controller test uses the production
  `FleetRightWorkloadTarget` with deterministic Kubernetes reads and fails
  closed at `VALIDATE_REPLACEMENT`. No success is fabricated.

### Verification

| Check | Result | Evidence / limit |
|---|---|---|
| Affected Python suites | PASS, 186 passed / 4 skipped | Workload controller, execution targets, V2 planner, PostgreSQL journal and control API. Four optional E2E tests skipped; one existing Starlette deprecation warning. |
| PostgreSQL journal and API | PASS | Full affected suite ran against the authenticated local PostgreSQL test DB; includes failed-safe admission, duplicate execute, unknown outcome, crash/takeover, and pause recovery. |
| Production API -> journal -> controller -> target | PASS, fail-closed | V2 plan action matches reverified artifacts; live-source identity reads pass in the deterministic adapter, missing replacement UID is recorded as `VALIDATE_REPLACEMENT`, and plan ends `FAILED_SAFE`. This is not a successful retirement preparation. |
| Original delayed CREATE race | PASS as a hazard reproduction | Late Pod UID remains unverified and does not count as Ready capacity. |
| Ruff, format, Python compilation, diff check | PASS | Changed Python files; full repository compile and final diff check recorded at commit. |
| Safety-lab health | PASS | 3 Nodes Ready, kube-proxy 3/3/3, authenticated PostgreSQL `SELECT 1` returned 1. |
| Live workload admission probe | BLOCKED | The only existing candidate workload is legacy and was rejected without mutation. |
| Live kind SCALE_UP / retirement E2E | NOT RUN | The dedicated least-privilege executor kubeconfig and health URL are unavailable; the provided admin kubeconfig was used only for read-only checks. |

### Prompt 2B gate decision

| Requirement | Result | Evidence |
|---|---|---|
| Retirement admission into `READY_FOR_SAFE_EVICTION` | BLOCKED | Production API/controller action is wired, but the V2 operation does not carry an observed replacement Pod UID; the target then also requires a CREATE persistence fence. |
| CREATE/retirement serialization | UNPROVEN | A pending new-protocol CREATE blocks admission; a late completion remains bound to its original operation. The legacy/stale-request counterexample creates a different UID after tombstone/source absence. No enforceable storage fence is proven. |
| Tombstone integration in production admission | BLOCKED | The production caller deliberately does not invoke the tombstone helper while the CREATE persistence fence is unproven. |
| Crash/takeover state preservation | PASS, read-only path | Operation identity and `OUTCOME_UNKNOWN` survive; a deterministic crash-before-target test recovers the read-only gate, and pause settles without dispatch. No mutating retirement adapter is qualified. |
| Live API/PostgreSQL/controller/kind transition | NOT RUN | No supported live workload and no separate executor kubeconfig/health endpoint. |
| Source eviction | DISABLED | No PDB eviction, Pod delete, scale-down, drain, deactivation or destructive compensation was enabled. |

**Next exact critical task:** add the missing journaled surge/readiness stage
that captures a verified replacement Pod UID, then prove an API-server
persistence fence for every Pod CREATE writer before writing a retirement
tombstone. Keep eviction disabled until its separate authority and PDB gate is
proven.

---

## Historical checkpoint before Prompt 2B production wiring

**Scope:** Prompt 2B only. Prompt 3 has not started. No destructive action was
enabled. Session started at 10:10:39 UTC from
`phase4/safe-execution-reconciliation` at `7bbb80561725748aec0364fc15a86dc236feb0a0`.

**Decision:** retirement execution remains BLOCKED. The repository has no
production source-retirement admission caller for
`_write_retired_slot_tombstone`; API admission rejects MOVE/DEACTIVATE/V2 and
Kubernetes scale-down, and the workload target rejects every downscale. The
helper is durable intent persistence only. A CR status CAS cannot cancel or
fence a Pod CREATE already admitted separately by the Kubernetes API server.

### Changes and limits

- Tombstone replay now re-reads the live source Pod and checks its exact UID,
  owner/slot identity and approved scheduling. A changed UID rejects replay.
  If the source has disappeared and the exact durable tombstone already exists,
  replay is idempotent; it never clears the tombstone.
- The helper rejects legacy/unmarked workload UIDs, unresolved
  `pendingPodCreate`, sticky `podCreateHistoryUncertain`, and changed source
  identity before appending a tombstone. New focused tests cover those checks
  and both winners in a deterministic reservation-versus-tombstone resource
  version race. This proves status CAS conflict behavior in the test adapter;
  it does not create a production retirement admission path.
- New-protocol CREATE reservations and retirement intent share the workload
  status resourceVersion, so participating writers cannot both commit from the
  same version. This does not prove that old controller binaries or an already
  admitted request are fenced. Lost responses and lease expiry do not cancel
  the request.
- Legacy UIDs stay uncertain across reconcile/takeover and are rejected. The
  preserved delayed-CREATE reproduction still creates a late Pod after source
  removal; reconcile marks it unverified and BLOCKED. A Pod LIST or temporary
  absence is not treated as proof that no request remains in flight.
- The tombstone record binds workload UID, logical slot, source Pod UID,
  generation, plan ID and operation ID. There is no production gate here to
  verify an approved operation, current plan, replacement UID, source Node
  identity, capacity or placement as one retirement authorization. Therefore
  this record is not retirement authority and cannot report source exclusion.

### Verification

| Check | Result | Evidence / limit |
|---|---|---|
| Focused workload-controller and execution-target tests | PASS, 107 | `python/tests/test_workload_controller.py` and `python/tests/test_prompt2b_execution_targets.py`; includes pending/legacy rejection, CAS race, source UID replay and monotonic scale-up coverage. |
| Original delayed CREATE reproduction | PASS, 1 | `test_pre_upgrade_inflight_create_can_recreate_a_retired_slot` remains a passing hazard reproduction: it demonstrates late stale Pod creation and BLOCKED classification, not safe retirement. |
| PostgreSQL journal integration | PASS, 34 | `python/tests/test_prompt2b_journal.py`, authenticated isolated `fleetright_test` database. Initial environment check also confirmed DB health and authenticated `SELECT 1`. |
| Control API and execution admission | PASS, 25 passed / 4 skipped | `python/tests/test_prompt2a_control_api.py`; skips require optional live Docker/Kind E2E setup. |
| Real Kubernetes downscale admission probe | PASS, rejected | Against `fleetright-safety-lab`, `fleetright-v2/cpu-api` rejected with `FleetRightWorkload Pod CREATE history is uncertain`; resourceVersion remained `37141`. Probe was read-only and made no mutation. |
| Existing Kind live SCALE_UP E2E | NOT RUN | The focused test skipped because `FLEETRIGHT_RUN_KIND_E2E=1` and the separate executor kubeconfig/controller context and health URL are unavailable. No executor credentials were created. |
| Live pre-admitted CREATE injection | NOT RUN | The deterministic regression uses a barrier; no API-server request was held after admission in this session. |
| Ruff check | PASS | `.venv/bin/python -m ruff check python`. |
| Ruff format | PARTIAL | All three changed Python files pass format check. Repository-wide check reports only untouched `experiment.py`, `reconciler.py` and `test_prompt2b_journal.py`. |
| Python compilation / diff | PASS | `compileall -q python`; `git diff --check`. |

### Gate decision

| Requirement | Result | Evidence |
|---|---|---|
| Tombstone integrated into real retirement admission | BLOCKED | No production caller or supported retirement transition exists. |
| Pending CREATE blocks unsafe retirement | PASS, fail-closed | Helper and target tests reject unresolved CREATE; downscale remains disabled. |
| Legacy uncertainty blocks unsafe retirement | PASS, fail-closed | Missing protocol is sticky uncertainty; live target rejects the legacy workload. |
| Concurrent admission is serialized | PASS, limited | Deterministic shared-resourceVersion race permits one status write; no real retirement admission exists. |
| Crash/takeover retains uncertainty and tombstone | PASS | Durable status survives retry/reconcile; tombstone is append-only. |
| Late Pods are classified correctly | PASS, test-only injection | Reproduction classifies the late Pod as unverified and blocks completion. |
| Source identity is revalidated | PASS, limited | Helper replay checks source Pod UID; source Node and replacement identity are not bound by an admission path. |
| No false source-exclusion completion | PASS, fail-closed | No destructive completion path exists; unexpected Pod remains unverified and BLOCKED. |
| Existing SCALE_UP remains functional | PASS, unit/integration | Focused controller/target and PostgreSQL/control API suites pass; live Kind E2E was not run. |
| Evidence is committed | PASS | Helper and tests: `eb9e0df`; this audit is committed separately. |

**RETIREMENT ADMISSION FAIL-CLOSED: PASS** — current execution policy and the
live target reject retirement/downscale. **Tombstone integration: BLOCKED** —
no production admission path exists. **RETIREMENT EXECUTION QUALIFICATION:
BLOCKED / NOT QUALIFIED.** No eviction, drain, deactivation, scale-down or
destructive compensation is enabled.

**Next critical task:** add a versioned source-bound retirement admission at a
real execution boundary only after all Pod CREATE writers, including old
controllers and admitted requests, can be fenced by a proven API-server-level
mechanism. Bind verified plan/operation, source Pod and Node UIDs, replacement
UID, generation and capacity/placement checks there. If that cross-system
fence cannot be proven, keep retirement disabled.

## 2026-10-11 Prompt 2B — delayed CREATE retirement boundary

**Scope:** Resolve stale Pod CREATE classification and durable slot exclusion.
Prompt 3 has not started. Destructive V2 actions remain disabled.

**Session:** started about 09:20 UTC; feature commit at about 10:04 UTC (~44 minutes).

**Branch / starting HEAD:** `phase4/safe-execution-reconciliation` /
`06c67c406bfe1f9bfb12324d70e0804eebbc3ccf`. The pre-existing untracked
`graphify-out/` is preserved.

**Feature commit:** `47e141763129722ccb16becf35630bb6f444e781`.

### Implemented

- New workload UIDs must carry immutable `podCreateProtocol:
  reserved-create-v1`. Legacy UIDs without it remain explicitly uncertain and
  are rejected by workload execution admission; they require a new workload UID.
- Each Pod CREATE now has a fresh attempt ID. The durable intent and authorized
  Pod UID record bind workload UID, Pod UID/name, logical slot, generation,
  plan, operation, and attempt. An old duplicate cannot match a later retry.
- Append-only authorization and retired-slot status are guarded by CRD CEL.
  Retired slots bind source Pod UID and operation identity. Ordinary reconcile
  cannot recreate a retired desired slot or report it reconciled; a different
  UID in that slot is unverified, contributes to surge accounting, and sets
  sticky uncertainty. The controller writes uncertainty and `BLOCKED` together.
- The execution target rejects legacy owners, pending CREATEs, uncertain or
  unverified Pods, and any retired-slot history. No eviction or other
  destructive action was added or enabled.

### Verification

| Gate | Result | Evidence |
|---|---|---|
| Focused controller/target regressions | PASS, 101 | `.venv/bin/python -m pytest -q python/tests/test_workload_controller.py python/tests/test_prompt2b_execution_targets.py` |
| Full Python/PostgreSQL/Docker suite | PASS, 205 passed, 1 skipped, 1 warning | Isolated test DB, `FLEETRIGHT_RUN_DOCKER_E2E=1`; traceback output suppressed. Existing journal takeover coverage ran. |
| Ruff / format / compile / diff | PASS | Four changed Python files; Python compileall and `git diff --check`. |
| CRD server dry-run and apply | PASS | Safety-lab API accepted the CEL schema after replacing quadratic append checks with map-list merge equality. |
| Live first status write and tombstone append | PASS | Isolated `fleetright-retirement-test` namespace; new workload status and tombstone accepted. |
| Live immutable retirement / sticky uncertainty checks | PASS | API rejected tombstone removal and clearing `podCreateHistoryUncertain`. |
| Live stale Pod observation / restart | PASS, classification only | API-assigned Pod UID scheduled on `fleetright-safety-lab-worker`; controller reported 1 unverified Pod, 0 ready, 1 surge, false Reconciled. After isolated controller restart, the tombstone and uncertainty remained. |
| Live already-admitted delayed CREATE injection | NOT RUN | The live test created an old-identity Pod after tombstoning; it did not emulate an API request already admitted before retirement. The deterministic barrier test retains that boundary. |
| PostgreSQL restart of the tombstone | NOT RUN | Retirement state lives in Kubernetes status; live controller restart was tested. The full PostgreSQL journal suite passed separately. |
| C++ | NOT RUN | No C++ contract or source changed. |

### Adversarial case classification

| Case | Result | Evidence / limit |
|---|---|---|
| 1–7: delayed CREATE, authority loss/takeover, stale identity and UID conflicts, concurrent status CAS, unknown outcome restart | PASS | Deterministic barriers, status-response loss, and controller restart/takeover regressions. |
| 8: retirement state across PostgreSQL restart | NOT RUN | Tombstones are stored in Kubernetes status; the live Kubernetes controller restart was tested, but no PostgreSQL restart was paired with this tombstone. |
| 9: ordinary reconcile cannot restore a retired slot | PASS | Tombstone takeover regression leaves the slot absent and blocked. |
| 10: no destructive admission after stale Pod disappearance | PASS, admission guard only | Sticky uncertainty and retired history block target fencing after the Pod disappears. Eviction itself remains disabled and was not invoked. |
| 11: unresolved legacy/pre-upgrade uncertainty | PASS | Legacy owner UID is durably marked uncertain and rejected by execution admission. |
| 12: monotonic SCALE_UP | PASS | Existing Python controller/target and PostgreSQL/Docker regressions passed; the shared legacy Kind workload remains intentionally ineligible. |

### Safety limit

The preserved `test_pre_upgrade_inflight_create_can_recreate_a_retired_slot`
still proves that Kubernetes may materialize an old request after source-slot
removal. The new controller classifies that Pod as unverified and blocks later
execution; a legacy workload UID is blocked before destructive admission. This
does not cancel the already admitted request or prove a source eviction safe.
The tombstone helper is not connected to an enabled retirement/eviction path.
Keep `MOVE_REPLICA`, `EVICT_SOURCE`, `DRAIN_NODE`, destructive scale-down, and
destructive compensation disabled. Do not claim Prompt 2B complete.

**Current result:** `RETIREMENT SAFETY GATE: BLOCKED` pending a defensible
legacy-request boundary and a separately qualified retirement/eviction path.
No push or milestone tag was created.

**Next exact task:** integrate durable retirement intent into a future staged
source-retirement path and prove it cannot proceed for a legacy or unresolved
CREATE; then qualify delayed Eviction fencing independently before enabling
any destructive operation.

## 2026-10-11 Prompt 2B continuation — partial CREATE intent barrier

**Scope:** Prompt 2B only. Prompt 3 has not started.

**Session start:** 2026-10-11 08:16:37 UTC; target finish 13:46:37 UTC;
absolute hard stop 14:16:37 UTC.

**Branch / starting HEAD:** `phase4/safe-execution-reconciliation` /
`33c2870ac57bbca703c172d13937c849b75a05e9`. Tracked worktree was clean at
start; pre-existing untracked `graphify-out/` was preserved. No push or
milestone-4 tag was made.

**Local implementation checkpoints:** `6c49823` rejects duplicate V2 Node
UIDs; `6472277` adds the pending-CREATE intent protocol and this session's
evidence. Neither commit was pushed.

**Decision:** INCOMPLETE. The current worktree adds a bounded pending-Pod-CREATE
barrier for new controller requests and rejects duplicate Node UIDs in V2
snapshots. It does not fence a pre-upgrade request already admitted by
Kubernetes and does not implement retired-slot tombstones, staged replacement,
Eviction, node drain, or destructive compensation. Keep destructive V2 actions
disabled.

### Implementation and evidence

| Check | Result | Evidence |
|---|---|---|
| V2 identity | PASS | Existing owner-contract v1 remains in use; the planner now rejects duplicate Node UIDs. `EvacuationV2Test.RejectsDuplicateNodeUids` plus the existing owner-drift tests pass. |
| New controller CREATE protocol | PASS, unit-level | `status.pendingPodCreate` binds workload UID, generation, deterministic Pod name, and logical slot. The controller reserves it before POST, retains it on unknown outcomes, observes the exact owner/slot/generation and approved placement after restart, and clears it only after known resolution. Tests cover delayed response, lost reservation response, lost CREATE response, CAS conflict, wrong generation, and definitive rejection. |
| Legacy delayed CREATE after slot removal | FAIL, reproduced | `test_pre_upgrade_inflight_create_can_recreate_a_retired_slot` models a CREATE admitted before the marker protocol; after takeover and source removal it creates a new Pod UID whose allowed placement still includes the source Node. This is a passing reproduction of the unsafe behavior, not a safety pass. |
| Target/CRD guard | PASS, unit and live API-server transition check | Target preflight requires the pending marker schema and root CEL rule; target fence/authorization reject a pending marker. The additive CRD is installed in the safety lab and the updated controller is deployed 1/1. A temporary workload in unwatched `default` accepted a pending marker, and a valid scale-up spec update was rejected with the CEL message; the marker was cleared and the probe deleted. |
| Authenticated Kind API E2E | PASS, 1 passed (80.61s) | `/tmp/fleetright-kind-e2e-final.log`; live run of `test_kubernetes_workload_api_recovers_after_ready_scale_up_crash` against `fleetright-v2/cpu-api`, real PostgreSQL schema, authenticated optimize/verify/approve/execute and idempotent replay, 4→5 scale-up, primary process kill after mutation while journal state was RUNNING, 30-second lease expiry, and recovery controller commit from observed Kubernetes state with one durable dispatch. The isolated test DB sequence was seeded from the existing target fence so its new schema did not issue a stale token. |
| Python + PostgreSQL + Docker E2E | PASS, 185 passed, 1 skipped, 1 warning (117.02s) | `FLEETRIGHT_RUN_DOCKER_E2E=1 .venv/bin/python -m pytest -q python/tests`; captured at `/tmp/fleetright-prompt2b-final-pytest.log`. Real isolated `fleetright_test` PostgreSQL; opt-in Docker process-crash, unknown-response and health-regression cases ran. The remaining aggregate skip is the opt-in Kind case, which passed separately above. |
| C++ | PASS, 38/38 | `cmake --build build --target fleetright_cpp_tests fleetright-plan fleetright-verify -j2`; `ctest --test-dir build --output-on-failure`. |
| Ruff / compile / diff | PASS, changed files | `ruff check python`, changed-file `ruff format --check`, Python `compileall`, and `git diff --check` pass. Repository-wide format check still reports three untouched files: `experiment.py`, `reconciler.py`, and `test_prompt2b_journal.py`. |
| Safety-lab read | PASS | After E2E: 3/3 Nodes Ready, controller 1/1, `cpu-api` 5/5 Ready, execution phase APPLIED, no pending Pod-create marker. The temporary short-lived executor kubeconfig and localhost-only port-forward were removed. |

The reservation protocol is intentionally limited. The CRD root validation
prevents spec changes while a new, journaled CREATE is unresolved. It cannot
cancel an old admitted request that has no marker; an unresolved marker may
block reconciliation indefinitely. This does not prove source retirement or
make eviction safe. The E2E fence-sequence seed is test-fixture setup for an
ephemeral PostgreSQL schema against a persistent lab object; production fencing
logic was not changed.

### Current operation boundary

| Operation | State |
|---|---|
| `SCALE_UP` | ENABLED for existing monotonic Kubernetes and supported Docker paths; Docker regressions and live Kind API→PostgreSQL scale-up/takeover passed in this session. |
| `SCALE_DOWN` | Docker admission remains evidence-gated; Kubernetes DISABLED. No new live scale-down qualification. |
| `MOVE_REPLICA` | DISABLED |
| `EVICT_SOURCE` | DISABLED |
| `DEACTIVATE_NODE` | UNSUPPORTED / rejected |
| `DRAIN_NODE` | DISABLED |
| destructive `COMPENSATE` | DISABLED / not implemented |

### Prompt 2B completion gates

| Gate | State | Evidence or limit |
|---|---|---|
| V2 owner contract consistent | PASS | Python API, C++ planner/verifier and owner-drift tests bind the managed owner identity. |
| Independent verifier correct | PASS | CTest 38/38, including tamper, owner drift, capacity and topology checks. |
| Stale-plan rejection | PASS | Existing plan-binding, expiry and changed-approval-binding tests pass. |
| Durable source retirement | FAIL | No retired-slot tombstone or staged retirement journal exists. |
| Delayed CREATE safety | FAIL | `test_pre_upgrade_inflight_create_can_recreate_a_retired_slot` reproduces the stale request after takeover. New requests are reserved, but the old admitted request has no marker. |
| Replacement identity correctness | BLOCKED | Planner binds V2 source identity; execution has no staged replacement operation. |
| Allowed destination enforcement | PASS, scale-up only | Controller checks Pod placement against approved Node name/UID; controller unit tests pass. |
| Resource and surge constraints | BLOCKED | Planner/verifier check them, but no runtime staged replacement path consumes that approval. |
| Destructive execution authority | BLOCKED | V2 destructive actions are not admitted; scoped executor has no Eviction permission. |
| Stale-controller safety | BLOCKED | Scale-up takeover passes; an older in-flight Pod CREATE remains unfenced. |
| Crash and takeover recovery | PASS, scale-up only | Live Kind and Docker controller-process crash/takeover tests commit from observed target state with one dispatch. |
| Durable staged replacement | BLOCKED | Not implemented. |
| Replacement readiness before eviction | BLOCKED | No source eviction execution path exists. |
| PDB-safe source eviction | BLOCKED | Executor never calls policy/v1 Eviction; no FleetRight PDB-safe eviction qualification. |
| Lost-response reconciliation | PASS, scale-up/create only | Journaled scale-up and new Pod-create outcomes reconcile; eviction unknown outcomes are unsupported. |
| Duplicate operation prevention | PASS, scale-up only | Authenticated execute replay returns the same claim; live takeover records one dispatch. |
| Node cordon and bounded drain | NOT RUN | Drain is disabled and no drain executor exists. |
| Partial-drain recovery | NOT RUN | No node-drain workflow exists to resume. |
| Health-driven compensation | BLOCKED | Health regression fails safely; restoring capacity and verifying recovery are not implemented. |
| Docker E2E | PASS | Final Python/PostgreSQL suite ran the opt-in Docker crash, timeout and health-regression cases. |
| Real Kubernetes E2E | BLOCKED | Authenticated API→PostgreSQL→controller scale-up/takeover passes; all required destructive live cases remain disabled and were not run. |
| PostgreSQL integration | PASS | Final full Python run used the isolated PostgreSQL test database. |
| Scoped RBAC positive and negative tests | PASS, scale-up only | Live harness verifies named workload patch access and denies Pod delete, Eviction, Secret read and Node patch. |
| Relevant Python regression suite | PASS | 185 passed, 1 Kind skip in the aggregate run; Kind E2E passed separately. |
| Relevant C++ regression suite | PASS | CTest 38/38. |
| Formatting and lint | BLOCKED, partial | Ruff check, changed-file format, compileall and diff check pass; repository-wide Ruff format still flags three untouched files. |

### Skills and independent review

- Frontier Engineering Max instructions and actual local interfaces were
  inspected; this session did not run as the requested GPT-6 Luna Max executor.
- Caveman Ultra and Ponytail full remained active. The existing Graphify map
  was reused; it was not regenerated.
- GPT-6.1 Sol High reviewed the V2 owner/controller contract and supplied the
  duplicate-Node-UID fix and test. GPT-6.1 Sol Max reviewed the CREATE fence;
  findings on Merge Patch deletion, unresolved outcomes, CAS orderings, and
  mixed-version spec writes were addressed in code/tests.
- The Claude Sonnet 5.5 High wrapper verified first-party subscription access,
  but the inference request failed with DNS `EAI_AGAIN` before review; no
  Claude result is claimed. Opus was not invoked because no destructive
  execution implementation reached its review boundary.

**Next exact task:** prove a retirement/quiescence protocol that also handles a
CREATE admitted before controller takeover, or keep retired-slot mutation and
all dependent destructive actions disabled. Prompt 2B remains incomplete; do
not create `milestone-4-safe-control-plane`.

## 2026-10-11 Prompt 2B continuation — owner contract and safety gate

**Session start:** 2026-10-11 07:41:47 UTC; target finish 13:11:47 UTC;
absolute hard stop 13:41:47 UTC.

**Branch / starting HEAD:** `phase4/safe-execution-reconciliation` /
`f543080` (`test: restore kind scale-up and stale-create coverage`). Tracked
worktree was clean at start; pre-existing untracked `graphify-out/` was
preserved. No milestone-4 tag exists.

**Decision:** INCOMPLETE. The V2 owner contract now explicitly supports only
`FleetRightWorkload` identity and the independent verifier checks it, but
target-side destructive authority and durable retired-slot exclusion remain
unproven. All destructive V2 actions remain disabled. Prompt 3 has not started.

| Check | Result | Evidence |
|---|---|---|
| V2 owner contract | PASS, contract-only | `owner_contract_version=1` binds workload kind/name/UID/resourceVersion/generation, controller type/protocol, Pod owner and logical slot. API, planner and independent verifier reject legacy Deployment/ReplicaSet identity fields. |
| Focused Python V2/execution/controller tests | PASS, 44 passed | `.venv/bin/python -m pytest -q python/tests/test_evacuation_v2.py python/tests/test_prompt2b_execution_targets.py` plus stale-generation and delayed-old-CREATE controller regressions. |
| C++ build and regression | PASS, 37/37 | `cmake --build build -j2`; `ctest --test-dir build --output-on-failure`, including independent-verifier rejection of owner drift. |
| Python repository suite without PostgreSQL | PASS, 114 passed, 59 skipped | `.venv/bin/python -m pytest -q python/tests` with `FLEETRIGHT_TEST_DATABASE_URL` unset. Database and opt-in integration coverage is not qualified by this run. |
| Formatting and compilation | PASS | Ruff check/format on changed Python files, `clang-format --dry-run --Werror` on changed C++, Python `compileall`, and `git diff --check`. |
| Delayed stale Pod CREATE regression | FAIL safety property reproduced | `test_delayed_old_create_can_recreate_removed_slot_after_takeover` passes by reproducing the violation. `test_stale_generation_cannot_create_pods_after_a_newer_workload_update` separately proves stale snapshots are rejected before CREATE; it does not cover a CREATE already in flight. |
| Docker crash/takeover E2E | BLOCKED before test setup | The configured isolated PostgreSQL endpoint refused the connection before the fixture created a schema or touched a Compose project. |
| Kind API-to-PostgreSQL E2E | NOT RUN | Nodes are 3/3 Ready, but CoreDNS is 0/2 Ready, kube-proxy is failing on all three nodes, and local-path storage is restarting. The existing `cpu-api` fixture is 4/4 rather than the test's required 3/3 baseline. No test mutation was attempted. |
| PostgreSQL integration | BLOCKED | The configured test endpoint is not listening; no PostgreSQL schema or data was changed. |

Claude Opus 5.5 Medium completed the requested bounded first-party review.
Its findings were corrected: Python now matches C++ controller freshness and
owner-UID uniqueness checks; the verifier test asserts the snapshot and plan
binding invariants pass before checking the source-owner invariant fails; and
legacy owner-field tests cover every retained Deployment/ReplicaSet field, the
Pod controller UID, and the operation controller UID. The rerun passed 114
Python tests and 37 C++ tests. GPT-6.1 Sol Max and Sol High separately reviewed
the destructive-operation and workload lifecycle boundaries. Their remaining
race finding is unchanged and keeps destructive V2 execution disabled.

The source-level blocker is unchanged: workload status CAS and PostgreSQL
leases do not serialize a Pod CREATE already admitted by the Kubernetes API.
The regression demonstrates that a delayed old CREATE can persist after
takeover and after the source ordinal is removed. A validating webhook or
resourceVersion check alone does not close that in-flight request boundary.
No Eviction authority exists in the executor RBAC, and the saved Kubernetes
1.37.0 evidence shows that PDB admission/resourceVersion behavior does not
provide a universal stale-request fence. Do not enable source eviction,
staged replacement, scale-down, node drain, or destructive compensation.

**Next exact implementation action:** prove and enforce a target-side barrier
that serializes retired-slot exclusion with in-flight Pod CREATE and destructive
dispatch; until its adversarial delayed-request test passes, keep all destructive
V2 actions disabled. No milestone tag was created.

## 2026-10-10 Prompt 2B continuation — live Kind validation

**Scope:** Prompt 2B continuation only. Prompt 3 has not started.

**Session start:** approximately 2026-10-10 19:33 UTC; two-hour hard stop
2026-10-10 21:33 UTC.

**Branch / starting HEAD:** `phase4/safe-execution-reconciliation` /
`840322db5204cdfe386232842bba1bbc2738c9fd`. The worktree was clean at start.

**Checkpoint status:** PARTIAL. Live authenticated Kubernetes scale-up and
controller crash/takeover now pass. No destructive V2 operation was enabled.

### Current-session evidence

| Check | Result | Evidence |
|---|---|---|
| Safety-lab health | PASS | Explicit `kind-fleetright-safety-lab` context: 3/3 Nodes Ready, all system Pods Running/Ready, Kubernetes v1.37.0, `/readyz?verbose` passed. `fs.inotify.max_user_instances` remains 16384. |
| FleetRight fixtures | PASS | Reinstalled the CRD, RBAC, controller and test workload for the recreated cluster. The live `cpu-api` workload reports APPLIED, 4 replicas, 4 Ready. |
| Executor authority | PASS | Live test accepted named `FleetRightWorkload` patch; Pod delete, Pod Eviction, Secret read and Node patch were denied. Restricted kubeconfig and port-forward were removed after the test. |
| Authenticated Kubernetes API E2E | PASS, 1 passed (80.28s) | `test_kubernetes_workload_api_recovers_after_ready_scale_up_crash`: authenticated optimize/approve/execute and idempotent replay; real 3→4 scale-up; controller process crash while journal remained RUNNING; 30-second lease expiry; takeover committed from observed state with one durable dispatch. One existing Starlette/httpx deprecation warning. |
| PostgreSQL | PASS | Full Python suite used isolated PostgreSQL 16.15 `fleetright_test` at loopback port 32976. The supplied private environment file was sourced without printing credentials. The temporary container had no mounts and was removed after the suite. |
| Python | PASS, 163 passed, 4 skipped (20.08s) | `.venv/bin/python -m pytest -q python/tests`. Three opt-in Docker variants were skipped (two dispatch-fault cases and one readiness-regression case); the Kind case was run separately and passed above. One existing deprecation warning. |
| C++ | PASS, 36/36 | `ctest --test-dir build --output-on-failure`, including all 13 `EvacuationV2Test` cases. |
| Quality checks | PASS | Ruff check/format, `compileall` on changed test modules, and `git diff --check`. |
| Destructive V2 execution | DISABLED | No eviction, scale-down, node deactivation or drain authority was added. |

The live test fixes three `kubectl auth can-i` probe mistakes in the existing
E2E harness: named-resource syntax, Eviction subresource syntax, and expected
denial exit codes. A controller regression now holds an old deterministic Pod
CREATE across takeover and a later operation, removes the old Pod, then proves
the delayed request can recreate that slot with a new UID and source-node
placement still allowed. The stale status resourceVersion conflict does not
undo the Kubernetes CREATE. This test passed in the full Python suite.

### Safety review and remaining gates

GPT-6.1 Sol Max and Sol High completed separate read-only reviews. Claude
Sonnet 5.5 High and Claude Opus 5.5 Medium completed focused reviews through
the first-party Claude subscription after approval to send selected project
source. Sonnet's terminal capture was truncated; Opus's full review confirmed
the following source-level blockers, which match the regression test:

- A fresh controller also recreates any missing ordinal while desired count
  remains unchanged. A replacement can therefore return to the source node.
- A stale Pod CREATE can complete after the source Pod was removed and after a
  newer operation was applied. Workload status CAS only rejects stale status;
  it does not fence the independent Kubernetes CREATE.
- FleetRightWorkload Pods are owned by the FleetRightWorkload, while the V2
  verifier requires Deployment/ReplicaSet source ownership. The managed PDB
  also does not encode the verifier's profile replica floor and N-1 reserve.
- Kubernetes 1.37.0's unready-Pod Eviction path does not establish a reliable
  resourceVersion fence for a delayed request. PDB admission does not reserve
  replacement readiness through later source deletion.

These conditions leave source identity, stale-controller rejection, safe
takeover, PDB preconditions and uncertain Eviction reconciliation unproven.
Keep Pod eviction, staged replacement, scale-down and node drain disabled.
Recovery, compensation and drain qualification remain NOT RUN. The existing
monotonic scale-up path was validated; no operation was newly enabled.

GPT-6 Luna Max ran the fixture restoration and live E2E. GPT-6.1 Sol Max
reviewed stale destructive effects; GPT-6.1 Sol High reviewed the workload
controller/PDB contract. Claude Sonnet independently reviewed live test and
crash cases; Claude Opus reviewed the narrow destructive protocol. Graphify
found no existing map; a rebuild was skipped because this continuation
already had completed safety analysis and prioritizes execution evidence.

**Next exact implementation action:** add a target-side retired-ordinal
tombstone/admission contract that rejects both fresh and delayed Pod CREATEs
for a retired source slot; first prove that invariant with race tests, then
consider wiring exact-UID Eviction. Do not enable Eviction until that contract,
replacement readiness, PDB safety, recovery, compensation and live drain gates
pass. Prompt 2B remains incomplete; do not create `milestone-4-safe-control-plane`.

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

**Checkpoint recorded:** 2026-10-10 18:36 UTC (about 5 hours 28 minutes elapsed;
about 3 hours 32 minutes remains to the hard stop).

**HEAD before this checkpoint update:** `8e5f902`
(`docs: record docker timeout recovery evidence`). This update adds an opt-in
live Kubernetes API scale-up/crash-takeover test and records the failed lab
health preflight. The test has not run. No FleetRight Kubernetes resources
were applied. The three nodes in the dedicated `fleetright-safety-lab` kind
cluster were restarted to clear system-container startup failures; API
`/readyz` responds, but kube-system Pods still report container startup errors.

**Safe code/evidence checkpoint:** `e93a44a`
(`test: add restricted kind scale-up recovery harness`). This is the current
implementation commit; it contains only the opt-in test harness and checkpoint
documentation, not a production execution-path change.

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
| Earlier Python and PostgreSQL suite | PASS, 163 passed, 1 skipped, 1 warning (7.80s) | Preceded the new Docker recovery case and readiness-timeout fix; fresh no-volume PostgreSQL 16 `fleetright_test` on loopback port 32976. |
| Targeted takeover replay regression | PASS | Included in the 163-passed Python suite; injects response loss after fake target authorization has published the request, then checks takeover replay and one authorization. |
| Docker API E2E | PASS, 1 passed (21.31s) | `python/tests/test_prompt2a_control_api.py::test_docker_api_execution_changes_real_compose_state`; authenticated optimize/verify/approve/execute, duplicate request, real Compose 3→4, healthcheck readiness for all four replicas, fresh SLO metrics, then kill a spawned controller process after mutation while the durable action remains `RUNNING`. After lease expiry, a second controller process observes the target and commits with exactly one recorded dispatch. Fresh no-volume `fleetright_test` on `127.0.0.1:32976`; isolated random project removed. |
| Docker post-scale readiness-loss E2E | PASS, 1 passed (47.46s) | `test_docker_api_health_regression_fails_safe_without_downscaling`; authenticated execution scales 3→4, then the isolated fixture stops one existing container after target response. The controller persists `FAILED` / `replacement-readiness-timeout`, reaches `FAILED_SAFE`, retains target count 4 with 3 Ready, and dispatches no compensation/downscale. |
| Docker process-crash E2E rerun | PASS, 1 passed (21.57s) | `test_docker_api_execution_changes_real_compose_state`; rerun with `FLEETRIGHT_RUN_DOCKER_E2E=1` after the shared test-controller helper change. Process kill after mutation, lease expiry, takeover, and one dispatch remained passing. |
| Earlier Python/PostgreSQL suite after readiness-timeout fix | PASS, 165 passed | Superseded by the opt-in target-timeout run below. |
| Docker target-timeout E2E | PASS | The `target-timeout` parameter runs real Compose scale 3→4, then withholds the CLI response past its 20-second deadline. The adapter records `OUTCOME_UNKNOWN`; reconciliation observes 4 Ready containers, commits, and retains one dispatch. |
| Python/PostgreSQL suite with target-timeout E2E | PASS, 166 passed, 1 warning (112.55s) | `.venv/bin/python -m pytest -q python/tests` with `FLEETRIGHT_RUN_DOCKER_E2E=1`; fresh no-volume PostgreSQL 16 `fleetright_test` on loopback port 32976. Process-crash/takeover, target-timeout, and post-scale readiness-loss Docker E2Es all ran. Temporary DB and Compose resources were removed. |
| CRD preflight unit and manifest checks | PASS | 5 focused tests; all 25 required runtime execution rules and required request/fence fields match the checked-in CRD. Live API-server validation remains blocked until the cluster cert is valid. |
| C++ regression | PASS, 36/36 | `ctest --test-dir build --output-on-failure` |
| Focused target/controller tests | PASS, 69 passed (0.21s) | `.venv/bin/python -m pytest -q python/tests/test_prompt2b_execution_targets.py python/tests/test_workload_controller.py` |
| Live Kubernetes API test harness | BLOCKED / NOT RUN | New opt-in `test_kubernetes_workload_api_recovers_after_ready_scale_up_crash`; it uses a short-lived executor ServiceAccount kubeconfig and checks named-workload patch plus negative Pod-delete, Eviction, Secret-read, and Node-patch RBAC. The test requires a healthy workload controller and Pod health path; the lab's kube-system Pods are currently failing, so no FleetRight resources were applied. |
| Changed Python quality checks | PASS | Ruff 0.15.20 check/format and `compileall` pass for the modified API test module; `pytest --collect-only -k kubernetes_workload_api` collects the test. |
| Changed YAML and CRD contract | PASS | PyYAML parsed the CRD, all 7 RBAC documents, and the portless Docker fixture; direct check confirmed all 25 execution CEL rules, required fields, and scale-up protocol enum |
| Supplied DB endpoint | BLOCKED | Credential file parsed without displaying secrets; configured database is `fleetright_test`, but `127.0.0.1:32768` refused connections and `fleetright-isolated-test-db` was absent |
| Safety-lab health | BLOCKED | Explicit API read succeeds at Kubernetes v1.37.0 and `/readyz?verbose` passes. All three Nodes report Ready, but kube-system shows CoreDNS, etcd, API server, controller manager, scheduler, kindnet, and kube-proxy Pods in `CreateContainerError`, `CrashLoopBackOff`, or `Error`; worker kube-proxy logs `fsnotify watcher init: too many open files`. Restarting the three named kind node containers did not stabilize these Pods. The FleetRight namespaces and CRD were absent; no FleetRight resource was applied. |
| Current V2 destructive execution | DISABLED | Source, verifier, current RBAC, ADR-008, and saved API-server race evidence reviewed; no eviction or cordon mutator exists at committed HEAD |
| Docker failure coverage | PARTIAL | Process-crash takeover, target timeout after real apply, and post-scale readiness-loss fail-safe pass. Destructive-operation compensation/recovery remains untested. |

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
- Added an opt-in target-timeout E2E. A test-local CLI shim runs the
  real Compose scale to completion, then withholds only that response
  past the target deadline. Reconciliation observes and commits the
  new state after `OUTCOME_UNKNOWN`, with one durable dispatch.
- A Docker readiness-loss E2E exposed a scale-up timeout path that left a
  durably confirmed action `RUNNING`, so `_fail_safe` correctly refused to
  close the execution without a takeover fence. The reconciler now marks the
  action `FAILED` first only when the dispatch response is durably recorded and
  the target still reports the requested replica count; ambiguous outcomes
  remain unresolved and fence-gated. The live test now reaches `FAILED_SAFE`
  after one replica stops, while keeping target count 4 and issuing no downscale.

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
- A second Docker API E2E lets real Compose apply 3→4, then delays the CLI
  response past the target deadline. Reconciliation observes the four Ready
  containers after `OUTCOME_UNKNOWN` and commits with one dispatch. A separate
  post-scale readiness-loss case reaches `FAILED_SAFE` without downscaling.
- Docker compensation and compensation failure remain NOT RUN. The checked-in
  Compose file still publishes fixed ports and remains single-replica; the
  adapter permits multiple replicas only after Compose confirms no published
  host ports. Docker's lock is host-local, not a fence against another host.
- PDB-safe replacement/eviction, bounded drain, partial-drain recovery, and
  V2 evacuation compensation remain unimplemented. The existing scale-up
  compensation foundation is not live-qualified. Prompt 3 must not start
  unless every mandatory Prompt 2B gate passes.

**Next executable action:** restore the dedicated kind lab's kube-system health
and verify all required controllers/network Pods before provisioning any
FleetRight test object. Then run the opt-in authenticated API scale-up/
crash-takeover test with the restricted executor ServiceAccount kubeconfig.
This can qualify only monotonic scale-up, not V2 destructive execution. Keep
eviction, drain, and compensation disabled; do not create milestone 4 or start
Prompt 3 while any mandatory gate remains incomplete.

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
