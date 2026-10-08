# Specialist review log

## Completed reviews

- GPT-6.1 Sol High reviewed profile semantics, units, snapshot identity, and placement/N-1 contracts.
  Findings applied: all-factor wide-integer rounding, fixed evaluation-time use, profile identity
  checks, and explicit N-1 recovery capacity.
- GPT-6.1 Sol Max independently reviewed the planner/verifier implementation and reproduced
  adversarial cases. Material fixes applied: bind current replica counts to snapshot placements;
  exclude deactivated nodes from verifier recovery; use checked action accumulation; recompute
  Wilson bounds from raw counts; enforce per-workload movement limits; consider every safe profile
  point; reject unknown failure enums; prevent wide-shape score wraparound; and bind node prices and
  dimensions to the machine catalog. Regression tests cover these cases.

## Failed or unavailable calls

- Claude Opus 5.5 Medium principal review ended without a model response because the selected
  provider/model was temporarily at capacity. The local Claude CLI diagnostic also reported OAuth
  refresh contention and zero generated tokens. The uncertain call was recorded and was not replayed.
- Claude Sonnet 5.5 High independent review was submitted through the first-party subscription CLI.
  The wrapper timed out after 300 seconds with inference attempted, completion unknown, empty stderr,
  and no review text. The uncertain request was not replayed. No Anthropic pay-as-you-go API was used.

## Follow-up correctness review

- GPT-6.1 Sol Max checked the weighted-objective changes and found three reproducible defects:
  minimum-improvement could suppress scale-up when current profile capacity did not cover demand;
  profile selection pruned by resource shape before weighted scoring; and hourly node cost overflow
  saturated to `UINT64_MAX`. The fixes now require empirical current-state evidence before the
  improvement gate, compare bounded full plans for alternate profiles, and reject unrepresentable
  cost totals. Regression tests reproduce each case.
- The same review found that planner and verifier could differ when current demand required more
  replicas than the configured maximum. The verifier now independently excludes such profile points;
  a regression checks objective recomputation even while the out-of-range no-op remains rejected.
- GPT-6.1 Sol High confirmed the profile-pruning issue and recommended bounded full-plan trials. The
  implementation evaluates a small stable portfolio of single-profile substitutions and documents
  that it does not enumerate profile combinations.

## Runtime measurement findings

- Docker characterization exposed a histogram counter-length mismatch when an SLO bucket duplicated
  a default boundary, first in sample collection and then in pooled histogram initialization. Both
  paths now size counters from the deduplicated bucket list; a regression test exercises the full
  sample-to-profile-statistics path.
- A one-concurrency measurement rounded to zero after the 10% integer haircut. Profile promotion
  now rejects zero effective QPS or concurrency, and a regression covers this admission rule.
- Offline lookahead initially planned to the profiled capacity boundary and violated simulated
  latency SLOs before that boundary under the convex model. It now targets 55% utilization; one
  memory-burst window remains infeasible because modeled memory pressure does not fall with replica
  count, and the limitation is reported.

No model opinion overrides compiler, test, sanitizer, fuzz, or measured benchmark evidence.

## Prompt 2B execution review

- GPT-6.1 Sol Max reviewed durable intent ordering, ambiguity, fencing, recovery, pause/cancel,
  scale-down progression, readiness, and compensation. The follow-up fixes added target-side fencing
  before mutation, unresolved-action recovery, a one-replica destructive step, health verification,
  and persisted compensation.
- Claude Sonnet 5.5 High reviewed the Prompt 2B implementation once through the first-party
  subscription CLI. It found six material issues: unresolved actions could wedge, scale-out could
  compensate by scaling down, approval bound a volatile Kubernetes resourceVersion, terminal
  failures leaked leases, scale-down could use stale health, and caller data could relax health
  limits. Fixes added observed-state failure resolution (including deleted targets), scale-in-only
  compensation, fresh resourceVersion CAS at mutation time, shared terminal lease release, fresh
  sample gating, and server-owned health ceilings. It also identified re-dispatch after a known
  response/external revert and broader Kubernetes RBAC defaults; those were fixed with regressions
  and a named-resource Role.
- Regression evidence: focused Prompt 2B/2A suite passed after the fixes (see final report for the
  final count). The Sonnet packet excluded `docs/implementation.md` because of a credential-pattern
  scan; the reviewer did not read that file. The call completed successfully and was not replayed.

## Prompt 2B final validation

- The current follow-up added durable pending/complete target fence receipts, atomic Kubernetes
  fence receipts from the JSON Patch response, same-token fence recovery, bounded re-observation
  when a target changes during fencing, and fail-safe closure for expired plans. A takeover no
  longer overwrites a known target response by converting every `RUNNING` action into unknown.
  PostgreSQL journal tests now pass 26/26, including crash after target fence, external generation
  changes around fencing, pause/cancel after apply, post-apply response loss, and plan expiry.
- GPT-6.1 Sol Max found material takeover-receipt, generation-adoption, and freshness/termination
  defects in the in-progress diff. The changes above address those findings; the new target-generation
  race and expiry tests pass. Sol Max also identified Kubernetes move/drain defects that remain
  out of scope for this follow-up because the API rejects those actions before claiming execution.
- Claude Sonnet 5.5 High found two further material recovery wedges. First, pause/cancel after an
  applied but unacknowledged scale could retry an illegal `OUTCOME_UNKNOWN -> RUNNING` transition
  forever; the controller now resolves the observed result directly, and parameterized pause/cancel
  tests pass. Second, an external generation change after a takeover fence but before its receipt
  commit could wedge same-controller restart; recovery now records the published fence from the
  live target and fails safely on the generation mismatch. The crash/interleaving regression passes.
  The focused call used the first-party Pro subscription at requested High effort. Its initial
  packet attempt did not reach inference because the base commit abbreviation was mistyped; the
  corrected call completed once. No pay-as-you-go API was used.
- GPT-6.1 Sol High and the drain-risk review found that the adapter's eviction helpers do not
  implement the verifier's staged replacement protocol, exact replacement identity, or durable
  partial-drain recovery. No unsafe integration was added. Durable PDB execution, node drain, and
  partial-drain recovery remain blocked; the final milestone tag is not appropriate.
- Final live kind evidence after both Sonnet fixes: kind v0.33.0, Kubernetes v1.37.0, dedicated
  cluster `fleetright-prompt2b-e2e`.
  Authenticated optimize/approve/execute scaled the Deployment 3→4. The controller was terminated
  after the API server applied the scale but before the journal recorded its response. A replacement
  controller observed four available replicas and committed with one logical mutation and action
  attempt. Duplicate execute replayed the same 202 response; anonymous execute returned 401. This
  run used a deterministic healthy observer for SLO fields; Kubernetes readiness/availability were
  real. A separate approved-plan external change ended `FAILED_SAFE` without a FleetRight mutation.
- Live least-privilege RBAC check with the checked-in Role: Deployment get/patch allowed; Pod
  eviction, Node read, and ClusterRoleBinding creation denied. A temporary test-only permission
  expansion enabled direct PDB helper checks: zero disruption allowance blocked eviction without
  deleting the Pod; allowed eviction used the Eviction API and the replacement became Ready. These
  are adapter checks, not durable reconciler execution. The disposable kind cluster had one worker
  Ready because of the host inotify quota; no host sysctl was changed.
- Docker Compose adapter smoke evidence from the previous validation remains valid. The real Docker
  API optimize/approve/execute flow did not qualify: the standard snapshot requires at least two
  replicas while the fixed-port Compose harness supports one. Real Docker API compensation remains
  unverified. Prior C++ evidence is 23/23; C++ was not changed in this follow-up.
- Final focused Python regression: 57 passed. Ruff check, Ruff format check, compileall,
  `git diff --check`, and a secret scan over all seven changed files passed (zero findings). C++
  source is unchanged and prior 23/23 evidence is reused. The prior full Python suite was 81 passed;
  no unrelated tests were rerun.

## 2026-10-08 evacuation contract review

- GPT-6.1 Sol High confirmed that schema-v1 `EvacuationStep` encodes exact source and destination
  placements that an ordinary Deployment cannot enforce. A distinct constraint-based contract
  would need to enumerate controller-selected outcomes and prove capacity, availability, topology,
  recovery reserve, and Pod/controller identity. No v2 plan was introduced because the available
  target cannot yet safely execute and reconcile it.
- GPT-6.1 Sol Max confirmed that a future narrower promise could permanently grow N to N+1 while
  retiring one named source Pod UID; this is not a temporary surge or node drain. The contract must
  account for N+2 physical overlap, all permitted destination pairs, cordon/recovery effects, and
  exact conditional uncordon. More critically, the pinned Kubernetes v1.37.0 Eviction handler may
  replace a supplied resourceVersion precondition for an unready Pod under `AlwaysAllow`, or under
  `IfHealthyBudget` when `CurrentHealthy >= DesiredHealthy > 0`, so a lease check cannot fence an old
  same-UID eviction request after takeover. The reviewer also found that current observation accepts
  a Ready destination Pod without binding it to the approved ReplicaSet and that the journal has no
  evacuation-stage/proof binding. No mutation path in this repository closes the
  check-to-final-mutation race. Evacuation remains disabled; no live eviction test was attempted.
- Claude Sonnet 5.5 High independently agreed that exact-placement evacuation cannot run safely and
  found an additional contract gap: `/v1/optimize` could mark a verifier-accepted plan `VERIFIED`
  even when its candidate actions were unsupported. Planning admission now rejects exact moves,
  node deactivation, resource resizing, and target-incompatible replica scaling. The approval-time
  action gate now runs in every API mode; the live target observation remains limited to EXECUTE.
  Regression checks cover this admission classifier and unsupported action approval in RECOMMEND
  and EXECUTE modes.
- Sonnet had no upstream-source evidence in its review packet and marked the Eviction precondition
  behavior uncertain. Direct inspection of the pinned v1.37.0 source resolves that disagreement:
  the unhealthy-Pod branch overwrites the caller's resourceVersion at lines 267-274, while its
  special conflict-to-429 handling at lines 278-284 is conditional on the caller omitting one. The
  same review found `observe_move` does not catch the helper's `TargetNotFound` when the source has
  disappeared and does not bind replacement identity; that unused helper remains unwired. Prior
  adapter checks on `fleetright-prompt2b-e2e` tested an eviction helper, not durable evacuation on
  the empty `fleetright-safety-lab` cluster.
- The Prompt 2A approval test was an existing uncommitted user change at session start; it was
  preserved and extended to check both RECOMMEND and EXECUTE approval gates. Final focused Python
  suite: 96 passed, including PostgreSQL-backed tests. C++ build and CTest: 23/23 passed. The
  AddressSanitizer test binary passed 23/23 with `ASAN_OPTIONS=detect_leaks=0`; default
  LeakSanitizer aborts in this environment after each test. Ruff check and changed-file format check
  passed. Whole-repository format check still reports only the pre-existing
  `python/fleetright/experiment.py` formatting difference. `gitleaks` was unavailable, so no
  dedicated secret-scanner result is claimed. No evacuation or live kind mutation test ran.

## 2026-10-08 target-side execution authority review

- GPT-6.1 Sol Max reviewed the execution and FleetRightWorkload paths, Kubernetes
  RBAC, and stale-controller failure model. It confirmed the Deployment
  resourceVersion JSON Patch is a same-object CAS only; it does not fence a
  separate Pod Eviction. It also found that FleetRightWorkload ordinary
  scale-down directly deleted a nonterminal Pod after a local readiness check.
  That path now fails closed with `ScaleDownRequiresStagedExecution`, with
  regressions for ready, pending, below-floor, and zero-disruption cases.
- Claude Opus 5.5 Medium reviewed the source, diff, tests, and API-server
  evidence through the first-party Pro subscription; the provider reported
  requested effort Medium and did not report effective effort. It agreed not
  to add an authority claim resource disconnected from final mutation, and to
  keep V2 Eviction/cordon disabled. It also found that `_ensure_pdb` can patch
  an older PDB spec from a stale workload snapshot after reading the PDB's
  current resourceVersion. A workload-generation high-water annotation blocks
  lower-generation snapshots only when already present. The first attempted
  bootstrap from an unmarked PDB was rejected after Sol Max identified that a
  stale snapshot could stamp a low generation and then loosen the budget. The
  controller now fails closed on unmarked PDBs; a trusted migration and
  mixed-version takeover remain unresolved blockers before drain or staged
  eviction.
- The live v1.37.0 test resolved the Eviction precondition question: the healthy
  stale-resourceVersion control was rejected, but an unready Pod under
  `IfHealthyBudget` was evicted with the same UID after its resourceVersion
  changed. The isolated test namespace was deleted. See
  `docs/evidence/execution-authority-race-2026-10-08.txt`.
- These reviews and API evidence do not establish V2 target-side mutation
  authority. No V2 destructive operation was enabled.
- Claude Sonnet 5.5 High reviewed the actual diff, tests, ADR, and live evidence
  through the first-party Pro subscription. It found that the new blocked
  scale-down status incorrectly set `Available=False`, and requested a direct
  UID-precondition test for terminal Pod cleanup plus clearer evidence of the
  unready-Pod state and PDB at request time. The controller now reports Ready
  and Available from observed Pod counts on the staged-scale-down block, adds a
  `Reconciled=False` condition/printer column, and has the UID request test. An
  isolated repeat with separate PDBs recorded Ready=False, PDB
  `currentHealthy=desiredHealthy=1`, `disruptionsAllowed=0`, stale RV change,
  API response code 201, and source UID absence. The focused 45-test suite,
  Ruff, formatting, compileall, CRD YAML parse, and diff check pass afterward.
- Sonnet also confirmed the ordinary scale-down change closes nonterminal Pod
  DELETE. The same-version `_ensure_pdb` stale-generation issue is guarded only
  for PDBs with a trusted marker; unmarked PDBs fail closed. The mixed-version
  caveat and lack of a final-mutation Eviction fence remain. `evict_one` and
  `cordon` have no production call sites; V2 action admission rejects them.
- GPT-6.1 Sol Max reviewed the new PDB high-water change. It confirmed whether
  marked-PDB stale writes are blocked by the marker/CAS protocol and confirmed
  that bootstrapping an unmarked PDB from a workload snapshot is unsafe. The
  code now fails closed for unmarked PDBs. Sol also noted that stale snapshots
  can still create Pods after the PDB write is skipped, older binaries can
  still patch stale budgets and delete nonterminal Pods, and the fake API did
  not model the resourceVersion conflict race. A deterministic stale-read/newer
  write conflict regression was added; all listed target-side mutation gaps
  remain blockers.

## 2026-10-08 Prompt 2B execution-authority continuation

- GPT-6.1 Sol Max reviewed the FleetRightWorkload authority and request-ordering
  model. The implementation keeps only monotonic scale-up enabled: one
  resourceVersion-guarded write stores the operation identity, execution
  generation, expected/target counts, and new replica high-water mark; the
  existing controller is the only Pod creator. Delayed Pod CREATE is not
  atomically fenced; deterministic ordinal names, immutable workload shape,
  owner UID checks, and the nondecreasing count make those creates compatible.
  Scale-down, terminal replacement, Eviction, and drain remain disabled.
- The first actual v1.37 JSON Patch race exposed that `kubectl` reduces a
  failed JSON Patch `test` to a generic invalid-request message. Although it
  failed closed, it obscured the concurrency result. Authorization now uses a
  Merge Patch with Kubernetes `metadata.resourceVersion` precondition, which
  the API server reports as a 409 Conflict. The actual concurrent race then
  yielded one winner and one classified conflict. Delayed stale authorization
  after a newer write also returned 409 and preserved the newer generation.
- Added a fail-closed executor precondition requiring a current `Reconciled=True`
  status for new operation identities, in addition to exact observed generation,
  prior `APPLIED` status, readiness, availability floor, object UID, and current
  replica preconditions. Same-operation takeover may advance its generation
  while reconciling that durable operation.
- Live v1.37 tests rejected scale-subresource downscale, replica growth without
  a durable request, and in-place promotion of a legacy workload. Three real
  Ready Pods had the expected owner UID, deterministic logical IDs, approved
  Node name/UID, and matching zero-disruption PDB. Impersonation verified the
  executor cannot create Pods/Evictions or patch Deployments, while the
  workload controller cannot delete Pods, create Evictions, patch workload
  spec, or mutate Nodes.
- The test controller initially timed out through the broken worker kube-proxy
  Service path. It was temporarily pinned to the healthy control-plane Node in
  the test cluster only, then all test namespaces, CRD, RBAC, and temporary
  image tags were removed. No host security settings were changed.
- An attempted Claude Opus read-only review was rejected by automatic review
  before any provider request because it would transmit repository source and
  the diff externally. No external review output was received; internal review
  and local/API-server evidence were used.
- PostgreSQL-backed full Python suite passed 142 tests after the fail-closed
  status assertion and ambiguous-workload completion test. The latter keeps
  `OUTCOME_UNKNOWN` until controller `APPLIED` status; the gate is scoped to
  FleetRightWorkload observations so existing Deployment takeover recovery is
  unchanged. CTest passed 36/36, Ruff format/check and `git diff
  --check` passed, and `detect-secrets` reported zero findings over all changed
  files. `gitleaks` is unavailable.
