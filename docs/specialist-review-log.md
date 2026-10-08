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
