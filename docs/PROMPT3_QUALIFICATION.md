# Prompt 3 qualification ledger (v1.0.0)

Durable progress and gate record. Start: `06b448b` (`milestone-4-safe-control-plane`, branch
`phase4/safe-execution-reconciliation`), new branch `phase5/final-qualification`. Final qualified commit:
see "Release decision" at the end. Nothing was pushed, merged or released.

## Work done

| Commit (short subject) | Purpose |
|---|---|
| perf: select placement destination by minimum | planner p95 332 -> ~156 ms, byte-identical plan |
| feat: seeded paired policy qualification harness | 6 policies + ablation, 15 scenarios x 20 seeds, tuning stage, bootstrap CIs |
| fix: planner never emits plans the verifier must reject | two real planner defects found by the oracle study (staging collocation, `max_moves`) + C++ property test |
| feat: forecast evaluation / placement suite / optimizer_quality | causal forecast metrics, 11 real-planner scenarios, 60-instance oracle + mutation study |
| build / test / docs commits | venv bootstrap, disposable DB script, qualification runner, role-fence + PostgreSQL-severance tests, methodology, report, walkthrough, README, limitations |
| fix: verdict rule gives hard violations precedence | independent-review finding (see below) |

Delegation actually performed: one Sonnet subagent ran the adversarial/security/sanitizer/live/Docker
stream (read-only on tracked files; report in `docs/evidence/prompt3-adversarial-security-live-2026-10-13.md`);
one Opus (medium effort) subagent performed the single independent release review. Opus High was not used.
The primary session did all implementation, benchmarking, fixes and documentation.

## Independent review (Opus, medium) and resolution

| # | Severity | Finding | Resolution |
|---|---|---|---|
| 1 | blocking | Verdict summed hard+soft windows, labelling S04/S11 "FR better" while FleetRight had hard violations static lacked | `verdict()` gives hard violations precedence; unit test; success classification table; all artifacts and tallies regenerated from the unchanged raw results (`qualify render`) |
| 2 | blocking (docs) | Report cited a non-existent ledger | this file |
| 3 | high | New role-fence and connection-severance tests had no recorded run at the release commit | run and recorded below |
| 4 | medium | "worse on both axes" was false (cost only) | reworded |
| 5 | medium | Live evidence commit unstated | stated: first run at `c88f24b`, rerun at the release candidate (below) |
| 6 | low | Steady/flash "identical traces" wording; S09 name | corrected in methodology |
| 7 | low | Missing provenance for pre-optimization figure; hand-copied artifacts | provenance README; `publish` copies planner/forecast/placement/quality |
| 8 | low | Test-DB script gaps | `chmod 600`, missing-env guard |
| 9 | low | No tests for verdict, simulator hooks, packing | added (10 tests in `test_qualify.py`) |

## Adversarial scenarios A-T

LIVE = real Kind API server; DET = deterministic fakes + real PostgreSQL; UNIT = unit/C++. Test names per
row are in the evidence file. All results below passed.

| | Scenario | Class | Notes |
|---|---|---|---|
| A | old controller resumes after takeover | DET + LIVE | |
| B | Pod CREATE response lost | LIVE + DET | `live_lost_create[drop_response]` via disposable TLS fault proxy |
| C | delayed CREATE after takeover | LIVE + DET | `[hold_request]` |
| D | source Pod UID changes | DET | no live UID swap |
| E | replacement unready | DET + LIVE | |
| F | PDB permits / blocks | DET + LIVE | `pdb-permitted`, `pdb-denied`, `pdb-blocked-drain` |
| G | eviction timeout | DET + LIVE | modelled by lost/held response; no injected HTTP timeout |
| H | late old eviction response | DET + LIVE | admission cannot revoke an already-admitted request (documented) |
| I | PostgreSQL connection drops | DET | `test_severed_postgres_connection_during_claim_leaves_no_partial_execution` (added) |
| J | Kubernetes API unavailable | DET (injected errors) | **not a real outage; partial** |
| K | crash during source movement | LIVE + DET | |
| L | crash during node drain | LIVE + DET | 4-Pod drain, kill with 3rd Eviction pending, exactly 4 Evictions |
| M | two executions, same workload | DET | no live two-process race |
| N | node identity changes | DET + LIVE | |
| O | insufficient destination capacity | UNIT + LIVE | `health-compensation-unplaceable` -> `FAILED_SAFE` |
| P | health regression -> compensation | DET + Docker | Kind 6-node `health-compensation` not re-run (Milestone 4 only) |
| Q | compensation cannot restore Ready | DET + LIVE | |
| R | final verification deadline | DET | needs real 10-minute waits live |
| S | unrelated Node admin changes cordon | DET + LIVE | |
| T | unsupported workload owner | DET + UNIT + LIVE (legacy workload only) | unsupported Pod owner during a live drain not exercised live |

## Test evidence

| Suite | Result | Class |
|---|---|---|
| C++ CTest (Release) | 52/52 | UNIT |
| C++ ASan+UBSan (GCC 13.3, RelWithDebInfo), final planner | 52/52, no reports; 1000w/500n plan equals Release plan except build id | UNIT |
| libFuzzer `resource_fuzz` (clang 18.1.3), seed 7, 90 s | 48,616 runs, no crash (earlier run: seed 1, 121 s, 1,516,499 runs). Smoke only: empty corpus | FUZZ |
| TSan | not applicable (single-threaded planner/verifier) | - |
| Python with PostgreSQL | see "Release candidate verification" | DET/UNIT |
| Live Kind (prompt2b lab) | see below | LIVE |
| Docker E2E | see below | LIVE |
| Secret scan | `detect-secrets` over tracked files: only SHA-256 hashes (results/, benchmarks), one `secretName:` reference, one printf format string; regex scan 0 hits; no kubeconfig/.env tracked | |

## Release gates

Legend: PASS / PWL = PASS WITH DOCUMENTED LIMITATION / FAIL / BLOCKED / NOT RUN.

| Gate | Status | Evidence / limitation |
|---|---|---|
| Prompt 1 / 2A / Milestone 4 remain correct; no architectural regression | PASS | `git diff --stat 06b448b..HEAD -- python/fleetright deploy cpp/src proto migrations workloads`: only the v1 `planner.cpp`, a `$HOME` default in `install-pod-create-admission.sh`, additive `benchmark.py`/`simulator.py`/`traces.py` hooks and the new qualification modules. `execution.py`, `reconciler.py`, `workload_controller.py`, `pod_admission.py`, `api.py`, `control.py`, both verifiers, `planner_v2.cpp`, `proto/` and `migrations/` are unchanged |
| C++ suite, independent verifier, deterministic replay | PASS | 52/52; replay 100/100 and 60/60 |
| Oracle comparisons | PWL | feasibility 60/60; gap optimal 23/30 (mean +7.6%, max +40%) where the move budget is non-binding; exact oracle covers one workload, no N-1 |
| Sanitizers | PASS | ASan/UBSan; TSan N/A; fuzz smoke |
| Python, PostgreSQL integration, durable recovery, authenticated API | PASS | |
| K8s supported scale-up | PWL | the Kind `monotonic-scale-up` E2E needs an executor kubeconfig and a 3/3 baseline and was **not re-run**; scale-up is exercised inside live replacement/drain runs, Docker E2E and the 2026-10-10 live qualification |
| K8s replacement/eviction, PDB permit/deny, node drain, crash/takeover, compensation | PASS | live rerun below |
| K8s RBAC | PWL | live `can-i` and Node-policy probes pass; the live `get secrets` denial probe was not run (no Secret rule in `rbac.yaml`) |
| Scientific validation (scenarios, baselines, pairing, statistics, cost, SLO, forecast, optimizer) | PASS | `docs/RELEASE_REPORT.md`; negative and mixed findings reported |
| Original thresholds assessed | PASS | planner p95 196.9 ms <= 250; verifier p95 20.7 ms <= 25 |
| Reproducibility | PWL | clean clone on this host reproduces tests and a bit-identical `results.jsonl`; not demonstrated on another machine/OS or for the Kind suites |
| Security tests, secrets scan, scoped RBAC, no uncontrolled destructive access | PWL | see RBAC row; API is loopback/static-token only |
| Documentation (README, architecture, methodology, limitations, walkthrough, release notes) | PASS | |
| Git: qualified commit identified, working tree reviewed, no AI co-author/contributor entries, milestones preserved | PASS | verified at tagging |

## Release candidate verification

Live suites (Kind lab `fleetright-prompt2b-lab`, Docker, isolated PostgreSQL) were run twice: by the
independent subagent at `c88f24b` (15 live + 3 Docker passed, evidence file) and by the primary session at
`7d4f0c4` (below). Between `7d4f0c4` and the release commit only the qualification harness, tests, scripts and
documentation changed (`git diff --stat 7d4f0c4..HEAD`); no execution-path file did.

| Module (7d4f0c4) | Result |
|---|---|
| `test_prompt2b_live_admission_negatives` | 3 passed (75 s) |
| `test_prompt2b_live_node_ownership` | 1 passed (2 s) |
| `test_prompt2b_live_replacement` (pdb-permitted, pdb-denied, Binding fence) | 3 passed (189 s) |
| `test_prompt2b_live_lost_create` (drop_response, hold_request) | 2 passed (127 s) |
| `test_prompt2b_live_drain_variants` (two-pod, pdb-blocked) | 2 passed (216 s) |
| `test_live_v2_authenticated_node_drain_partial_crash_recovery` (4 Pods, kill, takeover) | 1 passed (398 s) |
| `test_live_v2_api_journal_controller_prepares_replacement` (api-evacuation, takeover-held-response, unplaceable compensation) | 3 passed (298 s) |
| Docker E2E (`-k docker_api`: process-crash, target-timeout, health-regression fail-safe) | 3 passed (105 s) |

Not run live: `[health-compensation]` (needs a throw-away 6-node cluster), the Kind scale-up E2E (needs an
executor kubeconfig and a 3/3 `cpu-api` baseline), `get secrets` denial probe, real API-server outage,
real PostgreSQL outage. Lab placement note: the drain-variants module needs two workers without the controller
Pod; the harness script uncordons workers and waits for `frw-*` Pods between modules.

Python at the release candidate, isolated PostgreSQL, project venv: **499 passed, 20 skipped** (all 20 are
`FLEETRIGHT_RUN_KIND_E2E` / `FLEETRIGHT_RUN_DOCKER_E2E` gated and were run separately above), includes the
role-fence and connection-severance tests (`test_prompt3_gaps.py`, 2 passed). C++ CTest 52/52. `ruff check`,
`ruff format --check`, `git diff --check`, shell syntax all clean.
