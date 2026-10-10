# Prompt 2B validation checkpoint — 2026-10-10

Repository: `/home/neel/Projects/FleetRight`
Branch: `phase4/safe-execution-reconciliation`
Starting commit: `d70c276` (`fix: bound Kubernetes execution to workload authority`)
Local implementation checkpoint: `10292a2` (`fix: fence workload authority across takeovers`)
HEAD before this evidence update: `6fc660b` (`docs: correct current execution checkpoint`)
Production starting checkpoint: `27df715`; this checkpoint also fixes confirmed-applied scale-up readiness timeouts in `reconciler.py`.
Session start: 2026-10-10 13:07:50 UTC; hard stop: 2026-10-10 22:07:50 UTC.

## Executed checks

| Command / check | Result | Notes |
|---|---|---|
| `git diff --check` at start | PASS | Worktree was clean before this session. |
| Earlier `.venv/bin/python -m pytest -q python/tests` | PASS, 163 passed, 1 skipped, 1 warning (7.80s) | Run before the new E2E and readiness-timeout fix with a fresh no-volume PostgreSQL 16 `fleetright_test` on loopback port 32976. |
| `test_workload_takeover_replays_published_request_after_fence_response_loss` | PASS | Replays a published workload request after takeover fence persistence and crash before receipt commit; exactly one authorization and eventual commit. Included in the 163-test full-suite run. |
| `test_docker_api_execution_changes_real_compose_state` | PASS, 1 passed (21.31s) | Fresh `fleetright_test` PostgreSQL on loopback port 32976; unique portless Compose project. Authenticated optimize/approval/execute, unauthorized execute denial, duplicate execute replay, real 3→4 Compose scale, four healthy Docker healthchecks, fresh SLO metrics, then kill the spawned controller after mutation while the journal is `RUNNING`. After lease expiry a second process commits from observed state; the journal contains one dispatch. Target timeout and destructive-operation compensation were not exercised; readiness-loss is covered separately. Project and DB container were cleaned up. |
| `test_docker_api_health_regression_fails_safe_without_downscaling` | PASS, 1 passed (47.46s) | Authenticated execution scales a real portless Compose project 3→4; after the response is recorded, the test stops one existing container. The controller records `FAILED` / `replacement-readiness-timeout`, reaches `FAILED_SAFE`, retains target count 4 with 3 Ready, and dispatches no compensation/downscale. |
| `test_docker_api_execution_changes_real_compose_state` after helper change | PASS, 1 passed (21.57s) | Explicit opt-in `FLEETRIGHT_RUN_DOCKER_E2E=1`; process kill after mutation and lease-based takeover still commit with a single durable dispatch. |
| `.venv/bin/python -m pytest -q python/tests` with `FLEETRIGHT_RUN_DOCKER_E2E=1` after fix | PASS, 165 passed, 1 warning (76.53s) | Fresh no-volume PostgreSQL 16 `fleetright_test` on `127.0.0.1:32976`; process-crash/takeover and post-scale readiness-loss Docker E2Es both ran. Temporary DB and Compose resources were removed. |
| `ctest --test-dir build --output-on-failure` | PASS, 36/36 | Existing C++ build; no C++ source changes in this checkpoint. |
| Ruff and `compileall` on changed Python files | PASS | Ruff 0.15.20 check and Python compilation for `reconciler.py` and `test_prompt2a_control_api.py`; test-module format check passes. |
| PyYAML parse and CRD contract check | PASS | CRD, all 7 RBAC documents, and Docker fixture parsed; all 25 required runtime CEL rules, request/fence fields, and protocol enum match the manifest. API-server/CEL validation remains unrun for the current edits. |
| `git diff --check` | PASS | Checked after the current fencing and evidence updates. |
| Explicit safety-lab read using `/home/neel/.cache/fleetright/safety-lab.kubeconfig`, context `kind-fleetright-safety-lab` | BLOCKED | Sandbox denied the first localhost API socket request. One authorized elevated read reached Kubernetes client v1.37.1, but TLS rejected the API certificate as not yet valid (`NotBefore=2026-10-10T18:29:54Z`; request time was before then). No verification bypass or cluster mutation was attempted. |
| Docker target timeout and compensation/recovery | NOT RUN | Process crash recovery at one post-mutation/pre-response boundary and a separate post-scale readiness-loss fail-safe pass. Neither proves target-timeout recovery or destructive-operation compensation. |

The test database URL was sourced from the private environment file and overridden for a fresh no-volume `fleetright_test` container at `127.0.0.1:32976`; no credentials were printed. The full Python suite and Docker E2E each used the isolated endpoint, and each temporary container was stopped and removed. The existing `fleetright_fleetright-postgres` volume was not attached or modified.

## Kubernetes safety boundary (unchanged by this scale-up fix)

- The Kubernetes API target admits only monotonic scale-up for a newly created `FleetRightWorkload` marked `monotonic-scale-up-v1`. Workload UID, template, placement, and policy are immutable; `spec.replicas` is the high-water value and growth is bound to a durable request. There is no separate high-water field.
- The controller's `APPLIED` status means desired deterministic Pod ordinals exist and pass identity/template/placement checks. It does not mean they are Ready. The API reconciler separately checks the matching operation, observed generation/count, readiness/availability, and a healthy metrics window.
- Docker supports multiple replicas only for a Compose service whose config has no published host ports. Docker healthcheck status counts as readiness when configured; the checked-in services still have fixed host ports and remain single-replica. The isolated portless test fixture exercises 3→4.
- V2 destructive execution remains disabled: the independent V2 verifier returns `execution_admitted=false`; the API rejects unsupported destructive actions; the current target and RBAC expose no Pod Eviction or Node-cordon mutator.
- The saved live negative control in `execution-authority-race-2026-10-08.txt` shows Kubernetes v1.37.0 can accept a delayed same-UID unready-Pod Eviction under `IfHealthyBudget` after the caller's resourceVersion changes. PostgreSQL leases and the workload-object CAS do not fence that separate mutation.
- This continuation strengthens only scale-up authority. The target checks `fencedGeneration == metadata.generation` after observing the CR; CEL does not directly compare metadata generation. A delayed old authorization that wins before the new target fence is observed and adopted as the current request before re-fencing. An old authorization arriving after a newer target fence is rejected; stale receipts cannot close later dispatches.
- GPT-6.1 Sol High and Sol Max re-reviewed the scale-up fencing changes and reported no remaining production blockers in the reviewed path. They did not supply runtime evidence. The read-only Claude Sonnet request through the signed-in Claude Code subscription failed before inference because DNS could not reach its API (`EAI_AGAIN`); `ANTHROPIC_API_KEY` was absent, and no pay-as-you-go API was used.
- Durable staged replacement, PDB-safe eviction, node drain, partial-drain recovery, and live compensation remain unqualified. Docker's positive API path and one actual process-crash takeover boundary pass; target timeout and destructive-operation compensation remain unqualified. The post-scale readiness-loss case reaches `FAILED_SAFE` while leaving the observed target at 4 total / 3 Ready replicas; Prompt 3 and release qualification have not started.

## Current-session review

- GPT-6.1 Sol High: the checked-in current target/controller is scale-up-only; the docs must distinguish `APPLIED` ordinal existence from readiness and avoid describing an eviction helper as currently present.
- GPT-6.1 Sol Max: the enabled monotonic scale-up boundary has no identified fencing/crash violation within its immutable same-workload-UID, template/placement, and nondecreasing high-water limits. It is not an evacuation backend: a missing ordinal is recreated, while the V2 plan requires source-excluding destinations and binds different owner/Pod identities. The verifier requires atomic mutation fencing for evacuation and unconditionally leaves V2 execution unadmitted.
- Claude Opus 5.5 Medium: read-only principal review completed through the first-party Pro subscription. It confirmed Prompt 2B must remain incomplete and identified the Deployment/ReplicaSet versus FleetRightWorkload owner mismatch, the PDB's floor-only budget, normal ordinal recreation after eviction, and limits in the replay test. The wrapper treated two text `--conclusion` values as nonexistent file paths; their substance was present in the supplied evidence files, but the separate conclusion channel was not used as intended.

## Destructive-action protocol review

GPT-6.1 Sol High and Sol Max reviewed exact-UID retirement under delayed
Eviction, controller takeover, and readiness changes. They concluded it is
viable in principle only if every state during `OUTCOME_UNKNOWN` preserves
availability, capacity, topology, and the permitted failure reserve. The
current verifier/runtime does not establish those conditions. More directly,
the workload controller normally recreates an evicted ordinal while it remains
below the immutable desired replica count, using the old allowed-node set; a
stale in-flight CREATE can do the same after source UID retirement. The
replacement may return to the source node. Source exclusion and drain therefore
remain blocked.

Sol Max also confirmed from Kubernetes v1.37.0 handler ordering that a stale
same-name healthy-Pod Eviction can decrement PDB allowance and write
`disruptedPods` before a final UID mismatch rejects Pod mutation. The unready
branch can overwrite the caller's resourceVersion and bypass that PDB
bookkeeping. PDB status is not a fencing proof. A previously accepted request
can also delete its source after replacement readiness regresses. A strict
no-resend policy can leave `OUTCOME_UNKNOWN` indefinitely if the controller
crashes between durable dispatch recording and HTTP transmission.

The scale-up review's possible test-fake ordering finding was withdrawn after
the current fake was rechecked. A new journal regression now covers published
request + takeover fence response loss and passes. It does not expand the
enabled action boundary.

Claude Opus review further confirmed that V2's source inventory requires each
Pod controller UID to match the active ReplicaSet UID, but FleetRightWorkload
Pods are owned by the FleetRightWorkload. The current V2 contract cannot bind
to this execution target. The managed FleetRightWorkload PDB derives
`maxUnavailable` from `availabilityFloor`; it does not prove the profile
required replica floor or the V2 N-1 reserve. These are additional blockers.
