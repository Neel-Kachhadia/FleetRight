# Prompt 3 adversarial, security, sanitizer and live qualification (independent run)

Date 2026-10-13. Code under test: branch `phase5/final-qualification`, HEAD `c88f24b` (before the planner correctness fix `1cf56ac`, which only changes the v1 planner; the
live suites exercise the V2 evacuation planner/verifier and the Python controllers). The final-commit rerun is
recorded in `docs/PROMPT3_QUALIFICATION.md`. Host: Linux 7.0, gcc 13.3.0,
clang 18.1.3, Python 3.12.3, pytest 9.1.1. Logs were written to a temporary directory and are not archived; the pass/fail lines below were copied from them.

## 1. Deterministic adversarial (non-live, PostgreSQL env set)

| Run | Result | Duration |
|---|---|---|
| `test_prompt2b_{execution_targets,journal,target_authority_agent,tombstone_cas_authority}.py` | `224 passed in 15.82s` | 16 s |
| Whole `python/tests` (non-live, DB set) | `487 passed, 20 skipped, 1 warning in 21.58s` (all 20 skips are `FLEETRIGHT_RUN_KIND_E2E`/`RUN_DOCKER_E2E` gated; run in sections 4 and 5) | 22 s |

Classification key: LIVE = real Kind API server or real Docker; DET = deterministic fakes plus real PostgreSQL;
UNIT = pure unit or C++.

| Scenario | Covering tests (file: name) | Class | Result |
|---|---|---|---|
| A old controller resumes after takeover | journal: `test_stale_controller_cannot_extend_or_finish_verification`, `test_old_takeover_receipt_cannot_close_or_unfence_a_new_dispatch`, `test_execution_claim_is_idempotent_unique_and_controller_fenced`, `test_concurrent_execution_claims_allow_one_controller`; targets: `test_kubernetes_stale_scale_patch_loses_to_newer_target_fence`, `test_docker_process_fence_rejects_a_stale_controller`, `test_stale_controller_cannot_reclaim_a_released_node`, `test_takeover_target_fence_rejects_a_delayed_replacement_patch`; live: `live_admission_negatives::test_live_stale_or_forged_pod_create_is_denied_without_reservation` | DET + LIVE | PASS |
| B Pod CREATE response lost | live `live_lost_create[drop_response]`, `live_admission_negatives::test_live_lost_create_response_is_adopted_without_duplicate_pod`; DET `test_lost_response_after_target_success_recovers_without_duplicate_mutation`, `test_workload_takeover_replays_published_request_after_fence_response_loss`, `test_workload_authority_blocks_while_pod_create_outcome_is_unknown`, `test_pending_create_reservation_wins_authority_resource_version_cas` | LIVE + DET | PASS |
| C delayed CREATE after takeover | live `live_lost_create[hold_request]`; DET `test_authority_patch_wins_and_stale_create_reservation_cannot_post`, `test_workload_authority_stays_blocked_after_stale_pod_disappears`, `test_stale_pod_history_stays_blocked_after_stale_pod_disappears`, `test_authorized_create_history_capacity_blocks_before_pod_post` | LIVE + DET | PASS |
| D source Pod UID changes | targets: `test_source_retirement_target_revalidates_live_source_pod_and_node_uid`, `test_source_retirement_target_rejects_changed_source_authority`, `test_retirement_history_fences_only_the_exact_logical_source`; controller: `test_retirement_replay_revalidates_live_source_uid`; admission: `test_binding_uid_precondition_rejects_a_recreated_pod`; `test_takeover_does_not_adopt_completed_retirement_after_identity_change` | DET | PASS (no live UID-swap test; live forged/stale CREATE denial covers identity admission only) |
| E replacement unready | `test_eviction_admission_requires_replacement_readiness_to_remain_stable`, `test_admitted_eviction_readiness_loss_after_takeover_keeps_plan_unresolved`, `test_retirement_does_not_evict_without_fresh_healthy_slo`, `test_source_retirement_binds_observed_ready_replacement_uid_and_node`, `test_post_eviction_readiness_deadline_starts_at_observed_source_removal`; live `live_replacement` (3) | DET + LIVE | PASS |
| F PDB permit/block | `test_pdb_block_response_is_classified`, `test_eviction_distinguishes_pdb_429_from_api_throttling`, `test_pre_tombstone_pdb_block_retries_replacement_preparation`, `test_unready_source_preflight_matches_if_healthy_budget`; live `live_replacement[pdb-permitted/denied]`, `live_drain_variants[pdb-blocked-drain]` | DET + LIVE | PASS |
| G eviction timeout / unknown outcome | `test_retirement_unknown_outcome_stays_blocked_after_controller_takeover`, `test_retirement_action_fails_safe_at_unproven_create_fence`, `test_unknown_action_blocks_dispatch_until_observed`, `test_expired_plan_can_close_fenced_unknown_action_without_retry`; live `test_live_v2_api_journal_controller_prepares_replacement[takeover-held-response]` | DET + LIVE | PASS (no wall-clock HTTP timeout is injected on the Eviction call itself; unknown outcome is modelled by lost/held response) |
| H late old eviction response | `test_eviction_admission_cannot_revoke_a_request_already_allowed`, `test_eviction_rejects_a_fence_change_during_evidence_reads`, `test_eviction_rejects_stale_or_invalid_evidence`; live `[takeover-held-response]` (held accepted Eviction across takeover) | DET + LIVE | PASS. Known limit (docs/limitations.md): admission cannot revoke an already-admitted request |
| I PostgreSQL connection drops | only `test_database_readiness_reports_unavailable` (readiness) and crash/takeover tests (`test_takeover_*`, `test_workload_takeover_recovers_*`); no test severs a live PG connection mid-transaction | DET (indirect) | PARTIAL. Real connection-drop injection NOT COVERED |
| J Kubernetes API unavailable | `test_persistent_node_drain_observation_error_is_journaled_then_fails_safe`, `test_lost_drain_check_response_stays_running_and_counted`, `test_external_target_state_conflict_resolves_unknown_without_wedging`, `test_takeover_of_deleted_target_fails_safe_and_releases_plan_lease`, `test_node_drain_verification...`; no test takes the real API server down | DET (simulated errors) | PARTIAL. Real API outage NOT COVERED live |
| K crash during source movement | live `live_lost_create` (SIGKILL controller A mid-CREATE), `[takeover-held-response]` (takeover after accepted Eviction); DET `test_takeover_recovers_crash_after_target_fence_before_receipt_commit`, `test_workload_takeover_recovers_preapply_crash`, `test_retirement_takeover_stays_unresolved_without_target_fence`, `test_new_controller_recovers_expired_pending_retirement_fence` | LIVE + DET | PASS |
| L crash during node drain | live `test_live_v2_authenticated_node_drain_partial_crash_recovery` (process kill with 3rd Eviction pending, exactly 4 Evictions); DET `test_takeover_recovers_running_node_drain_verification`, `test_cordon_crash_after_patch_keeps_ownership_across_takeover`, `test_release_sweep_recovers_a_crash_between_terminal_commit_and_node_release` | LIVE + DET | PASS |
| M two executions, same workload | `test_concurrent_execution_claims_allow_one_controller`, `test_concurrent_workload_takeover_uses_resource_version_cas`, `test_persistent_idempotency_lease_conflict_and_artifact_integrity`, `test_execute_mode_is_authenticated_async_and_idempotent` (replayed key), `test_workload_takeover_blocks_new_operation_until_prior_request_is_applied`, `test_failed_safe_keeps_lock_when_*` | DET (PostgreSQL leases) | PASS. No live two-API-process race |
| N node identity changes | `test_node_cordon_rejects_changed_node_uid_before_any_mutation`, `test_release_rejects_changed_node_uid_other_plan_and_newer_fence`, `test_recovery_verification_rejects_recreated_node_and_empty_topology_domain`, `test_node_uid_change_blocks_new_pod_without_using_mutable_uid_label`, `test_restart_blocks_scheduled_pod_after_approved_node_uid_changes`; live ownership test (foreign UID refused, no write) | DET + LIVE | PASS |
| O insufficient destination capacity | C++ `EvacuationV2Test.SequentialDrainRejectsCumulativeDestinationOvercapacity`, `RejectsInsufficientSurgeCapacity`, `RequiresNMinusOneRecoveryCapacity`, `VerifierAdversarialTest.RejectsEvacuationThatOverfillsIntermediateDestination`; live `health-compensation-unplaceable` (recovery Pod Unschedulable -> FAILED_SAFE) | UNIT + LIVE | PASS |
| P health regression compensation | `test_destructive_slo_regression_restores_capacity_and_measures_recovery`, `test_source_retirement_health_regression_restores_bounded_kubernetes_capacity`, `test_completed_retirement_takeover_compensates_on_fresh_health_regression`, `test_compensation_is_persisted_once_before_dispatch`, `test_scale_out_health_regression_never_compensates_by_scaling_down`; Docker `test_docker_api_health_regression_fails_safe_without_downscaling` | DET + LIVE (Docker) | PASS. Live Kind `health-compensation` (6-node lab) NOT RUN (needs a throw-away cluster; out of the permitted labs) |
| Q compensation cannot restore Ready replica | `test_compensation_that_never_becomes_ready_fails_safe_after_timeout`, `test_compensating_final_check_is_bounded_by_a_deadline`; live `[health-compensation-unplaceable]` | DET + LIVE | PASS |
| R final verification deadline | `test_final_verification_that_never_succeeds_fails_safe_at_the_deadline`, `test_verification_deadline_survives_controller_takeover`, `test_final_verification_succeeds_after_bounded_retry`, `test_stale_controller_cannot_extend_or_finish_verification`, `test_node_uncordoned_after_drain_completion_blocks_commit` | DET | PASS. Not driven live (needs real 10-minute waits; same as ledger) |
| S external cordon change | `test_cordon_of_administratively_cordoned_node_is_not_owned_and_never_released`, `test_cordon_never_recordons_a_node_an_administrator_uncordoned`, `test_node_cordon_conflict_never_overwrites_concurrent_node_state`, `test_node_uncordoned_after_drain_completion_blocks_commit`; live `live_node_ownership` | DET + LIVE | PASS |
| T unsupported workload owner | `test_node_drain_blocks_unselected_ordinary_pod_on_source_node`, `test_node_drain_blocks_unbound_pod_that_could_bind_to_source`, `test_node_drain_classifies_daemonset_and_static_mirror_without_mutating_them`, `test_conflicting_pod_owner_or_replica_identity_is_rejected`, `test_v2_owner_contract_rejects_legacy_or_mismatched_identity`, `test_v2_inventory_rejects_duplicate_controller_owner_uid`, `test_legacy_workload_cannot_be_targeted_by_execution`, `test_direct_deployment_execution_target_is_disabled`; C++ `RejectsControllerNamespaceAndOwnerContractMismatches`; live `test_live_legacy_workload_is_refused_before_any_mutation` | DET + UNIT + LIVE (legacy workload only) | PASS. Unsupported Pod owner during a drain is not exercised live |

Honest gaps: I (PG connection drop) and J (real API outage) are only modelled indirectly; K8s API outage is
simulated by injected observation errors, not by stopping the API server.

## 2. Security negatives

| Area | Evidence | Class | Result |
|---|---|---|---|
| Unauthenticated execute / optimize | `test_execute_mode_is_authenticated_async_and_idempotent` (execute without token -> 401); `test_authenticated_api_idempotency_approval_restart_and_fail_closed_execute` (anonymous optimize -> 401) | DET | PASS |
| Role restrictions | same test: viewer optimize -> 403; recommender and viewer approve -> 403; only operator approves; viewer may GET `/v1/state`. GAP: no test asserts viewer/recommender 403 on `/execute` specifically (code path shares the operator role dependency; untested directly) | DET | PASS with gap |
| Replayed stale operation / idempotency | `test_persistent_idempotency_lease_conflict_and_artifact_integrity`, replay in execute test (same key -> same 202 body), `test_approval_marks_changed_binding_stale`, `test_expired_plan_and_tampered_replay_artifact_are_rejected`, `test_old_takeover_receipt_cannot_close_or_unfence_a_new_dispatch` | DET | PASS |
| Malformed input | `test_observe_mode_never_runs_planner_and_malformed_snapshot_is_422`, `test_api_rejects_oversized_request_body` (413 over 2 MiB), `test_prediction_only_scale_in_is_rejected_and_cannot_be_approved`, C++ `RejectsUnknownSnapshotRootFields`; `test_admission_review_accepts_api_server_timeout_query` | DET + UNIT | PASS |
| Forged source/replacement identity | `test_admission_rejects_forged_placement_stale_identity_and_old_writer`, `test_admission_rejects_direct_node_assignment_for_every_pod_create`, `test_eviction_rejects_stale_or_invalid_evidence`, `test_binding_invalid_identity_or_node_evidence_fails_closed`; live `test_live_stale_or_forged_pod_create_is_denied_without_reservation` | DET + LIVE | PASS |
| Unsupported workload kind | `test_workload_execute_requires_the_supported_explicit_namespace`, `test_direct_deployment_execution_target_is_disabled`, `test_legacy_workload_cannot_be_promoted_for_execution`, `test_approval_keeps_unsupported_actions_out_of_execution_journal`; live legacy refusal | DET + LIVE | PASS |
| RBAC | static `test_eviction_webhook_and_rbac_keep_direct_pod_delete_denied`, `test_kubernetes_roles_publish_requests_and_leave_pod_creation_to_controller`, `test_kubernetes_target_has_no_pod_eviction_or_node_cordon_mutators`; live `kubectl auth can-i` inside `live_replacement` (executor: create pods/eviction denied, delete pods denied, patch nodes allowed) and the executor-taint-denied check in `live_node_ownership` ran and passed. `rbac.yaml` has no `secrets` rule | DET + LIVE | PASS |
| Secret reads (`get secrets` -> no) | asserted only in `test_kubernetes_workload_api_recovers_after_ready_scale_up_crash` which needs a separate executor kubeconfig on the safety lab | LIVE (gated) | NOT RUN (executor kubeconfig not provisioned; not created). Static: no secrets verb in `deploy/kubernetes/rbac.yaml` |
| Subset run | `pytest -k "auth or idempot or unsupported or malformed or oversized or forged or reject or stale or tamper or rbac or direct_pod_delete or fail or legacy or owner or invalid or uid"` over 2a/pod_admission/evacuation_v2/workload_* | | `145 passed, 2 skipped, 92 deselected` |

Secret scan: `detect-secrets 1.5.0 scan --all-files` over the 144 `git ls-files` paths. 1046 raw findings in 10 files; 1045 are
`Hex High Entropy String` (SHA-256 content hashes in `results/phase1/**` 34, `results/phase2/**` 1010,
`docs/evidence/phase1-kind-2026-10-08.json` 1) and 1 is `Secret Keyword` at
`deploy/kubernetes/fleetright-pod-admission-webhook.yaml:155` (a `secretName:` volume reference, no value). None verified; no
real secret found. Extra regex over tracked text (private-key headers, AWS/GitHub token shapes, `postgres://user:pass@`):
0 hits. Tracked-file check for kubeconfig/`.env`/`.pem`/`.key`/credentials names: 0 files.

## 3. Sanitizers

- ASan+UBSan: `cmake -S . -B build-asan3 -DCMAKE_BUILD_TYPE=RelWithDebInfo -DFLEETRIGHT_ENABLE_SANITIZERS=ON`
  (c++ = gcc 13.3.0; `-fsanitize=address,undefined` confirmed in `flags.make`). Build ok. `ctest`: `100% tests passed, 0 tests
  failed out of 51`, 0.58 s. No sanitizer reports. PASS.
- libFuzzer: `CXX=clang++ cmake -S . -B build-fuzz3 -DCMAKE_BUILD_TYPE=RelWithDebInfo -DFLEETRIGHT_BUILD_FUZZERS=ON` (clang 18.1.3),
  target `resource_fuzz` (the only fuzz target, `cpp/fuzz/resource_fuzz.cpp`). Run:
  `build-fuzz3/resource_fuzz -max_total_time=120 -seed=1 corpus_dir` -> `Done 1516499 runs in 121 second(s)`, exit 0, no crash, no
  artifact. Fresh empty corpus, so this is a smoke run, not deep coverage. PASS.
- TSan: not built. The planner/verifier are single-threaded (docs/limitations.md: "TSan is not applicable until concurrency is
  added"). The Python controllers use threads in tests but are not covered by TSan. NOT APPLICABLE.
- `build-asan3/` and `build-fuzz3/` are git-ignored (verified with `git check-ignore`); no tracked file touched.

## 4. Live (Kind), classification LIVE

Labs verified before runs: `fleetright-prompt2b-lab` (context `kind-fleetright-prompt2b-lab`, kubeconfig
`/tmp/fleetright-prompt2b-lab.kubeconfig` existed): 1 control plane + 4 workers all Ready, admission x2 + controller Running.
`fleetright-safety-lab`: 3 nodes Ready, old legacy workloads Running. Env for all modules:
`FLEETRIGHT_RUN_KIND_E2E=1 FLEETRIGHT_LAB_ADMIN_KUBECONFIG=/tmp/fleetright-prompt2b-lab.kubeconfig FLEETRIGHT_LAB_CONTEXT=kind-fleetright-prompt2b-lab`
plus `source ~/.cache/fleetright/test-db.env`. All five modules were run on the prompt2b lab (not safety lab, see note on the
replacement default below). Safety lab was only read by the legacy-refusal test.

| Module | Result | Duration |
|---|---|---|
| `test_prompt2b_live_admission_negatives.py` | `3 passed in 77.98s` | 78 s |
| `test_prompt2b_live_node_ownership.py` | `1 passed in 2.09s` | 2 s |
| `test_prompt2b_live_replacement.py` | `3 passed in 187.27s` (pdb-permitted, pdb-denied, Binding fence) | 187 s |
| `test_prompt2b_live_lost_create.py` | `2 passed in 127.73s` (drop_response, hold_request) | 128 s |
| `test_prompt2b_live_drain_variants.py` | attempt 1: `2 failed in 0.71s` (infrastructure, see below). Retry after lab fix: `2 passed, 1 warning in 220.65s` | 221 s |
| Extra, same lab: `test_prompt2a_control_api.py::test_live_v2_authenticated_node_drain_partial_crash_recovery` (4-Pod drain, crash, partial restart) | `1 passed in 394.07s` | 394 s |
| Extra: `test_live_v2_api_journal_controller_prepares_replacement[api-evacuation, takeover-held-response, health-compensation-unplaceable]` | `3 passed in 301.16s` | 301 s |

Totals live: 15 passed, 0 failed (after one lab-only retry). Not run: `[health-compensation]` (needs a throw-away 6-node
cluster), `test_kubernetes_workload_api_recovers_after_ready_scale_up_crash` and the scale-up variants of the live API tests (need a
separate executor kubeconfig and health URL on the safety lab with a 3/3 `cpu-api` baseline).

Drain-variants attempt 1 failure (infrastructure, not product): `assert len(clean_nodes) >= 2` at
`test_prompt2b_live_drain_variants.py:110`, `AssertionError: assert 1 >= 2`. The test needs two workers hosting only
DaemonSet/static Pods, but the controller Deployment pod had landed on a third worker (admission pods on worker and worker3,
controller on worker4), leaving only worker2 clean. Lab-only fix, with admin kubectl on the verified prompt2b context: cordoned
worker2 and worker4, deleted the controller pod so it rescheduled to `worker`, then uncordoned both. Retry passed. Resets between
modules: waited for `frw-*` Pods to terminate and uncordoned the four workers.

Note: `test_prompt2b_live_replacement.py::test_live_workload_prepares_and_evacuates_source_with_real_pod_identity` defaults to
the safety lab kubeconfig/context; I overrode both with the env vars above so it ran on the prompt2b lab (the lab with the final
admission image, matching the ledger). A first attempt was killed when my shell tool call timed out; it left only terminating test
Pods, which cleared; the full rerun is the one reported.

Clean-up: no temp executor kubeconfigs were created, no stray pytest/fault-proxy processes remain, both labs left running; prompt2b
lab final state: 5 nodes Ready and schedulable, no `frw-*` Pods, admission x2 and controller Running. 

## 5. Docker E2E

`FLEETRIGHT_RUN_DOCKER_E2E=1` with the isolated `fleetright_test` PostgreSQL schema, fixture `tests/fixtures/docker-execution/compose.yml`
(Docker Compose 2.40.3): `pytest python/tests/test_prompt2a_control_api.py -k docker_api` ->
`3 passed, 32 deselected, 1 warning in 105.76s` (`test_docker_api_execution_changes_real_compose_state[process-crash]`,
`[target-timeout]`, `test_docker_api_health_regression_fails_safe_without_downscaling`). PASS, LIVE (real Docker Compose).

## Defects

None found. The only failure (drain_variants attempt 1) was lab placement, resolved without repo changes.
