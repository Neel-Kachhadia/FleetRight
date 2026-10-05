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

No model opinion overrides compiler, test, sanitizer, fuzz, or measured benchmark evidence.
