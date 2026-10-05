from __future__ import annotations

import random

from fleetright.characterize import Samples
from fleetright.profiles import RunMetrics, characterize_point, wilson_lower_ppm, wilson_upper_ppm
from fleetright.telemetry import Histogram, pooled_histogram, tail_confidence


def test_histogram_merge_aggregates_counts_instead_of_averaging_p99() -> None:
    left = Histogram().observe(1)
    for _ in range(99):
        left = left.observe(2)
    right = Histogram().observe(500)
    pooled = pooled_histogram([left, right])
    assert left.quantile_upper_ms(0.99) == 2
    assert right.quantile_upper_ms(0.99) == 500
    assert pooled.quantile_upper_ms(0.99) == 2
    assert pooled.count == 101
    assert (left.quantile_upper_ms(0.99) + right.quantile_upper_ms(0.99)) // 2 != 2


def test_prometheus_histogram_uses_cumulative_classic_buckets() -> None:
    histogram = Histogram((1, 5), (1, 2, 1), 12)
    text = histogram.prometheus()
    assert 'le="1"} 1' in text
    assert 'le="5"} 3' in text
    assert 'le="+Inf"} 4' in text
    assert "_count 4" in text


def test_slo_threshold_must_be_a_bucket_and_low_traffic_is_marked() -> None:
    histogram = Histogram().observe(10)
    assert histogram.count_leq(10) == 1
    try:
        histogram.count_leq(11)
    except ValueError as error:
        assert "explicit histogram bucket" in str(error)
    else:
        raise AssertionError("threshold outside histogram resolution was accepted")
    assert tail_confidence(9_999) == "low"
    assert tail_confidence(10_000) == "adequate"


def test_characterization_samples_accept_slo_that_is_already_a_bucket() -> None:
    samples = Samples(80)
    samples.add(80, error=False)
    point = characterize_point(
        [RunMetrics(samples.histogram, samples.count, samples.errors, 1_000, 1, 0, 0, 0)],
        80,
        1_000,
    )
    assert point["p99_upper_ms"] == 80
    assert point["threshold_trials"] == 1


def test_wilson_bounds_are_conservative_for_zero_errors_and_sparse_tail() -> None:
    assert wilson_lower_ppm(0, 100) == 0
    assert wilson_lower_ppm(9_990, 10_000) < 999_000
    assert wilson_upper_ppm(0, 10_000) > 0
    assert wilson_upper_ppm(0, 10_000) < 1_000


def test_capacity_point_keeps_raw_counts_and_haircut() -> None:
    histogram = Histogram()
    for _ in range(20_000):
        histogram = histogram.observe(80)
    runs = [RunMetrics(histogram, 20_000, 0, 300_000, 100, 0, 50_000, 0) for _ in range(3)]
    point = characterize_point(runs, 120, 1_000, haircut_ppm=100_000)
    assert point["request_count"] == 60_000
    assert point["error_count"] == 0
    assert point["threshold_successes"] == point["threshold_trials"] == 60_000
    assert point["safe_qps_milli"] == 270_000
    assert point["repeated_run_count"] == 3
    assert point["error_rate_wilson_upper_ppm"] <= 1_000


def test_capacity_point_rejects_histogram_count_mismatch() -> None:
    histogram = Histogram().observe(1)
    runs = [RunMetrics(histogram, 2, 0, 1, 1, 0, 0, 0), RunMetrics(histogram, 2, 0, 1, 1, 0, 0, 0)]
    try:
        characterize_point(runs, 120, 1_000)
    except ValueError as error:
        assert "match raw request counts" in str(error)
    else:
        raise AssertionError("inconsistent raw and histogram counts were accepted")


def test_zero_error_slo_does_not_tolerate_one_injected_error() -> None:
    histogram = Histogram().observe(10)
    point = characterize_point([RunMetrics(histogram, 1, 1, 100, 1, 0, 0, 0)], 120, 0)
    assert point["stable_across_runs"] is False


def test_deterministic_randomized_histogram_merge_conserves_samples() -> None:
    randomizer = random.Random(0xF1EE7)
    for _ in range(100):
        histograms = [Histogram() for _ in range(3)]
        for _ in range(randomizer.randrange(1, 200)):
            index = randomizer.randrange(len(histograms))
            histograms[index] = histograms[index].observe(randomizer.randrange(0, 1_500))
        pooled = pooled_histogram(histograms)
        assert pooled.count == sum(histogram.count for histogram in histograms)
        assert pooled.sum_ms == sum(histogram.sum_ms for histogram in histograms)
        assert pooled.counts == tuple(
            sum(hist.counts[index] for hist in histograms) for index in range(len(pooled.counts))
        )
