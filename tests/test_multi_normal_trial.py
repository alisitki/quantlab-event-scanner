from datetime import datetime, timedelta, timezone

import pytest

from quantlab_event_scanner.multi_normal_trial import (
    ACTIVITY_DISTANCE_METRICS,
    DEFAULT_MIN_ANCHOR_SPACING_SECONDS,
    QualityPassedNormalCandidate,
    activity_distance,
    default_phase2j_run_id,
    expected_multi_normal_profile_rows,
    exclusion_reason_counts,
    normal_percentile_rank,
    normal_zero_or_near_zero_count,
    select_activity_matched_candidates,
    z_score_vs_normal,
)


def test_default_phase2j_run_id_uses_utc_timestamp() -> None:
    now = datetime(2026, 4, 27, 10, 20, 30, tzinfo=timezone.utc)

    assert default_phase2j_run_id(now) == "phase2j_20260427T102030Z"


def test_activity_distance_uses_expected_exchange_metric_grid() -> None:
    exchanges = ("binance", "bybit", "okx")
    event_values = {
        (exchange, metric): 10.0
        for exchange in exchanges
        for metric in ACTIVITY_DISTANCE_METRICS
    }
    normal_values = {
        (exchange, metric): 10.0
        for exchange in exchanges
        for metric in ACTIVITY_DISTANCE_METRICS
    }

    assert activity_distance(event_values, normal_values, exchanges=exchanges) == 0.0


def test_activity_distance_fails_when_required_source_cell_is_missing() -> None:
    exchanges = ("binance", "bybit", "okx")
    event_values = {
        (exchange, metric): 10.0
        for exchange in exchanges
        for metric in ACTIVITY_DISTANCE_METRICS
    }
    normal_values = dict(event_values)
    normal_values.pop(("okx", "trade_notional_sum"))

    with pytest.raises(ValueError, match="okx.trade_notional_sum"):
        activity_distance(event_values, normal_values, exchanges=exchanges)


def test_exclusion_reason_counts_are_deterministic() -> None:
    counts = exclusion_reason_counts(
        (
            None,
            "candidate_inside_normal_window",
            "candidate_inside_normal_window",
            "candidate_within_anchor_proximity",
        )
    )

    assert counts == {
        "candidate_inside_normal_window": 2,
        "candidate_within_anchor_proximity": 1,
        "eligible": 1,
    }


def test_select_activity_matched_candidates_orders_after_quality_pass() -> None:
    base = datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc)
    candidates = (
        QualityPassedNormalCandidate(base + timedelta(seconds=7200), 0.2, 9999),
        QualityPassedNormalCandidate(base + timedelta(seconds=3600), 0.1, 2000),
        QualityPassedNormalCandidate(base + timedelta(seconds=10800), 0.1, 5000),
    )

    selected = select_activity_matched_candidates(candidates, selected_count=2)

    assert [candidate.normal_anchor_ts for candidate in selected] == [
        base + timedelta(seconds=10800),
        base + timedelta(seconds=3600),
    ]
    assert [candidate.normal_sample_id for candidate in selected] == ["normal_001", "normal_002"]


def test_select_activity_matched_candidates_enforces_min_anchor_spacing() -> None:
    base = datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc)
    candidates = (
        QualityPassedNormalCandidate(base, 0.1, 5000),
        QualityPassedNormalCandidate(
            base + timedelta(seconds=DEFAULT_MIN_ANCHOR_SPACING_SECONDS - 1),
            0.2,
            5000,
        ),
        QualityPassedNormalCandidate(
            base + timedelta(seconds=DEFAULT_MIN_ANCHOR_SPACING_SECONDS),
            0.3,
            5000,
        ),
    )

    selected = select_activity_matched_candidates(candidates, selected_count=3)

    assert [candidate.normal_anchor_ts for candidate in selected] == [
        base,
        base + timedelta(seconds=DEFAULT_MIN_ANCHOR_SPACING_SECONDS),
    ]


def test_distribution_helpers_handle_zero_counts_percentiles_and_z_scores() -> None:
    values = (None, 0.0, 1e-12, 3.0, -1e-12)

    assert normal_zero_or_near_zero_count(values) == 3
    assert normal_percentile_rank(2.0, values) == 0.75
    assert normal_percentile_rank(None, values) is None
    assert z_score_vs_normal(12.0, 10.0, 2.0) == 1.0
    assert z_score_vs_normal(12.0, 10.0, 0.0) is None


def test_expected_multi_normal_profile_rows_scales_single_normal_contract() -> None:
    assert expected_multi_normal_profile_rows(30, selected_count=10) == 300
