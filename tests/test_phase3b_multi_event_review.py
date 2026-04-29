from datetime import datetime, timezone
from pathlib import Path

import pytest

from quantlab_event_scanner.phase3b_multi_event_review import (
    CONTEXT_DOMINANCE_REPORT_COLUMNS,
    DIRECTION_SIGN_CONSISTENCY_REPORT_COLUMNS,
    EVENT_NARRATIVE_REPORT_COLUMNS,
    METRIC_RECURRENT_REPORT_COLUMNS,
    POLICY_CONTEXT_FILTER,
    POLICY_CORE_FEATURE,
    POLICY_DIAGNOSTIC,
    POLICY_EXCLUDE,
    PROFILE_FAMILY_PHASE3A_TOP_DIFF,
    RECOMMENDED_METRIC_POLICY_COLUMNS,
    REQUIRED_COMPARISON_SUBTABLES,
    REQUIRED_SUMMARY_SUBTABLES,
    REQUIRED_TOP_DIFF_SUBTABLES,
    TOP_DIFF_REQUIRED_COLUMNS,
    binance_relevance,
    context_dominance_report_rows,
    default_phase3b_review_run_id,
    direction_normalized_sign,
    direction_sign_consistency_report_rows,
    event_narrative_row,
    expected_sign_policy,
    is_event_proximate_window,
    is_suspected_late_reaction_window,
    metric_recurrence_report_rows,
    missing_required_columns,
    percentile_extremeness,
    recommend_metric_policy,
    sign_consistency_label,
    validate_phase3b_review_run_id,
    validate_required_columns,
)
from quantlab_event_scanner.profile_comparison_reports import (
    METRIC_GROUP_CONTEXT,
    METRIC_GROUP_PRICE_DISLOCATION,
    METRIC_GROUP_SIGNAL_CANDIDATE,
    METRIC_GROUP_UNSTABLE,
)


def _row(
    *,
    event_id: str = "event_001",
    direction: str = "UP",
    metric_group: str = METRIC_GROUP_PRICE_DISLOCATION,
    metric_name: str = "last_mid_return_bps",
    signed_diff: float = 2.0,
    rank: int = 1,
    rank_type: str = "top_absolute_diff_vs_mean",
    exchange: str = "binance",
    exchange_pair: str | None = None,
    window_label: str = "bucket_30_10s",
    denominator_risk: bool = False,
) -> dict:
    return {
        "profile_family": PROFILE_FAMILY_PHASE3A_TOP_DIFF,
        "source_event_id": event_id,
        "event_direction": direction,
        "report_group": "exchange_profile",
        "metric_group": metric_group,
        "metric_name": metric_name,
        "exchange": exchange,
        "exchange_pair": exchange_pair,
        "symbol_key": "btcusdt",
        "window_label": window_label,
        "from_bucket": None,
        "to_bucket": None,
        "rank_type": rank_type,
        "rank": rank,
        "signed_diff_vs_mean": signed_diff,
        "absolute_diff_vs_mean": abs(signed_diff),
        "z_score_vs_normal": signed_diff * 2,
        "normal_percentile_rank": 0.95 if signed_diff > 0 else 0.05,
        "ratio_unstable": denominator_risk,
        "relative_change_unstable": False,
        "small_denominator_flag": denominator_risk,
    }


def test_phase3b_run_id_validation() -> None:
    now = datetime(2026, 4, 29, 10, 20, 30, tzinfo=timezone.utc)

    assert default_phase3b_review_run_id(now) == "phase3b_20260429T102030Z"
    assert validate_phase3b_review_run_id(" phase3b_20260429T102030Z ") == (
        "phase3b_20260429T102030Z"
    )

    with pytest.raises(ValueError, match="must not be empty"):
        validate_phase3b_review_run_id("")
    with pytest.raises(ValueError, match="must start"):
        validate_phase3b_review_run_id("phase3a_20260429T102030Z")
    with pytest.raises(ValueError, match="not a path"):
        validate_phase3b_review_run_id("phase3b_20260429T102030Z/review")


def test_schema_and_input_contract_constants_are_stable() -> None:
    assert METRIC_RECURRENT_REPORT_COLUMNS[0] == "review_run_id"
    assert "direction_normalized_sign_consistency" in METRIC_RECURRENT_REPORT_COLUMNS
    assert DIRECTION_SIGN_CONSISTENCY_REPORT_COLUMNS[-1] == "created_at"
    assert CONTEXT_DOMINANCE_REPORT_COLUMNS == (
        "review_run_id",
        "source_run_id",
        "profile_family",
        "report_group",
        "rank_type",
        "top_k",
        "context_metric_count",
        "signal_candidate_count",
        "price_dislocation_count",
        "unstable_count",
        "denominator_risk_count",
        "context_share",
        "unstable_share",
        "denominator_risk_share",
        "dominant_metric_group",
        "recommended_action",
        "created_at",
    )
    assert EVENT_NARRATIVE_REPORT_COLUMNS[2] == "event_id"
    assert RECOMMENDED_METRIC_POLICY_COLUMNS[8] == "policy_bucket"
    assert REQUIRED_TOP_DIFF_SUBTABLES == (
        "top_diffs/exchange_profile_top_diffs",
        "top_diffs/cross_exchange_mid_diff_top_diffs",
        "top_diffs/bucket_change_profile_top_diffs",
    )
    assert "comparison_reports/exchange_profile_comparison" in REQUIRED_COMPARISON_SUBTABLES
    assert "summary/event_processing_status" in REQUIRED_SUMMARY_SUBTABLES
    assert "signed_diff_vs_mean" in TOP_DIFF_REQUIRED_COLUMNS


def test_required_column_validation_reports_missing_columns() -> None:
    available = tuple(column for column in TOP_DIFF_REQUIRED_COLUMNS if column != "rank")

    assert missing_required_columns(available, TOP_DIFF_REQUIRED_COLUMNS) == ("rank",)
    with pytest.raises(ValueError, match="rank"):
        validate_required_columns(available, TOP_DIFF_REQUIRED_COLUMNS, label="top_diffs")


def test_direction_sign_helpers_normalize_up_and_down_moves() -> None:
    assert direction_normalized_sign("UP", 4.0) == 1
    assert direction_normalized_sign("UP", -4.0) == -1
    assert direction_normalized_sign("DOWN", -4.0) == 1
    assert direction_normalized_sign("DOWN", 4.0) == -1
    assert direction_normalized_sign("DOWN", 0.0) == 0

    assert sign_consistency_label([1, 1, 0]) == ("positive", 1.0)
    assert sign_consistency_label([1, -1, -1]) == ("mixed", pytest.approx(2 / 3))
    assert sign_consistency_label([0, 0]) == ("none", None)


def test_metric_recurrence_counts_events_and_demotes_denominator_risk() -> None:
    created_at = datetime(2026, 4, 29, tzinfo=timezone.utc)
    rows = [
        _row(event_id="event_001", direction="UP", signed_diff=2.0, rank=1),
        _row(event_id="event_002", direction="DOWN", signed_diff=-3.0, rank=2),
        _row(event_id="event_003", direction="DOWN", signed_diff=-1.0, rank=30),
        _row(event_id="event_004", direction="UP", signed_diff=4.0, rank=3, denominator_risk=True),
    ]

    report = metric_recurrence_report_rows(
        rows,
        review_run_id="phase3b_test",
        source_run_id="phase3a_test",
        source_event_count=10,
        top_k=20,
        created_at=created_at,
    )

    assert len(report) == 1
    row = report[0]
    assert row["n_events_seen_in_top_k"] == 3
    assert row["n_up_events_seen"] == 2
    assert row["n_down_events_seen"] == 1
    assert row["median_rank"] == 2.0
    assert row["denominator_risk_event_count"] == 1
    assert row["direction_normalized_sign_consistency"] == "positive"
    assert row["recommendation"] == POLICY_DIAGNOSTIC


def test_direction_sign_consistency_counts_up_down_signs() -> None:
    created_at = datetime(2026, 4, 29, tzinfo=timezone.utc)
    report = direction_sign_consistency_report_rows(
        [
            _row(event_id="event_001", direction="UP", signed_diff=2.0),
            _row(event_id="event_002", direction="DOWN", signed_diff=-3.0),
            _row(event_id="event_003", direction="DOWN", signed_diff=1.0),
        ],
        review_run_id="phase3b_test",
        source_run_id="phase3a_test",
        created_at=created_at,
    )

    row = report[0]
    assert row["up_positive_count"] == 1
    assert row["down_negative_count"] == 1
    assert row["down_positive_count"] == 1
    assert row["observed_positive_count"] == 2
    assert row["observed_negative_count"] == 1
    assert row["direction_normalized_sign"] == "mixed"


def test_context_dominance_calculates_shares_and_action() -> None:
    created_at = datetime(2026, 4, 29, tzinfo=timezone.utc)
    rows = [
        _row(metric_group=METRIC_GROUP_CONTEXT, metric_name="trade_count_sum", rank=1),
        _row(metric_group=METRIC_GROUP_CONTEXT, metric_name="trade_volume_sum", rank=2),
        _row(metric_group=METRIC_GROUP_SIGNAL_CANDIDATE, metric_name="avg_spread_bps_mean", rank=3),
        _row(metric_group=METRIC_GROUP_UNSTABLE, metric_name="avg_spread_bps.relative_change", rank=4),
    ]

    report = context_dominance_report_rows(
        rows,
        review_run_id="phase3b_test",
        source_run_id="phase3a_test",
        top_k=20,
        created_at=created_at,
    )

    row = report[0]
    assert row["context_metric_count"] == 2
    assert row["signal_candidate_count"] == 1
    assert row["unstable_count"] == 1
    assert row["context_share"] == 0.5
    assert row["recommended_action"] == "review_context_as_filter"


def test_metric_policy_bucket_assignment_preserves_phase3b_limits() -> None:
    core = recommend_metric_policy(
        metric_name="last_mid_return_bps",
        metric_group=METRIC_GROUP_PRICE_DISLOCATION,
        denominator_risk_event_count=0,
        source_event_count=10,
        n_events_seen_in_top_k=4,
    )
    context = recommend_metric_policy(
        metric_name="trade_count_sum",
        metric_group=METRIC_GROUP_CONTEXT,
        denominator_risk_event_count=0,
        source_event_count=10,
        n_events_seen_in_top_k=8,
    )
    unstable = recommend_metric_policy(
        metric_name="avg_spread_bps.relative_change",
        metric_group=METRIC_GROUP_UNSTABLE,
        denominator_risk_event_count=0,
        source_event_count=10,
        n_events_seen_in_top_k=3,
    )
    excluded = recommend_metric_policy(
        metric_name="trade_imbalance_qty_ratio",
        metric_group=METRIC_GROUP_SIGNAL_CANDIDATE,
        denominator_risk_event_count=10,
        source_event_count=10,
        n_events_seen_in_top_k=10,
    )

    assert core.policy_bucket == POLICY_CORE_FEATURE
    assert core.requires_more_data
    assert core.phase4b_candidate
    assert context.policy_bucket == POLICY_CONTEXT_FILTER
    assert unstable.policy_bucket == POLICY_DIAGNOSTIC
    assert excluded.policy_bucket == POLICY_EXCLUDE


def test_event_narrative_generation_and_classifiers() -> None:
    created_at = datetime(2026, 4, 29, tzinfo=timezone.utc)
    event = {
        "event_id": "event_001",
        "event_rank": 1,
        "symbol": "BTCUSDT",
        "direction": "DOWN",
        "event_start_ts": created_at,
        "event_end_ts": created_at,
    }
    narrative = event_narrative_row(
        event,
        [
            _row(metric_name="avg_book_imbalance_mean", window_label="last_10s"),
            _row(metric_name="last_mid_return_bps", exchange_pair="binance__okx"),
        ],
        review_run_id="phase3b_test",
        source_run_id="phase3a_test",
        created_at=created_at,
        normal_sample_quality="selected_normal_count_valid",
    )

    assert narrative["event_id"] == "event_001"
    assert narrative["book_spread_pattern"] == "present_rows=1"
    assert narrative["price_return_pattern"] == "present_rows=1"
    assert narrative["suspected_late_reaction_flags"] == "last_10s"
    assert is_event_proximate_window("bucket_30_10s")
    assert is_suspected_late_reaction_window("bucket_10_0s")
    assert percentile_extremeness(0.1) == pytest.approx(0.9)
    assert binance_relevance("binance", None) == "binance_exchange"
    assert binance_relevance(None, "binance__okx") == "binance_pair"
    assert expected_sign_policy(METRIC_GROUP_PRICE_DISLOCATION, "last_abs_mid_diff_bps") == (
        "no_directional_sign"
    )


def test_phase3b_bundle_job_is_declared_with_placeholder_source_run_id() -> None:
    bundle = Path("databricks.yml").read_text()

    assert "phase3b_btc_multi_event_content_review_classic" in bundle
    assert "jobs/16_review_btc_multi_event_content.py" in bundle
    assert "phase3a_SOURCE_RUN_ID" in bundle
    assert "phase3a_20260429T085638Z" not in bundle
