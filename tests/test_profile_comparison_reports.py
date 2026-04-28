from datetime import datetime, timezone

import pytest

from quantlab_event_scanner.profile_comparison_reports import (
    BUCKET_CHANGE_COMPARISON_SOURCE_METRICS,
    BUCKET_CHANGE_COMPARISON_VALUE_FIELDS,
    CROSS_EXCHANGE_MID_DIFF_COMPARISON_METRICS,
    EXCHANGE_PROFILE_COMPARISON_METRICS,
    METRIC_GROUP_CONTEXT,
    METRIC_GROUP_PRICE_DISLOCATION,
    METRIC_GROUP_SIGNAL_CANDIDATE,
    METRIC_GROUP_UNSTABLE,
    SMALL_DENOMINATOR_ABS_THRESHOLD,
    bucket_change_comparison_metric_names,
    comparison_absolute_diff,
    comparison_ratio,
    comparison_signed_diff,
    default_phase2f_run_id,
    deterministic_exchange_pair_names,
    exchange_pair_name,
    expected_bucket_change_profile_comparison_rows,
    expected_cross_exchange_mid_diff_comparison_rows,
    expected_exchange_profile_comparison_rows,
    metric_group_for_metric,
    missing_curated_metrics,
    ratio_is_unstable,
    relative_change_is_unstable,
    small_denominator_flag,
    validate_curated_metrics,
)


def test_default_phase2f_run_id_uses_utc_timestamp() -> None:
    now = datetime(2026, 4, 27, 10, 20, 30, tzinfo=timezone.utc)

    assert default_phase2f_run_id(now) == "phase2f_20260427T102030Z"


def test_curated_metric_lists_match_approved_sets() -> None:
    assert len(EXCHANGE_PROFILE_COMPARISON_METRICS) == 22
    assert EXCHANGE_PROFILE_COMPARISON_METRICS == (
        "seconds_observed",
        "seconds_with_trades",
        "seconds_with_bbo_update",
        "seconds_forward_filled_bbo",
        "trade_count_sum",
        "trade_count_per_second_avg",
        "trade_count_per_second_max",
        "trade_volume_sum",
        "trade_notional_sum",
        "buy_qty_sum",
        "sell_qty_sum",
        "trade_imbalance_qty_sum",
        "trade_imbalance_qty_per_second",
        "trade_imbalance_qty_ratio",
        "last_trade_return_bps",
        "last_mid_return_bps",
        "avg_spread_bps_mean",
        "avg_spread_bps_max",
        "last_spread_bps_last",
        "avg_book_imbalance_mean",
        "last_book_imbalance_last",
        "bbo_quote_age_seconds_max",
    )
    assert CROSS_EXCHANGE_MID_DIFF_COMPARISON_METRICS == (
        "avg_mid_diff_bps",
        "max_abs_mid_diff_bps",
        "last_mid_diff_bps",
        "last_abs_mid_diff_bps",
        "seconds_compared",
    )
    assert BUCKET_CHANGE_COMPARISON_SOURCE_METRICS == (
        "trade_count_per_second",
        "trade_imbalance_qty_per_second",
        "avg_spread_bps",
        "avg_book_imbalance",
    )
    assert BUCKET_CHANGE_COMPARISON_VALUE_FIELDS == ("absolute_change", "relative_change")


def test_missing_curated_metric_validation_fails() -> None:
    available = ("trade_count_sum", "trade_volume_sum")

    assert missing_curated_metrics(available, ("trade_count_sum", "avg_spread_bps_mean")) == (
        "avg_spread_bps_mean",
    )
    with pytest.raises(ValueError, match="avg_spread_bps_mean"):
        validate_curated_metrics(available, ("trade_count_sum", "avg_spread_bps_mean"))


def test_comparison_helpers_handle_signed_and_absolute_diff() -> None:
    assert comparison_signed_diff(4.0, 10.0) == -6.0
    assert comparison_signed_diff(None, 4.0) is None
    assert comparison_absolute_diff(4.0, 10.0) == 6.0
    assert comparison_absolute_diff(None, 4.0) is None


def test_comparison_ratio_handles_null_and_zero_normal_value() -> None:
    assert comparison_ratio(10.0, 4.0) == 2.5
    assert comparison_ratio(10.0, 0.0) is None
    assert comparison_ratio(10.0, None) is None


def test_deterministic_exchange_pair_names_use_double_underscore() -> None:
    assert exchange_pair_name("Binance", "Bybit") == "binance__bybit"
    assert deterministic_exchange_pair_names() == (
        "binance__bybit",
        "binance__okx",
        "bybit__okx",
    )


def test_expected_row_count_helpers() -> None:
    assert expected_exchange_profile_comparison_rows() == 660
    assert expected_cross_exchange_mid_diff_comparison_rows() == 150
    assert expected_bucket_change_profile_comparison_rows() == 96


def test_bucket_change_comparison_emits_both_change_fields() -> None:
    names = bucket_change_comparison_metric_names()

    assert "trade_count_per_second.absolute_change" in names
    assert "trade_count_per_second.relative_change" in names
    assert len(names) == 8


def test_metric_taxonomy_maps_approved_examples() -> None:
    assert metric_group_for_metric("trade_notional_sum") == METRIC_GROUP_CONTEXT
    assert metric_group_for_metric("seconds_compared") == METRIC_GROUP_CONTEXT
    assert (
        metric_group_for_metric("trade_imbalance_qty_ratio")
        == METRIC_GROUP_SIGNAL_CANDIDATE
    )
    assert metric_group_for_metric("avg_spread_bps.absolute_change") == (
        METRIC_GROUP_SIGNAL_CANDIDATE
    )
    assert metric_group_for_metric("last_mid_return_bps") == METRIC_GROUP_PRICE_DISLOCATION
    assert metric_group_for_metric("avg_mid_diff_bps") == METRIC_GROUP_PRICE_DISLOCATION
    assert metric_group_for_metric("avg_book_imbalance.relative_change") == METRIC_GROUP_UNSTABLE

    with pytest.raises(ValueError, match="unknown_metric"):
        metric_group_for_metric("unknown_metric")


def test_interpretation_risk_flags_handle_small_denominators() -> None:
    assert ratio_is_unstable(None)
    assert ratio_is_unstable(0.0)
    assert ratio_is_unstable(SMALL_DENOMINATOR_ABS_THRESHOLD / 2)
    assert not ratio_is_unstable(1.0)

    assert relative_change_is_unstable("avg_spread_bps.relative_change", None)
    assert relative_change_is_unstable("avg_spread_bps.relative_change", 0.0)
    assert relative_change_is_unstable(
        "avg_spread_bps.relative_change",
        1.0,
        SMALL_DENOMINATOR_ABS_THRESHOLD / 2,
    )
    assert not relative_change_is_unstable("avg_spread_bps.absolute_change", None)
    assert not relative_change_is_unstable("avg_spread_bps.relative_change", 2.0, 3.0)

    assert small_denominator_flag("last_mid_return_bps", 0.0)
    assert metric_group_for_metric("last_mid_return_bps") == METRIC_GROUP_PRICE_DISLOCATION
