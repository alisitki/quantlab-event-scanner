from datetime import datetime, timezone

import pytest

from quantlab_event_scanner.pre_event_market_snapshots import (
    bbo_fill_flags,
    bbo_update_stats_for_second,
    default_phase2c_run_id,
    exchange_coverage,
    normalize_trade_side,
    second_before_event_values,
    trade_imbalance_qty,
    validate_window_metadata_compatibility,
)


def test_default_phase2c_run_id_uses_utc_timestamp() -> None:
    now = datetime(2026, 4, 27, 10, 20, 30, tzinfo=timezone.utc)

    assert default_phase2c_run_id(now) == "phase2c_20260427T102030Z"


def test_second_before_event_values_for_half_open_window() -> None:
    values = second_before_event_values(
        datetime(2026, 4, 23, 9, 59, 10, tzinfo=timezone.utc),
        datetime(2026, 4, 23, 10, 4, 10, tzinfo=timezone.utc),
    )

    assert values[0] == 300
    assert values[-1] == 1
    assert len(values) == 300


def test_validate_window_metadata_compatibility_rejects_mismatch() -> None:
    trade_metadata = {
        "event_id": "event_1",
        "source_event_run_id": "phase1d",
        "event_start_ts": datetime(2026, 4, 23, 10, 4, 10),
        "window_start_ts": datetime(2026, 4, 23, 9, 59, 10),
        "window_end_ts": datetime(2026, 4, 23, 10, 4, 10),
        "lookback_seconds": 300,
    }
    bbo_metadata = dict(trade_metadata)
    bbo_metadata["window_start_ts"] = datetime(2026, 4, 23, 9, 59, 11)

    with pytest.raises(ValueError, match="window_start_ts"):
        validate_window_metadata_compatibility(trade_metadata, bbo_metadata)


def test_exchange_coverage_reports_missing_case_insensitively() -> None:
    coverage = exchange_coverage(
        ("binance", "bybit", "okx"),
        ("BINANCE", "okx"),
    )

    assert coverage.expected == ("binance", "bybit", "okx")
    assert coverage.observed == ("binance", "okx")
    assert coverage.missing == ("bybit",)


def test_bbo_fill_flags_distinguish_update_forward_fill_and_pre_first_update() -> None:
    assert bbo_fill_flags(3, has_prior_in_window_quote=False).has_bbo_update is True
    assert bbo_fill_flags(3, has_prior_in_window_quote=False).is_bbo_forward_filled is False

    forward_filled = bbo_fill_flags(0, has_prior_in_window_quote=True)
    assert forward_filled.has_bbo_update is False
    assert forward_filled.is_bbo_forward_filled is True

    pre_first_update = bbo_fill_flags(0, has_prior_in_window_quote=False)
    assert pre_first_update.has_bbo_update is False
    assert pre_first_update.is_bbo_forward_filled is False


def test_bbo_update_stats_are_null_when_there_is_no_update() -> None:
    stats = bbo_update_stats_for_second(
        0,
        avg_spread_bps=1.5,
        max_spread_bps=2.0,
        avg_book_imbalance=0.25,
    )

    assert stats == (None, None, None)


def test_trade_side_normalization_and_imbalance() -> None:
    assert normalize_trade_side("BUY") == "buy"
    assert normalize_trade_side("b") == "buy"
    assert normalize_trade_side("Sell") == "sell"
    assert normalize_trade_side("s") == "sell"
    assert normalize_trade_side("unknown") is None
    assert normalize_trade_side(None) is None
    assert trade_imbalance_qty(7.5, 2.0) == 5.5
