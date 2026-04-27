from datetime import datetime, timezone
from decimal import Decimal

import pytest

from quantlab_event_scanner.pre_event_bbo import (
    calculate_bbo_metrics,
    count_negative_spreads,
    default_phase2b_run_id,
    missing_required_bbo_columns,
    missing_required_exchanges,
    validate_required_exchange_coverage,
)


def test_default_phase2b_run_id_uses_utc_timestamp() -> None:
    now = datetime(2026, 4, 27, 10, 20, 30, tzinfo=timezone.utc)

    assert default_phase2b_run_id(now) == "phase2b_20260427T102030Z"


def test_missing_required_exchange_coverage_is_case_insensitive() -> None:
    missing = missing_required_exchanges(
        expected_exchanges=("binance", "bybit", "okx"),
        selected_exchanges=("BINANCE", "okx"),
    )

    assert missing == ("bybit",)


def test_validate_required_exchange_coverage_fails_in_strict_mode() -> None:
    with pytest.raises(ValueError, match="Missing required BBO exchange coverage: bybit"):
        validate_required_exchange_coverage(
            expected_exchanges=("binance", "bybit", "okx"),
            selected_exchanges=("binance", "okx"),
            allow_partial_coverage=False,
        )


def test_validate_required_exchange_coverage_returns_missing_in_partial_mode() -> None:
    missing = validate_required_exchange_coverage(
        expected_exchanges=("binance", "bybit", "okx"),
        selected_exchanges=("binance",),
        allow_partial_coverage=True,
    )

    assert missing == ("bybit", "okx")


def test_calculate_bbo_metrics() -> None:
    metrics = calculate_bbo_metrics(
        bid_price=Decimal("100"),
        bid_qty=Decimal("3"),
        ask_price=Decimal("101"),
        ask_qty=Decimal("1"),
    )

    assert metrics.mid_price == Decimal("100.5")
    assert metrics.spread == Decimal("1")
    assert metrics.spread_bps == Decimal("99.50248756218905472636815920")
    assert metrics.book_imbalance == Decimal("0.5")


def test_calculate_bbo_metrics_handles_zero_denominators() -> None:
    metrics = calculate_bbo_metrics(
        bid_price=Decimal("-1"),
        bid_qty=Decimal("0"),
        ask_price=Decimal("1"),
        ask_qty=Decimal("0"),
    )

    assert metrics.mid_price == Decimal("0")
    assert metrics.spread == Decimal("2")
    assert metrics.spread_bps is None
    assert metrics.book_imbalance is None


def test_count_negative_spreads_ignores_nulls() -> None:
    assert count_negative_spreads((Decimal("1"), Decimal("-0.01"), None, Decimal("-2"))) == 2


def test_missing_required_bbo_columns() -> None:
    missing = missing_required_bbo_columns(
        ("ts_event", "ts_recv", "exchange", "symbol", "bid_price", "bid_qty")
    )

    assert missing == ("ask_price", "ask_qty")
