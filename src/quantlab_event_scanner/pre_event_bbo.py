"""Pure helpers for trial BBO pre-event window extraction."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterable


REQUIRED_BBO_COLUMNS = (
    "ts_event",
    "ts_recv",
    "exchange",
    "symbol",
    "bid_price",
    "bid_qty",
    "ask_price",
    "ask_qty",
)


@dataclass(frozen=True)
class BboMetrics:
    mid_price: Decimal | None
    spread: Decimal | None
    spread_bps: Decimal | None
    book_imbalance: Decimal | None


def default_phase2b_run_id(now: datetime) -> str:
    """Return a stable Phase 2B run ID for the given timestamp."""

    utc_now = _as_utc(now)
    return f"phase2b_{utc_now.strftime('%Y%m%dT%H%M%SZ')}"


def missing_required_bbo_columns(columns: Iterable[str]) -> tuple[str, ...]:
    """Return required BBO columns missing from an input column list."""

    column_set = set(columns)
    return tuple(column for column in REQUIRED_BBO_COLUMNS if column not in column_set)


def missing_required_exchanges(
    expected_exchanges: Iterable[str],
    selected_exchanges: Iterable[str],
) -> tuple[str, ...]:
    """Return expected exchanges missing from selected partition coverage."""

    selected = {exchange.lower() for exchange in selected_exchanges}
    return tuple(
        exchange.lower()
        for exchange in expected_exchanges
        if exchange.lower() not in selected
    )


def validate_required_exchange_coverage(
    expected_exchanges: Iterable[str],
    selected_exchanges: Iterable[str],
    *,
    allow_partial_coverage: bool,
) -> tuple[str, ...]:
    """Validate strict exchange coverage and return any missing exchanges."""

    missing = missing_required_exchanges(expected_exchanges, selected_exchanges)
    if missing and not allow_partial_coverage:
        raise ValueError(
            "Missing required BBO exchange coverage: "
            f"{', '.join(missing)}. Pass allow_partial_coverage=true to continue."
        )
    return missing


def calculate_bbo_metrics(
    bid_price: Decimal,
    bid_qty: Decimal,
    ask_price: Decimal,
    ask_qty: Decimal,
) -> BboMetrics:
    """Calculate basic BBO metrics using decimal arithmetic."""

    mid_price = (bid_price + ask_price) / Decimal("2")
    spread = ask_price - bid_price
    spread_bps = None
    if mid_price != 0:
        spread_bps = spread / mid_price * Decimal("10000")

    qty_sum = bid_qty + ask_qty
    book_imbalance = None
    if qty_sum != 0:
        book_imbalance = (bid_qty - ask_qty) / qty_sum

    return BboMetrics(
        mid_price=mid_price,
        spread=spread,
        spread_bps=spread_bps,
        book_imbalance=book_imbalance,
    )


def count_negative_spreads(spreads: Iterable[Decimal | None]) -> int:
    """Return the number of non-null negative spreads."""

    return sum(1 for spread in spreads if spread is not None and spread < 0)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
