"""Pure helpers for Phase 2C trial market snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping


METADATA_COMPATIBILITY_FIELDS = (
    "event_id",
    "source_event_run_id",
    "event_start_ts",
    "window_start_ts",
    "window_end_ts",
    "lookback_seconds",
)


@dataclass(frozen=True)
class ExchangeCoverage:
    expected: tuple[str, ...]
    observed: tuple[str, ...]
    missing: tuple[str, ...]


@dataclass(frozen=True)
class BboFillFlags:
    has_bbo_update: bool
    is_bbo_forward_filled: bool


def default_phase2c_run_id(now: datetime) -> str:
    """Return a stable Phase 2C run ID for the given timestamp."""

    utc_now = _as_utc(now)
    return f"phase2c_{utc_now.strftime('%Y%m%dT%H%M%SZ')}"


def second_before_event_values(
    window_start_ts: datetime,
    event_start_ts: datetime,
) -> tuple[int, ...]:
    """Return integer second offsets for half-open [window_start_ts, event_start_ts)."""

    start = _as_utc(window_start_ts)
    end = _as_utc(event_start_ts)
    total_seconds = int((end - start).total_seconds())
    if total_seconds <= 0:
        raise ValueError("event_start_ts must be after window_start_ts.")
    if start.timestamp() + total_seconds != end.timestamp():
        raise ValueError("Window duration must be an integer number of seconds.")
    return tuple(range(total_seconds, 0, -1))


def normalize_trade_side(side: object) -> str | None:
    """Normalize common trade side values to buy/sell."""

    if side is None:
        return None
    normalized = str(side).strip().lower()
    if normalized in {"buy", "b"}:
        return "buy"
    if normalized in {"sell", "s"}:
        return "sell"
    return None


def trade_imbalance_qty(buy_qty: float, sell_qty: float) -> float:
    """Return buy-minus-sell quantity imbalance."""

    return buy_qty - sell_qty


def bbo_fill_flags(update_count: int, has_prior_in_window_quote: bool) -> BboFillFlags:
    """Return explicit BBO update/forward-fill flags for one snapshot second."""

    if update_count < 0:
        raise ValueError("update_count must be non-negative.")
    has_update = update_count > 0
    return BboFillFlags(
        has_bbo_update=has_update,
        is_bbo_forward_filled=(not has_update and has_prior_in_window_quote),
    )


def bbo_update_stats_for_second(
    update_count: int,
    *,
    avg_spread_bps: float | None,
    max_spread_bps: float | None,
    avg_book_imbalance: float | None,
) -> tuple[float | None, float | None, float | None]:
    """Return update-based BBO stats, never forward-filled for empty seconds."""

    if update_count <= 0:
        return (None, None, None)
    return (avg_spread_bps, max_spread_bps, avg_book_imbalance)


def exchange_coverage(
    expected_exchanges: tuple[str, ...],
    observed_exchanges: tuple[str, ...],
) -> ExchangeCoverage:
    """Return expected, observed, and missing exchanges case-insensitively."""

    expected = tuple(exchange.lower() for exchange in expected_exchanges)
    observed = tuple(sorted({exchange.lower() for exchange in observed_exchanges}))
    observed_set = set(observed)
    missing = tuple(exchange for exchange in expected if exchange not in observed_set)
    return ExchangeCoverage(expected=expected, observed=observed, missing=missing)


def validate_window_metadata_compatibility(
    trade_metadata: Mapping[str, Any],
    bbo_metadata: Mapping[str, Any],
) -> None:
    """Require trade and BBO trial windows to describe the same event window."""

    mismatches = []
    for field in METADATA_COMPATIBILITY_FIELDS:
        if trade_metadata.get(field) != bbo_metadata.get(field):
            mismatches.append(field)
    if mismatches:
        raise ValueError(
            "Trade/BBO window metadata mismatch for fields: "
            f"{', '.join(mismatches)}"
        )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
