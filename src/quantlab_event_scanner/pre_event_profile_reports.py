"""Pure helpers for Phase 2D trial pre-event profile reports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


PROFILE_VERSION = "pre_event_profile_v1"


@dataclass(frozen=True)
class ProfileWindow:
    mode: str
    label: str
    min_second_before_event: int
    max_second_before_event: int


TRAILING_WINDOWS = (
    ProfileWindow("trailing", "last_300s", 1, 300),
    ProfileWindow("trailing", "last_120s", 1, 120),
    ProfileWindow("trailing", "last_60s", 1, 60),
    ProfileWindow("trailing", "last_30s", 1, 30),
    ProfileWindow("trailing", "last_10s", 1, 10),
)

BUCKET_WINDOWS = (
    ProfileWindow("bucket", "bucket_300_120s", 121, 300),
    ProfileWindow("bucket", "bucket_120_60s", 61, 120),
    ProfileWindow("bucket", "bucket_60_30s", 31, 60),
    ProfileWindow("bucket", "bucket_30_10s", 11, 30),
    ProfileWindow("bucket", "bucket_10_0s", 1, 10),
)

PROFILE_WINDOWS = (*TRAILING_WINDOWS, *BUCKET_WINDOWS)

BUCKET_CHANGE_PAIRS = (
    ("bucket_300_120s", "bucket_120_60s"),
    ("bucket_120_60s", "bucket_60_30s"),
    ("bucket_60_30s", "bucket_30_10s"),
    ("bucket_30_10s", "bucket_10_0s"),
)


def default_phase2d_run_id(now: datetime) -> str:
    """Return a stable Phase 2D run ID for the given timestamp."""

    utc_now = _as_utc(now)
    return f"phase2d_{utc_now.strftime('%Y%m%dT%H%M%SZ')}"


def first_second_before_event(window: ProfileWindow) -> int:
    """Return the oldest second in a profile window."""

    return window.max_second_before_event


def last_second_before_event(window: ProfileWindow) -> int:
    """Return the closest-to-event second in a profile window."""

    return window.min_second_before_event


def safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    """Return numerator / denominator, null-safe with zero-denominator protection."""

    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def return_bps(first_price: float | None, last_price: float | None) -> float | None:
    """Return price change in basis points from oldest to closest-to-event point."""

    ratio = safe_ratio(last_price, first_price)
    if ratio is None:
        return None
    return (ratio - 1.0) * 10000.0


def mid_diff_bps(
    mid_exchange_a: float | None,
    mid_exchange_b: float | None,
) -> float | None:
    """Return mid-price difference bps using exchange B as denominator."""

    if mid_exchange_a is None or mid_exchange_b is None or mid_exchange_b == 0:
        return None
    return (mid_exchange_a - mid_exchange_b) / mid_exchange_b * 10000.0


def absolute_change(
    from_bucket_value: float | None,
    to_bucket_value: float | None,
) -> float | None:
    """Return to-minus-from change, null-safe."""

    if from_bucket_value is None or to_bucket_value is None:
        return None
    return to_bucket_value - from_bucket_value


def relative_change(
    from_bucket_value: float | None,
    to_bucket_value: float | None,
) -> float | None:
    """Return adjacent bucket relative change with zero-denominator protection."""

    change = absolute_change(from_bucket_value, to_bucket_value)
    if change is None or from_bucket_value is None or from_bucket_value == 0:
        return None
    return change / abs(from_bucket_value)


def bucket_seconds_covered() -> tuple[int, ...]:
    """Return all second_before_event values covered by bucket windows."""

    values: list[int] = []
    for window in BUCKET_WINDOWS:
        values.extend(
            range(window.min_second_before_event, window.max_second_before_event + 1)
        )
    return tuple(sorted(values))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
