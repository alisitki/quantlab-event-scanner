"""Pure helpers for trial pre-event window extraction."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone


def default_phase2a_run_id(now: datetime) -> str:
    """Return a stable Phase 2A run ID for the given timestamp."""

    utc_now = _as_utc(now)
    return f"phase2a_{utc_now.strftime('%Y%m%dT%H%M%SZ')}"


def utc_partition_dates_for_window(
    window_start_ts: datetime,
    window_end_ts: datetime,
) -> tuple[str, ...]:
    """Return UTC YYYYMMDD partitions touched by half-open [start, end)."""

    start = _as_utc(window_start_ts)
    end = _as_utc(window_end_ts)
    if end <= start:
        raise ValueError("window_end_ts must be after window_start_ts.")

    last_included = end - timedelta(microseconds=1)
    current_date = start.date()
    end_date = last_included.date()
    dates: list[str] = []
    while current_date <= end_date:
        dates.append(current_date.strftime("%Y%m%d"))
        current_date += timedelta(days=1)
    return tuple(dates)


def is_in_half_open_window(
    timestamp: datetime,
    window_start_ts: datetime,
    window_end_ts: datetime,
) -> bool:
    """Return whether timestamp falls in [window_start_ts, window_end_ts)."""

    ts = _as_utc(timestamp)
    start = _as_utc(window_start_ts)
    end = _as_utc(window_end_ts)
    return start <= ts < end


def validate_selected_event_count(count: int) -> None:
    """Require exactly one selected event."""

    if count != 1:
        raise ValueError(f"Expected exactly one selected event, found {count}.")


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
