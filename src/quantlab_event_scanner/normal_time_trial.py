"""Pure helpers for Phase 2E normal-time trial selection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable


SAMPLE_TYPE_NORMAL = "normal"


@dataclass(frozen=True)
class NormalWindow:
    normal_window_start_ts: datetime
    normal_window_end_ts: datetime
    normal_anchor_ts: datetime


@dataclass(frozen=True)
class NormalSelectionMetadata:
    sample_type: str
    selected_date: str
    normal_window_start_ts: datetime
    normal_window_end_ts: datetime
    normal_anchor_ts: datetime
    lookback_seconds: int
    selection_reason: str
    excluded_candidate_count: int
    nearest_candidate_distance_seconds: int | None


def default_phase2e_run_id(now: datetime) -> str:
    """Return a stable Phase 2E run ID for the given timestamp."""

    utc_now = _as_utc(now)
    return f"phase2e_{utc_now.strftime('%Y%m%dT%H%M%SZ')}"


def normal_window_for_anchor(anchor_ts: datetime, lookback_seconds: int) -> NormalWindow:
    """Return the half-open normal window ending at anchor_ts."""

    anchor = _as_utc(anchor_ts)
    if lookback_seconds <= 0:
        raise ValueError("lookback_seconds must be positive.")
    return NormalWindow(
        normal_window_start_ts=anchor - timedelta(seconds=lookback_seconds),
        normal_window_end_ts=anchor,
        normal_anchor_ts=anchor,
    )


def second_before_anchor_values(lookback_seconds: int) -> tuple[int, ...]:
    """Return expected dense second offsets before a normal anchor."""

    if lookback_seconds <= 0:
        raise ValueError("lookback_seconds must be positive.")
    return tuple(range(lookback_seconds, 0, -1))


def selected_date_utc_bounds(selected_date: str) -> tuple[datetime, datetime]:
    """Return UTC start/end bounds for a YYYYMMDD selected date."""

    date_start = datetime.strptime(selected_date, "%Y%m%d").replace(tzinfo=timezone.utc)
    return date_start, date_start + timedelta(days=1)


def nearest_candidate_distance_seconds(
    anchor_ts: datetime,
    candidate_start_timestamps: Iterable[datetime],
) -> int | None:
    """Return nearest absolute candidate distance in seconds from anchor_ts."""

    anchor = _as_utc(anchor_ts)
    distances = [
        int(abs((_as_utc(candidate_ts) - anchor).total_seconds()))
        for candidate_ts in candidate_start_timestamps
    ]
    if not distances:
        return None
    return min(distances)


def exclusion_reason(
    anchor_ts: datetime,
    candidate_start_timestamps: Iterable[datetime],
    *,
    lookback_seconds: int,
    horizon_seconds: int,
    proximity_seconds: int,
) -> str | None:
    """Return why a normal anchor should be excluded, or None if eligible."""

    window = normal_window_for_anchor(anchor_ts, lookback_seconds)
    anchor = window.normal_anchor_ts
    candidates = tuple(_as_utc(candidate_ts) for candidate_ts in candidate_start_timestamps)

    for candidate_ts in candidates:
        distance = abs((candidate_ts - anchor).total_seconds())
        if distance <= proximity_seconds:
            return "candidate_within_anchor_proximity"

    post_anchor_end = anchor + timedelta(seconds=horizon_seconds)
    for candidate_ts in candidates:
        if anchor < candidate_ts <= post_anchor_end:
            return "candidate_inside_post_anchor_horizon"

    for candidate_ts in candidates:
        if window.normal_window_start_ts <= candidate_ts < window.normal_window_end_ts:
            return "candidate_inside_normal_window"

    return None


def select_first_quality_passing_window(
    candidate_anchors: Iterable[datetime],
    failed_anchors: Iterable[datetime],
) -> datetime | None:
    """Return earliest candidate anchor not present in failed_anchors."""

    failed = {_as_utc(anchor) for anchor in failed_anchors}
    eligible = sorted(_as_utc(anchor) for anchor in candidate_anchors if _as_utc(anchor) not in failed)
    if not eligible:
        return None
    return eligible[0]


def build_normal_selection_metadata(
    *,
    selected_date: str,
    window: NormalWindow,
    lookback_seconds: int,
    selection_reason: str,
    excluded_candidate_count: int,
    candidate_start_timestamps: Iterable[datetime],
) -> NormalSelectionMetadata:
    """Build standard normal-time selection metadata."""

    return NormalSelectionMetadata(
        sample_type=SAMPLE_TYPE_NORMAL,
        selected_date=selected_date,
        normal_window_start_ts=window.normal_window_start_ts,
        normal_window_end_ts=window.normal_window_end_ts,
        normal_anchor_ts=window.normal_anchor_ts,
        lookback_seconds=lookback_seconds,
        selection_reason=selection_reason,
        excluded_candidate_count=excluded_candidate_count,
        nearest_candidate_distance_seconds=nearest_candidate_distance_seconds(
            window.normal_anchor_ts,
            candidate_start_timestamps,
        ),
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
