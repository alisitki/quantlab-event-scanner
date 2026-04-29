"""Pure helpers for Phase 3A BTC multi-event trials."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Literal

from .profile_comparison_top_diffs import expected_top_diff_rows_from_group_counts


EventDirection = Literal["UP", "DOWN"]

DEFAULT_MAX_EVENTS = 5
DEFAULT_NORMAL_COUNT_PER_EVENT = 10
DEFAULT_MIN_EVENTS = 1
PHASE3A_TRIAL_PREFIX = "phase3a_"

STATUS_CANDIDATE = "candidate"
STATUS_EVENT_QUALITY_FAILED = "event_quality_failed"
STATUS_NORMAL_SELECTION_FAILED = "normal_selection_failed"
STATUS_SELECTED = "selected"


@dataclass(frozen=True)
class RawMoveCandidate:
    exchange: str
    symbol: str
    direction: EventDirection
    start_ts: datetime
    end_ts: datetime
    duration_seconds: int
    base_price: float
    touch_price: float
    move_pct: float


@dataclass(frozen=True)
class GroupedMoveEvent:
    event_id: str
    event_rank: int
    exchange: str
    symbol: str
    direction: EventDirection
    event_start_ts: datetime
    event_end_ts: datetime
    duration_seconds: int
    base_price: float
    touch_price: float
    move_pct: float
    selected_date: str
    raw_candidate_count: int
    candidate_group_start_ts: datetime
    candidate_group_end_ts: datetime
    is_ambiguous: bool = False


@dataclass(frozen=True)
class EventProcessingStatus:
    event_id: str
    event_rank: int
    direction: str
    processing_stage: str
    selected: bool
    exclusion_reason: str | None = None
    normal_selection_count: int = 0
    direction_balance_note: str | None = None
    row_count_status: str | None = None


def default_phase3a_run_id(now: datetime) -> str:
    """Return a stable Phase 3A run ID for the given timestamp."""

    utc_now = _as_utc(now)
    return f"phase3a_{utc_now.strftime('%Y%m%dT%H%M%SZ')}"


def phase3a_trial_run_path(output_root: str, run_id: str) -> str:
    """Return the Phase 3A umbrella trial path."""

    return f"{output_root.rstrip('/')}/btc_multi_event_trials/_trial/run_id={run_id}"


def validate_phase3a_source_run_id(source_run_id: str) -> str:
    """Validate and normalize a Phase 3A source run id for finalization."""

    normalized = source_run_id.strip()
    if not normalized:
        raise ValueError("source_run_id must not be empty.")
    if "/" in normalized or "\\" in normalized:
        raise ValueError("source_run_id must be a run id, not a path.")
    if not normalized.startswith(PHASE3A_TRIAL_PREFIX):
        raise ValueError("source_run_id must start with 'phase3a_'.")
    return normalized


def phase3a_trial_subpath(output_root: str, source_run_id: str, *parts: str) -> str:
    """Return a subpath under a Phase 3A trial run."""

    run_id = validate_phase3a_source_run_id(source_run_id)
    base = phase3a_trial_run_path(output_root, run_id)
    suffix = "/".join(part.strip("/") for part in parts if part.strip("/"))
    if not suffix:
        return base
    return f"{base}/{suffix}"


def group_time_clustered_candidates(
    candidates: Iterable[RawMoveCandidate],
    *,
    horizon_seconds: int,
) -> tuple[GroupedMoveEvent, ...]:
    """Group raw move candidates by connected horizon windows.

    Candidates are grouped within exchange + symbol + direction. Each raw start
    owns a `[start_ts, start_ts + horizon_seconds]` detection interval; a later
    candidate joins the active group when its interval overlaps or touches the
    current group interval.
    """

    if horizon_seconds <= 0:
        raise ValueError("horizon_seconds must be positive.")

    ordered = sorted(
        candidates,
        key=lambda item: (
            item.exchange.lower(),
            item.symbol.lower(),
            item.direction,
            _as_utc(item.start_ts),
            _as_utc(item.end_ts),
        ),
    )
    grouped: list[dict[str, object]] = []
    active: dict[tuple[str, str, str], int] = {}
    group_numbers: dict[tuple[str, str, str], int] = {}

    for candidate in ordered:
        exchange = candidate.exchange.lower()
        symbol = candidate.symbol
        symbol_key = symbol.lower()
        direction = candidate.direction
        key = (exchange, symbol_key, direction)
        start = _drop_tz(_as_utc(candidate.start_ts))
        end = _drop_tz(_as_utc(candidate.end_ts))
        horizon_end = start + timedelta(seconds=horizon_seconds)

        active_index = active.get(key)
        starts_new_group = active_index is None
        if active_index is not None:
            current_interval_end = grouped[active_index]["_cluster_interval_end_ts"]
            starts_new_group = start > current_interval_end

        if starts_new_group:
            group_number = group_numbers.get(key, 0) + 1
            group_numbers[key] = group_number
            selected_date = start.strftime("%Y%m%d")
            grouped.append(
                {
                    "event_id": _format_phase3a_event_id(
                        exchange,
                        symbol,
                        selected_date,
                        direction,
                        group_number,
                    ),
                    "event_rank": 0,
                    "exchange": exchange,
                    "symbol": symbol,
                    "direction": direction,
                    "event_start_ts": start,
                    "event_end_ts": end,
                    "duration_seconds": int(candidate.duration_seconds),
                    "base_price": float(candidate.base_price),
                    "touch_price": float(candidate.touch_price),
                    "move_pct": float(candidate.move_pct),
                    "selected_date": selected_date,
                    "raw_candidate_count": 1,
                    "candidate_group_start_ts": start,
                    "candidate_group_end_ts": start,
                    "_cluster_interval_end_ts": horizon_end,
                }
            )
            active[key] = len(grouped) - 1
            continue

        current = grouped[active_index]
        current["raw_candidate_count"] = int(current["raw_candidate_count"]) + 1
        current["candidate_group_end_ts"] = start
        if end > current["event_end_ts"]:
            current["event_end_ts"] = end
            current["duration_seconds"] = int((end - current["event_start_ts"]).total_seconds())
        if horizon_end > current["_cluster_interval_end_ts"]:
            current["_cluster_interval_end_ts"] = horizon_end
        if abs(float(candidate.move_pct)) > abs(float(current["move_pct"])):
            current["move_pct"] = float(candidate.move_pct)
            current["touch_price"] = float(candidate.touch_price)

    return tuple(
        GroupedMoveEvent(
            event_rank=index,
            **{
                key: value
                for key, value in row.items()
                if not key.startswith("_") and key != "event_rank"
            },
        )
        for index, row in enumerate(_sort_grouped_rows(grouped), start=1)
    )


def select_direction_balanced_events(
    events: Iterable[GroupedMoveEvent],
    *,
    max_events: int = DEFAULT_MAX_EVENTS,
) -> tuple[GroupedMoveEvent, ...]:
    """Select events with best-effort UP/DOWN balance and deterministic fallback."""

    if max_events <= 0:
        raise ValueError("max_events must be positive.")

    ordered = sorted(events, key=_event_sort_key)
    by_direction = {
        "UP": [event for event in ordered if event.direction == "UP"],
        "DOWN": [event for event in ordered if event.direction == "DOWN"],
    }
    target_down = max_events // 2
    target_up = max_events - target_down
    selected: list[GroupedMoveEvent] = []
    selected.extend(by_direction["UP"][:target_up])
    selected.extend(by_direction["DOWN"][:target_down])
    selected_ids = {event.event_id for event in selected}

    if len(selected) < max_events:
        for event in ordered:
            if event.event_id in selected_ids:
                continue
            selected.append(event)
            selected_ids.add(event.event_id)
            if len(selected) == max_events:
                break

    return tuple(sorted(selected, key=_event_sort_key))


def direction_balance_note(events: Iterable[GroupedMoveEvent], selected: Iterable[GroupedMoveEvent]) -> str:
    """Return a compact direction-balance audit note."""

    available_counts = _direction_counts(events)
    selected_counts = _direction_counts(selected)
    if selected_counts.get("UP", 0) == selected_counts.get("DOWN", 0):
        return "balanced"
    return (
        "direction_balance_best_effort:"
        f"available_up={available_counts.get('UP', 0)},"
        f"available_down={available_counts.get('DOWN', 0)},"
        f"selected_up={selected_counts.get('UP', 0)},"
        f"selected_down={selected_counts.get('DOWN', 0)}"
    )


def raw_candidate_start_is_excluded(
    anchor_ts: datetime,
    raw_candidate_starts: Iterable[datetime],
    *,
    lookback_seconds: int,
    horizon_seconds: int,
    exclusion_seconds: int,
) -> bool:
    """Return whether a normal anchor violates raw-candidate exclusion rules."""

    anchor = _drop_tz(_as_utc(anchor_ts))
    window_start = anchor - timedelta(seconds=lookback_seconds)
    future_end = anchor + timedelta(seconds=horizon_seconds)
    for raw_start in raw_candidate_starts:
        candidate_ts = _drop_tz(_as_utc(raw_start))
        if abs((candidate_ts - anchor).total_seconds()) <= exclusion_seconds:
            return True
        if window_start <= candidate_ts < anchor:
            return True
        if anchor < candidate_ts <= future_end:
            return True
    return False


def expected_event_snapshot_rows(event_count: int, exchange_count: int, lookback_seconds: int) -> int:
    return _non_negative(event_count) * _positive(exchange_count) * _positive(lookback_seconds)


def expected_normal_snapshot_rows(
    event_count: int,
    normal_count_per_event: int,
    exchange_count: int,
    lookback_seconds: int,
) -> int:
    return (
        _non_negative(event_count)
        * _positive(normal_count_per_event)
        * _positive(exchange_count)
        * _positive(lookback_seconds)
    )


def expected_event_profile_rows(event_count: int, rows_per_event: int) -> int:
    return _non_negative(event_count) * _positive(rows_per_event)


def expected_normal_profile_rows(
    event_count: int,
    normal_count_per_event: int,
    rows_per_normal: int,
) -> int:
    return _non_negative(event_count) * _positive(normal_count_per_event) * _positive(rows_per_normal)


def expected_comparison_rows(event_count: int, rows_per_event: int) -> int:
    return _non_negative(event_count) * _positive(rows_per_event)


def expected_top_diff_rows_dynamic(source_group_counts: Iterable[int], top_n: int) -> int:
    """Phase 3A top-diff expected rows from actual source group counts."""

    return expected_top_diff_rows_from_group_counts(source_group_counts, top_n)


def processing_status(
    event: GroupedMoveEvent,
    *,
    stage: str,
    selected: bool,
    exclusion_reason: str | None = None,
    normal_selection_count: int = 0,
    direction_balance_note_value: str | None = None,
    row_count_status: str | None = None,
) -> EventProcessingStatus:
    """Build an event-processing status row."""

    return EventProcessingStatus(
        event_id=event.event_id,
        event_rank=event.event_rank,
        direction=event.direction,
        processing_stage=stage,
        selected=selected,
        exclusion_reason=exclusion_reason,
        normal_selection_count=normal_selection_count,
        direction_balance_note=direction_balance_note_value,
        row_count_status=row_count_status,
    )


def _format_phase3a_event_id(
    exchange: str,
    symbol: str,
    selected_date: str,
    direction: str,
    group_number: int,
) -> str:
    return (
        f"{exchange.lower()}_{symbol.lower()}_{selected_date}_"
        f"{direction.lower()}_{group_number:03d}"
    )


def _sort_grouped_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(
        rows,
        key=lambda row: (
            row["event_start_ts"],
            str(row["exchange"]),
            str(row["symbol"]).lower(),
            str(row["direction"]),
            str(row["event_id"]),
        ),
    )


def _event_sort_key(event: GroupedMoveEvent) -> tuple[datetime, str, str, str]:
    return (
        _drop_tz(_as_utc(event.event_start_ts)),
        event.exchange.lower(),
        event.symbol.lower(),
        event.event_id,
    )


def _direction_counts(events: Iterable[GroupedMoveEvent]) -> dict[str, int]:
    counts = {"UP": 0, "DOWN": 0}
    for event in events:
        counts[event.direction] = counts.get(event.direction, 0) + 1
    return counts


def _positive(value: int) -> int:
    if value <= 0:
        raise ValueError("value must be positive.")
    return value


def _non_negative(value: int) -> int:
    if value < 0:
        raise ValueError("value must be non-negative.")
    return value


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _drop_tz(value: datetime) -> datetime:
    return value.replace(tzinfo=None)
