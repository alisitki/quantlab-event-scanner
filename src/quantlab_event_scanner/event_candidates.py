"""Pure helpers for Phase 1 trade move candidate scans."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal


CandidateDirection = Literal["UP", "DOWN", "AMBIGUOUS"]


@dataclass(frozen=True)
class DirectionResolution:
    """Resolved candidate direction from first UP/DOWN threshold touches."""

    direction: CandidateDirection | None
    ambiguous: bool


@dataclass(frozen=True)
class CandidateRecord:
    """Small pure-Python candidate record for grouping tests."""

    exchange: str
    symbol: str
    direction: Literal["UP", "DOWN"]
    start_ts: datetime


@dataclass(frozen=True)
class CandidateGroup:
    """Pure-Python grouped candidate summary."""

    exchange: str
    symbol: str
    direction: Literal["UP", "DOWN"]
    group_number: int
    raw_candidate_count: int
    group_start_ts: datetime
    group_end_ts: datetime


def resolve_candidate_direction(
    up_hit_second: int | None,
    down_hit_second: int | None,
) -> DirectionResolution:
    """Resolve candidate direction from first threshold hit seconds.

    Seconds are durations from the candidate start, not wall-clock seconds.
    """

    if up_hit_second is None and down_hit_second is None:
        return DirectionResolution(direction=None, ambiguous=False)
    if up_hit_second is None:
        return DirectionResolution(direction="DOWN", ambiguous=False)
    if down_hit_second is None:
        return DirectionResolution(direction="UP", ambiguous=False)
    if up_hit_second == down_hit_second:
        return DirectionResolution(direction="AMBIGUOUS", ambiguous=True)
    if up_hit_second < down_hit_second:
        return DirectionResolution(direction="UP", ambiguous=False)
    return DirectionResolution(direction="DOWN", ambiguous=False)


def format_trial_event_id(
    exchange: str,
    symbol: str,
    selected_date: str,
    direction: str,
    group_number: int,
) -> str:
    """Return deterministic trial event ID text."""

    return (
        f"{exchange.lower()}_{symbol.lower()}_{selected_date}_"
        f"{direction.lower()}_{group_number:03d}"
    )


def group_candidate_records(
    candidates: list[CandidateRecord],
    horizon_seconds: int,
) -> list[CandidateGroup]:
    """Group candidates by exchange, symbol, direction, and horizon from group start."""

    ordered = sorted(
        candidates,
        key=lambda item: (item.exchange, item.symbol, item.direction, item.start_ts),
    )
    groups: list[CandidateGroup] = []
    active: dict[tuple[str, str, str], CandidateGroup] = {}
    group_numbers: dict[tuple[str, str, str], int] = {}

    for candidate in ordered:
        key = (candidate.exchange, candidate.symbol, candidate.direction)
        current = active.get(key)
        starts_new_group = (
            current is None
            or (candidate.start_ts - current.group_start_ts).total_seconds() > horizon_seconds
        )
        if starts_new_group:
            group_number = group_numbers.get(key, 0) + 1
            group_numbers[key] = group_number
            current = CandidateGroup(
                exchange=candidate.exchange,
                symbol=candidate.symbol,
                direction=candidate.direction,
                group_number=group_number,
                raw_candidate_count=1,
                group_start_ts=candidate.start_ts,
                group_end_ts=candidate.start_ts,
            )
            active[key] = current
            groups.append(current)
            continue

        updated = CandidateGroup(
            exchange=current.exchange,
            symbol=current.symbol,
            direction=current.direction,
            group_number=current.group_number,
            raw_candidate_count=current.raw_candidate_count + 1,
            group_start_ts=current.group_start_ts,
            group_end_ts=candidate.start_ts,
        )
        active[key] = updated
        groups[groups.index(current)] = updated

    return groups
