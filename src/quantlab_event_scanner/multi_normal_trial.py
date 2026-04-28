"""Pure helpers for Phase 2J multi-normal comparison trials."""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Mapping


ACTIVITY_DISTANCE_WINDOW_LABEL = "last_300s"
ACTIVITY_DISTANCE_METRICS = (
    "trade_count_sum",
    "trade_volume_sum",
    "trade_notional_sum",
)

DEFAULT_NORMAL_SAMPLE_COUNT = 10
DEFAULT_MIN_ANCHOR_SPACING_SECONDS = 1800
SMALL_DENOMINATOR_ABS_THRESHOLD = 1e-9


@dataclass(frozen=True)
class QualityPassedNormalCandidate:
    normal_anchor_ts: datetime
    activity_distance: float
    nearest_candidate_distance_seconds: int | None


@dataclass(frozen=True)
class RankedNormalCandidate:
    normal_anchor_ts: datetime
    activity_distance: float
    nearest_candidate_distance_seconds: int | None
    normal_sample_rank: int
    normal_sample_id: str


def default_phase2j_run_id(now: datetime) -> str:
    """Return a stable Phase 2J run ID for the given timestamp."""

    utc_now = _as_utc(now)
    return f"phase2j_{utc_now.strftime('%Y%m%dT%H%M%SZ')}"


def activity_distance(
    event_values: Mapping[tuple[str, str], float | int | None],
    normal_values: Mapping[tuple[str, str], float | int | None],
    *,
    exchanges: Iterable[str],
    metrics: tuple[str, ...] = ACTIVITY_DISTANCE_METRICS,
) -> float:
    """Return mean log1p activity distance across exchange/metric keys."""

    distances: list[float] = []
    for exchange in exchanges:
        exchange_key = exchange.lower()
        for metric in metrics:
            event_value = event_values.get((exchange_key, metric))
            normal_value = normal_values.get((exchange_key, metric))
            if event_value is None or normal_value is None:
                raise ValueError(f"Missing activity value for {exchange_key}.{metric}.")
            if event_value < 0 or normal_value < 0:
                raise ValueError(f"Activity value must be non-negative for {exchange_key}.{metric}.")
            distances.append(abs(math.log1p(normal_value) - math.log1p(event_value)))
    if not distances:
        raise ValueError("At least one activity value is required.")
    return sum(distances) / len(distances)


def exclusion_reason_counts(reasons: Iterable[str | None]) -> dict[str, int]:
    """Return deterministic counts for anchor exclusion reasons."""

    counter = Counter(reason or "eligible" for reason in reasons)
    return dict(sorted(counter.items()))


def select_activity_matched_candidates(
    candidates: Iterable[QualityPassedNormalCandidate],
    *,
    selected_count: int = DEFAULT_NORMAL_SAMPLE_COUNT,
    min_anchor_spacing_seconds: int = DEFAULT_MIN_ANCHOR_SPACING_SECONDS,
) -> tuple[RankedNormalCandidate, ...]:
    """Select activity-matched candidates with deterministic tie-breakers."""

    if selected_count <= 0:
        raise ValueError("selected_count must be positive.")
    if min_anchor_spacing_seconds < 0:
        raise ValueError("min_anchor_spacing_seconds must be non-negative.")

    ordered = sorted(
        candidates,
        key=lambda candidate: (
            candidate.activity_distance,
            -_distance_for_sort(candidate.nearest_candidate_distance_seconds),
            _as_utc(candidate.normal_anchor_ts),
        ),
    )
    selected: list[QualityPassedNormalCandidate] = []
    for candidate in ordered:
        anchor = _as_utc(candidate.normal_anchor_ts)
        if any(
            abs((anchor - _as_utc(existing.normal_anchor_ts)).total_seconds())
            < min_anchor_spacing_seconds
            for existing in selected
        ):
            continue
        selected.append(candidate)
        if len(selected) == selected_count:
            break

    return tuple(
        RankedNormalCandidate(
            normal_anchor_ts=_as_utc(candidate.normal_anchor_ts),
            activity_distance=candidate.activity_distance,
            nearest_candidate_distance_seconds=candidate.nearest_candidate_distance_seconds,
            normal_sample_rank=index,
            normal_sample_id=f"normal_{index:03d}",
        )
        for index, candidate in enumerate(selected, start=1)
    )


def normal_zero_or_near_zero_count(
    values: Iterable[float | int | None],
    *,
    threshold: float = SMALL_DENOMINATOR_ABS_THRESHOLD,
) -> int:
    """Return non-null normal values whose absolute value is near zero."""

    return sum(1 for value in values if value is not None and abs(value) <= threshold)


def normal_percentile_rank(
    event_value: float | int | None,
    normal_values: Iterable[float | int | None],
) -> float | None:
    """Return exact ECDF share of non-null normal values less than or equal to event."""

    if event_value is None:
        return None
    observed = [value for value in normal_values if value is not None]
    if not observed:
        return None
    return sum(1 for value in observed if value <= event_value) / len(observed)


def z_score_vs_normal(
    event_value: float | int | None,
    normal_mean: float | int | None,
    normal_stddev: float | int | None,
) -> float | None:
    """Return event-vs-normal z-like score with zero-stddev protection."""

    if event_value is None or normal_mean is None or normal_stddev in (None, 0):
        return None
    return (event_value - normal_mean) / normal_stddev


def expected_multi_normal_profile_rows(
    single_normal_rows: int,
    selected_count: int = DEFAULT_NORMAL_SAMPLE_COUNT,
) -> int:
    """Return expected multi-normal profile rows for one profile subtable."""

    if single_normal_rows < 0:
        raise ValueError("single_normal_rows must be non-negative.")
    if selected_count <= 0:
        raise ValueError("selected_count must be positive.")
    return single_normal_rows * selected_count


def _distance_for_sort(value: int | None) -> int:
    if value is None:
        return -1
    return value


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
