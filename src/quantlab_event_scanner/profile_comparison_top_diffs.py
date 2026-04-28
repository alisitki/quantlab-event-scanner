"""Pure helpers for Phase 2G profile comparison top-diff inspection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable


RANK_TOP_ABSOLUTE_DIFF = "top_absolute_diff"
RANK_TOP_SIGNED_POSITIVE = "top_signed_positive"
RANK_TOP_SIGNED_NEGATIVE = "top_signed_negative"

TOP_DIFF_RANK_TYPES = (
    RANK_TOP_ABSOLUTE_DIFF,
    RANK_TOP_SIGNED_POSITIVE,
    RANK_TOP_SIGNED_NEGATIVE,
)

TOP_DIFF_TIE_BREAKER_COLUMNS = (
    "report_group",
    "metric_name",
    "exchange",
    "symbol_key",
    "exchange_pair",
    "window_mode",
    "window_label",
    "from_bucket",
    "to_bucket",
)

TOP_DIFF_PARTITION_COLUMNS = (
    "report_group",
    "metric_group",
)

TOP_DIFF_REQUIRED_COLUMNS = (
    "comparison_run_id",
    "report_group",
    "metric_name",
    "metric_group",
    "exchange",
    "symbol",
    "symbol_key",
    "exchange_pair",
    "window_mode",
    "window_label",
    "from_bucket",
    "to_bucket",
    "event_value",
    "normal_value",
    "signed_diff",
    "absolute_diff",
    "ratio",
    "ratio_unstable",
    "relative_change_unstable",
    "small_denominator_flag",
    "event_from_bucket_value",
    "normal_from_bucket_value",
    "event_to_bucket_value",
    "normal_to_bucket_value",
    "source_event_id",
    "event_direction",
    "event_start_ts",
    "normal_window_start_ts",
    "normal_window_end_ts",
    "normal_anchor_ts",
    "profile_version",
)


@dataclass(frozen=True)
class TopDiffRankingDefinition:
    rank_type: str
    primary_column: str
    ascending: bool


TOP_DIFF_RANKING_DEFINITIONS = (
    TopDiffRankingDefinition(RANK_TOP_ABSOLUTE_DIFF, "absolute_diff", False),
    TopDiffRankingDefinition(RANK_TOP_SIGNED_POSITIVE, "signed_diff", False),
    TopDiffRankingDefinition(RANK_TOP_SIGNED_NEGATIVE, "signed_diff", True),
)


@dataclass(frozen=True)
class SignedDiffDistribution:
    positive_count: int
    negative_count: int
    zero_count: int
    null_count: int


def default_phase2g_run_id(now: datetime) -> str:
    """Return a stable Phase 2G run ID for the given timestamp."""

    utc_now = _as_utc(now)
    return f"phase2g_{utc_now.strftime('%Y%m%dT%H%M%SZ')}"


def expected_top_diff_rows(top_n: int = 20) -> int:
    """Return expected top-diff rows per report group."""

    if top_n <= 0:
        raise ValueError("top_n must be positive.")
    return len(TOP_DIFF_RANK_TYPES) * top_n


def expected_top_diff_rows_for_metric_group(source_rows: int, top_n: int = 20) -> int:
    """Return expected top-diff rows for one report_group + metric_group."""

    if source_rows < 0:
        raise ValueError("source_rows must be non-negative.")
    if top_n <= 0:
        raise ValueError("top_n must be positive.")
    return len(TOP_DIFF_RANK_TYPES) * min(top_n, source_rows)


def expected_top_diff_rows_from_group_counts(
    source_group_counts: Iterable[int],
    top_n: int = 20,
) -> int:
    """Return expected top-diff rows summed over metric-group source counts."""

    return sum(
        expected_top_diff_rows_for_metric_group(source_rows, top_n)
        for source_rows in source_group_counts
    )


def missing_required_columns(
    available_columns: tuple[str, ...] | list[str] | set[str],
    required_columns: tuple[str, ...] = TOP_DIFF_REQUIRED_COLUMNS,
) -> tuple[str, ...]:
    """Return required Phase 2G source columns that are absent."""

    available = set(available_columns)
    return tuple(column for column in required_columns if column not in available)


def validate_required_columns(
    available_columns: tuple[str, ...] | list[str] | set[str],
    required_columns: tuple[str, ...] = TOP_DIFF_REQUIRED_COLUMNS,
) -> None:
    """Fail fast when required Phase 2G source columns are absent."""

    missing = missing_required_columns(available_columns, required_columns)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def signed_diff_distribution(values: Iterable[float | None]) -> SignedDiffDistribution:
    """Count positive, negative, zero, and null signed-diff values."""

    positive = 0
    negative = 0
    zero = 0
    null = 0
    for value in values:
        if value is None:
            null += 1
        elif value > 0:
            positive += 1
        elif value < 0:
            negative += 1
        else:
            zero += 1
    return SignedDiffDistribution(
        positive_count=positive,
        negative_count=negative,
        zero_count=zero,
        null_count=null,
    )


def absolute_diff_is_valid(
    signed_diff: float | None,
    absolute_diff: float | None,
    *,
    tolerance: float = 1e-9,
) -> bool:
    """Return whether absolute_diff is non-negative and matches abs(signed_diff)."""

    if absolute_diff is not None and absolute_diff < 0:
        return False
    if signed_diff is None or absolute_diff is None:
        return True
    return abs(abs(signed_diff) - absolute_diff) <= tolerance


def invalid_absolute_diff_rows(
    rows: Iterable[tuple[float | None, float | None]],
    *,
    tolerance: float = 1e-9,
) -> tuple[tuple[float | None, float | None], ...]:
    """Return rows whose absolute_diff contract is invalid."""

    return tuple(
        (signed_diff, absolute_diff)
        for signed_diff, absolute_diff in rows
        if not absolute_diff_is_valid(
            signed_diff,
            absolute_diff,
            tolerance=tolerance,
        )
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
