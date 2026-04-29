"""Pure helpers for Phase 3B BTC multi-event content review."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import median
from typing import Any, Iterable, Mapping

from .profile_comparison_reports import (
    METRIC_GROUP_CONTEXT,
    METRIC_GROUP_PRICE_DISLOCATION,
    METRIC_GROUP_SIGNAL_CANDIDATE,
    METRIC_GROUP_UNSTABLE,
)


PHASE3B_REVIEW_PREFIX = "phase3b_"

POLICY_CORE_FEATURE = "core_feature"
POLICY_CONTEXT_FILTER = "context_filter"
POLICY_DIAGNOSTIC = "diagnostic"
POLICY_EXCLUDE = "exclude"

POLICY_BUCKETS = (
    POLICY_CORE_FEATURE,
    POLICY_CONTEXT_FILTER,
    POLICY_DIAGNOSTIC,
    POLICY_EXCLUDE,
)

EXPECTED_SIGN_DIRECTION_NORMALIZED = "direction_normalized"
EXPECTED_SIGN_OBSERVED_SIGNED = "observed_signed"
EXPECTED_SIGN_CONTEXT = "context_no_signal_sign"
EXPECTED_SIGN_UNSTABLE = "unstable_no_primary_sign"
EXPECTED_SIGN_NONE = "no_directional_sign"

PROFILE_FAMILY_PHASE3A_TOP_DIFF = "phase3a_top_diff"
PROFILE_FAMILY_PHASE3A_COMPARISON = "phase3a_comparison"

REQUIRED_TOP_DIFF_SUBTABLES = (
    "top_diffs/exchange_profile_top_diffs",
    "top_diffs/cross_exchange_mid_diff_top_diffs",
    "top_diffs/bucket_change_profile_top_diffs",
)

REQUIRED_COMPARISON_SUBTABLES = (
    "comparison_reports/exchange_profile_comparison",
    "comparison_reports/cross_exchange_mid_diff_comparison",
    "comparison_reports/bucket_change_profile_comparison",
)

REQUIRED_SUMMARY_SUBTABLES = (
    "summary/event_counts_by_direction",
    "summary/metric_group_summary",
    "summary/normal_selection_quality",
    "summary/event_processing_status",
)

TOP_DIFF_REQUIRED_COLUMNS = (
    "source_event_id",
    "event_direction",
    "report_group",
    "metric_group",
    "rank_type",
    "rank",
    "exchange",
    "symbol_key",
    "exchange_pair",
    "window_label",
    "from_bucket",
    "to_bucket",
    "metric_name",
    "signed_diff_vs_mean",
    "absolute_diff_vs_mean",
    "z_score_vs_normal",
    "normal_percentile_rank",
    "ratio_unstable",
    "relative_change_unstable",
    "small_denominator_flag",
)

SUMMARY_REQUIRED_COLUMNS = (
    "event_id",
    "event_rank",
    "direction",
    "normal_selection_count",
)

METRIC_RECURRENT_REPORT_COLUMNS = (
    "review_run_id",
    "source_run_id",
    "source_event_count",
    "profile_family",
    "report_group",
    "metric_group",
    "metric_name",
    "exchange",
    "exchange_pair",
    "symbol_key",
    "window_label",
    "from_bucket",
    "to_bucket",
    "rank_scope",
    "top_k",
    "n_events_seen_in_top_k",
    "n_up_events_seen",
    "n_down_events_seen",
    "median_rank",
    "median_abs_diff_vs_mean",
    "median_abs_z_score",
    "median_percentile_extremeness",
    "denominator_risk_event_count",
    "unstable_event_count",
    "direction_normalized_sign_consistency",
    "recommendation",
    "created_at",
)

DIRECTION_SIGN_CONSISTENCY_REPORT_COLUMNS = (
    "review_run_id",
    "source_run_id",
    "profile_family",
    "report_group",
    "metric_group",
    "metric_name",
    "exchange",
    "exchange_pair",
    "symbol_key",
    "window_label",
    "from_bucket",
    "to_bucket",
    "expected_sign_policy",
    "up_positive_count",
    "up_negative_count",
    "down_positive_count",
    "down_negative_count",
    "observed_positive_count",
    "observed_negative_count",
    "observed_zero_count",
    "sign_consistency_rate",
    "direction_normalized_sign",
    "notes",
    "created_at",
)

CONTEXT_DOMINANCE_REPORT_COLUMNS = (
    "review_run_id",
    "source_run_id",
    "profile_family",
    "report_group",
    "rank_type",
    "top_k",
    "context_metric_count",
    "signal_candidate_count",
    "price_dislocation_count",
    "unstable_count",
    "denominator_risk_count",
    "context_share",
    "unstable_share",
    "denominator_risk_share",
    "dominant_metric_group",
    "recommended_action",
    "created_at",
)

EVENT_NARRATIVE_REPORT_COLUMNS = (
    "review_run_id",
    "source_run_id",
    "event_id",
    "event_rank",
    "symbol",
    "direction",
    "event_start_ts",
    "event_end_ts",
    "dominant_exchange_pattern",
    "cross_exchange_pattern",
    "book_spread_pattern",
    "trade_flow_pattern",
    "price_return_pattern",
    "normal_sample_quality",
    "metric_anomalies",
    "suspected_late_reaction_flags",
    "notes",
    "created_at",
)

RECOMMENDED_METRIC_POLICY_COLUMNS = (
    "review_run_id",
    "source_run_id",
    "metric_name",
    "metric_group",
    "profile_family",
    "exchange",
    "exchange_pair",
    "window_label",
    "policy_bucket",
    "reason",
    "requires_more_data",
    "phase4b_candidate",
    "denominator_risk_policy",
    "created_at",
)

EARLIER_WINDOWS = (
    "last_300s",
    "last_120s",
    "last_60s",
    "bucket_300_120s",
    "bucket_120_60s",
    "bucket_60_30s",
)

EVENT_PROXIMATE_WINDOWS = (
    "last_30s",
    "last_10s",
    "bucket_30_10s",
    "bucket_10_0s",
)

LATE_REACTION_WINDOWS = (
    "last_10s",
    "bucket_10_0s",
)


@dataclass(frozen=True)
class MetricPolicyRecommendation:
    policy_bucket: str
    reason: str
    requires_more_data: bool
    phase4b_candidate: bool
    denominator_risk_policy: str


def default_phase3b_review_run_id(now: datetime) -> str:
    """Return a stable Phase 3B review run ID for the given timestamp."""

    utc_now = _as_utc(now)
    return f"{PHASE3B_REVIEW_PREFIX}{utc_now.strftime('%Y%m%dT%H%M%SZ')}"


def validate_phase3b_review_run_id(run_id: str) -> str:
    """Validate and normalize a Phase 3B review run id."""

    normalized = run_id.strip()
    if not normalized:
        raise ValueError("review_run_id must not be empty.")
    if "/" in normalized or "\\" in normalized:
        raise ValueError("review_run_id must be a run id, not a path.")
    if not normalized.startswith(PHASE3B_REVIEW_PREFIX):
        raise ValueError("review_run_id must start with 'phase3b_'.")
    return normalized


def missing_required_columns(
    available_columns: Iterable[str],
    required_columns: Iterable[str],
) -> tuple[str, ...]:
    """Return required columns missing from an available column collection."""

    available = set(available_columns)
    return tuple(column for column in required_columns if column not in available)


def validate_required_columns(
    available_columns: Iterable[str],
    required_columns: Iterable[str],
    *,
    label: str,
) -> None:
    """Fail fast when a required input schema column is missing."""

    missing = missing_required_columns(available_columns, required_columns)
    if missing:
        raise ValueError(f"{label} missing required columns: {', '.join(missing)}")


def metric_identity(row: Mapping[str, Any]) -> tuple[Any, ...]:
    """Return the Phase 3B metric identity used for recurrence aggregation."""

    return (
        row.get("profile_family", PROFILE_FAMILY_PHASE3A_TOP_DIFF),
        row.get("report_group"),
        row.get("metric_group"),
        row.get("metric_name"),
        row.get("exchange"),
        row.get("exchange_pair"),
        row.get("symbol_key"),
        row.get("window_label"),
        row.get("from_bucket"),
        row.get("to_bucket"),
    )


def percentile_extremeness(percentile_rank: float | None) -> float | None:
    """Return two-tailed percentile extremeness in the closed interval [0.5, 1]."""

    if percentile_rank is None:
        return None
    return max(float(percentile_rank), 1.0 - float(percentile_rank))


def expected_sign_policy(metric_group: str | None, metric_name: str | None) -> str:
    """Return Phase 3B sign interpretation policy for a metric."""

    name = metric_name or ""
    if "abs" in name:
        return EXPECTED_SIGN_NONE
    if metric_group == METRIC_GROUP_UNSTABLE or name.endswith(".relative_change"):
        return EXPECTED_SIGN_UNSTABLE
    if metric_group == METRIC_GROUP_CONTEXT:
        return EXPECTED_SIGN_CONTEXT
    if metric_group == METRIC_GROUP_PRICE_DISLOCATION and (
        "return_bps" in name or name.endswith("mid_diff_bps")
    ):
        return EXPECTED_SIGN_DIRECTION_NORMALIZED
    if metric_group == METRIC_GROUP_SIGNAL_CANDIDATE:
        return EXPECTED_SIGN_OBSERVED_SIGNED
    return EXPECTED_SIGN_OBSERVED_SIGNED


def signed_value_sign(value: float | None) -> int:
    """Return -1, 0, or 1 for a nullable signed metric value."""

    if value is None:
        return 0
    numeric = float(value)
    if numeric > 0:
        return 1
    if numeric < 0:
        return -1
    return 0


def direction_normalized_sign(event_direction: str | None, signed_diff: float | None) -> int:
    """Return sign normalized so positive means aligned with event direction."""

    sign = signed_value_sign(signed_diff)
    if sign == 0:
        return 0
    direction = (event_direction or "").upper()
    if direction == "DOWN":
        return -sign
    if direction == "UP":
        return sign
    return sign


def sign_consistency_label(normalized_signs: Iterable[int]) -> tuple[str, float | None]:
    """Return compact sign label and majority consistency rate for non-zero signs."""

    signs = [sign for sign in normalized_signs if sign != 0]
    if not signs:
        return "none", None
    positive = sum(1 for sign in signs if sign > 0)
    negative = sum(1 for sign in signs if sign < 0)
    if positive and negative:
        return "mixed", max(positive, negative) / len(signs)
    if positive:
        return "positive", 1.0
    return "negative", 1.0


def has_denominator_risk(row: Mapping[str, Any]) -> bool:
    """Return whether a comparison/top-diff row carries denominator risk."""

    return bool(
        row.get("ratio_unstable")
        or row.get("relative_change_unstable")
        or row.get("small_denominator_flag")
    )


def is_unstable_metric(metric_group: str | None, metric_name: str | None) -> bool:
    """Return whether the metric should be treated as unstable/diagnostic."""

    return metric_group == METRIC_GROUP_UNSTABLE or bool(
        metric_name and metric_name.endswith(".relative_change")
    )


def is_event_proximate_window(window_label: str | None) -> bool:
    """Return whether a window is close enough to the event for proximity review."""

    return window_label in EVENT_PROXIMATE_WINDOWS


def is_suspected_late_reaction_window(window_label: str | None) -> bool:
    """Return whether a window is likely too late for pre-event signal use."""

    return window_label in LATE_REACTION_WINDOWS


def binance_relevance(exchange: str | None, exchange_pair: str | None) -> str:
    """Classify whether a row is directly relevant to Binance executable side."""

    exchange_key = (exchange or "").lower()
    pair_key = (exchange_pair or "").lower()
    if exchange_key == "binance":
        return "binance_exchange"
    if "binance" in pair_key:
        return "binance_pair"
    if pair_key:
        return "non_binance_pair"
    return "no_binance_reference"


def recommend_metric_policy(
    *,
    metric_name: str,
    metric_group: str,
    denominator_risk_event_count: int,
    source_event_count: int,
    n_events_seen_in_top_k: int,
) -> MetricPolicyRecommendation:
    """Return Phase 3B policy recommendation without claiming signal validation."""

    if denominator_risk_event_count < 0 or source_event_count < 0 or n_events_seen_in_top_k < 0:
        raise ValueError("event counts must be non-negative.")

    if is_unstable_metric(metric_group, metric_name):
        return MetricPolicyRecommendation(
            policy_bucket=POLICY_DIAGNOSTIC,
            reason="unstable_or_relative_change_metric",
            requires_more_data=True,
            phase4b_candidate=False,
            denominator_risk_policy="appendix_only",
        )

    if source_event_count and denominator_risk_event_count >= source_event_count:
        return MetricPolicyRecommendation(
            policy_bucket=POLICY_EXCLUDE,
            reason="denominator_risk_in_all_source_events",
            requires_more_data=True,
            phase4b_candidate=False,
            denominator_risk_policy="exclude_primary",
        )

    if denominator_risk_event_count > 0:
        return MetricPolicyRecommendation(
            policy_bucket=POLICY_DIAGNOSTIC,
            reason="denominator_risk_present",
            requires_more_data=True,
            phase4b_candidate=False,
            denominator_risk_policy="exclude_primary",
        )

    if metric_group == METRIC_GROUP_CONTEXT:
        return MetricPolicyRecommendation(
            policy_bucket=POLICY_CONTEXT_FILTER,
            reason="context_metric_for_matching_regime_or_diagnostic",
            requires_more_data=True,
            phase4b_candidate=False,
            denominator_risk_policy="none_observed",
        )

    if metric_group in (METRIC_GROUP_PRICE_DISLOCATION, METRIC_GROUP_SIGNAL_CANDIDATE):
        return MetricPolicyRecommendation(
            policy_bucket=POLICY_CORE_FEATURE,
            reason=f"{metric_group}_candidate_seen_in_{n_events_seen_in_top_k}_events",
            requires_more_data=True,
            phase4b_candidate=True,
            denominator_risk_policy="none_observed",
        )

    return MetricPolicyRecommendation(
        policy_bucket=POLICY_DIAGNOSTIC,
        reason="unknown_metric_group",
        requires_more_data=True,
        phase4b_candidate=False,
        denominator_risk_policy="review_required",
    )


def metric_recurrence_report_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    review_run_id: str,
    source_run_id: str,
    source_event_count: int,
    top_k: int,
    created_at: datetime,
) -> list[dict[str, Any]]:
    """Build recurrence report rows from top-diff-like mappings."""

    if source_event_count < 0:
        raise ValueError("source_event_count must be non-negative.")
    if top_k <= 0:
        raise ValueError("top_k must be positive.")

    grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = {}
    for row in rows:
        rank = row.get("rank")
        if rank is not None and int(rank) > top_k:
            continue
        grouped.setdefault(metric_identity(row), []).append(row)

    output: list[dict[str, Any]] = []
    for identity, group_rows in sorted(grouped.items(), key=lambda item: tuple(str(v) for v in item[0])):
        (
            profile_family,
            report_group,
            metric_group,
            metric_name,
            exchange,
            exchange_pair,
            symbol_key,
            window_label,
            from_bucket,
            to_bucket,
        ) = identity
        event_ids = {row.get("source_event_id") for row in group_rows if row.get("source_event_id")}
        up_events = {
            row.get("source_event_id")
            for row in group_rows
            if str(row.get("event_direction")).upper() == "UP" and row.get("source_event_id")
        }
        down_events = {
            row.get("source_event_id")
            for row in group_rows
            if str(row.get("event_direction")).upper() == "DOWN" and row.get("source_event_id")
        }
        denominator_risk_events = {
            row.get("source_event_id")
            for row in group_rows
            if has_denominator_risk(row) and row.get("source_event_id")
        }
        unstable_events = {
            row.get("source_event_id")
            for row in group_rows
            if is_unstable_metric(metric_group, metric_name) and row.get("source_event_id")
        }
        normalized_label, _ = sign_consistency_label(
            direction_normalized_sign(row.get("event_direction"), _float_or_none(row.get("signed_diff_vs_mean")))
            for row in group_rows
        )
        recommendation = recommend_metric_policy(
            metric_name=str(metric_name or ""),
            metric_group=str(metric_group or ""),
            denominator_risk_event_count=len(denominator_risk_events),
            source_event_count=source_event_count,
            n_events_seen_in_top_k=len(event_ids),
        )

        output.append(
            {
                "review_run_id": review_run_id,
                "source_run_id": source_run_id,
                "source_event_count": source_event_count,
                "profile_family": profile_family,
                "report_group": report_group,
                "metric_group": metric_group,
                "metric_name": metric_name,
                "exchange": exchange,
                "exchange_pair": exchange_pair,
                "symbol_key": symbol_key,
                "window_label": window_label,
                "from_bucket": from_bucket,
                "to_bucket": to_bucket,
                "rank_scope": "top_k_by_rank_type",
                "top_k": top_k,
                "n_events_seen_in_top_k": len(event_ids),
                "n_up_events_seen": len(up_events),
                "n_down_events_seen": len(down_events),
                "median_rank": _median_numeric(row.get("rank") for row in group_rows),
                "median_abs_diff_vs_mean": _median_numeric(
                    row.get("absolute_diff_vs_mean") for row in group_rows
                ),
                "median_abs_z_score": _median_numeric(
                    abs(value)
                    for value in (
                        _float_or_none(row.get("z_score_vs_normal")) for row in group_rows
                    )
                    if value is not None
                ),
                "median_percentile_extremeness": _median_numeric(
                    percentile_extremeness(_float_or_none(row.get("normal_percentile_rank")))
                    for row in group_rows
                ),
                "denominator_risk_event_count": len(denominator_risk_events),
                "unstable_event_count": len(unstable_events),
                "direction_normalized_sign_consistency": normalized_label,
                "recommendation": recommendation.policy_bucket,
                "created_at": created_at,
            }
        )
    return output


def direction_sign_consistency_report_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    review_run_id: str,
    source_run_id: str,
    created_at: datetime,
) -> list[dict[str, Any]]:
    """Build direction/sign consistency rows from comparison/top-diff mappings."""

    grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(metric_identity(row), []).append(row)

    output: list[dict[str, Any]] = []
    for identity, group_rows in sorted(grouped.items(), key=lambda item: tuple(str(v) for v in item[0])):
        (
            profile_family,
            report_group,
            metric_group,
            metric_name,
            exchange,
            exchange_pair,
            symbol_key,
            window_label,
            from_bucket,
            to_bucket,
        ) = identity
        observed_signs = [signed_value_sign(_float_or_none(row.get("signed_diff_vs_mean"))) for row in group_rows]
        normalized_signs = [
            direction_normalized_sign(row.get("event_direction"), _float_or_none(row.get("signed_diff_vs_mean")))
            for row in group_rows
        ]
        normalized_label, consistency_rate = sign_consistency_label(normalized_signs)

        output.append(
            {
                "review_run_id": review_run_id,
                "source_run_id": source_run_id,
                "profile_family": profile_family,
                "report_group": report_group,
                "metric_group": metric_group,
                "metric_name": metric_name,
                "exchange": exchange,
                "exchange_pair": exchange_pair,
                "symbol_key": symbol_key,
                "window_label": window_label,
                "from_bucket": from_bucket,
                "to_bucket": to_bucket,
                "expected_sign_policy": expected_sign_policy(metric_group, metric_name),
                "up_positive_count": _direction_sign_count(group_rows, "UP", 1),
                "up_negative_count": _direction_sign_count(group_rows, "UP", -1),
                "down_positive_count": _direction_sign_count(group_rows, "DOWN", 1),
                "down_negative_count": _direction_sign_count(group_rows, "DOWN", -1),
                "observed_positive_count": sum(1 for sign in observed_signs if sign > 0),
                "observed_negative_count": sum(1 for sign in observed_signs if sign < 0),
                "observed_zero_count": sum(1 for sign in observed_signs if sign == 0),
                "sign_consistency_rate": consistency_rate,
                "direction_normalized_sign": normalized_label,
                "notes": "descriptive_only_not_signal_validation",
                "created_at": created_at,
            }
        )
    return output


def context_dominance_report_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    review_run_id: str,
    source_run_id: str,
    top_k: int,
    created_at: datetime,
) -> list[dict[str, Any]]:
    """Build context-dominance rows from top-diff-like mappings."""

    if top_k <= 0:
        raise ValueError("top_k must be positive.")

    grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = {}
    for row in rows:
        rank = row.get("rank")
        if rank is not None and int(rank) > top_k:
            continue
        key = (
            row.get("profile_family", PROFILE_FAMILY_PHASE3A_TOP_DIFF),
            row.get("report_group"),
            row.get("rank_type"),
        )
        grouped.setdefault(key, []).append(row)

    output: list[dict[str, Any]] = []
    for (profile_family, report_group, rank_type), group_rows in sorted(
        grouped.items(),
        key=lambda item: tuple(str(v) for v in item[0]),
    ):
        total = len(group_rows)
        context_count = _metric_group_count(group_rows, METRIC_GROUP_CONTEXT)
        signal_count = _metric_group_count(group_rows, METRIC_GROUP_SIGNAL_CANDIDATE)
        dislocation_count = _metric_group_count(group_rows, METRIC_GROUP_PRICE_DISLOCATION)
        unstable_count = _metric_group_count(group_rows, METRIC_GROUP_UNSTABLE)
        denominator_count = sum(1 for row in group_rows if has_denominator_risk(row))
        counts = {
            METRIC_GROUP_CONTEXT: context_count,
            METRIC_GROUP_SIGNAL_CANDIDATE: signal_count,
            METRIC_GROUP_PRICE_DISLOCATION: dislocation_count,
            METRIC_GROUP_UNSTABLE: unstable_count,
        }
        dominant_metric_group = max(counts, key=counts.get) if counts else None
        output.append(
            {
                "review_run_id": review_run_id,
                "source_run_id": source_run_id,
                "profile_family": profile_family,
                "report_group": report_group,
                "rank_type": rank_type,
                "top_k": top_k,
                "context_metric_count": context_count,
                "signal_candidate_count": signal_count,
                "price_dislocation_count": dislocation_count,
                "unstable_count": unstable_count,
                "denominator_risk_count": denominator_count,
                "context_share": _share(context_count, total),
                "unstable_share": _share(unstable_count, total),
                "denominator_risk_share": _share(denominator_count, total),
                "dominant_metric_group": dominant_metric_group,
                "recommended_action": _context_dominance_action(
                    context_count,
                    unstable_count,
                    denominator_count,
                    total,
                ),
                "created_at": created_at,
            }
        )
    return output


def event_narrative_row(
    event: Mapping[str, Any],
    rows: Iterable[Mapping[str, Any]],
    *,
    review_run_id: str,
    source_run_id: str,
    created_at: datetime,
    normal_sample_quality: str = "not_reviewed",
) -> dict[str, Any]:
    """Build one compact descriptive narrative row for an event."""

    row_list = list(rows)
    exchanges = sorted({str(row.get("exchange")) for row in row_list if row.get("exchange")})
    pairs = sorted({str(row.get("exchange_pair")) for row in row_list if row.get("exchange_pair")})
    denominator_count = sum(1 for row in row_list if has_denominator_risk(row))
    unstable_count = sum(
        1
        for row in row_list
        if is_unstable_metric(row.get("metric_group"), row.get("metric_name"))
    )
    late_windows = sorted(
        {
            str(row.get("window_label"))
            for row in row_list
            if is_suspected_late_reaction_window(row.get("window_label"))
        }
    )
    return {
        "review_run_id": review_run_id,
        "source_run_id": source_run_id,
        "event_id": event.get("event_id"),
        "event_rank": event.get("event_rank"),
        "symbol": event.get("symbol"),
        "direction": event.get("direction"),
        "event_start_ts": event.get("event_start_ts"),
        "event_end_ts": event.get("event_end_ts"),
        "dominant_exchange_pattern": _join_or_none(exchanges),
        "cross_exchange_pattern": _join_or_none(pairs),
        "book_spread_pattern": _metric_presence(row_list, ("book", "spread")),
        "trade_flow_pattern": _metric_presence(row_list, ("trade_imbalance", "trade_count")),
        "price_return_pattern": _metric_presence(row_list, ("return", "mid_diff")),
        "normal_sample_quality": normal_sample_quality,
        "metric_anomalies": f"denominator_risk_rows={denominator_count};unstable_rows={unstable_count}",
        "suspected_late_reaction_flags": _join_or_none(late_windows),
        "notes": "descriptive_only_not_signal_validation",
        "created_at": created_at,
    }


def _direction_sign_count(rows: Iterable[Mapping[str, Any]], direction: str, sign: int) -> int:
    return sum(
        1
        for row in rows
        if str(row.get("event_direction")).upper() == direction
        and signed_value_sign(_float_or_none(row.get("signed_diff_vs_mean"))) == sign
    )


def _metric_group_count(rows: Iterable[Mapping[str, Any]], metric_group: str) -> int:
    return sum(1 for row in rows if row.get("metric_group") == metric_group)


def _context_dominance_action(
    context_count: int,
    unstable_count: int,
    denominator_count: int,
    total: int,
) -> str:
    if total <= 0:
        return "no_rows_to_review"
    if _share(context_count, total) >= 0.5:
        return "review_context_as_filter"
    if unstable_count or denominator_count:
        return "demote_unstable_or_denominator_risk"
    return "review_signal_candidate_content"


def _share(count: int, total: int) -> float | None:
    if total <= 0:
        return None
    return count / total


def _median_numeric(values: Iterable[Any]) -> float | None:
    numeric = [_float_or_none(value) for value in values]
    present = [value for value in numeric if value is not None]
    if not present:
        return None
    return float(median(present))


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _metric_presence(rows: Iterable[Mapping[str, Any]], tokens: tuple[str, ...]) -> str:
    count = sum(
        1
        for row in rows
        if any(token in str(row.get("metric_name", "")) for token in tokens)
    )
    if count:
        return f"present_rows={count}"
    return "not_observed_in_review_rows"


def _join_or_none(values: Iterable[str]) -> str | None:
    clean = [value for value in values if value]
    if not clean:
        return None
    return ",".join(clean)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
