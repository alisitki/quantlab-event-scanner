"""Pure helpers for Phase 2F event-vs-normal profile comparisons."""

from __future__ import annotations

from datetime import datetime, timezone


METRIC_GROUP_CONTEXT = "context"
METRIC_GROUP_SIGNAL_CANDIDATE = "signal_candidate"
METRIC_GROUP_PRICE_DISLOCATION = "price_dislocation"
METRIC_GROUP_UNSTABLE = "unstable"

METRIC_GROUPS = (
    METRIC_GROUP_CONTEXT,
    METRIC_GROUP_SIGNAL_CANDIDATE,
    METRIC_GROUP_PRICE_DISLOCATION,
    METRIC_GROUP_UNSTABLE,
)

SMALL_DENOMINATOR_ABS_THRESHOLD = 1e-9

EXCHANGE_PROFILE_COMPARISON_METRICS = (
    "seconds_observed",
    "seconds_with_trades",
    "seconds_with_bbo_update",
    "seconds_forward_filled_bbo",
    "trade_count_sum",
    "trade_count_per_second_avg",
    "trade_count_per_second_max",
    "trade_volume_sum",
    "trade_notional_sum",
    "buy_qty_sum",
    "sell_qty_sum",
    "trade_imbalance_qty_sum",
    "trade_imbalance_qty_per_second",
    "trade_imbalance_qty_ratio",
    "last_trade_return_bps",
    "last_mid_return_bps",
    "avg_spread_bps_mean",
    "avg_spread_bps_max",
    "last_spread_bps_last",
    "avg_book_imbalance_mean",
    "last_book_imbalance_last",
    "bbo_quote_age_seconds_max",
)

CROSS_EXCHANGE_MID_DIFF_COMPARISON_METRICS = (
    "avg_mid_diff_bps",
    "max_abs_mid_diff_bps",
    "last_mid_diff_bps",
    "last_abs_mid_diff_bps",
    "seconds_compared",
)

BUCKET_CHANGE_COMPARISON_SOURCE_METRICS = (
    "trade_count_per_second",
    "trade_imbalance_qty_per_second",
    "avg_spread_bps",
    "avg_book_imbalance",
)

BUCKET_CHANGE_COMPARISON_VALUE_FIELDS = ("absolute_change", "relative_change")

DETERMINISTIC_EXCHANGE_PAIRS = (
    ("binance", "bybit"),
    ("binance", "okx"),
    ("bybit", "okx"),
)

CONTEXT_METRICS = (
    "seconds_observed",
    "seconds_with_trades",
    "seconds_with_bbo_update",
    "seconds_forward_filled_bbo",
    "seconds_compared",
    "trade_count_sum",
    "trade_count_per_second_avg",
    "trade_count_per_second_max",
    "trade_volume_sum",
    "trade_notional_sum",
    "buy_qty_sum",
    "sell_qty_sum",
    "bbo_quote_age_seconds_max",
    "trade_count_per_second.absolute_change",
)

SIGNAL_CANDIDATE_METRICS = (
    "buy_trade_count",
    "sell_trade_count",
    "trade_imbalance_qty_sum",
    "trade_imbalance_qty_per_second",
    "trade_imbalance_qty_ratio",
    "buy_notional",
    "sell_notional",
    "avg_spread_bps_mean",
    "avg_spread_bps_max",
    "last_spread_bps_last",
    "avg_book_imbalance_mean",
    "last_book_imbalance_last",
    "trade_imbalance_qty_per_second.absolute_change",
    "avg_spread_bps.absolute_change",
    "avg_book_imbalance.absolute_change",
)

PRICE_DISLOCATION_METRICS = (
    "last_trade_return_bps",
    "last_mid_return_bps",
    "avg_mid_diff_bps",
    "max_abs_mid_diff_bps",
    "last_mid_diff_bps",
    "last_abs_mid_diff_bps",
)


def default_phase2f_run_id(now: datetime) -> str:
    """Return a stable Phase 2F run ID for the given timestamp."""

    utc_now = _as_utc(now)
    return f"phase2f_{utc_now.strftime('%Y%m%dT%H%M%SZ')}"


def comparison_signed_diff(
    event_value: float | None,
    normal_value: float | None,
) -> float | None:
    """Return event-minus-normal difference, null-safe."""

    if event_value is None or normal_value is None:
        return None
    return event_value - normal_value


def comparison_absolute_diff(
    event_value: float | None,
    normal_value: float | None,
) -> float | None:
    """Return absolute event-vs-normal difference, null-safe."""

    signed_diff = comparison_signed_diff(event_value, normal_value)
    if signed_diff is None:
        return None
    return abs(signed_diff)


def comparison_ratio(
    event_value: float | None,
    normal_value: float | None,
) -> float | None:
    """Return event / normal, null-safe with zero-denominator protection."""

    if event_value is None or normal_value is None or normal_value == 0:
        return None
    return event_value / normal_value


def metric_group_for_metric(metric_name: str) -> str:
    """Return the approved Phase 2I metric group for a curated metric."""

    if metric_name.endswith(".relative_change"):
        return METRIC_GROUP_UNSTABLE
    if metric_name in CONTEXT_METRICS:
        return METRIC_GROUP_CONTEXT
    if metric_name in SIGNAL_CANDIDATE_METRICS:
        return METRIC_GROUP_SIGNAL_CANDIDATE
    if metric_name in PRICE_DISLOCATION_METRICS:
        return METRIC_GROUP_PRICE_DISLOCATION
    raise ValueError(f"Unknown curated metric group for metric: {metric_name}")


def near_small_denominator(
    value: float | None,
    *,
    threshold: float = SMALL_DENOMINATOR_ABS_THRESHOLD,
) -> bool:
    """Return whether a denominator is null or technically near zero."""

    return value is None or abs(value) <= threshold


def ratio_is_unstable(
    normal_value: float | None,
    *,
    threshold: float = SMALL_DENOMINATOR_ABS_THRESHOLD,
) -> bool:
    """Return whether event/normal ratio has a weak denominator."""

    return near_small_denominator(normal_value, threshold=threshold)


def relative_change_is_unstable(
    metric_name: str,
    *from_bucket_values: float | None,
    threshold: float = SMALL_DENOMINATOR_ABS_THRESHOLD,
) -> bool:
    """Return whether a relative-change metric has a weak source bucket."""

    if not metric_name.endswith(".relative_change"):
        return False
    return any(
        near_small_denominator(value, threshold=threshold)
        for value in from_bucket_values
    )


def small_denominator_flag(
    metric_name: str,
    normal_value: float | None,
    from_bucket_value: float | None = None,
    *,
    threshold: float = SMALL_DENOMINATOR_ABS_THRESHOLD,
) -> bool:
    """Return the row-level interpretation-risk flag for weak denominators."""

    return ratio_is_unstable(
        normal_value,
        threshold=threshold,
    ) or relative_change_is_unstable(
        metric_name,
        from_bucket_value,
        threshold=threshold,
    )


def exchange_pair_name(exchange_a: str, exchange_b: str) -> str:
    """Return the deterministic double-underscore exchange-pair name."""

    return f"{exchange_a.lower()}__{exchange_b.lower()}"


def deterministic_exchange_pair_names() -> tuple[str, ...]:
    """Return approved Phase 2F exchange-pair names in deterministic order."""

    return tuple(
        exchange_pair_name(exchange_a, exchange_b)
        for exchange_a, exchange_b in DETERMINISTIC_EXCHANGE_PAIRS
    )


def missing_curated_metrics(
    available_columns: tuple[str, ...] | list[str] | set[str],
    required_metrics: tuple[str, ...],
) -> tuple[str, ...]:
    """Return curated metrics that are absent from a source table."""

    available = set(available_columns)
    return tuple(metric for metric in required_metrics if metric not in available)


def validate_curated_metrics(
    available_columns: tuple[str, ...] | list[str] | set[str],
    required_metrics: tuple[str, ...],
) -> None:
    """Fail fast when any curated metric is missing."""

    missing = missing_curated_metrics(available_columns, required_metrics)
    if missing:
        raise ValueError(f"Missing curated metrics: {', '.join(missing)}")


def expected_exchange_profile_comparison_rows(
    exchange_count: int = 3,
    window_count: int = 10,
) -> int:
    """Return expected exchange-profile comparison rows."""

    return exchange_count * window_count * len(EXCHANGE_PROFILE_COMPARISON_METRICS)


def expected_cross_exchange_mid_diff_comparison_rows(
    exchange_pair_count: int = 3,
    window_count: int = 10,
) -> int:
    """Return expected cross-exchange mid-diff comparison rows."""

    return exchange_pair_count * window_count * len(CROSS_EXCHANGE_MID_DIFF_COMPARISON_METRICS)


def expected_bucket_change_profile_comparison_rows(
    exchange_count: int = 3,
    bucket_transition_count: int = 4,
) -> int:
    """Return expected bucket-change comparison rows."""

    return (
        exchange_count
        * bucket_transition_count
        * len(BUCKET_CHANGE_COMPARISON_SOURCE_METRICS)
        * len(BUCKET_CHANGE_COMPARISON_VALUE_FIELDS)
    )


def bucket_change_comparison_metric_names() -> tuple[str, ...]:
    """Return output metric names emitted by bucket-change comparisons."""

    return tuple(
        f"{source_metric}.{value_field}"
        for source_metric in BUCKET_CHANGE_COMPARISON_SOURCE_METRICS
        for value_field in BUCKET_CHANGE_COMPARISON_VALUE_FIELDS
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
