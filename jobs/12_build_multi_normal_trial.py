#!/usr/bin/env python
"""Phase 2J multi-normal comparison trial."""

from __future__ import annotations

import argparse
import importlib.util
import logging
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable


def _script_path() -> Path | None:
    script_name = globals().get("__file__") or globals().get("filename")
    if not isinstance(script_name, str):
        return None
    return Path(script_name).resolve()


def _bootstrap_src_path() -> None:
    job_file = _script_path()
    candidates = [Path.cwd() / "src"]
    if job_file is not None:
        candidates.insert(0, job_file.parents[1] / "src")

    for candidate in candidates:
        if candidate.exists():
            sys.path.insert(0, str(candidate))
            return


_bootstrap_src_path()

from pyspark.sql import DataFrame, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from quantlab_event_scanner.config import load_config  # noqa: E402
from quantlab_event_scanner.manifest import ManifestPartition  # noqa: E402
from quantlab_event_scanner.manifest import load_manifest_from_s3_with_spark  # noqa: E402
from quantlab_event_scanner.multi_normal_trial import (  # noqa: E402
    ACTIVITY_DISTANCE_METRICS,
    ACTIVITY_DISTANCE_WINDOW_LABEL,
    DEFAULT_MIN_ANCHOR_SPACING_SECONDS,
    DEFAULT_NORMAL_SAMPLE_COUNT,
    QualityPassedNormalCandidate,
    default_phase2j_run_id,
    expected_multi_normal_profile_rows,
    exclusion_reason_counts,
    select_activity_matched_candidates,
)
from quantlab_event_scanner.normal_time_trial import (  # noqa: E402
    SAMPLE_TYPE_NORMAL,
    build_normal_selection_metadata,
    exclusion_reason,
    normal_window_for_anchor,
    selected_date_utc_bounds,
)
from quantlab_event_scanner.paths import (  # noqa: E402
    multi_normal_comparison_reports_trial_run_path,
    multi_normal_market_snapshots_trial_run_path,
    multi_normal_profile_reports_trial_run_path,
    multi_normal_top_diffs_trial_run_path,
    multi_normal_windows_trial_run_path,
    pre_event_profile_reports_trial_run_path,
)
from quantlab_event_scanner.pre_event_market_snapshots import exchange_coverage  # noqa: E402
from quantlab_event_scanner.profile_comparison_reports import (  # noqa: E402
    BUCKET_CHANGE_COMPARISON_SOURCE_METRICS,
    BUCKET_CHANGE_COMPARISON_VALUE_FIELDS,
    CONTEXT_METRICS,
    CROSS_EXCHANGE_MID_DIFF_COMPARISON_METRICS,
    EXCHANGE_PROFILE_COMPARISON_METRICS,
    METRIC_GROUP_CONTEXT,
    METRIC_GROUP_PRICE_DISLOCATION,
    METRIC_GROUP_SIGNAL_CANDIDATE,
    METRIC_GROUP_UNSTABLE,
    PRICE_DISLOCATION_METRICS,
    SIGNAL_CANDIDATE_METRICS,
    SMALL_DENOMINATOR_ABS_THRESHOLD,
    expected_bucket_change_profile_comparison_rows,
    expected_cross_exchange_mid_diff_comparison_rows,
    expected_exchange_profile_comparison_rows,
)
from quantlab_event_scanner.probe import partition_paths  # noqa: E402
from quantlab_event_scanner.trade_move_scan import (  # noqa: E402
    REQUIRED_TRADE_MOVE_COLUMNS,
    build_price_1s,
    scan_move_candidates,
    timestamp_expression,
    validate_required_columns,
)


LOGGER = logging.getLogger("quantlab_event_scanner.phase2j_multi_normal_trial")

DEFAULT_DATE = "20260423"
DEFAULT_EVENT_PROFILE_RUN_ID = "phase2d_20260427T090707Z"
DEFAULT_EVENT_ID = "binance_btcusdt_20260423_down_001"
DEFAULT_LOOKBACK_SECONDS = 300
DEFAULT_THRESHOLD_PCT = 1.0
DEFAULT_HORIZON_SECONDS = 60
DEFAULT_PROXIMITY_SECONDS = 1800
DEFAULT_ANCHOR_STEP_SECONDS = 60

EXCHANGE_PROFILE_KEYS = ("exchange", "symbol_key", "window_mode", "window_label")
CROSS_EXCHANGE_KEYS = ("symbol_key", "window_mode", "window_label", "exchange_pair")
BUCKET_CHANGE_KEYS = (
    "exchange",
    "symbol_key",
    "from_bucket",
    "to_bucket",
    "source_metric_name",
    "comparison_value_field",
)

MULTI_NORMAL_RANKINGS = (
    ("top_absolute_diff_vs_mean", "absolute_diff_vs_mean", False),
    ("top_signed_positive_vs_mean", "signed_diff_vs_mean", False),
    ("top_signed_negative_vs_mean", "signed_diff_vs_mean", True),
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


@dataclass(frozen=True)
class Phase2JNormalMetadata:
    sample_type: str
    selected_date: str
    normal_window_start_ts: datetime
    normal_window_end_ts: datetime
    normal_anchor_ts: datetime
    lookback_seconds: int
    selection_reason: str
    excluded_candidate_count: int
    nearest_candidate_distance_seconds: int | None
    normal_sample_id: str
    normal_sample_rank: int
    activity_distance: float
    selected_normal_count: int
    min_anchor_spacing_seconds: int


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    phase2e = _load_job_module("phase2e_normal_time_trial", "09_build_normal_time_trial.py")
    phase2f = _load_job_module(
        "phase2f_profile_comparison_trial",
        "10_compare_event_vs_normal_profiles_trial.py",
    )

    config_path = _resolve_path(args.config)
    config = load_config(config_path)
    run_id = args.run_id or default_phase2j_run_id(datetime.now(timezone.utc))
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)

    windows_output_path = multi_normal_windows_trial_run_path(config, run_id)
    snapshot_output_path = multi_normal_market_snapshots_trial_run_path(config, run_id)
    profile_output_path = multi_normal_profile_reports_trial_run_path(config, run_id)
    comparison_output_path = multi_normal_comparison_reports_trial_run_path(config, run_id)
    top_diff_output_path = multi_normal_top_diffs_trial_run_path(config, run_id)
    event_profile_path = pre_event_profile_reports_trial_run_path(
        config,
        args.event_profile_run_id,
    )

    LOGGER.info("Using config: %s", config_path)
    LOGGER.info("Run ID: %s", run_id)
    LOGGER.info("Event profile run ID: %s", args.event_profile_run_id)
    LOGGER.info("Event ID: %s", args.event_id)
    LOGGER.info("Selected date target: %s", args.date)
    LOGGER.info("Expected exchanges: %s", ", ".join(config.exchanges))
    LOGGER.info("Selected normal count: %s", args.selected_normal_count)
    LOGGER.info("Min anchor spacing seconds: %s", args.min_anchor_spacing_seconds)
    LOGGER.info("Windows output path: %s", windows_output_path)
    LOGGER.info("Snapshot output path: %s", snapshot_output_path)
    LOGGER.info("Profile output path: %s", profile_output_path)
    LOGGER.info("Comparison output path: %s", comparison_output_path)
    LOGGER.info("Top diff output path: %s", top_diff_output_path)

    spark = _get_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    LOGGER.info("Spark timezone: %s", spark.conf.get("spark.sql.session.timeZone"))

    event_exchange = _read_event_profile_subtable(
        spark,
        event_profile_path,
        "exchange_profile",
        args.event_id,
    ).cache()
    event_cross = _read_event_profile_subtable(
        spark,
        event_profile_path,
        "cross_exchange_mid_diff",
        args.event_id,
    ).cache()
    event_bucket = _read_event_profile_subtable(
        spark,
        event_profile_path,
        "bucket_change_profile",
        args.event_id,
    ).cache()
    event_activity = _event_activity_source(event_exchange, config.exchanges).cache()

    manifest = load_manifest_from_s3_with_spark(spark, config.manifest_path)
    trade_partitions = _select_partitions(
        manifest.partitions,
        exchanges=config.exchanges,
        stream="trade",
        date=args.date,
    )
    bbo_partitions = _select_partitions(
        manifest.partitions,
        exchanges=config.exchanges,
        stream="bbo",
        date=args.date,
    )
    _log_partition_coverage("trade manifest", config.exchanges, trade_partitions)
    _log_partition_coverage("BBO manifest", config.exchanges, bbo_partitions)

    trade_day = _read_partitions(spark, trade_partitions, config.input_root, "trade").cache()
    bbo_day = _read_partitions(spark, bbo_partitions, config.input_root, "BBO").cache()
    _validate_required_trade_columns(trade_day)
    _validate_required_bbo_columns(bbo_day)

    price_1s = build_price_1s(trade_day)
    if price_1s is None:
        raise RuntimeError("Unable to build 1s price rows for normal window selection.")
    price_1s = price_1s.cache()
    price_1s_count = price_1s.count()
    LOGGER.info("Phase 2J price_1s_count: %s", price_1s_count)
    if price_1s_count == 0:
        raise RuntimeError("1s price row count is 0.")

    candidates, ambiguous = scan_move_candidates(
        price_1s,
        threshold_pct=args.threshold_pct,
        lookahead_seconds=args.horizon_seconds,
    )
    candidates = candidates.cache()
    ambiguous = ambiguous.cache()
    non_ambiguous_candidate_count = candidates.count()
    ambiguous_count = ambiguous.count()
    raw_candidate_count = non_ambiguous_candidate_count + ambiguous_count
    candidate_starts = _collect_raw_candidate_starts(candidates, ambiguous)
    LOGGER.info("Phase 2J raw candidate count: %s", raw_candidate_count)
    LOGGER.info("Phase 2J non-ambiguous candidate count: %s", non_ambiguous_candidate_count)
    LOGGER.info("Phase 2J ambiguous candidate count: %s", ambiguous_count)
    LOGGER.info("Phase 2J distinct raw candidate start count: %s", len(candidate_starts))

    anchor_candidates, anchor_exclusion_counts, total_anchors = _eligible_anchor_candidates(
        price_1s,
        candidate_starts,
        selected_date=args.date,
        lookback_seconds=args.lookback_seconds,
        horizon_seconds=args.horizon_seconds,
        proximity_seconds=args.proximity_seconds,
        anchor_step_seconds=args.anchor_step_seconds,
    )
    LOGGER.info("Phase 2J total anchor candidates enumerated: %s", total_anchors)
    LOGGER.info("Phase 2J eligible anchor candidate count: %s", len(anchor_candidates))
    LOGGER.info("Phase 2J exclusion count by reason: %s", anchor_exclusion_counts)
    if not anchor_candidates:
        raise RuntimeError("No eligible normal anchor candidates found.")

    activity_ranked_candidates = _activity_ranked_anchor_candidates(
        trade_day,
        event_activity,
        config.exchanges,
        anchor_candidates,
        candidate_starts,
        lookback_seconds=args.lookback_seconds,
    )
    LOGGER.info("Phase 2J activity-ranked candidate count: %s", len(activity_ranked_candidates))

    (
        selected,
        quality_failure_counts,
        quality_probe_count,
        quality_pass_count,
        spacing_skipped_count,
    ) = _select_quality_passed_candidates(
        phase2e,
        trade_day,
        bbo_day,
        config.exchanges,
        args.date,
        run_id,
        created_at,
        activity_ranked_candidates,
        candidate_starts,
        lookback_seconds=args.lookback_seconds,
        excluded_candidate_count=total_anchors - len(anchor_candidates),
        selected_normal_count=args.selected_normal_count,
        min_anchor_spacing_seconds=args.min_anchor_spacing_seconds,
        max_quality_candidates=args.max_quality_candidates,
    )
    LOGGER.info("Phase 2J quality probes attempted: %s", quality_probe_count)
    LOGGER.info("Phase 2J quality-failed count by reason: %s", dict(sorted(quality_failure_counts.items())))
    LOGGER.info("Phase 2J quality-passing candidate count: %s", quality_pass_count)
    LOGGER.info("Phase 2J spacing-skipped count: %s", spacing_skipped_count)
    LOGGER.info("Phase 2J selected normal count: %s", len(selected))
    for candidate in selected:
        LOGGER.info("Phase 2J selected normal candidate: %s", candidate)
    if len(selected) != args.selected_normal_count:
        raise RuntimeError(
            "Selected normal count mismatch: "
            f"expected={args.selected_normal_count}, observed={len(selected)}"
        )

    (
        normal_trade,
        normal_bbo,
        normal_snapshot,
        normal_exchange,
        normal_cross,
        normal_bucket,
    ) = _build_selected_outputs(
        phase2e,
        trade_day,
        bbo_day,
        config.exchanges,
        args.date,
        run_id,
        created_at,
        selected,
        candidate_starts,
        lookback_seconds=args.lookback_seconds,
        excluded_candidate_count=total_anchors - len(anchor_candidates),
        selected_normal_count=args.selected_normal_count,
        min_anchor_spacing_seconds=args.min_anchor_spacing_seconds,
    )

    _validate_selected_output_counts(
        normal_snapshot,
        normal_exchange,
        normal_cross,
        normal_bucket,
        exchange_count=len(config.exchanges),
        lookback_seconds=args.lookback_seconds,
        selected_normal_count=args.selected_normal_count,
    )

    phase2e._write_and_validate(
        normal_trade,
        f"{windows_output_path}/trade_windows",
        "multi_normal_windows.trade_windows",
        args.sample_size,
    )
    phase2e._write_and_validate(
        normal_bbo,
        f"{windows_output_path}/bbo_windows",
        "multi_normal_windows.bbo_windows",
        args.sample_size,
    )
    phase2e._write_and_validate(
        normal_snapshot,
        snapshot_output_path,
        "multi_normal_market_snapshots",
        args.sample_size,
    )
    phase2e._write_and_validate(
        normal_exchange,
        f"{profile_output_path}/exchange_profile",
        "multi_normal_profile_reports.exchange_profile",
        args.sample_size,
    )
    phase2e._write_and_validate(
        normal_cross,
        f"{profile_output_path}/cross_exchange_mid_diff",
        "multi_normal_profile_reports.cross_exchange_mid_diff",
        args.sample_size,
    )
    phase2e._write_and_validate(
        normal_bucket,
        f"{profile_output_path}/bucket_change_profile",
        "multi_normal_profile_reports.bucket_change_profile",
        args.sample_size,
    )

    exchange_comparison = _build_exchange_profile_distribution_comparison(
        phase2f,
        event_exchange,
        normal_exchange,
        run_id,
        args.event_profile_run_id,
        created_at,
    ).cache()
    cross_comparison = _build_cross_exchange_distribution_comparison(
        phase2f,
        event_cross,
        normal_cross,
        run_id,
        args.event_profile_run_id,
        created_at,
    ).cache()
    bucket_comparison = _build_bucket_change_distribution_comparison(
        phase2f,
        event_bucket,
        normal_bucket,
        run_id,
        args.event_profile_run_id,
        created_at,
    ).cache()

    _validate_comparison_count(
        exchange_comparison,
        "multi_normal_comparison_reports.exchange_profile_comparison",
        expected_exchange_profile_comparison_rows(),
    )
    _validate_normal_sample_count(
        exchange_comparison,
        "multi_normal_comparison_reports.exchange_profile_comparison",
        args.selected_normal_count,
    )
    _validate_comparison_count(
        cross_comparison,
        "multi_normal_comparison_reports.cross_exchange_mid_diff_comparison",
        expected_cross_exchange_mid_diff_comparison_rows(),
    )
    _validate_normal_sample_count(
        cross_comparison,
        "multi_normal_comparison_reports.cross_exchange_mid_diff_comparison",
        args.selected_normal_count,
    )
    _validate_comparison_count(
        bucket_comparison,
        "multi_normal_comparison_reports.bucket_change_profile_comparison",
        expected_bucket_change_profile_comparison_rows(),
    )
    _validate_normal_sample_count(
        bucket_comparison,
        "multi_normal_comparison_reports.bucket_change_profile_comparison",
        args.selected_normal_count,
    )
    _validate_metric_group_coverage(exchange_comparison, "exchange_profile_comparison")
    _validate_metric_group_coverage(cross_comparison, "cross_exchange_mid_diff_comparison")
    _validate_metric_group_coverage(bucket_comparison, "bucket_change_profile_comparison")
    _validate_absolute_diff(exchange_comparison, "exchange_profile_comparison")
    _validate_absolute_diff(cross_comparison, "cross_exchange_mid_diff_comparison")
    _validate_absolute_diff(bucket_comparison, "bucket_change_profile_comparison")

    phase2e._write_and_validate(
        exchange_comparison,
        f"{comparison_output_path}/exchange_profile_comparison",
        "multi_normal_comparison_reports.exchange_profile_comparison",
        args.sample_size,
    )
    phase2e._write_and_validate(
        cross_comparison,
        f"{comparison_output_path}/cross_exchange_mid_diff_comparison",
        "multi_normal_comparison_reports.cross_exchange_mid_diff_comparison",
        args.sample_size,
    )
    phase2e._write_and_validate(
        bucket_comparison,
        f"{comparison_output_path}/bucket_change_profile_comparison",
        "multi_normal_comparison_reports.bucket_change_profile_comparison",
        args.sample_size,
    )

    exchange_comparison_for_top_diffs = spark.read.parquet(
        f"{comparison_output_path}/exchange_profile_comparison"
    ).cache()
    cross_comparison_for_top_diffs = spark.read.parquet(
        f"{comparison_output_path}/cross_exchange_mid_diff_comparison"
    ).cache()
    bucket_comparison_for_top_diffs = spark.read.parquet(
        f"{comparison_output_path}/bucket_change_profile_comparison"
    ).cache()
    LOGGER.info("Phase 2J top-diff source: persisted multi-normal comparison readbacks")

    _write_top_diffs(
        phase2e,
        exchange_comparison_for_top_diffs,
        f"{top_diff_output_path}/exchange_profile_top_diffs",
        "multi_normal_top_diffs.exchange_profile_top_diffs",
        run_id,
        args.top_n,
        created_at,
        args.sample_size,
    )
    _write_top_diffs(
        phase2e,
        cross_comparison_for_top_diffs,
        f"{top_diff_output_path}/cross_exchange_mid_diff_top_diffs",
        "multi_normal_top_diffs.cross_exchange_mid_diff_top_diffs",
        run_id,
        args.top_n,
        created_at,
        args.sample_size,
    )
    _write_top_diffs(
        phase2e,
        bucket_comparison_for_top_diffs,
        f"{top_diff_output_path}/bucket_change_profile_top_diffs",
        "multi_normal_top_diffs.bucket_change_profile_top_diffs",
        run_id,
        args.top_n,
        created_at,
        args.sample_size,
    )

    bucket_comparison_for_top_diffs.unpersist()
    cross_comparison_for_top_diffs.unpersist()
    exchange_comparison_for_top_diffs.unpersist()
    bucket_comparison.unpersist()
    cross_comparison.unpersist()
    exchange_comparison.unpersist()
    normal_bucket.unpersist()
    normal_cross.unpersist()
    normal_exchange.unpersist()
    normal_snapshot.unpersist()
    normal_bbo.unpersist()
    normal_trade.unpersist()
    ambiguous.unpersist()
    candidates.unpersist()
    price_1s.unpersist()
    bbo_day.unpersist()
    trade_day.unpersist()
    event_activity.unpersist()
    event_bucket.unpersist()
    event_cross.unpersist()
    event_exchange.unpersist()
    LOGGER.info("Phase 2J multi-normal comparison trial complete.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Phase 2J multi-normal comparison trial.")
    parser.add_argument("--config", default="configs/dev.yaml")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--event-profile-run-id", default=DEFAULT_EVENT_PROFILE_RUN_ID)
    parser.add_argument("--event-id", default=DEFAULT_EVENT_ID)
    parser.add_argument("--date", default=DEFAULT_DATE)
    parser.add_argument("--lookback-seconds", type=int, default=DEFAULT_LOOKBACK_SECONDS)
    parser.add_argument("--threshold-pct", type=float, default=DEFAULT_THRESHOLD_PCT)
    parser.add_argument("--horizon-seconds", type=int, default=DEFAULT_HORIZON_SECONDS)
    parser.add_argument("--proximity-seconds", type=int, default=DEFAULT_PROXIMITY_SECONDS)
    parser.add_argument("--anchor-step-seconds", type=int, default=DEFAULT_ANCHOR_STEP_SECONDS)
    parser.add_argument("--selected-normal-count", type=int, default=DEFAULT_NORMAL_SAMPLE_COUNT)
    parser.add_argument(
        "--min-anchor-spacing-seconds",
        type=int,
        default=DEFAULT_MIN_ANCHOR_SPACING_SECONDS,
    )
    parser.add_argument(
        "--max-quality-candidates",
        type=int,
        default=0,
        help="Maximum eligible anchors to quality-probe; 0 means all eligible anchors.",
    )
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--sample-size", type=int, default=20)
    return parser.parse_args()


def _select_partitions(
    partitions: Iterable[ManifestPartition],
    exchanges: tuple[str, ...],
    stream: str,
    date: str,
) -> tuple[ManifestPartition, ...]:
    exchange_set = {exchange.lower() for exchange in exchanges}
    selected = tuple(
        partition
        for partition in partitions
        if partition.available is not False
        and partition.exchange is not None
        and partition.exchange.lower() in exchange_set
        and partition.stream == stream
        and partition.symbol is not None
        and "btc" in partition.symbol.lower()
        and partition.date == date
    )
    if not selected:
        raise RuntimeError(f"No {stream} partitions selected for date={date}.")
    return selected


def _log_partition_coverage(
    label: str,
    expected_exchanges: tuple[str, ...],
    partitions: tuple[ManifestPartition, ...],
) -> None:
    observed = tuple(sorted({partition.exchange.lower() for partition in partitions if partition.exchange}))
    coverage = exchange_coverage(expected_exchanges, observed)
    LOGGER.info(
        "%s coverage: expected=%s observed=%s missing=%s",
        label,
        ", ".join(coverage.expected) or "none",
        ", ".join(coverage.observed) or "none",
        ", ".join(coverage.missing) or "none",
    )
    if coverage.missing:
        raise RuntimeError(f"Missing required {label} coverage: {coverage.missing}")


def _read_partitions(
    spark: Any,
    partitions: tuple[ManifestPartition, ...],
    input_root: str,
    label: str,
) -> DataFrame:
    paths = partition_paths(partitions, input_root=input_root)
    LOGGER.info("%s selected parquet path count: %s", label, len(paths))
    for path in paths[:20]:
        LOGGER.info("  %s", path)
    return spark.read.parquet(*paths)


def _validate_required_trade_columns(frame: DataFrame) -> None:
    missing = validate_required_columns(frame, REQUIRED_TRADE_MOVE_COLUMNS)
    if missing:
        raise RuntimeError(f"Missing required trade columns: {', '.join(missing)}")


def _validate_required_bbo_columns(frame: DataFrame) -> None:
    required = ("ts_event", "ts_recv", "exchange", "symbol", "bid_price", "bid_qty", "ask_price", "ask_qty")
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise RuntimeError(f"Missing required BBO columns: {', '.join(missing)}")


def _collect_raw_candidate_starts(candidates: DataFrame, ambiguous: DataFrame) -> tuple[datetime, ...]:
    rows = (
        candidates.select("start_ts")
        .unionByName(ambiguous.select("start_ts"))
        .distinct()
        .orderBy("start_ts")
        .collect()
    )
    return tuple(row["start_ts"] for row in rows)


def _nearest_candidate_distance_seconds(
    anchor_ts: datetime,
    candidate_start_timestamps: tuple[datetime, ...],
) -> int | None:
    if not candidate_start_timestamps:
        return None
    return min(int(abs((candidate_ts - anchor_ts).total_seconds())) for candidate_ts in candidate_start_timestamps)


def _eligible_anchor_candidates(
    price_1s: DataFrame,
    candidate_starts: tuple[datetime, ...],
    *,
    selected_date: str,
    lookback_seconds: int,
    horizon_seconds: int,
    proximity_seconds: int,
    anchor_step_seconds: int,
) -> tuple[tuple[datetime, ...], dict[str, int], int]:
    ranges = price_1s.groupBy("exchange").agg(
        F.min("second").alias("min_second"),
        F.max("second").alias("max_second"),
    ).collect()
    common_start = max(row["min_second"] for row in ranges)
    common_end = min(row["max_second"] for row in ranges)
    date_start, date_end = selected_date_utc_bounds(selected_date)
    date_start = date_start.replace(tzinfo=None)
    date_end = date_end.replace(tzinfo=None)
    first_anchor = max(
        common_start + timedelta(seconds=lookback_seconds),
        date_start + timedelta(seconds=lookback_seconds),
    )
    last_anchor = min(
        common_end - timedelta(seconds=horizon_seconds),
        date_end - timedelta(seconds=horizon_seconds),
    )
    LOGGER.info(
        "Phase 2J anchor bounds: common_start=%s common_end=%s date_start=%s date_end=%s "
        "first_anchor=%s last_anchor=%s",
        common_start,
        common_end,
        date_start,
        date_end,
        first_anchor,
        last_anchor,
    )
    if first_anchor > last_anchor:
        return (), {}, 0

    anchors: list[datetime] = []
    reasons: list[str | None] = []
    current = first_anchor
    while current <= last_anchor:
        reason = exclusion_reason(
            current,
            candidate_starts,
            lookback_seconds=lookback_seconds,
            horizon_seconds=horizon_seconds,
            proximity_seconds=proximity_seconds,
        )
        reasons.append(reason)
        if reason is None:
            anchors.append(current)
        current += timedelta(seconds=anchor_step_seconds)
    return tuple(anchors), exclusion_reason_counts(reasons), len(reasons)


def _activity_ranked_anchor_candidates(
    trade_day: DataFrame,
    event_activity: DataFrame,
    expected_exchanges: tuple[str, ...],
    anchor_candidates: tuple[datetime, ...],
    candidate_starts: tuple[datetime, ...],
    *,
    lookback_seconds: int,
) -> tuple[QualityPassedNormalCandidate, ...]:
    spark = trade_day.sparkSession
    anchor_rows = [
        (
            anchor_ts,
            anchor_ts - timedelta(seconds=lookback_seconds),
            _nearest_candidate_distance_seconds(anchor_ts, candidate_starts),
        )
        for anchor_ts in anchor_candidates
    ]
    anchor_frame = spark.createDataFrame(
        anchor_rows,
        ("normal_anchor_ts", "normal_window_start_ts", "nearest_candidate_distance_seconds"),
    )
    ts_event_expr = timestamp_expression(trade_day, "ts_event")
    if ts_event_expr is None:
        raise RuntimeError("Unable to convert trade ts_event to timestamp.")
    trades = (
        trade_day.withColumn("ts_event_ts", ts_event_expr)
        .select(
            F.lower(F.col("exchange")).alias("exchange"),
            "ts_event_ts",
            F.col("price").cast("double").alias("price"),
            F.col("qty").cast("double").alias("qty"),
        )
        .where(F.col("exchange").isin([exchange.lower() for exchange in expected_exchanges]))
        .where(F.col("ts_event_ts").isNotNull())
        .where(F.col("price").isNotNull())
        .where(F.col("qty").isNotNull())
    )
    windowed = trades.join(
        F.broadcast(anchor_frame),
        (F.col("ts_event_ts") >= F.col("normal_window_start_ts"))
        & (F.col("ts_event_ts") < F.col("normal_anchor_ts")),
        "inner",
    ).withColumn("trade_notional", F.col("price") * F.col("qty"))
    by_exchange = windowed.groupBy(
        "normal_anchor_ts",
        "nearest_candidate_distance_seconds",
        "exchange",
    ).agg(
        F.count("*").cast("double").alias("trade_count_sum"),
        F.sum("qty").alias("trade_volume_sum"),
        F.sum("trade_notional").alias("trade_notional_sum"),
    )
    normal_activity = by_exchange.select(
        "normal_anchor_ts",
        "nearest_candidate_distance_seconds",
        "exchange",
        F.expr(_stack_expr(ACTIVITY_DISTANCE_METRICS, "normal_activity_value")),
    )
    expected_activity_rows = len(expected_exchanges) * len(ACTIVITY_DISTANCE_METRICS)
    ranked = (
        normal_activity.alias("normal")
        .join(event_activity.alias("event"), ["exchange", "metric_name"], "inner")
        .groupBy("normal_anchor_ts", "nearest_candidate_distance_seconds")
        .agg(
            F.count("*").alias("activity_source_rows"),
            F.avg(
                F.abs(
                    F.log(F.col("normal.normal_activity_value") + F.lit(1.0))
                    - F.log(F.col("event.event_activity_value") + F.lit(1.0))
                )
            ).alias("activity_distance"),
        )
        .where(F.col("activity_source_rows") == F.lit(expected_activity_rows))
        .orderBy(
            F.asc("activity_distance"),
            F.desc_nulls_last("nearest_candidate_distance_seconds"),
            F.asc("normal_anchor_ts"),
        )
    )
    rows = ranked.collect()
    if not rows:
        raise RuntimeError("No activity-ranked normal anchor candidates found.")
    return tuple(
        QualityPassedNormalCandidate(
            normal_anchor_ts=row["normal_anchor_ts"],
            activity_distance=float(row["activity_distance"]),
            nearest_candidate_distance_seconds=row["nearest_candidate_distance_seconds"],
        )
        for row in rows
    )


def _select_quality_passed_candidates(
    phase2e: ModuleType,
    trade_day: DataFrame,
    bbo_day: DataFrame,
    expected_exchanges: tuple[str, ...],
    selected_date: str,
    run_id: str,
    created_at: datetime,
    activity_ranked_candidates: tuple[QualityPassedNormalCandidate, ...],
    candidate_starts: tuple[datetime, ...],
    *,
    lookback_seconds: int,
    excluded_candidate_count: int,
    selected_normal_count: int,
    min_anchor_spacing_seconds: int,
    max_quality_candidates: int,
) -> tuple[tuple[Any, ...], Counter[str], int, int, int]:
    quality_candidates: list[QualityPassedNormalCandidate] = []
    failure_counts: Counter[str] = Counter()
    spacing_skipped_count = 0
    probe_count = 0
    candidates = (
        activity_ranked_candidates[:max_quality_candidates]
        if max_quality_candidates > 0
        else activity_ranked_candidates
    )
    LOGGER.info("Phase 2J quality probes candidate limit: %s", len(candidates))

    for attempt, candidate in enumerate(candidates, start=1):
        anchor_ts = candidate.normal_anchor_ts
        if _is_within_selected_spacing(
            anchor_ts,
            quality_candidates,
            min_anchor_spacing_seconds=min_anchor_spacing_seconds,
        ):
            spacing_skipped_count += 1
            LOGGER.info(
                "Phase 2J spacing skip candidate_index=%s anchor=%s activity_distance=%s",
                attempt,
                anchor_ts,
                candidate.activity_distance,
            )
            continue
        probe_count += 1
        window = normal_window_for_anchor(anchor_ts, lookback_seconds)
        metadata = build_normal_selection_metadata(
            selected_date=selected_date,
            window=window,
            lookback_seconds=lookback_seconds,
            selection_reason="phase2j_quality_probe",
            excluded_candidate_count=excluded_candidate_count,
            candidate_start_timestamps=candidate_starts,
        )
        LOGGER.info(
            "Phase 2J quality probe attempt=%s anchor=%s activity_distance=%s "
            "nearest_candidate_distance_seconds=%s",
            attempt,
            anchor_ts,
            candidate.activity_distance,
            candidate.nearest_candidate_distance_seconds,
        )
        normal_trade = phase2e._extract_normal_trade_window(
            trade_day,
            run_id,
            metadata,
            created_at,
        ).cache()
        normal_bbo = phase2e._extract_normal_bbo_window(
            bbo_day,
            run_id,
            metadata,
            created_at,
        ).cache()
        trade_count = normal_trade.count()
        bbo_count = normal_bbo.count()
        if trade_count == 0:
            failure_counts["trade_count_zero"] += 1
            normal_trade.unpersist()
            normal_bbo.unpersist()
            continue
        if bbo_count == 0:
            failure_counts["bbo_count_zero"] += 1
            normal_trade.unpersist()
            normal_bbo.unpersist()
            continue
        if not phase2e._has_expected_exchange_counts(normal_trade, expected_exchanges, minimum_count=1):
            failure_counts["trade_exchange_coverage"] += 1
            normal_trade.unpersist()
            normal_bbo.unpersist()
            continue
        if not phase2e._has_expected_exchange_counts(normal_bbo, expected_exchanges, minimum_count=1):
            failure_counts["bbo_exchange_coverage"] += 1
            normal_trade.unpersist()
            normal_bbo.unpersist()
            continue

        normal_snapshot = phase2e._build_normal_snapshot(
            normal_trade,
            normal_bbo,
            expected_exchanges,
            run_id,
            metadata,
            created_at,
        ).cache()
        snapshot_count = normal_snapshot.count()
        if snapshot_count != len(expected_exchanges) * lookback_seconds:
            failure_counts["snapshot_row_count"] += 1
            normal_snapshot.unpersist()
            normal_trade.unpersist()
            normal_bbo.unpersist()
            continue
        if not phase2e._has_expected_exchange_counts(
            normal_snapshot,
            expected_exchanges,
            minimum_count=lookback_seconds,
        ):
            failure_counts["snapshot_exchange_coverage"] += 1
            normal_snapshot.unpersist()
            normal_trade.unpersist()
            normal_bbo.unpersist()
            continue

        quality_candidates.append(
            QualityPassedNormalCandidate(
                normal_anchor_ts=anchor_ts,
                activity_distance=candidate.activity_distance,
                nearest_candidate_distance_seconds=metadata.nearest_candidate_distance_seconds,
            )
        )
        LOGGER.info(
            "Phase 2J quality pass attempt=%s anchor=%s activity_distance=%s "
            "nearest_candidate_distance_seconds=%s",
            attempt,
            anchor_ts,
            candidate.activity_distance,
            metadata.nearest_candidate_distance_seconds,
        )
        normal_snapshot.unpersist()
        normal_trade.unpersist()
        normal_bbo.unpersist()
        selected = select_activity_matched_candidates(
            quality_candidates,
            selected_count=selected_normal_count,
            min_anchor_spacing_seconds=min_anchor_spacing_seconds,
        )
        if len(selected) == selected_normal_count:
            return (
                selected,
                failure_counts,
                probe_count,
                len(quality_candidates),
                spacing_skipped_count,
            )

    raise RuntimeError(
        "Unable to select enough quality-passing normal candidates: "
        f"required={selected_normal_count}, quality_passed={len(quality_candidates)}, "
        f"probed={probe_count}, spacing_skipped={spacing_skipped_count}"
    )


def _build_selected_outputs(
    phase2e: ModuleType,
    trade_day: DataFrame,
    bbo_day: DataFrame,
    expected_exchanges: tuple[str, ...],
    selected_date: str,
    run_id: str,
    created_at: datetime,
    selected: tuple[Any, ...],
    candidate_starts: tuple[datetime, ...],
    *,
    lookback_seconds: int,
    excluded_candidate_count: int,
    selected_normal_count: int,
    min_anchor_spacing_seconds: int,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    trade_frames: list[DataFrame] = []
    bbo_frames: list[DataFrame] = []
    snapshot_frames: list[DataFrame] = []
    exchange_frames: list[DataFrame] = []
    cross_frames: list[DataFrame] = []
    bucket_frames: list[DataFrame] = []

    for candidate in selected:
        window = normal_window_for_anchor(candidate.normal_anchor_ts, lookback_seconds)
        base_metadata = build_normal_selection_metadata(
            selected_date=selected_date,
            window=window,
            lookback_seconds=lookback_seconds,
            selection_reason="activity_matched_multi_normal",
            excluded_candidate_count=excluded_candidate_count,
            candidate_start_timestamps=candidate_starts,
        )
        metadata = Phase2JNormalMetadata(
            sample_type=SAMPLE_TYPE_NORMAL,
            selected_date=base_metadata.selected_date,
            normal_window_start_ts=base_metadata.normal_window_start_ts,
            normal_window_end_ts=base_metadata.normal_window_end_ts,
            normal_anchor_ts=base_metadata.normal_anchor_ts,
            lookback_seconds=base_metadata.lookback_seconds,
            selection_reason=base_metadata.selection_reason,
            excluded_candidate_count=base_metadata.excluded_candidate_count,
            nearest_candidate_distance_seconds=base_metadata.nearest_candidate_distance_seconds,
            normal_sample_id=candidate.normal_sample_id,
            normal_sample_rank=candidate.normal_sample_rank,
            activity_distance=candidate.activity_distance,
            selected_normal_count=selected_normal_count,
            min_anchor_spacing_seconds=min_anchor_spacing_seconds,
        )
        normal_trade = _with_phase2j_metadata(
            phase2e._extract_normal_trade_window(trade_day, run_id, metadata, created_at),
            metadata,
        ).cache()
        normal_bbo = _with_phase2j_metadata(
            phase2e._extract_normal_bbo_window(bbo_day, run_id, metadata, created_at),
            metadata,
        ).cache()
        normal_snapshot = _with_phase2j_metadata(
            phase2e._build_normal_snapshot(
                normal_trade,
                normal_bbo,
                expected_exchanges,
                run_id,
                metadata,
                created_at,
            ),
            metadata,
        ).cache()
        exchange_profile, cross_exchange_mid_diff, bucket_change_profile = phase2e._build_normal_profiles(
            normal_snapshot,
            expected_exchanges,
            run_id,
            metadata,
            created_at,
        )
        trade_frames.append(normal_trade)
        bbo_frames.append(normal_bbo)
        snapshot_frames.append(normal_snapshot)
        exchange_frames.append(_with_phase2j_metadata(exchange_profile, metadata).cache())
        cross_frames.append(_with_phase2j_metadata(cross_exchange_mid_diff, metadata).cache())
        bucket_frames.append(_with_phase2j_metadata(bucket_change_profile, metadata).cache())

    return (
        _union_all(trade_frames).cache(),
        _union_all(bbo_frames).cache(),
        _union_all(snapshot_frames).cache(),
        _union_all(exchange_frames).cache(),
        _union_all(cross_frames).cache(),
        _union_all(bucket_frames).cache(),
    )


def _with_phase2j_metadata(frame: DataFrame, metadata: Phase2JNormalMetadata) -> DataFrame:
    return (
        frame.withColumn("normal_sample_id", F.lit(metadata.normal_sample_id))
        .withColumn("normal_sample_rank", F.lit(metadata.normal_sample_rank))
        .withColumn("activity_distance", F.lit(metadata.activity_distance).cast("double"))
        .withColumn(
            "nearest_candidate_distance_seconds",
            F.lit(metadata.nearest_candidate_distance_seconds).cast("int"),
        )
        .withColumn("selected_normal_count", F.lit(metadata.selected_normal_count))
        .withColumn("min_anchor_spacing_seconds", F.lit(metadata.min_anchor_spacing_seconds))
    )


def _read_event_profile_subtable(
    spark: Any,
    base_path: str,
    subtable: str,
    event_id: str,
) -> DataFrame:
    path = f"{base_path}/{subtable}"
    LOGGER.info("Event profile %s path: %s", subtable, path)
    frame = spark.read.parquet(path).where(F.col("event_id") == event_id)
    count = frame.count()
    LOGGER.info("Event profile %s row count for event_id=%s: %s", subtable, event_id, count)
    if count == 0:
        raise RuntimeError(f"Event profile {subtable} has no rows for event_id={event_id}.")
    return frame


def _event_activity_source(event_exchange: DataFrame, expected_exchanges: tuple[str, ...]) -> DataFrame:
    activity = event_exchange.where(F.col("window_label") == ACTIVITY_DISTANCE_WINDOW_LABEL).select(
        F.lower(F.col("exchange")).alias("exchange"),
        F.expr(_stack_expr(ACTIVITY_DISTANCE_METRICS, "event_activity_value")),
    )
    expected_count = len(expected_exchanges) * len(ACTIVITY_DISTANCE_METRICS)
    observed_count = activity.count()
    LOGGER.info(
        "Phase 2J event activity source: window_label=%s exchanges=%s metrics=%s rows=%s",
        ACTIVITY_DISTANCE_WINDOW_LABEL,
        ",".join(expected_exchanges),
        ",".join(ACTIVITY_DISTANCE_METRICS),
        observed_count,
    )
    if observed_count != expected_count:
        raise RuntimeError(
            "Event activity source row count mismatch: "
            f"expected={expected_count}, observed={observed_count}"
        )
    return activity


def _activity_distance_from_profile(
    normal_exchange_profile: DataFrame,
    event_activity: DataFrame,
    expected_exchanges: tuple[str, ...],
) -> float:
    normal_activity = normal_exchange_profile.where(
        F.col("window_label") == ACTIVITY_DISTANCE_WINDOW_LABEL
    ).select(
        F.lower(F.col("exchange")).alias("exchange"),
        F.expr(_stack_expr(ACTIVITY_DISTANCE_METRICS, "normal_activity_value")),
    )
    joined = (
        normal_activity.alias("normal")
        .join(event_activity.alias("event"), ["exchange", "metric_name"], "inner")
        .select(
            "exchange",
            "metric_name",
            F.col("normal.normal_activity_value").cast("double").alias("normal_activity_value"),
            F.col("event.event_activity_value").cast("double").alias("event_activity_value"),
        )
    )
    expected_count = len(expected_exchanges) * len(ACTIVITY_DISTANCE_METRICS)
    observed_count = joined.count()
    if observed_count != expected_count:
        raise RuntimeError(
            "Activity distance source row count mismatch: "
            f"expected={expected_count}, observed={observed_count}"
        )
    row = joined.agg(
        F.avg(
            F.abs(
                F.log(F.col("normal_activity_value") + F.lit(1.0))
                - F.log(F.col("event_activity_value") + F.lit(1.0))
            )
        ).alias("activity_distance")
    ).collect()[0]
    distance = row["activity_distance"]
    if distance is None:
        raise RuntimeError("Activity distance is null.")
    return float(distance)


def _build_exchange_profile_distribution_comparison(
    phase2f: ModuleType,
    event_exchange: DataFrame,
    normal_exchange: DataFrame,
    run_id: str,
    event_profile_run_id: str,
    created_at: datetime,
) -> DataFrame:
    event_long = phase2f._exchange_profile_long(event_exchange, "event_value")
    normal_long = phase2f._exchange_profile_long(normal_exchange, "normal_value")
    return _distribution_comparison(
        event_long,
        normal_long,
        keys=EXCHANGE_PROFILE_KEYS,
        report_group="exchange_profile",
        run_id=run_id,
        event_profile_run_id=event_profile_run_id,
        created_at=created_at,
        exchange=F.col("exchange"),
        symbol=F.col("event.symbol"),
        symbol_key=F.col("symbol_key"),
        exchange_pair=F.lit(None).cast("string"),
        window_mode=F.col("window_mode"),
        window_label=F.col("window_label"),
        from_bucket=F.lit(None).cast("string"),
        to_bucket=F.lit(None).cast("string"),
    )


def _build_cross_exchange_distribution_comparison(
    phase2f: ModuleType,
    event_cross: DataFrame,
    normal_cross: DataFrame,
    run_id: str,
    event_profile_run_id: str,
    created_at: datetime,
) -> DataFrame:
    event_long = phase2f._cross_exchange_long(event_cross, "event_value")
    normal_long = phase2f._cross_exchange_long(normal_cross, "normal_value")
    return _distribution_comparison(
        event_long,
        normal_long,
        keys=CROSS_EXCHANGE_KEYS,
        report_group="cross_exchange_mid_diff",
        run_id=run_id,
        event_profile_run_id=event_profile_run_id,
        created_at=created_at,
        exchange=F.lit(None).cast("string"),
        symbol=F.lit(None).cast("string"),
        symbol_key=F.col("symbol_key"),
        exchange_pair=F.col("exchange_pair"),
        window_mode=F.col("window_mode"),
        window_label=F.col("window_label"),
        from_bucket=F.lit(None).cast("string"),
        to_bucket=F.lit(None).cast("string"),
    )


def _build_bucket_change_distribution_comparison(
    phase2f: ModuleType,
    event_bucket: DataFrame,
    normal_bucket: DataFrame,
    run_id: str,
    event_profile_run_id: str,
    created_at: datetime,
) -> DataFrame:
    event_filtered = event_bucket.where(
        F.col("metric_name").isin(list(BUCKET_CHANGE_COMPARISON_SOURCE_METRICS))
    )
    normal_filtered = normal_bucket.where(
        F.col("metric_name").isin(list(BUCKET_CHANGE_COMPARISON_SOURCE_METRICS))
    )
    event_long = phase2f._bucket_change_long(event_filtered, "event_value")
    normal_long = phase2f._bucket_change_long(normal_filtered, "normal_value")
    return _distribution_comparison(
        event_long,
        normal_long,
        keys=BUCKET_CHANGE_KEYS,
        report_group="bucket_change_profile",
        run_id=run_id,
        event_profile_run_id=event_profile_run_id,
        created_at=created_at,
        exchange=F.col("exchange"),
        symbol=F.col("event.symbol"),
        symbol_key=F.col("symbol_key"),
        exchange_pair=F.lit(None).cast("string"),
        window_mode=F.lit(None).cast("string"),
        window_label=F.lit(None).cast("string"),
        from_bucket=F.col("from_bucket"),
        to_bucket=F.col("to_bucket"),
        event_from_bucket_value=F.col("event.from_bucket_value"),
        normal_from_bucket_value=F.col("normal.from_bucket_value"),
        event_to_bucket_value=F.col("event.to_bucket_value"),
        normal_to_bucket_value=F.col("normal.to_bucket_value"),
    )


def _distribution_comparison(
    event_long: DataFrame,
    normal_long: DataFrame,
    *,
    keys: tuple[str, ...],
    report_group: str,
    run_id: str,
    event_profile_run_id: str,
    created_at: datetime,
    exchange: Any,
    symbol: Any,
    symbol_key: Any,
    exchange_pair: Any,
    window_mode: Any,
    window_label: Any,
    from_bucket: Any,
    to_bucket: Any,
    event_from_bucket_value: Any | None = None,
    normal_from_bucket_value: Any | None = None,
    event_to_bucket_value: Any | None = None,
    normal_to_bucket_value: Any | None = None,
) -> DataFrame:
    joined = event_long.alias("event").join(
        normal_long.alias("normal"),
        [*keys, "metric_name"],
        "inner",
    )
    event_from_value = (
        event_from_bucket_value.cast("double")
        if event_from_bucket_value is not None
        else F.lit(None).cast("double")
    )
    normal_from_value = (
        normal_from_bucket_value.cast("double")
        if normal_from_bucket_value is not None
        else F.lit(None).cast("double")
    )
    event_to_value = (
        event_to_bucket_value.cast("double")
        if event_to_bucket_value is not None
        else F.lit(None).cast("double")
    )
    normal_to_value = (
        normal_to_bucket_value.cast("double")
        if normal_to_bucket_value is not None
        else F.lit(None).cast("double")
    )
    samples = joined.select(
        F.lit(run_id).alias("comparison_run_id"),
        F.lit(event_profile_run_id).alias("event_profile_run_id"),
        F.lit(run_id).alias("multi_normal_profile_run_id"),
        F.lit(report_group).alias("report_group"),
        F.col("metric_name"),
        _metric_group_column(F.col("metric_name")).alias("metric_group"),
        F.col("event.source_event_id").alias("source_event_id"),
        F.col("event.event_direction").alias("event_direction"),
        F.col("event.event_start_ts").alias("event_start_ts"),
        exchange.alias("exchange"),
        symbol.alias("symbol"),
        symbol_key.alias("symbol_key"),
        exchange_pair.alias("exchange_pair"),
        window_mode.alias("window_mode"),
        window_label.alias("window_label"),
        from_bucket.alias("from_bucket"),
        to_bucket.alias("to_bucket"),
        F.col("event.profile_version").alias("profile_version"),
        F.col("event.event_value").cast("double").alias("event_value"),
        F.col("normal.normal_value").cast("double").alias("normal_value"),
        event_from_value.alias("event_from_bucket_value"),
        normal_from_value.alias("normal_from_bucket_value"),
        event_to_value.alias("event_to_bucket_value"),
        normal_to_value.alias("normal_to_bucket_value"),
    )
    group_columns = (
        "comparison_run_id",
        "event_profile_run_id",
        "multi_normal_profile_run_id",
        "report_group",
        "metric_name",
        "metric_group",
        "source_event_id",
        "event_direction",
        "event_start_ts",
        "exchange",
        "symbol",
        "symbol_key",
        "exchange_pair",
        "window_mode",
        "window_label",
        "from_bucket",
        "to_bucket",
        "profile_version",
        "event_value",
        "event_from_bucket_value",
        "event_to_bucket_value",
    )
    aggregated = samples.groupBy(*group_columns).agg(
        F.avg("normal_value").alias("normal_mean"),
        F.expr("percentile_approx(normal_value, 0.5)").alias("normal_median"),
        F.min("normal_value").alias("normal_min"),
        F.max("normal_value").alias("normal_max"),
        F.expr("percentile_approx(normal_value, 0.25)").alias("normal_p25"),
        F.expr("percentile_approx(normal_value, 0.75)").alias("normal_p75"),
        F.stddev_samp("normal_value").alias("normal_stddev"),
        F.count("*").alias("normal_sample_count"),
        F.count("normal_value").alias("normal_non_null_count"),
        F.sum(
            F.when(
                F.col("normal_value").isNotNull()
                & (F.abs(F.col("normal_value")) <= F.lit(SMALL_DENOMINATOR_ABS_THRESHOLD)),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("normal_zero_or_near_zero_count"),
        F.avg("normal_from_bucket_value").alias("normal_from_bucket_value"),
        F.avg("normal_to_bucket_value").alias("normal_to_bucket_value"),
        F.sum(
            F.when(
                F.col("event_value").isNotNull()
                & F.col("normal_value").isNotNull()
                & (F.col("normal_value") <= F.col("event_value")),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("normal_lte_event_count"),
    )
    with_diffs = (
        aggregated.withColumn(
            "signed_diff_vs_mean",
            F.when(
                F.col("event_value").isNotNull() & F.col("normal_mean").isNotNull(),
                F.col("event_value") - F.col("normal_mean"),
            ),
        )
        .withColumn(
            "absolute_diff_vs_mean",
            F.when(F.col("signed_diff_vs_mean").isNotNull(), F.abs(F.col("signed_diff_vs_mean"))),
        )
        .withColumn(
            "ratio_vs_mean",
            F.when(
                F.col("event_value").isNotNull()
                & F.col("normal_mean").isNotNull()
                & (F.col("normal_mean") != F.lit(0.0)),
                F.col("event_value") / F.col("normal_mean"),
            ),
        )
        .withColumn(
            "z_score_vs_normal",
            F.when(
                F.col("event_value").isNotNull()
                & F.col("normal_mean").isNotNull()
                & F.col("normal_stddev").isNotNull()
                & (F.col("normal_stddev") != F.lit(0.0)),
                (F.col("event_value") - F.col("normal_mean")) / F.col("normal_stddev"),
            ),
        )
        .withColumn(
            "normal_percentile_rank",
            F.when(
                F.col("event_value").isNotNull() & (F.col("normal_non_null_count") > F.lit(0)),
                F.col("normal_lte_event_count") / F.col("normal_non_null_count"),
            ),
        )
        .withColumn(
            "ratio_unstable",
            F.col("normal_mean").isNull()
            | (F.abs(F.col("normal_mean")) <= F.lit(SMALL_DENOMINATOR_ABS_THRESHOLD)),
        )
        .withColumn(
            "relative_change_unstable",
            F.col("metric_name").endswith(".relative_change")
            & (
                F.col("event_from_bucket_value").isNull()
                | F.col("normal_from_bucket_value").isNull()
                | (F.abs(F.col("event_from_bucket_value")) <= F.lit(SMALL_DENOMINATOR_ABS_THRESHOLD))
                | (F.abs(F.col("normal_from_bucket_value")) <= F.lit(SMALL_DENOMINATOR_ABS_THRESHOLD))
            ),
        )
        .withColumn(
            "small_denominator_flag",
            F.col("ratio_unstable") | F.col("relative_change_unstable"),
        )
    )
    return with_diffs.select(
        "comparison_run_id",
        "event_profile_run_id",
        "multi_normal_profile_run_id",
        "report_group",
        "metric_name",
        "metric_group",
        "source_event_id",
        "event_direction",
        "event_start_ts",
        "exchange",
        "symbol",
        "symbol_key",
        "exchange_pair",
        "window_mode",
        "window_label",
        "from_bucket",
        "to_bucket",
        "profile_version",
        "event_value",
        "normal_mean",
        "normal_median",
        "normal_min",
        "normal_max",
        "normal_p25",
        "normal_p75",
        "normal_stddev",
        "normal_sample_count",
        "normal_non_null_count",
        "normal_zero_or_near_zero_count",
        "signed_diff_vs_mean",
        "absolute_diff_vs_mean",
        "ratio_vs_mean",
        "z_score_vs_normal",
        "normal_percentile_rank",
        "ratio_unstable",
        "relative_change_unstable",
        "small_denominator_flag",
        "event_from_bucket_value",
        "normal_from_bucket_value",
        "event_to_bucket_value",
        "normal_to_bucket_value",
        F.lit(created_at).cast("timestamp").alias("created_at"),
    )


def _write_top_diffs(
    phase2e: ModuleType,
    comparison: DataFrame,
    output_path: str,
    label: str,
    run_id: str,
    top_n: int,
    created_at: datetime,
    sample_size: int,
) -> None:
    top_diffs = _build_top_diffs(comparison, run_id=run_id, top_n=top_n, created_at=created_at).cache()
    _log_top_diff_group_counts(comparison, top_diffs, label)
    phase2e._write_and_validate(top_diffs, output_path, label, sample_size)
    top_diffs.unpersist()


def _is_within_selected_spacing(
    anchor_ts: datetime,
    selected_candidates: list[QualityPassedNormalCandidate],
    *,
    min_anchor_spacing_seconds: int,
) -> bool:
    anchor = anchor_ts.replace(tzinfo=timezone.utc) if anchor_ts.tzinfo is None else anchor_ts.astimezone(timezone.utc)
    for selected in select_activity_matched_candidates(
        selected_candidates,
        selected_count=max(len(selected_candidates), 1),
        min_anchor_spacing_seconds=min_anchor_spacing_seconds,
    ):
        selected_anchor = selected.normal_anchor_ts
        if abs((anchor - selected_anchor).total_seconds()) < min_anchor_spacing_seconds:
            return True
    return False


def _build_top_diffs(
    frame: DataFrame,
    *,
    run_id: str,
    top_n: int,
    created_at: datetime,
) -> DataFrame:
    outputs = [
        _rank_top_diffs(
            frame,
            rank_type=rank_type,
            primary_column=primary_column,
            ascending=ascending,
            run_id=run_id,
            top_n=top_n,
            created_at=created_at,
        )
        for rank_type, primary_column, ascending in MULTI_NORMAL_RANKINGS
    ]
    return _union_all(outputs)


def _rank_top_diffs(
    frame: DataFrame,
    *,
    rank_type: str,
    primary_column: str,
    ascending: bool,
    run_id: str,
    top_n: int,
    created_at: datetime,
) -> DataFrame:
    primary_order = (
        F.asc_nulls_last(primary_column)
        if ascending
        else F.desc_nulls_last(primary_column)
    )
    tie_breakers = [F.asc_nulls_last(column) for column in TOP_DIFF_TIE_BREAKER_COLUMNS]
    rank_window = Window.partitionBy("report_group", "metric_group").orderBy(
        primary_order,
        *tie_breakers,
    )
    return (
        frame.withColumn("rank_type", F.lit(rank_type))
        .withColumn("rank", F.row_number().over(rank_window))
        .where(F.col("rank") <= F.lit(top_n))
        .select(
            F.lit(run_id).alias("run_id"),
            F.col("comparison_run_id").alias("source_comparison_run_id"),
            "report_group",
            "metric_group",
            "rank_type",
            "rank",
            "exchange",
            "symbol",
            "symbol_key",
            "exchange_pair",
            "window_mode",
            "window_label",
            "from_bucket",
            "to_bucket",
            "metric_name",
            "event_value",
            "normal_mean",
            "normal_median",
            "normal_min",
            "normal_max",
            "normal_p25",
            "normal_p75",
            "normal_stddev",
            "normal_sample_count",
            "normal_non_null_count",
            "normal_zero_or_near_zero_count",
            "signed_diff_vs_mean",
            "absolute_diff_vs_mean",
            "ratio_vs_mean",
            "z_score_vs_normal",
            "normal_percentile_rank",
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
            "profile_version",
            F.lit(created_at).cast("timestamp").alias("created_at"),
        )
    )


def _validate_selected_output_counts(
    normal_snapshot: DataFrame,
    normal_exchange: DataFrame,
    normal_cross: DataFrame,
    normal_bucket: DataFrame,
    *,
    exchange_count: int,
    lookback_seconds: int,
    selected_normal_count: int,
) -> None:
    _validate_comparison_count(
        normal_snapshot,
        "multi_normal_market_snapshots",
        exchange_count * lookback_seconds * selected_normal_count,
    )
    _validate_comparison_count(
        normal_exchange,
        "multi_normal_profile_reports.exchange_profile",
        expected_multi_normal_profile_rows(30, selected_normal_count),
    )
    _validate_comparison_count(
        normal_cross,
        "multi_normal_profile_reports.cross_exchange_mid_diff",
        expected_multi_normal_profile_rows(30, selected_normal_count),
    )
    _validate_comparison_count(
        normal_bucket,
        "multi_normal_profile_reports.bucket_change_profile",
        expected_multi_normal_profile_rows(48, selected_normal_count),
    )


def _validate_comparison_count(frame: DataFrame, label: str, expected_count: int) -> None:
    observed_count = frame.count()
    LOGGER.info("%s expected vs observed rows: expected=%s observed=%s", label, expected_count, observed_count)
    if observed_count != expected_count:
        raise RuntimeError(f"{label} row count mismatch: expected={expected_count}, observed={observed_count}")


def _validate_normal_sample_count(frame: DataFrame, label: str, expected_count: int) -> None:
    invalid_count = frame.where(F.col("normal_sample_count") != F.lit(expected_count)).count()
    LOGGER.info("%s normal_sample_count invalid rows: %s", label, invalid_count)
    if invalid_count:
        raise RuntimeError(f"{label} contains rows with normal_sample_count != {expected_count}.")


def _validate_metric_group_coverage(frame: DataFrame, label: str) -> None:
    null_count = frame.where(F.col("metric_group").isNull()).count()
    LOGGER.info("Phase 2J %s metric_group null count: %s", label, null_count)
    if null_count:
        raise RuntimeError(f"{label} metric_group contains null rows.")
    _show_df(
        f"Phase 2J {label} metric_group counts:",
        frame.groupBy("metric_group").count().orderBy("metric_group"),
    )


def _validate_absolute_diff(frame: DataFrame, label: str) -> None:
    negative_count = frame.where(F.col("absolute_diff_vs_mean") < F.lit(0.0)).count()
    LOGGER.info("Phase 2J %s absolute_diff_vs_mean negative count: %s", label, negative_count)
    if negative_count:
        raise RuntimeError(f"{label} absolute_diff_vs_mean contains negative rows.")


def _log_top_diff_group_counts(source: DataFrame, output: DataFrame, label: str) -> None:
    _show_df(
        f"{label} source metric_group counts:",
        source.groupBy("report_group", "metric_group").count().orderBy("report_group", "metric_group"),
    )
    _show_df(
        f"{label} output metric_group counts:",
        output.groupBy("report_group", "metric_group", "rank_type")
        .count()
        .orderBy("report_group", "metric_group", "rank_type"),
    )


def _metric_group_column(metric_name: Any) -> Any:
    return (
        F.when(metric_name.endswith(".relative_change"), F.lit(METRIC_GROUP_UNSTABLE))
        .when(metric_name.isin(list(CONTEXT_METRICS)), F.lit(METRIC_GROUP_CONTEXT))
        .when(
            metric_name.isin(list(SIGNAL_CANDIDATE_METRICS)),
            F.lit(METRIC_GROUP_SIGNAL_CANDIDATE),
        )
        .when(
            metric_name.isin(list(PRICE_DISLOCATION_METRICS)),
            F.lit(METRIC_GROUP_PRICE_DISLOCATION),
        )
    )


def _stack_expr(metrics: tuple[str, ...], value_alias: str) -> str:
    entries = ", ".join(f"'{metric}', cast(`{metric}` as double)" for metric in metrics)
    return f"stack({len(metrics)}, {entries}) AS (metric_name, {value_alias})"


def _union_all(frames: list[DataFrame]) -> DataFrame:
    if not frames:
        raise ValueError("At least one DataFrame is required.")
    output = frames[0]
    for frame in frames[1:]:
        output = output.unionByName(frame)
    return output


def _show_df(label: str, frame: DataFrame, rows: int = 20) -> None:
    LOGGER.info("%s", label)
    sample = frame.take(rows)
    if not sample:
        LOGGER.info("  <empty>")
        return
    for row in sample:
        LOGGER.info("  %s", row.asDict(recursive=True))


def _validate_required_profile_metrics() -> None:
    if not EXCHANGE_PROFILE_COMPARISON_METRICS:
        raise RuntimeError("Exchange profile comparison metrics are empty.")
    if not CROSS_EXCHANGE_MID_DIFF_COMPARISON_METRICS:
        raise RuntimeError("Cross-exchange comparison metrics are empty.")
    if not BUCKET_CHANGE_COMPARISON_VALUE_FIELDS:
        raise RuntimeError("Bucket-change comparison value fields are empty.")


def _load_job_module(module_name: str, filename: str) -> ModuleType:
    job_file = _script_path()
    if job_file is None:
        raise RuntimeError("Unable to resolve current job path.")
    path = job_file.with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load job module: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _resolve_path(path: str) -> Path:
    candidate = Path(path)
    if candidate.exists():
        return candidate
    bases = [Path.cwd()]
    job_file = _script_path()
    if job_file is not None:
        bases.insert(0, job_file.parents[1])
    for base in bases:
        resolved = base / path
        if resolved.exists():
            return resolved
    raise FileNotFoundError(f"Config file not found: {path}")


def _get_spark_session() -> Any:
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except Exception:  # pragma: no cover - exercised only in Databricks fallback.
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()


if __name__ == "__main__":
    _validate_required_profile_metrics()
    main()
