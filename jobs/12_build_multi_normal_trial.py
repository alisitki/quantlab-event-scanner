#!/usr/bin/env python
"""Phase 2J multi-normal comparison trial."""

from __future__ import annotations

import argparse
import importlib.util
import logging
import sys
import time
from collections import Counter
from contextlib import contextmanager
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
DEFAULT_RAW_DAY_REPARTITION_PARTITIONS = 96
DEFAULT_RAW_DAY_BUCKET_SECONDS = 60
DEFAULT_SELECTED_OUTPUT_PARTITIONS = 200
DEFAULT_SMALL_OUTPUT_PARTITIONS = 1

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

PHASE2J_PROFILE_AUDIT_COLUMNS = (
    "run_id",
    "source_snapshot_run_id",
    "sample_type",
    "selected_date",
    "event_id",
    "event_direction",
    "event_start_ts",
    "normal_window_start_ts",
    "normal_window_end_ts",
    "normal_anchor_ts",
    "profile_version",
    "created_at",
)

PHASE2J_METADATA_COLUMNS = (
    "normal_sample_id",
    "normal_sample_rank",
    "activity_distance",
    "nearest_candidate_distance_seconds",
    "selected_normal_count",
    "min_anchor_spacing_seconds",
)
PHASE2J_EXTRA_METADATA_COLUMNS = tuple(
    column for column in PHASE2J_METADATA_COLUMNS if column != "nearest_candidate_distance_seconds"
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


@contextmanager
def _phase2j_timer(step: str) -> Any:
    start = time.monotonic()
    try:
        yield
    finally:
        elapsed = time.monotonic() - start
        LOGGER.info("[PHASE2J_TIMER] step=%s seconds=%.3f", step, elapsed)


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
    LOGGER.info("Validation mode: %s", args.validation_mode)
    LOGGER.info("Raw day repartition partitions: %s", args.raw_day_repartition_partitions)
    LOGGER.info("Raw day bucket seconds: %s", args.raw_day_bucket_seconds)
    LOGGER.info("Selected output partitions: %s", args.selected_output_partitions)
    LOGGER.info("Small output partitions: %s", args.small_output_partitions)

    spark = _get_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    LOGGER.info("Spark timezone: %s", spark.conf.get("spark.sql.session.timeZone"))
    _log_spark_runtime_context(spark, run_id)

    with _phase2j_timer("manifest_event_profile_load"):
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
        _log_df_metrics("event_exchange", event_exchange)
        _log_df_metrics("event_cross", event_cross)
        _log_df_metrics("event_bucket", event_bucket)
        _log_df_metrics("event_activity", event_activity)

    with _phase2j_timer("manifest_load"):
        manifest = load_manifest_from_s3_with_spark(spark, config.manifest_path)
    with _phase2j_timer("raw_trade_path_selection"):
        trade_partitions = _select_partitions(
            manifest.partitions,
            exchanges=config.exchanges,
            stream="trade",
            date=args.date,
        )
    with _phase2j_timer("raw_bbo_path_selection"):
        bbo_partitions = _select_partitions(
            manifest.partitions,
            exchanges=config.exchanges,
            stream="bbo",
            date=args.date,
        )
    _log_partition_coverage("trade manifest", config.exchanges, trade_partitions)
    _log_partition_coverage("BBO manifest", config.exchanges, bbo_partitions)

    with _phase2j_timer("trade_day_read_cache_materialize"):
        trade_day = _repartition_raw_day(
            _read_partitions(spark, trade_partitions, config.input_root, "trade"),
            "trade_day",
            partitions=args.raw_day_repartition_partitions,
            bucket_seconds=args.raw_day_bucket_seconds,
        ).cache()
        _log_df_metrics("trade_day", trade_day, include_count=True, exchange=True, symbol=True)
    with _phase2j_timer("bbo_day_read_cache_materialize"):
        bbo_day = _repartition_raw_day(
            _read_partitions(spark, bbo_partitions, config.input_root, "BBO"),
            "bbo_day",
            partitions=args.raw_day_repartition_partitions,
            bucket_seconds=args.raw_day_bucket_seconds,
        ).cache()
        _log_df_metrics("bbo_day", bbo_day, include_count=True, exchange=True, symbol=True)
    _validate_required_trade_columns(trade_day)
    _validate_required_bbo_columns(bbo_day)

    with _phase2j_timer("price_1s_build"):
        price_1s = build_price_1s(trade_day)
        if price_1s is None:
            raise RuntimeError("Unable to build 1s price rows for normal window selection.")
        price_1s = price_1s.cache()
        price_1s_count = _log_df_metrics("price_1s", price_1s, include_count=True, exchange=True, symbol=True)
    LOGGER.info("Phase 2J price_1s_count: %s", price_1s_count)
    if price_1s_count == 0:
        raise RuntimeError("1s price row count is 0.")

    with _phase2j_timer("candidate_scan"):
        candidates, ambiguous = scan_move_candidates(
            price_1s,
            threshold_pct=args.threshold_pct,
            lookahead_seconds=args.horizon_seconds,
        )
        candidates = candidates.cache()
        ambiguous = ambiguous.cache()
        non_ambiguous_candidate_count = _log_df_metrics("candidate_df", candidates, include_count=True)
        ambiguous_count = _log_df_metrics("ambiguous_candidate_df", ambiguous, include_count=True)
        raw_candidate_count = non_ambiguous_candidate_count + ambiguous_count
        candidate_starts = _collect_raw_candidate_starts(candidates, ambiguous)
    LOGGER.info("Phase 2J raw candidate count: %s", raw_candidate_count)
    LOGGER.info("Phase 2J non-ambiguous candidate count: %s", non_ambiguous_candidate_count)
    LOGGER.info("Phase 2J ambiguous candidate count: %s", ambiguous_count)
    LOGGER.info("Phase 2J distinct raw candidate start count: %s", len(candidate_starts))

    with _phase2j_timer("anchor_eligibility"):
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
    LOGGER.info(
        "[PHASE2J_DF] name=eligible_anchors rows=%s partitions=not_dataframe schema=normal_anchor_ts",
        len(anchor_candidates),
    )
    LOGGER.info("Phase 2J exclusion count by reason: %s", anchor_exclusion_counts)
    if not anchor_candidates:
        raise RuntimeError("No eligible normal anchor candidates found.")

    with _phase2j_timer("activity_ranking"):
        activity_ranked_candidates = _activity_ranked_anchor_candidates(
            trade_day,
            event_activity,
            config.exchanges,
            anchor_candidates,
            candidate_starts,
            lookback_seconds=args.lookback_seconds,
        )
    LOGGER.info("Phase 2J activity-ranked candidate count: %s", len(activity_ranked_candidates))
    LOGGER.info(
        "[PHASE2J_DF] name=ranked_anchors rows=%s partitions=not_dataframe schema=normal_anchor_ts,activity_distance,nearest_candidate_distance_seconds",
        len(activity_ranked_candidates),
    )

    with _phase2j_timer("quality_probe"):
        (
            selected,
            quality_failure_counts,
            quality_probe_count,
            quality_pass_count,
            spacing_skipped_count,
        ) = _select_quality_passed_candidates(
            trade_day,
            bbo_day,
            config.exchanges,
            activity_ranked_candidates,
            lookback_seconds=args.lookback_seconds,
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

    with _phase2j_timer("selected_normal_extraction"):
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
            selected_output_partitions=args.selected_output_partitions,
            selected_join_bucket_seconds=args.raw_day_bucket_seconds,
        )
        _log_df_metrics("normal_trade_windows", normal_trade, normal_sample_id=True, exchange=True, symbol=True)
        _log_df_metrics("normal_bbo_windows", normal_bbo, normal_sample_id=True, exchange=True, symbol=True)
        _log_df_metrics("multi_normal_market_snapshots", normal_snapshot, normal_sample_id=True, exchange=True, symbol=True)
        _log_df_metrics("exchange_profile", normal_exchange, normal_sample_id=True, exchange=True, symbol=True)
        _log_df_metrics("cross_exchange_mid_diff", normal_cross, normal_sample_id=True)
        _log_df_metrics("bucket_change_profile", normal_bucket, normal_sample_id=True, exchange=True, symbol=True)

    with _phase2j_timer("selected_output_count_validation"):
        selected_output_counts = _validate_selected_output_counts(
            normal_snapshot,
            normal_exchange,
            normal_cross,
            normal_bucket,
            exchange_count=len(config.exchanges),
            lookback_seconds=args.lookback_seconds,
            selected_normal_count=args.selected_normal_count,
        )

    with _phase2j_timer("write_validate_multi_normal_windows_trade_windows"):
        phase2e._write_and_validate(
            normal_trade,
            f"{windows_output_path}/trade_windows",
            "multi_normal_windows.trade_windows",
            args.sample_size,
            validation_mode=args.validation_mode,
        )
    with _phase2j_timer("write_validate_multi_normal_windows_bbo_windows"):
        phase2e._write_and_validate(
            normal_bbo,
            f"{windows_output_path}/bbo_windows",
            "multi_normal_windows.bbo_windows",
            args.sample_size,
            validation_mode=args.validation_mode,
        )
    with _phase2j_timer("write_validate_multi_normal_market_snapshots"):
        phase2e._write_and_validate(
            normal_snapshot,
            snapshot_output_path,
            "multi_normal_market_snapshots",
            args.sample_size,
            validation_mode=args.validation_mode,
            row_count=selected_output_counts["multi_normal_market_snapshots"],
        )
    with _phase2j_timer("write_validate_multi_normal_profile_exchange_profile"):
        phase2e._write_and_validate(
            normal_exchange,
            f"{profile_output_path}/exchange_profile",
            "multi_normal_profile_reports.exchange_profile",
            args.sample_size,
            validation_mode=args.validation_mode,
            row_count=selected_output_counts["multi_normal_profile_reports.exchange_profile"],
        )
    with _phase2j_timer("write_validate_multi_normal_profile_cross_exchange_mid_diff"):
        phase2e._write_and_validate(
            normal_cross,
            f"{profile_output_path}/cross_exchange_mid_diff",
            "multi_normal_profile_reports.cross_exchange_mid_diff",
            args.sample_size,
            validation_mode=args.validation_mode,
            row_count=selected_output_counts["multi_normal_profile_reports.cross_exchange_mid_diff"],
        )
    with _phase2j_timer("write_validate_multi_normal_profile_bucket_change_profile"):
        phase2e._write_and_validate(
            normal_bucket,
            f"{profile_output_path}/bucket_change_profile",
            "multi_normal_profile_reports.bucket_change_profile",
            args.sample_size,
            validation_mode=args.validation_mode,
            row_count=selected_output_counts["multi_normal_profile_reports.bucket_change_profile"],
        )

    with _phase2j_timer("comparison_profile_lineage_boundary_read"):
        normal_exchange_for_comparison = spark.read.parquet(f"{profile_output_path}/exchange_profile").cache()
        normal_cross_for_comparison = spark.read.parquet(f"{profile_output_path}/cross_exchange_mid_diff").cache()
        normal_bucket_for_comparison = spark.read.parquet(f"{profile_output_path}/bucket_change_profile").cache()
        LOGGER.info("Phase 2J comparison input source: persisted multi-normal profile readbacks")
        _log_df_metrics("comparison_input_exchange_profile", normal_exchange_for_comparison, normal_sample_id=True, exchange=True, symbol=True)
        _log_df_metrics("comparison_input_cross_exchange_mid_diff", normal_cross_for_comparison, normal_sample_id=True)
        _log_df_metrics("comparison_input_bucket_change_profile", normal_bucket_for_comparison, normal_sample_id=True, exchange=True, symbol=True)

    with _phase2j_timer("comparison_build"):
        exchange_comparison = _coalesce_small_output(
            _build_exchange_profile_distribution_comparison(
                phase2f,
                event_exchange,
                normal_exchange_for_comparison,
                run_id,
                args.event_profile_run_id,
                created_at,
            ),
            args.small_output_partitions,
            "exchange_profile_comparison",
        ).cache()
        cross_comparison = _coalesce_small_output(
            _build_cross_exchange_distribution_comparison(
                phase2f,
                event_cross,
                normal_cross_for_comparison,
                run_id,
                args.event_profile_run_id,
                created_at,
            ),
            args.small_output_partitions,
            "cross_exchange_mid_diff_comparison",
        ).cache()
        bucket_comparison = _coalesce_small_output(
            _build_bucket_change_distribution_comparison(
                phase2f,
                event_bucket,
                normal_bucket_for_comparison,
                run_id,
                args.event_profile_run_id,
                created_at,
            ),
            args.small_output_partitions,
            "bucket_change_profile_comparison",
        ).cache()
        _log_df_metrics("exchange_profile_comparison", exchange_comparison, normal_sample_id=False)
        _log_df_metrics("cross_exchange_mid_diff_comparison", cross_comparison, normal_sample_id=False)
        _log_df_metrics("bucket_change_profile_comparison", bucket_comparison, normal_sample_id=False)

    with _phase2j_timer("comparison_validation"):
        exchange_comparison_count = _validate_comparison_count(
            exchange_comparison,
            "multi_normal_comparison_reports.exchange_profile_comparison",
            expected_exchange_profile_comparison_rows(),
        )
        _validate_normal_sample_count(
            exchange_comparison,
            "multi_normal_comparison_reports.exchange_profile_comparison",
            args.selected_normal_count,
        )
        cross_comparison_count = _validate_comparison_count(
            cross_comparison,
            "multi_normal_comparison_reports.cross_exchange_mid_diff_comparison",
            expected_cross_exchange_mid_diff_comparison_rows(),
        )
        _validate_normal_sample_count(
            cross_comparison,
            "multi_normal_comparison_reports.cross_exchange_mid_diff_comparison",
            args.selected_normal_count,
        )
        bucket_comparison_count = _validate_comparison_count(
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

    with _phase2j_timer("write_validate_comparison_exchange_profile"):
        phase2e._write_and_validate(
            exchange_comparison,
            f"{comparison_output_path}/exchange_profile_comparison",
            "multi_normal_comparison_reports.exchange_profile_comparison",
            args.sample_size,
            validation_mode=args.validation_mode,
            row_count=exchange_comparison_count,
        )
    with _phase2j_timer("write_validate_comparison_cross_exchange_mid_diff"):
        phase2e._write_and_validate(
            cross_comparison,
            f"{comparison_output_path}/cross_exchange_mid_diff_comparison",
            "multi_normal_comparison_reports.cross_exchange_mid_diff_comparison",
            args.sample_size,
            validation_mode=args.validation_mode,
            row_count=cross_comparison_count,
        )
    with _phase2j_timer("write_validate_comparison_bucket_change_profile"):
        phase2e._write_and_validate(
            bucket_comparison,
            f"{comparison_output_path}/bucket_change_profile_comparison",
            "multi_normal_comparison_reports.bucket_change_profile_comparison",
            args.sample_size,
            validation_mode=args.validation_mode,
            row_count=bucket_comparison_count,
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
        args.validation_mode,
        args.small_output_partitions,
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
        args.validation_mode,
        args.small_output_partitions,
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
        args.validation_mode,
        args.small_output_partitions,
    )

    bucket_comparison_for_top_diffs.unpersist()
    cross_comparison_for_top_diffs.unpersist()
    exchange_comparison_for_top_diffs.unpersist()
    bucket_comparison.unpersist()
    cross_comparison.unpersist()
    exchange_comparison.unpersist()
    normal_bucket_for_comparison.unpersist()
    normal_cross_for_comparison.unpersist()
    normal_exchange_for_comparison.unpersist()
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
    parser.add_argument(
        "--validation-mode",
        choices=("strict", "light"),
        default="light",
        help="Output validation mode. Phase 2J defaults to light to avoid repeated readback actions.",
    )
    parser.add_argument(
        "--raw-day-repartition-partitions",
        type=int,
        default=DEFAULT_RAW_DAY_REPARTITION_PARTITIONS,
        help="Partition count after raw trade/BBO day reads; 0 disables raw-day repartition.",
    )
    parser.add_argument(
        "--raw-day-bucket-seconds",
        type=int,
        default=DEFAULT_RAW_DAY_BUCKET_SECONDS,
        help="Time-bucket width used with exchange/symbol for raw-day repartition.",
    )
    parser.add_argument(
        "--selected-output-partitions",
        type=int,
        default=DEFAULT_SELECTED_OUTPUT_PARTITIONS,
        help="Partition count for selected normal trade/BBO/snapshot/profile DataFrames.",
    )
    parser.add_argument(
        "--small-output-partitions",
        type=int,
        default=DEFAULT_SMALL_OUTPUT_PARTITIONS,
        help="Coalesced partition count for small comparison and top-diff outputs; 0 disables coalesce.",
    )
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


def _safe_spark_conf(spark: Any, key: str) -> str:
    try:
        return str(spark.conf.get(key))
    except Exception as exc:  # pragma: no cover - Databricks/Spark-version dependent.
        return f"unavailable:{type(exc).__name__}"


def _log_spark_runtime_context(spark: Any, run_id: str) -> None:
    context = spark.sparkContext
    LOGGER.info("[PHASE2J_CONF] key=run_id value=%s", run_id)
    LOGGER.info("[PHASE2J_CONF] key=spark.app.id value=%s", context.applicationId)
    LOGGER.info("[PHASE2J_CONF] key=spark.app.name value=%s", context.appName)
    LOGGER.info("[PHASE2J_CONF] key=spark.master value=%s", context.master)
    for key in (
        "spark.databricks.clusterUsageTags.clusterId",
        "spark.databricks.clusterUsageTags.clusterName",
        "spark.databricks.clusterUsageTags.sparkVersion",
        "spark.databricks.clusterUsageTags.nodeType",
        "spark.databricks.clusterUsageTags.driverNodeType",
        "spark.databricks.clusterUsageTags.numWorkers",
        "spark.databricks.clusterUsageTags.clusterScalingType",
        "spark.databricks.photon.enabled",
        "spark.sql.shuffle.partitions",
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.skewJoin.enabled",
        "spark.default.parallelism",
    ):
        LOGGER.info("[PHASE2J_CONF] key=%s value=%s", key, _safe_spark_conf(spark, key))
    try:
        executor_infos = context._jsc.sc().getExecutorMemoryStatus().size()
        LOGGER.info("[PHASE2J_CONF] key=spark.executor.memoryStatus.count value=%s", executor_infos)
    except Exception as exc:  # pragma: no cover - Databricks/JVM dependent.
        LOGGER.info("[PHASE2J_CONF] key=spark.executor.memoryStatus.count value=unavailable:%s", type(exc).__name__)
    _log_databricks_job_context()


def _log_databricks_job_context() -> None:
    dbutils_obj = globals().get("dbutils")
    if dbutils_obj is None:
        LOGGER.info("[PHASE2J_CONF] key=databricks.job.context value=unavailable:no_dbutils")
        return
    try:
        context = dbutils_obj.notebook.entry_point.getDbutils().notebook().getContext()
        tags = context.tags()
        for key in ("jobId", "jobRunId", "taskKey", "taskRunId", "clusterId"):
            value = tags.get(key).getOrElse("unavailable")
            LOGGER.info("[PHASE2J_CONF] key=databricks.%s value=%s", key, value)
    except Exception as exc:  # pragma: no cover - Databricks runtime dependent.
        LOGGER.info("[PHASE2J_CONF] key=databricks.job.context value=unavailable:%s", type(exc).__name__)


def _partition_count(frame: DataFrame) -> int | str:
    try:
        return frame.rdd.getNumPartitions()
    except Exception as exc:  # pragma: no cover - Spark execution dependent.
        return f"unavailable:{type(exc).__name__}"


def _log_df_metrics(
    name: str,
    frame: DataFrame,
    *,
    rows: int | None = None,
    include_count: bool = False,
    normal_sample_id: bool = False,
    exchange: bool = False,
    symbol: bool = False,
) -> int | None:
    observed_rows = rows
    if observed_rows is None and include_count:
        observed_rows = frame.count()
    LOGGER.info(
        "[PHASE2J_DF] name=%s rows=%s partitions=%s schema=%s",
        name,
        observed_rows if observed_rows is not None else "not_counted",
        _partition_count(frame),
        ",".join(frame.columns),
    )
    if normal_sample_id and "normal_sample_id" in frame.columns:
        LOGGER.info(
            "[PHASE2J_DF] name=%s distinct_normal_sample_id=%s",
            name,
            frame.select("normal_sample_id").distinct().count(),
        )
    if exchange and "exchange" in frame.columns:
        coverage = [row["exchange"] for row in frame.select(F.lower(F.col("exchange")).alias("exchange")).distinct().collect()]
        LOGGER.info("[PHASE2J_DF] name=%s exchanges=%s", name, ",".join(sorted(coverage)))
    if symbol and "symbol" in frame.columns:
        symbols = [row["symbol"] for row in frame.select("symbol").distinct().collect()]
        LOGGER.info("[PHASE2J_DF] name=%s symbols=%s", name, ",".join(sorted(str(symbol) for symbol in symbols)))
    return observed_rows


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


def _repartition_raw_day(
    frame: DataFrame,
    name: str,
    *,
    partitions: int,
    bucket_seconds: int,
) -> DataFrame:
    if partitions <= 0:
        LOGGER.info("[PHASE2J_DF] name=%s raw_day_repartition=disabled", name)
        return frame
    if bucket_seconds <= 0:
        raise ValueError("raw-day bucket seconds must be greater than 0 when repartition is enabled.")

    ts_event_expr = timestamp_expression(frame, "ts_event")
    if ts_event_expr is None:
        LOGGER.info(
            "[PHASE2J_DF] name=%s raw_day_repartition_partitions=%s "
            "bucket_seconds=unavailable fallback=exchange_symbol",
            name,
            partitions,
        )
        return frame.repartition(partitions, F.lower(F.col("exchange")), F.lower(F.col("symbol")))

    bucket_column = "_phase2j_raw_time_bucket"
    ts_column = "_phase2j_raw_ts_event_ts"
    LOGGER.info(
        "[PHASE2J_DF] name=%s raw_day_repartition_partitions=%s "
        "bucket_seconds=%s keys=exchange,symbol,time_bucket",
        name,
        partitions,
        bucket_seconds,
    )
    return (
        frame.withColumn(ts_column, ts_event_expr)
        .withColumn(
            bucket_column,
            F.floor(F.col(ts_column).cast("double") / F.lit(bucket_seconds)).cast("long"),
        )
        .repartition(
            partitions,
            F.lower(F.col("exchange")),
            F.lower(F.col("symbol")),
            F.col(bucket_column),
        )
        .drop(ts_column)
    )


def _repartition_selected_output(
    frame: DataFrame,
    partitions: int,
    label: str,
    *columns: str,
) -> DataFrame:
    if partitions <= 0:
        LOGGER.info("[PHASE2J_DF] name=%s selected_output_repartition=disabled", label)
        return frame.repartition(*columns)
    LOGGER.info(
        "[PHASE2J_DF] name=%s selected_output_repartition_partitions=%s keys=%s",
        label,
        partitions,
        ",".join(columns),
    )
    return frame.repartition(partitions, *columns)


def _coalesce_small_output(frame: DataFrame, partitions: int, label: str) -> DataFrame:
    if partitions <= 0:
        LOGGER.info("[PHASE2J_DF] name=%s small_output_coalesce=disabled", label)
        return frame
    LOGGER.info("[PHASE2J_DF] name=%s small_output_coalesce_partitions=%s", label, partitions)
    return frame.coalesce(partitions)


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
    trade_day: DataFrame,
    bbo_day: DataFrame,
    expected_exchanges: tuple[str, ...],
    activity_ranked_candidates: tuple[QualityPassedNormalCandidate, ...],
    *,
    lookback_seconds: int,
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

    quality_metrics = _batch_quality_metrics(
        trade_day,
        bbo_day,
        expected_exchanges,
        candidates,
        lookback_seconds=lookback_seconds,
    )
    LOGGER.info("Phase 2J batch quality metric rows: %s", len(quality_metrics))

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
        metrics = quality_metrics.get(_timestamp_key(anchor_ts))
        if metrics is None:
            failure_counts["missing_quality_metrics"] += 1
            continue
        LOGGER.info(
            "Phase 2J quality probe attempt=%s anchor=%s activity_distance=%s "
            "nearest_candidate_distance_seconds=%s trade_count=%s bbo_count=%s "
            "trade_exchange_count=%s bbo_exchange_count=%s snapshot_row_count=%s",
            attempt,
            anchor_ts,
            candidate.activity_distance,
            candidate.nearest_candidate_distance_seconds,
            metrics["trade_count"],
            metrics["bbo_count"],
            metrics["trade_exchange_count"],
            metrics["bbo_exchange_count"],
            metrics["snapshot_row_count"],
        )
        failure_reason = _quality_failure_reason(
            metrics,
            expected_exchange_count=len(expected_exchanges),
            expected_snapshot_rows=len(expected_exchanges) * lookback_seconds,
        )
        if failure_reason is not None:
            failure_counts[failure_reason] += 1
            continue

        quality_candidates.append(
            QualityPassedNormalCandidate(
                normal_anchor_ts=anchor_ts,
                activity_distance=candidate.activity_distance,
                nearest_candidate_distance_seconds=candidate.nearest_candidate_distance_seconds,
            )
        )
        LOGGER.info(
            "Phase 2J quality pass attempt=%s anchor=%s activity_distance=%s "
            "nearest_candidate_distance_seconds=%s",
            attempt,
            anchor_ts,
            candidate.activity_distance,
            candidate.nearest_candidate_distance_seconds,
        )
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


def _batch_quality_metrics(
    trade_day: DataFrame,
    bbo_day: DataFrame,
    expected_exchanges: tuple[str, ...],
    candidates: tuple[QualityPassedNormalCandidate, ...],
    *,
    lookback_seconds: int,
) -> dict[str, dict[str, int]]:
    if not candidates:
        return {}
    spark = trade_day.sparkSession
    candidate_frame = _candidate_windows_frame(spark, candidates, lookback_seconds=lookback_seconds).cache()
    _log_df_metrics("quality_candidate_windows", candidate_frame, include_count=True)
    expected_exchange_values = [exchange.lower() for exchange in expected_exchanges]

    trade_ts_expr = timestamp_expression(trade_day, "ts_event")
    if trade_ts_expr is None:
        raise RuntimeError("Unable to convert trade ts_event to timestamp.")
    trade_base = (
        trade_day.withColumn("ts_event_ts", trade_ts_expr)
        .select(
            F.lower(F.col("exchange")).alias("exchange"),
            "symbol",
            "ts_event_ts",
        )
        .where(F.col("exchange").isin(expected_exchange_values))
        .where(F.col("ts_event_ts").isNotNull())
    )
    trade_windowed = trade_base.join(
        F.broadcast(candidate_frame),
        (F.col("ts_event_ts") >= F.col("normal_window_start_ts"))
        & (F.col("ts_event_ts") < F.col("normal_anchor_ts")),
        "inner",
    )
    trade_counts = trade_windowed.groupBy("normal_anchor_ts").agg(
        F.count("*").alias("trade_count"),
        F.countDistinct("exchange").alias("trade_exchange_count"),
        F.countDistinct("exchange", "symbol").alias("trade_exchange_symbol_count"),
    )

    bbo_ts_expr = timestamp_expression(bbo_day, "ts_event")
    if bbo_ts_expr is None:
        raise RuntimeError("Unable to convert BBO ts_event to timestamp.")
    bbo_base = (
        bbo_day.withColumn("ts_event_ts", bbo_ts_expr)
        .select(
            F.lower(F.col("exchange")).alias("exchange"),
            "symbol",
            "ts_event_ts",
        )
        .where(F.col("exchange").isin(expected_exchange_values))
        .where(F.col("ts_event_ts").isNotNull())
    )
    bbo_windowed = bbo_base.join(
        F.broadcast(candidate_frame),
        (F.col("ts_event_ts") >= F.col("normal_window_start_ts"))
        & (F.col("ts_event_ts") < F.col("normal_anchor_ts")),
        "inner",
    )
    bbo_counts = bbo_windowed.groupBy("normal_anchor_ts").agg(
        F.count("*").alias("bbo_count"),
        F.countDistinct("exchange").alias("bbo_exchange_count"),
        F.countDistinct("exchange", "symbol").alias("bbo_exchange_symbol_count"),
    )
    exchange_symbols = (
        trade_windowed.select("normal_anchor_ts", "exchange", "symbol")
        .unionByName(bbo_windowed.select("normal_anchor_ts", "exchange", "symbol"))
        .distinct()
        .groupBy("normal_anchor_ts")
        .agg(
            F.count("*").alias("snapshot_exchange_symbol_count"),
            F.countDistinct("exchange").alias("snapshot_exchange_count"),
        )
    )
    rows = (
        candidate_frame.select("normal_anchor_ts")
        .join(trade_counts, "normal_anchor_ts", "left")
        .join(bbo_counts, "normal_anchor_ts", "left")
        .join(exchange_symbols, "normal_anchor_ts", "left")
        .fillna(
            0,
            subset=[
                "trade_count",
                "trade_exchange_count",
                "trade_exchange_symbol_count",
                "bbo_count",
                "bbo_exchange_count",
                "bbo_exchange_symbol_count",
                "snapshot_exchange_symbol_count",
                "snapshot_exchange_count",
            ],
        )
        .withColumn("snapshot_row_count", F.col("snapshot_exchange_symbol_count") * F.lit(lookback_seconds))
        .collect()
    )
    candidate_frame.unpersist()
    metrics: dict[str, dict[str, int]] = {}
    for row in rows:
        metrics[_timestamp_key(row["normal_anchor_ts"])] = {
            "trade_count": int(row["trade_count"]),
            "trade_exchange_count": int(row["trade_exchange_count"]),
            "trade_exchange_symbol_count": int(row["trade_exchange_symbol_count"]),
            "bbo_count": int(row["bbo_count"]),
            "bbo_exchange_count": int(row["bbo_exchange_count"]),
            "bbo_exchange_symbol_count": int(row["bbo_exchange_symbol_count"]),
            "snapshot_exchange_symbol_count": int(row["snapshot_exchange_symbol_count"]),
            "snapshot_exchange_count": int(row["snapshot_exchange_count"]),
            "snapshot_row_count": int(row["snapshot_row_count"]),
        }
    return metrics


def _quality_failure_reason(
    metrics: dict[str, int],
    *,
    expected_exchange_count: int,
    expected_snapshot_rows: int,
) -> str | None:
    if metrics["trade_count"] == 0:
        return "trade_count_zero"
    if metrics["bbo_count"] == 0:
        return "bbo_count_zero"
    if metrics["trade_exchange_count"] < expected_exchange_count:
        return "trade_exchange_coverage"
    if metrics["bbo_exchange_count"] < expected_exchange_count:
        return "bbo_exchange_coverage"
    if metrics["snapshot_row_count"] != expected_snapshot_rows:
        return "snapshot_row_count"
    if metrics["snapshot_exchange_count"] < expected_exchange_count:
        return "snapshot_exchange_coverage"
    return None


def _timestamp_key(value: datetime) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.isoformat()


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
    selected_output_partitions: int,
    selected_join_bucket_seconds: int,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    del candidate_starts
    spark = trade_day.sparkSession
    selected_normals_df = _selected_normals_frame(
        spark,
        selected,
        selected_date=selected_date,
        lookback_seconds=lookback_seconds,
        selection_reason="activity_matched_multi_normal",
        excluded_candidate_count=excluded_candidate_count,
        selected_normal_count=selected_normal_count,
        min_anchor_spacing_seconds=min_anchor_spacing_seconds,
    ).cache()
    _log_df_metrics("selected_normals_df", selected_normals_df, include_count=True, normal_sample_id=True)

    with _phase2j_timer("normal_trade_output_build"):
        normal_trade = _repartition_selected_output(
            _extract_multi_normal_trade_window(
                trade_day,
                selected_normals_df,
                run_id,
                created_at,
                selected_join_bucket_seconds=selected_join_bucket_seconds,
            ),
            selected_output_partitions,
            "normal_trade_windows",
            "normal_sample_id",
            "exchange",
            "symbol",
        ).cache()
    with _phase2j_timer("normal_bbo_output_build"):
        normal_bbo = _repartition_selected_output(
            _extract_multi_normal_bbo_window(
                bbo_day,
                selected_normals_df,
                run_id,
                created_at,
                selected_join_bucket_seconds=selected_join_bucket_seconds,
            ),
            selected_output_partitions,
            "normal_bbo_windows",
            "normal_sample_id",
            "exchange",
            "symbol",
        ).cache()
    with _phase2j_timer("snapshot_build"):
        normal_snapshot = _repartition_selected_output(
            _build_multi_normal_snapshot(
                phase2e,
                normal_trade,
                normal_bbo,
                selected_normals_df,
                expected_exchanges,
                run_id,
                created_at,
            ),
            selected_output_partitions,
            "multi_normal_market_snapshots",
            "normal_sample_id",
            "exchange",
            "symbol",
        ).cache()
    with _phase2j_timer("profile_build"):
        normal_exchange, normal_cross, normal_bucket = _build_multi_normal_profiles(
            phase2e,
            normal_snapshot,
            expected_exchanges,
            run_id,
            created_at,
        )
        normal_exchange = _repartition_selected_output(
            normal_exchange,
            selected_output_partitions,
            "exchange_profile",
            "normal_sample_id",
            "exchange",
            "symbol",
        ).cache()
        normal_cross = _repartition_selected_output(
            normal_cross,
            selected_output_partitions,
            "cross_exchange_mid_diff",
            "normal_sample_id",
            "symbol_key",
        ).cache()
        normal_bucket = _repartition_selected_output(
            normal_bucket,
            selected_output_partitions,
            "bucket_change_profile",
            "normal_sample_id",
            "exchange",
            "symbol",
        ).cache()
    selected_normals_df.unpersist()
    return (
        normal_trade,
        normal_bbo,
        normal_snapshot,
        normal_exchange,
        normal_cross,
        normal_bucket,
    )


def _candidate_windows_frame(
    spark: Any,
    candidates: tuple[QualityPassedNormalCandidate, ...],
    *,
    lookback_seconds: int,
) -> DataFrame:
    rows = [
        (
            index,
            candidate.normal_anchor_ts,
            candidate.normal_anchor_ts - timedelta(seconds=lookback_seconds),
            candidate.activity_distance,
            candidate.nearest_candidate_distance_seconds,
        )
        for index, candidate in enumerate(candidates, start=1)
    ]
    return spark.createDataFrame(
        rows,
        (
            "candidate_index",
            "normal_anchor_ts",
            "normal_window_start_ts",
            "activity_distance",
            "nearest_candidate_distance_seconds",
        ),
    ).select(
        "candidate_index",
        F.col("normal_anchor_ts").cast("timestamp").alias("normal_anchor_ts"),
        F.col("normal_window_start_ts").cast("timestamp").alias("normal_window_start_ts"),
        "activity_distance",
        "nearest_candidate_distance_seconds",
    )


def _selected_normals_frame(
    spark: Any,
    selected: tuple[Any, ...],
    *,
    selected_date: str,
    lookback_seconds: int,
    selection_reason: str,
    excluded_candidate_count: int,
    selected_normal_count: int,
    min_anchor_spacing_seconds: int,
) -> DataFrame:
    rows = []
    for candidate in selected:
        anchor = candidate.normal_anchor_ts
        if anchor.tzinfo is not None:
            anchor = anchor.astimezone(timezone.utc).replace(tzinfo=None)
        rows.append(
            (
                SAMPLE_TYPE_NORMAL,
                selected_date,
                anchor - timedelta(seconds=lookback_seconds),
                anchor,
                anchor,
                lookback_seconds,
                selection_reason,
                excluded_candidate_count,
                candidate.nearest_candidate_distance_seconds,
                candidate.normal_sample_id,
                candidate.normal_sample_rank,
                candidate.activity_distance,
                selected_normal_count,
                min_anchor_spacing_seconds,
            )
        )
    return spark.createDataFrame(
        rows,
        (
            "sample_type",
            "selected_date",
            "normal_window_start_ts",
            "normal_window_end_ts",
            "normal_anchor_ts",
            "lookback_seconds",
            "selection_reason",
            "excluded_candidate_count",
            "nearest_candidate_distance_seconds",
            "normal_sample_id",
            "normal_sample_rank",
            "activity_distance",
            "selected_normal_count",
            "min_anchor_spacing_seconds",
        ),
    )


def _selected_normals_with_time_buckets(selected_normals: DataFrame, *, bucket_seconds: int) -> DataFrame:
    if bucket_seconds <= 0:
        raise ValueError("selected-normal join bucket seconds must be greater than 0.")
    start_bucket = F.floor(
        F.col("normal_window_start_ts").cast("double") / F.lit(bucket_seconds)
    ).cast("long")
    end_bucket = F.floor(
        (F.col("normal_window_end_ts").cast("double") - F.lit(0.000001)) / F.lit(bucket_seconds)
    ).cast("long")
    return selected_normals.withColumn(
        "_phase2j_window_bucket",
        F.explode(F.sequence(start_bucket, end_bucket)),
    )


def _metadata_columns_from_normal_alias(alias: str, created_at: datetime) -> tuple[Any, ...]:
    return (
        F.col(f"{alias}.sample_type").alias("sample_type"),
        F.col(f"{alias}.selected_date").alias("selected_date"),
        F.col(f"{alias}.normal_window_start_ts").cast("timestamp").alias("normal_window_start_ts"),
        F.col(f"{alias}.normal_window_end_ts").cast("timestamp").alias("normal_window_end_ts"),
        F.col(f"{alias}.normal_anchor_ts").cast("timestamp").alias("normal_anchor_ts"),
        F.col(f"{alias}.lookback_seconds").alias("lookback_seconds"),
        F.col(f"{alias}.selection_reason").alias("selection_reason"),
        F.col(f"{alias}.excluded_candidate_count").alias("excluded_candidate_count"),
        F.col(f"{alias}.nearest_candidate_distance_seconds").alias("nearest_candidate_distance_seconds"),
        F.lit(created_at).cast("timestamp").alias("created_at"),
    )


def _phase2j_columns_from_normal_alias(alias: str) -> tuple[Any, ...]:
    return (
        F.col(f"{alias}.normal_sample_id").alias("normal_sample_id"),
        F.col(f"{alias}.normal_sample_rank").alias("normal_sample_rank"),
        F.col(f"{alias}.activity_distance").cast("double").alias("activity_distance"),
        F.col(f"{alias}.selected_normal_count").alias("selected_normal_count"),
        F.col(f"{alias}.min_anchor_spacing_seconds").alias("min_anchor_spacing_seconds"),
    )


def _extract_multi_normal_trade_window(
    trade_day: DataFrame,
    selected_normals: DataFrame,
    run_id: str,
    created_at: datetime,
    *,
    selected_join_bucket_seconds: int,
) -> DataFrame:
    ts_event_expr = timestamp_expression(trade_day, "ts_event")
    ts_recv_expr = timestamp_expression(trade_day, "ts_recv")
    if ts_event_expr is None or ts_recv_expr is None:
        raise RuntimeError("Unable to convert trade ts_event or ts_recv to timestamp.")
    trade_base = trade_day.withColumn("ts_event_ts", ts_event_expr).withColumn("ts_recv_ts", ts_recv_expr)
    if "_phase2j_raw_time_bucket" not in trade_base.columns:
        trade_base = trade_base.withColumn(
            "_phase2j_raw_time_bucket",
            F.floor(
                F.col("ts_event_ts").cast("double") / F.lit(selected_join_bucket_seconds)
            ).cast("long"),
        )
    selected_buckets = _selected_normals_with_time_buckets(
        selected_normals,
        bucket_seconds=selected_join_bucket_seconds,
    )
    enriched = (
        trade_base.alias("trade")
        .join(
            F.broadcast(selected_buckets.alias("normal")),
            (F.col("trade._phase2j_raw_time_bucket") == F.col("normal._phase2j_window_bucket"))
            & (F.col("trade.ts_event_ts") >= F.col("normal.normal_window_start_ts"))
            & (F.col("trade.ts_event_ts") < F.col("normal.normal_window_end_ts")),
            "inner",
        )
        .withColumn(
            "second_before_anchor",
            (
                F.col("normal.normal_anchor_ts").cast("timestamp").cast("double")
                - F.col("trade.ts_event_ts").cast("double")
            ),
        )
    )
    return enriched.select(
        F.lit(run_id).alias("run_id"),
        *_metadata_columns_from_normal_alias("normal", created_at),
        F.lower(F.col("trade.exchange")).alias("exchange"),
        F.col("trade.symbol"),
        F.lit("trade").alias("source_stream"),
        F.col("trade.ts_event"),
        F.col("trade.ts_event_ts"),
        F.col("trade.ts_recv"),
        F.col("trade.ts_recv_ts"),
        _column_or_null_from_alias(trade_day, "trade", "seq").alias("seq"),
        F.col("trade.price"),
        F.col("trade.qty"),
        _column_or_null_from_alias(trade_day, "trade", "side").alias("side"),
        _column_or_null_from_alias(trade_day, "trade", "trade_id").alias("trade_id"),
        F.col("second_before_anchor"),
        *_phase2j_columns_from_normal_alias("normal"),
    )


def _extract_multi_normal_bbo_window(
    bbo_day: DataFrame,
    selected_normals: DataFrame,
    run_id: str,
    created_at: datetime,
    *,
    selected_join_bucket_seconds: int,
) -> DataFrame:
    ts_event_expr = timestamp_expression(bbo_day, "ts_event")
    ts_recv_expr = timestamp_expression(bbo_day, "ts_recv")
    if ts_event_expr is None or ts_recv_expr is None:
        raise RuntimeError("Unable to convert BBO ts_event or ts_recv to timestamp.")
    bbo_base = bbo_day.withColumn("ts_event_ts", ts_event_expr).withColumn("ts_recv_ts", ts_recv_expr)
    if "_phase2j_raw_time_bucket" not in bbo_base.columns:
        bbo_base = bbo_base.withColumn(
            "_phase2j_raw_time_bucket",
            F.floor(
                F.col("ts_event_ts").cast("double") / F.lit(selected_join_bucket_seconds)
            ).cast("long"),
        )
    selected_buckets = _selected_normals_with_time_buckets(
        selected_normals,
        bucket_seconds=selected_join_bucket_seconds,
    )
    enriched = (
        bbo_base.alias("bbo")
        .join(
            F.broadcast(selected_buckets.alias("normal")),
            (F.col("bbo._phase2j_raw_time_bucket") == F.col("normal._phase2j_window_bucket"))
            & (F.col("bbo.ts_event_ts") >= F.col("normal.normal_window_start_ts"))
            & (F.col("bbo.ts_event_ts") < F.col("normal.normal_window_end_ts")),
            "inner",
        )
        .withColumn(
            "second_before_anchor",
            (
                F.col("normal.normal_anchor_ts").cast("timestamp").cast("double")
                - F.col("bbo.ts_event_ts").cast("double")
            ),
        )
        .withColumn("bid_price_num", F.col("bbo.bid_price").cast("double"))
        .withColumn("bid_qty_num", F.col("bbo.bid_qty").cast("double"))
        .withColumn("ask_price_num", F.col("bbo.ask_price").cast("double"))
        .withColumn("ask_qty_num", F.col("bbo.ask_qty").cast("double"))
        .withColumn("mid_price", (F.col("bid_price_num") + F.col("ask_price_num")) / F.lit(2.0))
        .withColumn("spread", F.col("ask_price_num") - F.col("bid_price_num"))
        .withColumn(
            "spread_bps",
            F.when(F.col("mid_price") != 0, F.col("spread") / F.col("mid_price") * F.lit(10000.0)),
        )
        .withColumn("book_qty_sum", F.col("bid_qty_num") + F.col("ask_qty_num"))
        .withColumn(
            "book_imbalance",
            F.when(
                F.col("book_qty_sum") != 0,
                (F.col("bid_qty_num") - F.col("ask_qty_num")) / F.col("book_qty_sum"),
            ),
        )
    )
    return enriched.select(
        F.lit(run_id).alias("run_id"),
        *_metadata_columns_from_normal_alias("normal", created_at),
        F.lower(F.col("bbo.exchange")).alias("exchange"),
        F.col("bbo.symbol"),
        F.lit("bbo").alias("source_stream"),
        F.col("bbo.ts_event"),
        F.col("bbo.ts_event_ts"),
        F.col("bbo.ts_recv"),
        F.col("bbo.ts_recv_ts"),
        _column_or_null_from_alias(bbo_day, "bbo", "seq").alias("seq"),
        F.col("bbo.bid_price"),
        F.col("bbo.bid_qty"),
        F.col("bbo.ask_price"),
        F.col("bbo.ask_qty"),
        F.col("mid_price"),
        F.col("spread"),
        F.col("spread_bps"),
        F.col("book_imbalance"),
        F.col("second_before_anchor"),
        *_phase2j_columns_from_normal_alias("normal"),
    )


def _column_or_null_from_alias(frame: DataFrame, alias: str, column: str) -> Any:
    if column in frame.columns:
        return F.col(f"{alias}.{column}")
    return F.lit(None)


def _build_multi_normal_snapshot(
    phase2e: ModuleType,
    normal_trade: DataFrame,
    normal_bbo: DataFrame,
    selected_normals: DataFrame,
    expected_exchanges: tuple[str, ...],
    run_id: str,
    created_at: datetime,
) -> DataFrame:
    exchange_symbols = (
        normal_trade.select("normal_sample_id", "exchange", "symbol")
        .unionByName(normal_bbo.select("normal_sample_id", "exchange", "symbol"))
        .where(F.col("exchange").isin([exchange.lower() for exchange in expected_exchanges]))
        .distinct()
    )
    metadata = selected_normals.select(
        "sample_type",
        "selected_date",
        "normal_window_start_ts",
        "normal_window_end_ts",
        "normal_anchor_ts",
        "lookback_seconds",
        "selection_reason",
        "excluded_candidate_count",
        "nearest_candidate_distance_seconds",
        *PHASE2J_EXTRA_METADATA_COLUMNS,
    )
    second_grid = (
        exchange_symbols.join(metadata, "normal_sample_id", "inner")
        .withColumn(
            "ts_second",
            F.explode(
                F.sequence(
                    F.col("normal_window_start_ts").cast("timestamp"),
                    F.expr("normal_window_end_ts - interval 1 second"),
                    F.expr("interval 1 second"),
                )
            ),
        )
        .withColumn(
            "second_before_anchor",
            (F.col("normal_anchor_ts").cast("long") - F.col("ts_second").cast("long")).cast("int"),
        )
    )
    trade_snapshots = _build_multi_trade_snapshots(phase2e, normal_trade)
    bbo_snapshots = _build_multi_bbo_snapshots(phase2e, normal_bbo)
    output = (
        second_grid.join(
            trade_snapshots,
            ["normal_sample_id", "exchange", "symbol", "ts_second"],
            "left",
        )
        .join(
            bbo_snapshots,
            ["normal_sample_id", "exchange", "symbol", "ts_second"],
            "left",
        )
        .transform(phase2e._coalesce_empty_trade_seconds)
        .transform(_coalesce_empty_multi_bbo_seconds)
    )
    return output.select(
        F.lit(run_id).alias("run_id"),
        "sample_type",
        "selected_date",
        "normal_window_start_ts",
        "normal_window_end_ts",
        "normal_anchor_ts",
        "lookback_seconds",
        "selection_reason",
        "excluded_candidate_count",
        "nearest_candidate_distance_seconds",
        F.lit(created_at).cast("timestamp").alias("created_at"),
        "exchange",
        "symbol",
        "second_before_anchor",
        "ts_second",
        *[F.col(column) for column in phase2e.TRADE_ZERO_COLUMNS],
        "last_trade_price",
        "min_trade_price",
        "max_trade_price",
        "bbo_update_count",
        "has_bbo_update",
        "is_bbo_forward_filled",
        "last_bbo_update_ts",
        "bbo_quote_age_seconds",
        *[F.col(column) for column in phase2e.BBO_LAST_COLUMNS],
        "avg_spread_bps",
        "max_spread_bps",
        "avg_book_imbalance",
        *PHASE2J_EXTRA_METADATA_COLUMNS,
    )


def _build_multi_trade_snapshots(phase2e: ModuleType, normal_trade: DataFrame) -> DataFrame:
    selected = (
        normal_trade.select(
            "normal_sample_id",
            "exchange",
            "symbol",
            F.date_trunc("second", F.col("ts_event_ts")).alias("ts_second"),
            "ts_event_ts",
            "seq",
            "trade_id",
            F.col("price").cast("double").alias("price"),
            F.col("qty").cast("double").alias("qty"),
            F.lower(F.trim(F.col("side"))).alias("side_norm"),
        )
        .where(F.col("ts_second").isNotNull())
        .where(F.col("price").isNotNull())
        .where(F.col("qty").isNotNull())
    )
    ranked = selected.withColumn(
        "last_rank",
        F.row_number().over(
            Window.partitionBy("normal_sample_id", "exchange", "symbol", "ts_second").orderBy(
                F.col("ts_event_ts").desc(),
                F.col("seq").desc_nulls_last(),
                F.col("trade_id").desc_nulls_last(),
            )
        ),
    ).withColumn("notional", F.col("price") * F.col("qty"))
    buy = F.col("side_norm").isin("buy", "b")
    sell = F.col("side_norm").isin("sell", "s")
    return (
        ranked.groupBy("normal_sample_id", "exchange", "symbol", "ts_second")
        .agg(
            F.count("*").alias("trade_count"),
            F.sum("qty").alias("trade_volume"),
            F.sum("notional").alias("trade_notional"),
            F.sum(F.when(buy, 1).otherwise(0)).alias("buy_trade_count"),
            F.sum(F.when(sell, 1).otherwise(0)).alias("sell_trade_count"),
            F.sum(F.when(buy, F.col("qty")).otherwise(0.0)).alias("buy_qty"),
            F.sum(F.when(sell, F.col("qty")).otherwise(0.0)).alias("sell_qty"),
            F.sum(F.when(buy, F.col("notional")).otherwise(0.0)).alias("buy_notional"),
            F.sum(F.when(sell, F.col("notional")).otherwise(0.0)).alias("sell_notional"),
            F.max(F.when(F.col("last_rank") == 1, F.col("price"))).alias("last_trade_price"),
            F.min("price").alias("min_trade_price"),
            F.max("price").alias("max_trade_price"),
        )
        .withColumn("trade_imbalance_qty", F.col("buy_qty") - F.col("sell_qty"))
    )


def _build_multi_bbo_snapshots(phase2e: ModuleType, normal_bbo: DataFrame) -> DataFrame:
    ranked = normal_bbo.withColumn(
        "ts_second",
        F.date_trunc("second", F.col("ts_event_ts")),
    ).withColumn(
        "last_rank",
        F.row_number().over(
            Window.partitionBy("normal_sample_id", "exchange", "symbol", "ts_second").orderBy(
                F.col("ts_event_ts").desc(),
                F.col("seq").desc_nulls_last(),
                F.col("ts_recv_ts").desc_nulls_last(),
            )
        ),
    )
    per_second = ranked.groupBy("normal_sample_id", "exchange", "symbol", "ts_second").agg(
        F.count("*").alias("bbo_update_count"),
        F.max(F.when(F.col("last_rank") == 1, F.col("ts_event_ts"))).alias("last_bbo_update_ts_raw"),
        F.max(F.when(F.col("last_rank") == 1, F.col("bid_price").cast("double"))).alias("last_bid_price_raw"),
        F.max(F.when(F.col("last_rank") == 1, F.col("ask_price").cast("double"))).alias("last_ask_price_raw"),
        F.max(F.when(F.col("last_rank") == 1, F.col("bid_qty").cast("double"))).alias("last_bid_qty_raw"),
        F.max(F.when(F.col("last_rank") == 1, F.col("ask_qty").cast("double"))).alias("last_ask_qty_raw"),
        F.max(F.when(F.col("last_rank") == 1, F.col("mid_price"))).alias("last_mid_price_raw"),
        F.max(F.when(F.col("last_rank") == 1, F.col("spread"))).alias("last_spread_raw"),
        F.max(F.when(F.col("last_rank") == 1, F.col("spread_bps"))).alias("last_spread_bps_raw"),
        F.avg("spread_bps").alias("avg_spread_bps"),
        F.max("spread_bps").alias("max_spread_bps"),
        F.max(F.when(F.col("last_rank") == 1, F.col("book_imbalance"))).alias("last_book_imbalance_raw"),
        F.avg("book_imbalance").alias("avg_book_imbalance"),
    )
    fill_window = (
        Window.partitionBy("normal_sample_id", "exchange", "symbol")
        .orderBy("ts_second")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return per_second.select(
        "normal_sample_id",
        "exchange",
        "symbol",
        "ts_second",
        "bbo_update_count",
        "avg_spread_bps",
        "max_spread_bps",
        "avg_book_imbalance",
        F.last("last_bbo_update_ts_raw", True).over(fill_window).alias("last_bbo_update_ts"),
        *[
            F.last(f"{column}_raw", True).over(fill_window).alias(column)
            for column in phase2e.BBO_LAST_COLUMNS
            if column != "last_bbo_update_ts"
        ],
    )


def _coalesce_empty_multi_bbo_seconds(frame: DataFrame) -> DataFrame:
    with_count = frame.withColumn(
        "bbo_update_count",
        F.coalesce(F.col("bbo_update_count"), F.lit(0)).cast("long"),
    )
    fill_window = (
        Window.partitionBy("normal_sample_id", "exchange", "symbol")
        .orderBy("ts_second")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    with_filled = with_count
    for column in ("last_bbo_update_ts", "last_bid_price", "last_ask_price", "last_bid_qty", "last_ask_qty", "last_mid_price", "last_spread", "last_spread_bps", "last_book_imbalance"):
        with_filled = with_filled.withColumn(column, F.last(column, True).over(fill_window))
    return (
        with_filled.withColumn("has_bbo_update", F.col("bbo_update_count") > F.lit(0))
        .withColumn(
            "is_bbo_forward_filled",
            (F.col("bbo_update_count") == F.lit(0)) & F.col("last_bbo_update_ts").isNotNull(),
        )
        .withColumn(
            "bbo_quote_age_seconds",
            F.when(
                F.col("last_bbo_update_ts").isNotNull(),
                F.col("ts_second").cast("long") - F.col("last_bbo_update_ts").cast("long"),
            ),
        )
    )


def _build_multi_normal_profiles(
    phase2e: ModuleType,
    normal_snapshot: DataFrame,
    expected_exchanges: tuple[str, ...],
    run_id: str,
    created_at: datetime,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    spark = normal_snapshot.sparkSession
    windows = phase2e._profile_windows_frame(spark)
    profiled = normal_snapshot.withColumn("second_before_event", F.col("second_before_anchor"))
    exchange_profile = _build_multi_exchange_profile(phase2e, profiled, windows, run_id, created_at)
    cross_exchange_mid_diff = _build_multi_cross_exchange_mid_diff(
        phase2e,
        profiled,
        windows,
        expected_exchanges,
        run_id,
        created_at,
    )
    bucket_change_profile = _build_multi_bucket_change_profile(phase2e, spark, exchange_profile, run_id, created_at)
    return exchange_profile, cross_exchange_mid_diff, bucket_change_profile


def _profile_metadata_group_columns() -> tuple[str, ...]:
    return (
        "sample_type",
        "selected_date",
        "normal_window_start_ts",
        "normal_window_end_ts",
        "normal_anchor_ts",
        *PHASE2J_METADATA_COLUMNS,
    )


def _profile_audit_select(run_id: str, phase2e: ModuleType, created_at: datetime) -> tuple[Any, ...]:
    return (
        F.lit(run_id).alias("run_id"),
        F.lit(run_id).alias("source_snapshot_run_id"),
        F.col("sample_type"),
        F.col("selected_date"),
        F.lit(None).cast("string").alias("event_id"),
        F.lit(None).cast("string").alias("event_direction"),
        F.lit(None).cast("timestamp").alias("event_start_ts"),
        F.col("normal_window_start_ts"),
        F.col("normal_window_end_ts"),
        F.col("normal_anchor_ts"),
        F.lit(phase2e.PROFILE_VERSION).alias("profile_version"),
        F.lit(created_at).cast("timestamp").alias("created_at"),
    )


def _build_multi_exchange_profile(
    phase2e: ModuleType,
    snapshot: DataFrame,
    windows: DataFrame,
    run_id: str,
    created_at: datetime,
) -> DataFrame:
    metadata_columns = _profile_metadata_group_columns()
    windowed = (
        snapshot.crossJoin(windows)
        .where(F.col("second_before_event") >= F.col("window_min_second_before_event"))
        .where(F.col("second_before_event") <= F.col("window_max_second_before_event"))
    )
    partition = Window.partitionBy("normal_sample_id", "exchange", "symbol", "window_mode", "window_label")
    ranked = (
        windowed.withColumn(
            "first_rank",
            F.row_number().over(partition.orderBy(F.col("second_before_event").desc())),
        )
        .withColumn(
            "last_rank",
            F.row_number().over(partition.orderBy(F.col("second_before_event").asc())),
        )
        .withColumn("imbalance_qty", F.col("buy_qty") - F.col("sell_qty"))
    )
    grouped = ranked.groupBy(
        *metadata_columns,
        "exchange",
        "symbol",
        "window_mode",
        "window_label",
        "window_min_second_before_event",
        "window_max_second_before_event",
    ).agg(
        F.count("*").alias("seconds_observed"),
        F.sum(F.when(F.col("trade_count") > 0, 1).otherwise(0)).alias("seconds_with_trades"),
        F.sum(F.when(F.col("bbo_update_count") > 0, 1).otherwise(0)).alias("seconds_with_bbo_update"),
        F.sum(F.when(F.col("is_bbo_forward_filled"), 1).otherwise(0)).alias("seconds_forward_filled_bbo"),
        F.sum("trade_count").alias("trade_count_sum"),
        F.avg("trade_count").alias("trade_count_per_second_avg"),
        F.max("trade_count").alias("trade_count_per_second_max"),
        F.sum("trade_volume").alias("trade_volume_sum"),
        F.sum("trade_notional").alias("trade_notional_sum"),
        F.sum("buy_qty").alias("buy_qty_sum"),
        F.sum("sell_qty").alias("sell_qty_sum"),
        F.sum("imbalance_qty").alias("trade_imbalance_qty_sum"),
        F.avg("imbalance_qty").alias("trade_imbalance_qty_per_second"),
        F.max(F.when(F.col("first_rank") == 1, F.col("last_trade_price"))).alias("first_last_trade_price"),
        F.max(F.when(F.col("last_rank") == 1, F.col("last_trade_price"))).alias("last_last_trade_price"),
        F.min("last_trade_price").alias("min_last_trade_price"),
        F.max("last_trade_price").alias("max_last_trade_price"),
        F.max(F.when(F.col("first_rank") == 1, F.col("last_mid_price"))).alias("first_last_mid_price"),
        F.max(F.when(F.col("last_rank") == 1, F.col("last_mid_price"))).alias("last_last_mid_price"),
        F.avg("avg_spread_bps").alias("avg_spread_bps_mean"),
        F.max("avg_spread_bps").alias("avg_spread_bps_max"),
        F.max(F.when(F.col("last_rank") == 1, F.col("last_spread_bps"))).alias("last_spread_bps_last"),
        F.avg("avg_book_imbalance").alias("avg_book_imbalance_mean"),
        F.max(F.when(F.col("last_rank") == 1, F.col("last_book_imbalance"))).alias("last_book_imbalance_last"),
        F.max("bbo_quote_age_seconds").alias("bbo_quote_age_seconds_max"),
    )
    with_ratios = (
        grouped.withColumn(
            "trade_imbalance_qty_ratio",
            F.when(
                (F.col("buy_qty_sum") + F.col("sell_qty_sum")) != 0,
                F.col("trade_imbalance_qty_sum") / (F.col("buy_qty_sum") + F.col("sell_qty_sum")),
            ),
        )
        .withColumn(
            "last_trade_return_bps",
            F.when(
                F.col("first_last_trade_price").isNotNull()
                & (F.col("first_last_trade_price") != 0)
                & F.col("last_last_trade_price").isNotNull(),
                (F.col("last_last_trade_price") / F.col("first_last_trade_price") - F.lit(1.0))
                * F.lit(10000.0),
            ),
        )
        .withColumn(
            "last_mid_return_bps",
            F.when(
                F.col("first_last_mid_price").isNotNull()
                & (F.col("first_last_mid_price") != 0)
                & F.col("last_last_mid_price").isNotNull(),
                (F.col("last_last_mid_price") / F.col("first_last_mid_price") - F.lit(1.0))
                * F.lit(10000.0),
            ),
        )
    )
    return with_ratios.select(
        *_profile_audit_select(run_id, phase2e, created_at),
        "exchange",
        "symbol",
        "window_mode",
        "window_label",
        "window_min_second_before_event",
        "window_max_second_before_event",
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
        "first_last_trade_price",
        "last_last_trade_price",
        "min_last_trade_price",
        "max_last_trade_price",
        "last_trade_return_bps",
        "first_last_mid_price",
        "last_last_mid_price",
        "last_mid_return_bps",
        "avg_spread_bps_mean",
        "avg_spread_bps_max",
        "last_spread_bps_last",
        "avg_book_imbalance_mean",
        "last_book_imbalance_last",
        "bbo_quote_age_seconds_max",
        *PHASE2J_METADATA_COLUMNS,
    )


def _build_multi_cross_exchange_mid_diff(
    phase2e: ModuleType,
    snapshot: DataFrame,
    windows: DataFrame,
    expected_exchanges: tuple[str, ...],
    run_id: str,
    created_at: datetime,
) -> DataFrame:
    metadata_columns = _profile_metadata_group_columns()
    pairs = phase2e._exchange_pairs_frame(snapshot.sparkSession, expected_exchanges)
    base = snapshot.select(
        *metadata_columns,
        F.lower(F.col("symbol")).alias("symbol_key"),
        F.lower(F.col("exchange")).alias("exchange"),
        "second_before_event",
        "ts_second",
        F.col("last_mid_price").cast("double").alias("last_mid_price"),
    )
    joined = (
        base.alias("a")
        .join(
            base.alias("b"),
            (F.col("a.normal_sample_id") == F.col("b.normal_sample_id"))
            & (F.col("a.symbol_key") == F.col("b.symbol_key"))
            & (F.col("a.ts_second") == F.col("b.ts_second")),
            "inner",
        )
        .join(
            pairs,
            (F.col("a.exchange") == F.col("exchange_a"))
            & (F.col("b.exchange") == F.col("exchange_b")),
            "inner",
        )
        .where(F.col("a.last_mid_price").isNotNull())
        .where(F.col("b.last_mid_price").isNotNull())
        .select(
            *[F.col(f"a.{column}").alias(column) for column in metadata_columns],
            F.col("a.symbol_key").alias("symbol_key"),
            F.col("a.second_before_event").alias("second_before_event"),
            F.col("a.ts_second").alias("ts_second"),
            F.col("exchange_pair"),
            F.col("exchange_a"),
            F.col("exchange_b"),
            F.col("a.last_mid_price").alias("mid_exchange_a"),
            F.col("b.last_mid_price").alias("mid_exchange_b"),
        )
        .withColumn("mid_diff", F.col("mid_exchange_a") - F.col("mid_exchange_b"))
        .withColumn(
            "mid_diff_bps",
            F.when(F.col("mid_exchange_b") != 0, F.col("mid_diff") / F.col("mid_exchange_b") * F.lit(10000.0)),
        )
        .withColumn("abs_mid_diff_bps", F.abs(F.col("mid_diff_bps")))
    )
    windowed = (
        joined.crossJoin(windows)
        .where(F.col("second_before_event") >= F.col("window_min_second_before_event"))
        .where(F.col("second_before_event") <= F.col("window_max_second_before_event"))
    )
    partition = Window.partitionBy("normal_sample_id", "symbol_key", "window_mode", "window_label", "exchange_pair")
    ranked = windowed.withColumn(
        "last_rank",
        F.row_number().over(partition.orderBy(F.col("second_before_event").asc())),
    )
    grouped = ranked.groupBy(
        *metadata_columns,
        "symbol_key",
        "window_mode",
        "window_label",
        "exchange_pair",
        "exchange_a",
        "exchange_b",
    ).agg(
        F.count("*").alias("seconds_compared"),
        F.avg("mid_diff_bps").alias("avg_mid_diff_bps"),
        F.max("abs_mid_diff_bps").alias("max_abs_mid_diff_bps"),
        F.max(F.when(F.col("last_rank") == 1, F.col("mid_diff_bps"))).alias("last_mid_diff_bps"),
        F.max(F.when(F.col("last_rank") == 1, F.col("abs_mid_diff_bps"))).alias("last_abs_mid_diff_bps"),
    )
    return grouped.select(
        *_profile_audit_select(run_id, phase2e, created_at),
        "symbol_key",
        "window_mode",
        "window_label",
        "exchange_pair",
        "exchange_a",
        "exchange_b",
        "seconds_compared",
        "avg_mid_diff_bps",
        "max_abs_mid_diff_bps",
        "last_mid_diff_bps",
        "last_abs_mid_diff_bps",
        *PHASE2J_METADATA_COLUMNS,
    )


def _build_multi_bucket_change_profile(
    phase2e: ModuleType,
    spark: Any,
    exchange_profile: DataFrame,
    run_id: str,
    created_at: datetime,
) -> DataFrame:
    bucket_pairs = spark.createDataFrame(phase2e.BUCKET_CHANGE_PAIRS, ("from_bucket", "to_bucket"))
    audit_columns = (*PHASE2J_PROFILE_AUDIT_COLUMNS, *PHASE2J_METADATA_COLUMNS)
    bucket_metrics = exchange_profile.where(F.col("window_mode") == "bucket").select(
        *audit_columns,
        "exchange",
        "symbol",
        F.col("window_label").alias("bucket_label"),
        F.expr(
            "stack(4, "
            "'trade_count_per_second', trade_count_per_second_avg, "
            "'trade_imbalance_qty_per_second', trade_imbalance_qty_per_second, "
            "'avg_spread_bps', avg_spread_bps_mean, "
            "'avg_book_imbalance', avg_book_imbalance_mean"
            ") AS (metric_name, metric_value)"
        ),
    )
    joined = (
        bucket_pairs.join(
            bucket_metrics.alias("from_metric"),
            F.col("from_bucket") == F.col("from_metric.bucket_label"),
            "inner",
        )
        .join(
            bucket_metrics.alias("to_metric"),
            (F.col("to_bucket") == F.col("to_metric.bucket_label"))
            & (F.col("from_metric.normal_sample_id") == F.col("to_metric.normal_sample_id"))
            & (F.col("from_metric.exchange") == F.col("to_metric.exchange"))
            & (F.col("from_metric.symbol") == F.col("to_metric.symbol"))
            & (F.col("from_metric.metric_name") == F.col("to_metric.metric_name")),
            "inner",
        )
        .select(
            *[F.col(f"from_metric.{column}").alias(column) for column in audit_columns],
            F.col("from_metric.exchange").alias("exchange"),
            F.col("from_metric.symbol").alias("symbol"),
            F.col("from_metric.metric_name").alias("metric_name"),
            F.col("from_bucket"),
            F.col("to_bucket"),
            F.col("from_metric.metric_value").alias("from_bucket_value"),
            F.col("to_metric.metric_value").alias("to_bucket_value"),
        )
        .withColumn("absolute_change", F.col("to_bucket_value") - F.col("from_bucket_value"))
        .withColumn(
            "relative_change",
            F.when(
                F.col("from_bucket_value").isNotNull()
                & (F.col("from_bucket_value") != 0)
                & F.col("to_bucket_value").isNotNull(),
                F.col("absolute_change") / F.abs(F.col("from_bucket_value")),
            ),
        )
    )
    return joined.select(
        *_profile_audit_select(run_id, phase2e, created_at),
        "exchange",
        "symbol",
        "metric_name",
        "from_bucket",
        "to_bucket",
        "from_bucket_value",
        "to_bucket_value",
        "absolute_change",
        "relative_change",
        *PHASE2J_METADATA_COLUMNS,
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
    validation_mode: str,
    small_output_partitions: int,
) -> None:
    with _phase2j_timer(f"top_diff_build_{label}"):
        top_diffs = _coalesce_small_output(
            _build_top_diffs(comparison, run_id=run_id, top_n=top_n, created_at=created_at),
            small_output_partitions,
            label,
        ).cache()
        top_diff_count = _log_df_metrics(label, top_diffs, include_count=True)
    _log_top_diff_group_counts(comparison, top_diffs, label)
    with _phase2j_timer(f"write_validate_{label}"):
        phase2e._write_and_validate(
            top_diffs,
            output_path,
            label,
            sample_size,
            validation_mode=validation_mode,
            row_count=top_diff_count,
        )
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
) -> dict[str, int]:
    counts = {}
    counts["multi_normal_market_snapshots"] = _validate_comparison_count(
        normal_snapshot,
        "multi_normal_market_snapshots",
        exchange_count * lookback_seconds * selected_normal_count,
    )
    counts["multi_normal_profile_reports.exchange_profile"] = _validate_comparison_count(
        normal_exchange,
        "multi_normal_profile_reports.exchange_profile",
        expected_multi_normal_profile_rows(30, selected_normal_count),
    )
    counts["multi_normal_profile_reports.cross_exchange_mid_diff"] = _validate_comparison_count(
        normal_cross,
        "multi_normal_profile_reports.cross_exchange_mid_diff",
        expected_multi_normal_profile_rows(30, selected_normal_count),
    )
    counts["multi_normal_profile_reports.bucket_change_profile"] = _validate_comparison_count(
        normal_bucket,
        "multi_normal_profile_reports.bucket_change_profile",
        expected_multi_normal_profile_rows(48, selected_normal_count),
    )
    return counts


def _validate_comparison_count(frame: DataFrame, label: str, expected_count: int) -> int:
    observed_count = frame.count()
    LOGGER.info("%s expected vs observed rows: expected=%s observed=%s", label, expected_count, observed_count)
    if observed_count != expected_count:
        raise RuntimeError(f"{label} row count mismatch: expected={expected_count}, observed={observed_count}")
    return observed_count


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
