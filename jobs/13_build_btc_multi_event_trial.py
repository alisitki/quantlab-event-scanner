#!/usr/bin/env python
"""Phase 3A BTC multi-event trial."""

from __future__ import annotations

import argparse
import importlib.util
import logging
import sys
import time
from contextlib import contextmanager
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
from pyspark.sql import types as T  # noqa: E402

from quantlab_event_scanner.btc_multi_event_trial import (  # noqa: E402
    DEFAULT_MAX_EVENTS,
    DEFAULT_MIN_EVENTS,
    DEFAULT_NORMAL_COUNT_PER_EVENT,
    STATUS_EVENT_QUALITY_FAILED,
    STATUS_NORMAL_SELECTION_FAILED,
    STATUS_SELECTED,
    GroupedMoveEvent,
    RawMoveCandidate,
    default_phase3a_run_id,
    direction_balance_note,
    expected_comparison_rows,
    expected_event_profile_rows,
    expected_event_snapshot_rows,
    expected_normal_profile_rows,
    expected_normal_snapshot_rows,
    expected_top_diff_rows_dynamic,
    group_time_clustered_candidates,
    processing_status,
    select_direction_balanced_events,
)
from quantlab_event_scanner.config import load_config  # noqa: E402
from quantlab_event_scanner.manifest import ManifestPartition  # noqa: E402
from quantlab_event_scanner.manifest import load_manifest_from_s3_with_spark  # noqa: E402
from quantlab_event_scanner.paths import btc_multi_event_trial_run_path  # noqa: E402
from quantlab_event_scanner.pre_event_market_snapshots import exchange_coverage  # noqa: E402
from quantlab_event_scanner.profile_comparison_reports import (  # noqa: E402
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


LOGGER = logging.getLogger("quantlab_event_scanner.phase3a_btc_multi_event_trial")

DEFAULT_START_DATE = "20260423"
DEFAULT_END_DATE = "20260423"
DEFAULT_LOOKBACK_SECONDS = 300
DEFAULT_HORIZON_SECONDS = 60
DEFAULT_MOVE_THRESHOLD_PCT = 1.0
DEFAULT_EXCLUSION_SECONDS = 1800
DEFAULT_ANCHOR_STEP_SECONDS = 60
DEFAULT_RAW_DAY_REPARTITION_PARTITIONS = 96
DEFAULT_RAW_DAY_BUCKET_SECONDS = 60
DEFAULT_SELECTED_OUTPUT_PARTITIONS = 200
DEFAULT_SMALL_OUTPUT_PARTITIONS = 1
DEFAULT_TOP_N = 20

RAW_NORMAL_ID_RE = r"^(.*)__normal_[0-9]{3}$"
PHASE3A_TOP_DIFF_RANKINGS = (
    ("top_absolute_diff_vs_mean", "absolute_diff_vs_mean", False),
    ("top_signed_positive_vs_mean", "signed_diff_vs_mean", False),
    ("top_signed_negative_vs_mean", "signed_diff_vs_mean", True),
)


@contextmanager
def _phase3a_timer(step: str) -> Any:
    start = time.monotonic()
    try:
        yield
    finally:
        LOGGER.info("[PHASE3A_TIMER] step=%s seconds=%.3f", step, time.monotonic() - start)


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    phase2d = _load_job_module("phase2d_pre_event_profile_report", "08_profile_pre_event_market_snapshots_trial.py")
    phase2e = _load_job_module("phase2e_normal_time_trial", "09_build_normal_time_trial.py")
    phase2f = _load_job_module("phase2f_profile_comparison_trial", "10_compare_event_vs_normal_profiles_trial.py")
    phase2j = _load_job_module("phase2j_multi_normal_trial", "12_build_multi_normal_trial.py")

    config_path = _resolve_path(args.config)
    config = load_config(config_path)
    run_id = args.run_id or default_phase3a_run_id(datetime.now(timezone.utc))
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    output_root = btc_multi_event_trial_run_path(config, run_id)

    LOGGER.info("Using config: %s", config_path)
    LOGGER.info("Run ID: %s", run_id)
    LOGGER.info("Output root: %s", output_root)
    for key, value in sorted(vars(args).items()):
        LOGGER.info("[PHASE3A_CONF] key=%s value=%s", key, value)

    spark = _get_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    _log_spark_runtime_context(spark, run_id)

    with _phase3a_timer("manifest_load"):
        manifest = load_manifest_from_s3_with_spark(spark, config.manifest_path)
        trade_partitions = _select_partitions(
            manifest.partitions,
            exchanges=config.exchanges,
            stream="trade",
            start_date=args.start_date,
            end_date=args.end_date,
        )
        bbo_partitions = _select_partitions(
            manifest.partitions,
            exchanges=config.exchanges,
            stream="bbo",
            start_date=args.start_date,
            end_date=args.end_date,
        )
    _log_partition_coverage("trade manifest", config.exchanges, trade_partitions)
    _log_partition_coverage("BBO manifest", config.exchanges, bbo_partitions)
    trade_coverage = _coverage_by_date(trade_partitions, config.exchanges)
    bbo_coverage = _coverage_by_date(bbo_partitions, config.exchanges)

    with _phase3a_timer("raw_trade_read_cache_materialize"):
        trade_day = phase2j._repartition_raw_day(
            _read_partitions(spark, trade_partitions, config.input_root, "trade"),
            "phase3a_trade_day",
            partitions=args.raw_day_repartition_partitions,
            bucket_seconds=args.raw_day_bucket_seconds,
        ).cache()
        _log_df_metrics("trade_day", trade_day, include_count=True, exchange=True, symbol=True)
    with _phase3a_timer("raw_bbo_read_cache_materialize"):
        bbo_day = phase2j._repartition_raw_day(
            _read_partitions(spark, bbo_partitions, config.input_root, "BBO"),
            "phase3a_bbo_day",
            partitions=args.raw_day_repartition_partitions,
            bucket_seconds=args.raw_day_bucket_seconds,
        ).cache()
        _log_df_metrics("bbo_day", bbo_day, include_count=True, exchange=True, symbol=True)
    _validate_required_trade_columns(trade_day)
    _validate_required_bbo_columns(bbo_day)

    with _phase3a_timer("candidate_scan"):
        price_1s = build_price_1s(trade_day)
        if price_1s is None:
            raise RuntimeError("Unable to build 1s price rows for event scan.")
        price_1s = price_1s.cache()
        _log_df_metrics("price_1s", price_1s, include_count=True, exchange=True, symbol=True)
        candidates, ambiguous = scan_move_candidates(
            price_1s,
            threshold_pct=args.move_threshold_pct,
            lookahead_seconds=args.horizon_seconds,
        )
        candidates = candidates.cache()
        ambiguous = ambiguous.cache()
        non_ambiguous_count = _log_df_metrics("candidate_df", candidates, include_count=True)
        ambiguous_count = _log_df_metrics("ambiguous_candidate_df", ambiguous, include_count=True)
        raw_candidate_starts = phase2j._collect_raw_candidate_starts(candidates, ambiguous)
    LOGGER.info("Phase 3A raw candidate starts used for normal exclusion: %s", len(raw_candidate_starts))
    LOGGER.info("Phase 3A non-ambiguous candidates: %s ambiguous candidates: %s", non_ambiguous_count, ambiguous_count)

    with _phase3a_timer("group_events"):
        grouped_events = _group_events_from_candidates(candidates, args.horizon_seconds, args.directions)
        LOGGER.info("[PHASE3A_DF] name=grouped_events rows=%s partitions=not_dataframe schema=driver_rows", len(grouped_events))
        if not grouped_events:
            raise RuntimeError("No grouped non-ambiguous BTC events found.")

    statuses = []
    coverage_pass_events: list[GroupedMoveEvent] = []
    for event in grouped_events:
        reason = _event_quality_reason(
            event,
            trade_coverage=trade_coverage,
            bbo_coverage=bbo_coverage,
            expected_exchanges=config.exchanges,
            lookback_seconds=args.lookback_seconds,
            allow_partial_coverage=args.allow_partial_coverage,
        )
        if reason is not None:
            statuses.append(
                processing_status(
                    event,
                    stage=STATUS_EVENT_QUALITY_FAILED,
                    selected=False,
                    exclusion_reason=reason,
                )
            )
            continue
        coverage_pass_events.append(event)

    if not coverage_pass_events:
        _write_status_summary(spark, statuses, f"{output_root}/summary/event_processing_status")
        raise RuntimeError("No grouped events passed coverage/pre-window quality checks.")

    with _phase3a_timer("event_output_build"):
        coverage_pass_df = _events_frame(
            spark,
            coverage_pass_events,
            run_id,
            created_at,
            lookback_seconds=args.lookback_seconds,
        ).cache()
        event_trade = _extract_event_trade_window(
            trade_day,
            coverage_pass_df,
            run_id,
            created_at,
            selected_join_bucket_seconds=args.raw_day_bucket_seconds,
        ).cache()
        event_bbo = _extract_event_bbo_window(
            bbo_day,
            coverage_pass_df,
            run_id,
            created_at,
            selected_join_bucket_seconds=args.raw_day_bucket_seconds,
        ).cache()
        event_snapshot = _build_event_snapshot(
            phase2e,
            event_trade,
            event_bbo,
            coverage_pass_df,
            config.exchanges,
            run_id,
            created_at,
        ).cache()
        event_windows = phase2d._profile_windows_frame(spark)
        event_exchange_all = phase2d._build_exchange_profile(
            event_snapshot,
            event_windows,
            run_id=run_id,
            source_snapshot_run_id=run_id,
            created_at=created_at,
        ).cache()
        event_cross_all = phase2d._build_cross_exchange_mid_diff(
            event_snapshot,
            event_windows,
            config.exchanges,
            run_id=run_id,
            source_snapshot_run_id=run_id,
            created_at=created_at,
        ).cache()
        event_bucket_all = phase2d._build_bucket_change_profile(
            spark,
            event_exchange_all,
            run_id=run_id,
            source_snapshot_run_id=run_id,
            created_at=created_at,
        ).cache()
        _log_df_metrics("event_market_snapshots_all_quality_pass", event_snapshot, include_count=True)
        _log_df_metrics("event_exchange_profile_all_quality_pass", event_exchange_all, include_count=True)

    selected_events: list[GroupedMoveEvent] = []
    selected_normals_by_event: dict[str, tuple[Any, ...]] = {}
    ranked_event_order = _event_processing_order(coverage_pass_events, args.max_events)
    with _phase3a_timer("event_selection_and_normal_backfill"):
        for event in ranked_event_order:
            if len(selected_events) >= args.max_events:
                break
            event_exchange = event_exchange_all.where(F.col("event_id") == event.event_id).cache()
            if event_exchange.count() == 0:
                statuses.append(
                    processing_status(
                        event,
                        stage=STATUS_EVENT_QUALITY_FAILED,
                        selected=False,
                        exclusion_reason="insufficient_data",
                    )
                )
                event_exchange.unpersist()
                continue

            try:
                event_activity = phase2j._event_activity_source(event_exchange, config.exchanges).cache()
                anchor_candidates, exclusion_counts, total_anchors = phase2j._eligible_anchor_candidates(
                    price_1s,
                    raw_candidate_starts,
                    selected_date=event.selected_date,
                    lookback_seconds=args.lookback_seconds,
                    horizon_seconds=args.horizon_seconds,
                    proximity_seconds=args.exclusion_seconds,
                    anchor_step_seconds=args.anchor_step_seconds,
                )
                LOGGER.info(
                    "Phase 3A event=%s total_anchors=%s eligible_anchors=%s exclusion_counts=%s",
                    event.event_id,
                    total_anchors,
                    len(anchor_candidates),
                    exclusion_counts,
                )
                activity_ranked = phase2j._activity_ranked_anchor_candidates(
                    trade_day,
                    event_activity,
                    config.exchanges,
                    anchor_candidates,
                    raw_candidate_starts,
                    lookback_seconds=args.lookback_seconds,
                )
                selected_normals, failures, probes, quality_passes, spacing_skips = (
                    phase2j._select_quality_passed_candidates(
                        trade_day,
                        bbo_day,
                        config.exchanges,
                        activity_ranked,
                        lookback_seconds=args.lookback_seconds,
                        selected_normal_count=args.normal_count_per_event,
                        min_anchor_spacing_seconds=args.min_normal_anchor_spacing_seconds,
                        max_quality_candidates=args.max_quality_candidates,
                    )
                )
                selected_normals_by_event[event.event_id] = selected_normals
                selected_events.append(event)
                statuses.append(
                    processing_status(
                        event,
                        stage=STATUS_SELECTED,
                        selected=True,
                        normal_selection_count=len(selected_normals),
                        row_count_status="pending",
                    )
                )
                LOGGER.info(
                    "Phase 3A selected event=%s normals=%s probes=%s quality_passes=%s spacing_skips=%s failures=%s",
                    event.event_id,
                    len(selected_normals),
                    probes,
                    quality_passes,
                    spacing_skips,
                    dict(sorted(failures.items())),
                )
                event_activity.unpersist()
            except Exception as exc:
                statuses.append(
                    processing_status(
                        event,
                        stage=STATUS_NORMAL_SELECTION_FAILED,
                        selected=False,
                        exclusion_reason=f"insufficient_normal_windows:{type(exc).__name__}",
                    )
                )
                LOGGER.warning("Phase 3A event=%s normal selection failed: %s", event.event_id, exc)
            finally:
                event_exchange.unpersist()

    selected_count = len(selected_events)
    LOGGER.info("Phase 3A selected_event_count: %s", selected_count)
    if selected_count == 0:
        _write_status_summary(spark, statuses, f"{output_root}/summary/event_processing_status")
        raise RuntimeError("No events selected after normal-window quality/backfill.")
    if selected_count < args.min_events:
        _write_status_summary(spark, statuses, f"{output_root}/summary/event_processing_status")
        raise RuntimeError(
            f"selected_event_count below min-events: selected={selected_count}, min={args.min_events}"
        )

    balance_note = direction_balance_note(coverage_pass_events, selected_events)
    LOGGER.info("Phase 3A direction balance note: %s", balance_note)
    statuses = [
        status if not status.selected else _status_with_balance(status, balance_note)
        for status in statuses
    ]

    selected_event_ids = [event.event_id for event in selected_events]
    selected_events_df = _events_frame(
        spark,
        selected_events,
        run_id,
        created_at,
        lookback_seconds=args.lookback_seconds,
    ).cache()
    selected_normals_df = _selected_normals_frame(
        spark,
        selected_events,
        selected_normals_by_event,
        lookback_seconds=args.lookback_seconds,
        normal_count_per_event=args.normal_count_per_event,
        min_anchor_spacing_seconds=args.min_normal_anchor_spacing_seconds,
    ).cache()
    _log_df_metrics("normal_samples", selected_normals_df, include_count=True)

    with _phase3a_timer("normal_output_build"):
        normal_trade = phase2j._repartition_selected_output(
            phase2j._extract_multi_normal_trade_window(
                trade_day,
                selected_normals_df,
                run_id,
                created_at,
                selected_join_bucket_seconds=args.raw_day_bucket_seconds,
            ),
            args.selected_output_partitions,
            "phase3a_normal_trade_windows",
            "normal_sample_id",
            "exchange",
            "symbol",
        ).transform(_with_matched_event_id).cache()
        normal_bbo = phase2j._repartition_selected_output(
            phase2j._extract_multi_normal_bbo_window(
                bbo_day,
                selected_normals_df,
                run_id,
                created_at,
                selected_join_bucket_seconds=args.raw_day_bucket_seconds,
            ),
            args.selected_output_partitions,
            "phase3a_normal_bbo_windows",
            "normal_sample_id",
            "exchange",
            "symbol",
        ).transform(_with_matched_event_id).cache()
        normal_snapshot = phase2j._repartition_selected_output(
            phase2j._build_multi_normal_snapshot(
                phase2e,
                normal_trade.drop("matched_event_id"),
                normal_bbo.drop("matched_event_id"),
                selected_normals_df,
                config.exchanges,
                run_id,
                created_at,
            ),
            args.selected_output_partitions,
            "phase3a_normal_market_snapshots",
            "normal_sample_id",
            "exchange",
            "symbol",
        ).transform(_with_matched_event_id).cache()
        normal_exchange, normal_cross, normal_bucket = phase2j._build_multi_normal_profiles(
            phase2e,
            normal_snapshot.drop("matched_event_id"),
            config.exchanges,
            run_id,
            created_at,
        )
        normal_exchange = phase2j._repartition_selected_output(
            normal_exchange,
            args.selected_output_partitions,
            "phase3a_normal_exchange_profile",
            "normal_sample_id",
            "exchange",
            "symbol",
        ).transform(_with_matched_event_id).cache()
        normal_cross = phase2j._repartition_selected_output(
            normal_cross,
            args.selected_output_partitions,
            "phase3a_normal_cross_exchange_mid_diff",
            "normal_sample_id",
            "symbol_key",
        ).transform(_with_matched_event_id).cache()
        normal_bucket = phase2j._repartition_selected_output(
            normal_bucket,
            args.selected_output_partitions,
            "phase3a_normal_bucket_change_profile",
            "normal_sample_id",
            "exchange",
            "symbol",
        ).transform(_with_matched_event_id).cache()

    selected_event_snapshot = event_snapshot.where(F.col("event_id").isin(selected_event_ids)).cache()
    selected_event_exchange = event_exchange_all.where(F.col("event_id").isin(selected_event_ids)).cache()
    selected_event_cross = event_cross_all.where(F.col("event_id").isin(selected_event_ids)).cache()
    selected_event_bucket = event_bucket_all.where(F.col("event_id").isin(selected_event_ids)).cache()

    with _phase3a_timer("row_count_validation"):
        _validate_count(
            selected_event_snapshot,
            "event_market_snapshots",
            expected_event_snapshot_rows(selected_count, len(config.exchanges), args.lookback_seconds),
        )
        _validate_count(
            normal_snapshot,
            "normal_market_snapshots",
            expected_normal_snapshot_rows(
                selected_count,
                args.normal_count_per_event,
                len(config.exchanges),
                args.lookback_seconds,
            ),
        )
        _validate_count(selected_event_exchange, "event_profile.exchange_profile", expected_event_profile_rows(selected_count, 30))
        _validate_count(selected_event_cross, "event_profile.cross_exchange_mid_diff", expected_event_profile_rows(selected_count, 30))
        _validate_count(selected_event_bucket, "event_profile.bucket_change_profile", expected_event_profile_rows(selected_count, 48))
        _validate_count(normal_exchange, "normal_profile.exchange_profile", expected_normal_profile_rows(selected_count, args.normal_count_per_event, 30))
        _validate_count(normal_cross, "normal_profile.cross_exchange_mid_diff", expected_normal_profile_rows(selected_count, args.normal_count_per_event, 30))
        _validate_count(normal_bucket, "normal_profile.bucket_change_profile", expected_normal_profile_rows(selected_count, args.normal_count_per_event, 48))
    statuses = [
        _status_with_row_count(status, "validated") if status.selected else status
        for status in statuses
    ]

    with _phase3a_timer("comparison_build"):
        exchange_comparison = phase2j._coalesce_small_output(
            _union_all(
                [
                    phase2j._build_exchange_profile_distribution_comparison(
                        phase2f,
                        selected_event_exchange.where(F.col("event_id") == event_id),
                        normal_exchange.where(F.col("matched_event_id") == event_id).drop("matched_event_id"),
                        run_id,
                        run_id,
                        created_at,
                    )
                    for event_id in selected_event_ids
                ]
            ),
            args.small_output_partitions,
            "phase3a_exchange_profile_comparison",
        ).cache()
        cross_comparison = phase2j._coalesce_small_output(
            _union_all(
                [
                    phase2j._build_cross_exchange_distribution_comparison(
                        phase2f,
                        selected_event_cross.where(F.col("event_id") == event_id),
                        normal_cross.where(F.col("matched_event_id") == event_id).drop("matched_event_id"),
                        run_id,
                        run_id,
                        created_at,
                    )
                    for event_id in selected_event_ids
                ]
            ),
            args.small_output_partitions,
            "phase3a_cross_exchange_mid_diff_comparison",
        ).cache()
        bucket_comparison = phase2j._coalesce_small_output(
            _union_all(
                [
                    phase2j._build_bucket_change_distribution_comparison(
                        phase2f,
                        selected_event_bucket.where(F.col("event_id") == event_id),
                        normal_bucket.where(F.col("matched_event_id") == event_id).drop("matched_event_id"),
                        run_id,
                        run_id,
                        created_at,
                    )
                    for event_id in selected_event_ids
                ]
            ),
            args.small_output_partitions,
            "phase3a_bucket_change_profile_comparison",
        ).cache()

    with _phase3a_timer("comparison_validation"):
        _validate_count(exchange_comparison, "comparison.exchange_profile", expected_comparison_rows(selected_count, expected_exchange_profile_comparison_rows()))
        _validate_count(cross_comparison, "comparison.cross_exchange_mid_diff", expected_comparison_rows(selected_count, expected_cross_exchange_mid_diff_comparison_rows()))
        _validate_count(bucket_comparison, "comparison.bucket_change_profile", expected_comparison_rows(selected_count, expected_bucket_change_profile_comparison_rows()))
        for label, frame in (
            ("exchange_profile_comparison", exchange_comparison),
            ("cross_exchange_mid_diff_comparison", cross_comparison),
            ("bucket_change_profile_comparison", bucket_comparison),
        ):
            phase2j._validate_normal_sample_count(frame, label, args.normal_count_per_event)
            phase2j._validate_metric_group_coverage(frame, label)
            phase2j._validate_absolute_diff(frame, label)

    with _phase3a_timer("write_comparison_stage_outputs"):
        phase2e._write_and_validate(selected_events_df, f"{output_root}/events", "events", args.sample_size, validation_mode=args.validation_mode, row_count=selected_count)
        phase2e._write_and_validate(selected_event_snapshot, f"{output_root}/event_market_snapshots", "event_market_snapshots", args.sample_size, validation_mode=args.validation_mode)
        phase2e._write_and_validate(selected_event_exchange, f"{output_root}/event_profile_reports/exchange_profile", "event_profile_reports.exchange_profile", args.sample_size, validation_mode=args.validation_mode)
        phase2e._write_and_validate(selected_event_cross, f"{output_root}/event_profile_reports/cross_exchange_mid_diff", "event_profile_reports.cross_exchange_mid_diff", args.sample_size, validation_mode=args.validation_mode)
        phase2e._write_and_validate(selected_event_bucket, f"{output_root}/event_profile_reports/bucket_change_profile", "event_profile_reports.bucket_change_profile", args.sample_size, validation_mode=args.validation_mode)
        phase2e._write_and_validate(selected_normals_df, f"{output_root}/normal_samples", "normal_samples", args.sample_size, validation_mode=args.validation_mode)
        phase2e._write_and_validate(normal_snapshot, f"{output_root}/normal_market_snapshots", "normal_market_snapshots", args.sample_size, validation_mode=args.validation_mode)
        phase2e._write_and_validate(normal_exchange, f"{output_root}/normal_profile_reports/exchange_profile", "normal_profile_reports.exchange_profile", args.sample_size, validation_mode=args.validation_mode)
        phase2e._write_and_validate(normal_cross, f"{output_root}/normal_profile_reports/cross_exchange_mid_diff", "normal_profile_reports.cross_exchange_mid_diff", args.sample_size, validation_mode=args.validation_mode)
        phase2e._write_and_validate(normal_bucket, f"{output_root}/normal_profile_reports/bucket_change_profile", "normal_profile_reports.bucket_change_profile", args.sample_size, validation_mode=args.validation_mode)
        phase2e._write_and_validate(exchange_comparison, f"{output_root}/comparison_reports/exchange_profile_comparison", "comparison_reports.exchange_profile_comparison", args.sample_size, validation_mode=args.validation_mode)
        phase2e._write_and_validate(cross_comparison, f"{output_root}/comparison_reports/cross_exchange_mid_diff_comparison", "comparison_reports.cross_exchange_mid_diff_comparison", args.sample_size, validation_mode=args.validation_mode)
        phase2e._write_and_validate(bucket_comparison, f"{output_root}/comparison_reports/bucket_change_profile_comparison", "comparison_reports.bucket_change_profile_comparison", args.sample_size, validation_mode=args.validation_mode)
        if args.write_raw_windows:
            phase2e._write_and_validate(event_trade, f"{output_root}/raw_event_trade_windows", "raw_event_trade_windows", args.sample_size, validation_mode=args.validation_mode)
            phase2e._write_and_validate(event_bbo, f"{output_root}/raw_event_bbo_windows", "raw_event_bbo_windows", args.sample_size, validation_mode=args.validation_mode)
            phase2e._write_and_validate(normal_trade, f"{output_root}/raw_normal_trade_windows", "raw_normal_trade_windows", args.sample_size, validation_mode=args.validation_mode)
            phase2e._write_and_validate(normal_bbo, f"{output_root}/raw_normal_bbo_windows", "raw_normal_bbo_windows", args.sample_size, validation_mode=args.validation_mode)

    LOGGER.info(
        "[PHASE3A_OUTPUT] root=%s selected_event_count=%s stage=comparison_reports_complete",
        output_root,
        selected_count,
    )
    LOGGER.info("Phase 3A BTC multi-event trial comparison stage complete.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Phase 3A BTC multi-event trial.")
    parser.add_argument("--config", default="configs/dev.yaml")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--max-events", type=int, default=DEFAULT_MAX_EVENTS)
    parser.add_argument("--normal-count-per-event", type=int, default=DEFAULT_NORMAL_COUNT_PER_EVENT)
    parser.add_argument("--lookback-seconds", type=int, default=DEFAULT_LOOKBACK_SECONDS)
    parser.add_argument("--horizon-seconds", type=int, default=DEFAULT_HORIZON_SECONDS)
    parser.add_argument("--move-threshold-pct", type=float, default=DEFAULT_MOVE_THRESHOLD_PCT)
    parser.add_argument("--exclusion-seconds", type=int, default=DEFAULT_EXCLUSION_SECONDS)
    parser.add_argument("--min-normal-anchor-spacing-seconds", type=int, default=DEFAULT_EXCLUSION_SECONDS)
    parser.add_argument("--anchor-step-seconds", type=int, default=DEFAULT_ANCHOR_STEP_SECONDS)
    parser.add_argument("--validation-mode", choices=("strict", "light"), default="light")
    parser.add_argument("--write-raw-windows", type=_parse_bool, default=False)
    parser.add_argument("--sample-size", type=int, default=20)
    parser.add_argument("--directions", default="UP,DOWN")
    parser.add_argument("--min-events", type=int, default=DEFAULT_MIN_EVENTS)
    parser.add_argument("--allow-partial-coverage", type=_parse_bool, default=False)
    parser.add_argument("--max-quality-candidates", type=int, default=0)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    parser.add_argument("--raw-day-repartition-partitions", type=int, default=DEFAULT_RAW_DAY_REPARTITION_PARTITIONS)
    parser.add_argument("--raw-day-bucket-seconds", type=int, default=DEFAULT_RAW_DAY_BUCKET_SECONDS)
    parser.add_argument("--selected-output-partitions", type=int, default=DEFAULT_SELECTED_OUTPUT_PARTITIONS)
    parser.add_argument("--small-output-partitions", type=int, default=DEFAULT_SMALL_OUTPUT_PARTITIONS)
    return parser.parse_args()


def _select_partitions(
    partitions: Iterable[ManifestPartition],
    exchanges: tuple[str, ...],
    stream: str,
    start_date: str,
    end_date: str,
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
        and partition.date is not None
        and start_date <= partition.date <= end_date
    )
    if not selected:
        raise RuntimeError(f"No {stream} BTC partitions selected for {start_date}..{end_date}.")
    return selected


def _coverage_by_date(
    partitions: tuple[ManifestPartition, ...],
    expected_exchanges: tuple[str, ...],
) -> dict[str, tuple[str, ...]]:
    expected = {exchange.lower() for exchange in expected_exchanges}
    observed: dict[str, set[str]] = {}
    for partition in partitions:
        if partition.date is not None and partition.exchange is not None:
            exchange = partition.exchange.lower()
            if exchange in expected:
                observed.setdefault(partition.date, set()).add(exchange)
    return {date: tuple(sorted(exchanges)) for date, exchanges in observed.items()}


def _event_quality_reason(
    event: GroupedMoveEvent,
    *,
    trade_coverage: dict[str, tuple[str, ...]],
    bbo_coverage: dict[str, tuple[str, ...]],
    expected_exchanges: tuple[str, ...],
    lookback_seconds: int,
    allow_partial_coverage: bool,
) -> str | None:
    if event.is_ambiguous:
        return "ambiguous_event"
    required_dates = _pre_window_dates(event.event_start_ts, lookback_seconds)
    event_date = event.selected_date
    if not _has_coverage(trade_coverage.get(event_date), expected_exchanges, allow_partial_coverage):
        return "missing_trade_coverage"
    if not _has_coverage(bbo_coverage.get(event_date), expected_exchanges, allow_partial_coverage):
        return "missing_bbo_coverage"
    for date in required_dates:
        if not _has_coverage(trade_coverage.get(date), expected_exchanges, allow_partial_coverage):
            return "pre_window_partition_missing"
        if not _has_coverage(bbo_coverage.get(date), expected_exchanges, allow_partial_coverage):
            return "pre_window_partition_missing"
    return None


def _has_coverage(
    observed: tuple[str, ...] | None,
    expected_exchanges: tuple[str, ...],
    allow_partial_coverage: bool,
) -> bool:
    if observed is None:
        return False
    if allow_partial_coverage:
        return bool(observed)
    return set(observed) == {exchange.lower() for exchange in expected_exchanges}


def _pre_window_dates(event_start_ts: datetime, lookback_seconds: int) -> tuple[str, ...]:
    start = event_start_ts - timedelta(seconds=lookback_seconds)
    end = event_start_ts - timedelta(microseconds=1)
    dates = {start.strftime("%Y%m%d"), end.strftime("%Y%m%d")}
    return tuple(sorted(dates))


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


def _group_events_from_candidates(
    candidates: DataFrame,
    horizon_seconds: int,
    directions_arg: str,
) -> tuple[GroupedMoveEvent, ...]:
    allowed = {item.strip().upper() for item in directions_arg.split(",") if item.strip()}
    rows = candidates.orderBy("exchange", "symbol", "direction", "start_ts").collect()
    records = []
    for row in rows:
        direction = row["direction"]
        if direction not in allowed:
            continue
        records.append(
            RawMoveCandidate(
                exchange=row["exchange"],
                symbol=row["symbol"],
                direction=direction,
                start_ts=row["start_ts"],
                end_ts=row["end_ts"],
                duration_seconds=int(row["duration_seconds"]),
                base_price=float(row["base_price"]),
                touch_price=float(row["hit_price"]),
                move_pct=float(row["move_pct"]),
            )
        )
    return group_time_clustered_candidates(records, horizon_seconds=horizon_seconds)


def _event_processing_order(events: list[GroupedMoveEvent], max_events: int) -> tuple[GroupedMoveEvent, ...]:
    balanced = list(select_direction_balanced_events(events, max_events=max_events))
    selected_ids = {event.event_id for event in balanced}
    fallback = [event for event in sorted(events, key=lambda item: (item.event_start_ts, item.exchange, item.symbol, item.event_id)) if event.event_id not in selected_ids]
    return tuple(balanced + fallback)


def _events_frame(
    spark: Any,
    events: Iterable[GroupedMoveEvent],
    run_id: str,
    created_at: datetime,
    *,
    lookback_seconds: int,
) -> DataFrame:
    rows = [
        (
            event.event_id,
            event.event_rank,
            run_id,
            event.exchange,
            event.symbol,
            "trade",
            event.direction,
            event.event_start_ts,
            event.event_end_ts,
            event.duration_seconds,
            event.base_price,
            event.touch_price,
            event.move_pct,
            event.selected_date,
            3,
            event.raw_candidate_count,
            event.is_ambiguous,
            event.candidate_group_start_ts,
            event.candidate_group_end_ts,
            created_at,
        )
        for event in events
    ]
    return spark.createDataFrame(
        rows,
        (
            "event_id",
            "event_rank",
            "run_id",
            "exchange",
            "symbol",
            "source_stream",
            "direction",
            "event_start_ts",
            "event_end_ts",
            "duration_seconds",
            "base_price",
            "touch_price",
            "move_pct",
            "selected_date",
            "coverage_count",
            "raw_candidate_count",
            "is_ambiguous",
            "candidate_group_start_ts",
            "candidate_group_end_ts",
            "created_at",
        ),
    ).withColumn(
        "window_start_ts",
        F.expr(f"event_start_ts - interval {lookback_seconds} seconds"),
    ).withColumn(
        "lookback_seconds",
        F.lit(int(lookback_seconds)),
    )


def _selected_normals_frame(
    spark: Any,
    selected_events: list[GroupedMoveEvent],
    selected_normals_by_event: dict[str, tuple[Any, ...]],
    *,
    lookback_seconds: int,
    normal_count_per_event: int,
    min_anchor_spacing_seconds: int,
) -> DataFrame:
    rows = []
    for event in selected_events:
        for normal in selected_normals_by_event[event.event_id]:
            anchor = normal.normal_anchor_ts
            if anchor.tzinfo is not None:
                anchor = anchor.astimezone(timezone.utc).replace(tzinfo=None)
            rows.append(
                (
                    event.event_id,
                    "normal",
                    event.selected_date,
                    anchor - timedelta(seconds=lookback_seconds),
                    anchor,
                    anchor,
                    lookback_seconds,
                    "phase3a_activity_matched_multi_event",
                    0,
                    normal.nearest_candidate_distance_seconds,
                    f"{event.event_id}__{normal.normal_sample_id}",
                    normal.normal_sample_rank,
                    normal.activity_distance,
                    normal_count_per_event,
                    min_anchor_spacing_seconds,
                )
            )
    return spark.createDataFrame(
        rows,
        (
            "matched_event_id",
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


def _event_windows_with_time_buckets(events: DataFrame, *, bucket_seconds: int) -> DataFrame:
    return events.withColumn(
        "_phase3a_window_bucket",
        F.explode(
            F.sequence(
                F.floor(F.col("window_start_ts").cast("double") / F.lit(bucket_seconds)).cast("long"),
                F.floor((F.col("event_start_ts").cast("double") - F.lit(0.000001)) / F.lit(bucket_seconds)).cast("long"),
            )
        ),
    )


def _extract_event_trade_window(
    trade_day: DataFrame,
    events: DataFrame,
    run_id: str,
    created_at: datetime,
    *,
    selected_join_bucket_seconds: int,
) -> DataFrame:
    ts_event_expr = timestamp_expression(trade_day, "ts_event")
    ts_recv_expr = timestamp_expression(trade_day, "ts_recv")
    if ts_event_expr is None or ts_recv_expr is None:
        raise RuntimeError("Unable to convert trade timestamps.")
    trade_base = trade_day.withColumn("ts_event_ts", ts_event_expr).withColumn("ts_recv_ts", ts_recv_expr)
    if "_phase2j_raw_time_bucket" not in trade_base.columns:
        trade_base = trade_base.withColumn(
            "_phase2j_raw_time_bucket",
            F.floor(F.col("ts_event_ts").cast("double") / F.lit(selected_join_bucket_seconds)).cast("long"),
        )
    event_buckets = _event_windows_with_time_buckets(events, bucket_seconds=selected_join_bucket_seconds)
    enriched = (
        trade_base.alias("trade")
        .join(
            F.broadcast(event_buckets.alias("event")),
            (F.col("trade._phase2j_raw_time_bucket") == F.col("event._phase3a_window_bucket"))
            & (F.col("trade.ts_event_ts") >= F.col("event.window_start_ts"))
            & (F.col("trade.ts_event_ts") < F.col("event.event_start_ts")),
            "inner",
        )
        .withColumn("second_before_event", F.col("event.event_start_ts").cast("double") - F.col("trade.ts_event_ts").cast("double"))
    )
    return enriched.select(
        F.lit(run_id).alias("run_id"),
        F.col("event.event_id"),
        F.col("event.run_id").alias("source_event_run_id"),
        F.col("event.exchange").alias("event_exchange"),
        F.col("event.symbol").alias("event_symbol"),
        F.col("event.direction").alias("event_direction"),
        F.col("event.event_start_ts"),
        F.col("event.window_start_ts"),
        F.col("event.event_start_ts").alias("window_end_ts"),
        F.lit("trade_1s_60s_1pct_phase3a").alias("detection_version"),
        F.lit(True).alias("is_trial"),
        F.col("event.lookback_seconds").alias("lookback_seconds"),
        F.lower(F.col("trade.exchange")).alias("exchange"),
        F.col("trade.symbol"),
        F.col("trade.ts_event"),
        F.col("trade.ts_event_ts"),
        F.col("trade.ts_recv"),
        F.col("trade.ts_recv_ts"),
        _column_or_null_from_alias(trade_day, "trade", "seq").alias("seq"),
        F.col("trade.price"),
        F.col("trade.qty"),
        _column_or_null_from_alias(trade_day, "trade", "side").alias("side"),
        _column_or_null_from_alias(trade_day, "trade", "trade_id").alias("trade_id"),
        F.col("second_before_event"),
        F.lit(created_at).cast("timestamp").alias("created_at"),
    )


def _extract_event_bbo_window(
    bbo_day: DataFrame,
    events: DataFrame,
    run_id: str,
    created_at: datetime,
    *,
    selected_join_bucket_seconds: int,
) -> DataFrame:
    ts_event_expr = timestamp_expression(bbo_day, "ts_event")
    ts_recv_expr = timestamp_expression(bbo_day, "ts_recv")
    if ts_event_expr is None or ts_recv_expr is None:
        raise RuntimeError("Unable to convert BBO timestamps.")
    bbo_base = bbo_day.withColumn("ts_event_ts", ts_event_expr).withColumn("ts_recv_ts", ts_recv_expr)
    if "_phase2j_raw_time_bucket" not in bbo_base.columns:
        bbo_base = bbo_base.withColumn(
            "_phase2j_raw_time_bucket",
            F.floor(F.col("ts_event_ts").cast("double") / F.lit(selected_join_bucket_seconds)).cast("long"),
        )
    event_buckets = _event_windows_with_time_buckets(events, bucket_seconds=selected_join_bucket_seconds)
    enriched = (
        bbo_base.alias("bbo")
        .join(
            F.broadcast(event_buckets.alias("event")),
            (F.col("bbo._phase2j_raw_time_bucket") == F.col("event._phase3a_window_bucket"))
            & (F.col("bbo.ts_event_ts") >= F.col("event.window_start_ts"))
            & (F.col("bbo.ts_event_ts") < F.col("event.event_start_ts")),
            "inner",
        )
        .withColumn("bid_price_num", F.col("bbo.bid_price").cast("double"))
        .withColumn("bid_qty_num", F.col("bbo.bid_qty").cast("double"))
        .withColumn("ask_price_num", F.col("bbo.ask_price").cast("double"))
        .withColumn("ask_qty_num", F.col("bbo.ask_qty").cast("double"))
        .withColumn("mid_price", (F.col("bid_price_num") + F.col("ask_price_num")) / F.lit(2.0))
        .withColumn("spread", F.col("ask_price_num") - F.col("bid_price_num"))
        .withColumn("spread_bps", F.when(F.col("mid_price") != 0, F.col("spread") / F.col("mid_price") * F.lit(10000.0)))
        .withColumn("book_qty_sum", F.col("bid_qty_num") + F.col("ask_qty_num"))
        .withColumn("book_imbalance", F.when(F.col("book_qty_sum") != 0, (F.col("bid_qty_num") - F.col("ask_qty_num")) / F.col("book_qty_sum")))
        .withColumn("second_before_event", F.col("event.event_start_ts").cast("double") - F.col("bbo.ts_event_ts").cast("double"))
    )
    return enriched.select(
        F.lit(run_id).alias("run_id"),
        F.col("event.event_id"),
        F.col("event.run_id").alias("source_event_run_id"),
        F.col("event.exchange").alias("event_exchange"),
        F.col("event.symbol").alias("event_symbol"),
        F.col("event.direction").alias("event_direction"),
        F.col("event.event_start_ts"),
        F.col("event.window_start_ts"),
        F.col("event.event_start_ts").alias("window_end_ts"),
        F.lit("trade_1s_60s_1pct_phase3a").alias("detection_version"),
        F.lit(True).alias("is_trial"),
        F.col("event.lookback_seconds").alias("lookback_seconds"),
        F.lower(F.col("bbo.exchange")).alias("exchange"),
        F.col("bbo.symbol"),
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
        F.col("second_before_event"),
        F.lit(created_at).cast("timestamp").alias("created_at"),
    )


def _build_event_snapshot(
    phase2e: ModuleType,
    event_trade: DataFrame,
    event_bbo: DataFrame,
    events: DataFrame,
    expected_exchanges: tuple[str, ...],
    run_id: str,
    created_at: datetime,
) -> DataFrame:
    exchange_symbols = (
        event_trade.select("event_id", "exchange", "symbol")
        .unionByName(event_bbo.select("event_id", "exchange", "symbol"))
        .where(F.col("exchange").isin([exchange.lower() for exchange in expected_exchanges]))
        .distinct()
    )
    second_grid = (
        exchange_symbols.join(
            events.select(
                "event_id",
                "run_id",
                "exchange",
                "symbol",
                "direction",
                "event_start_ts",
                "window_start_ts",
                "lookback_seconds",
            ).withColumnRenamed("run_id", "source_event_run_id")
            .withColumnRenamed("exchange", "event_exchange")
            .withColumnRenamed("symbol", "event_symbol")
            .withColumnRenamed("direction", "event_direction"),
            "event_id",
            "inner",
        )
        .withColumn(
            "ts_second",
            F.explode(
                F.sequence(
                    F.col("window_start_ts").cast("timestamp"),
                    F.expr("event_start_ts - interval 1 second"),
                    F.expr("interval 1 second"),
                )
            ),
        )
        .withColumn("second_before_event", (F.col("event_start_ts").cast("long") - F.col("ts_second").cast("long")).cast("int"))
    )
    trade_snapshots = _build_event_trade_snapshots(event_trade)
    bbo_snapshots = _build_event_bbo_snapshots(phase2e, event_bbo)
    output = (
        second_grid.join(trade_snapshots, ["event_id", "exchange", "symbol", "ts_second"], "left")
        .join(bbo_snapshots, ["event_id", "exchange", "symbol", "ts_second"], "left")
        .transform(phase2e._coalesce_empty_trade_seconds)
        .transform(_coalesce_empty_event_bbo_seconds)
    )
    return output.select(
        F.lit(run_id).alias("run_id"),
        "source_event_run_id",
        "event_id",
        F.lit("trade_1s_60s_1pct_phase3a").alias("detection_version"),
        F.lit(True).alias("is_trial"),
        "event_exchange",
        "event_symbol",
        "event_direction",
        "event_start_ts",
        "window_start_ts",
        F.col("event_start_ts").alias("window_end_ts"),
        "lookback_seconds",
        "exchange",
        "symbol",
        "second_before_event",
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
        F.lit(created_at).cast("timestamp").alias("created_at"),
    )


def _build_event_trade_snapshots(event_trade: DataFrame) -> DataFrame:
    selected = event_trade.select(
        "event_id",
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
    ranked = selected.withColumn(
        "last_rank",
        F.row_number().over(
            Window.partitionBy("event_id", "exchange", "symbol", "ts_second").orderBy(
                F.col("ts_event_ts").desc(),
                F.col("seq").desc_nulls_last(),
                F.col("trade_id").desc_nulls_last(),
            )
        ),
    ).withColumn("notional", F.col("price") * F.col("qty"))
    buy = F.col("side_norm").isin("buy", "b")
    sell = F.col("side_norm").isin("sell", "s")
    return (
        ranked.groupBy("event_id", "exchange", "symbol", "ts_second")
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


def _build_event_bbo_snapshots(phase2e: ModuleType, event_bbo: DataFrame) -> DataFrame:
    ranked = event_bbo.withColumn("ts_second", F.date_trunc("second", F.col("ts_event_ts"))).withColumn(
        "last_rank",
        F.row_number().over(
            Window.partitionBy("event_id", "exchange", "symbol", "ts_second").orderBy(
                F.col("ts_event_ts").desc(),
                F.col("seq").desc_nulls_last(),
                F.col("ts_recv_ts").desc_nulls_last(),
            )
        ),
    )
    per_second = ranked.groupBy("event_id", "exchange", "symbol", "ts_second").agg(
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
        Window.partitionBy("event_id", "exchange", "symbol")
        .orderBy("ts_second")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return per_second.select(
        "event_id",
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


def _coalesce_empty_event_bbo_seconds(frame: DataFrame) -> DataFrame:
    with_count = frame.withColumn("bbo_update_count", F.coalesce(F.col("bbo_update_count"), F.lit(0)).cast("long"))
    fill_window = (
        Window.partitionBy("event_id", "exchange", "symbol")
        .orderBy("ts_second")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    with_filled = with_count
    for column in ("last_bbo_update_ts", "last_bid_price", "last_ask_price", "last_bid_qty", "last_ask_qty", "last_mid_price", "last_spread", "last_spread_bps", "last_book_imbalance"):
        with_filled = with_filled.withColumn(column, F.last(column, True).over(fill_window))
    return (
        with_filled.withColumn("has_bbo_update", F.col("bbo_update_count") > F.lit(0))
        .withColumn("is_bbo_forward_filled", (F.col("bbo_update_count") == F.lit(0)) & F.col("last_bbo_update_ts").isNotNull())
        .withColumn("bbo_quote_age_seconds", F.when(F.col("last_bbo_update_ts").isNotNull(), F.col("ts_second").cast("long") - F.col("last_bbo_update_ts").cast("long")))
    )


def _with_matched_event_id(frame: DataFrame) -> DataFrame:
    return frame.withColumn("matched_event_id", F.regexp_extract(F.col("normal_sample_id"), RAW_NORMAL_ID_RE, 1))


def _build_top_diffs(
    frame: DataFrame,
    *,
    run_id: str,
    top_n: int,
    created_at: datetime,
) -> DataFrame:
    return _union_all(
        [
            _rank_top_diffs(
                frame,
                run_id=run_id,
                rank_type=rank_type,
                primary_column=primary_column,
                ascending=ascending,
                top_n=top_n,
                created_at=created_at,
            )
            for rank_type, primary_column, ascending in PHASE3A_TOP_DIFF_RANKINGS
        ]
    )


def _rank_top_diffs(
    frame: DataFrame,
    *,
    run_id: str,
    rank_type: str,
    primary_column: str,
    ascending: bool,
    top_n: int,
    created_at: datetime,
) -> DataFrame:
    primary_order = F.asc_nulls_last(primary_column) if ascending else F.desc_nulls_last(primary_column)
    tie_breakers = [
        F.asc_nulls_last(column)
        for column in (
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
    ]
    rank_window = Window.partitionBy("source_event_id", "report_group", "metric_group").orderBy(primary_order, *tie_breakers)
    return (
        frame.withColumn("rank_type", F.lit(rank_type))
        .withColumn("rank", F.row_number().over(rank_window))
        .where(F.col("rank") <= F.lit(top_n))
        .select(
            F.lit(run_id).alias("run_id"),
            F.col("comparison_run_id").alias("source_comparison_run_id"),
            "source_event_id",
            "event_direction",
            "event_start_ts",
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
            "profile_version",
            F.lit(created_at).cast("timestamp").alias("created_at"),
        )
    )


def _validate_top_diff_count(source: DataFrame, top_diffs: DataFrame, label: str, top_n: int) -> None:
    counts = [
        row["count"]
        for row in source.groupBy("source_event_id", "report_group", "metric_group").count().collect()
    ]
    expected = expected_top_diff_rows_dynamic(counts, top_n)
    observed = top_diffs.count()
    LOGGER.info("%s expected vs observed rows: expected=%s observed=%s", label, expected, observed)
    if observed != expected:
        raise RuntimeError(f"{label} row count mismatch: expected={expected}, observed={observed}")


def _normal_selection_quality(selected_normals: DataFrame) -> DataFrame:
    return selected_normals.groupBy("matched_event_id").agg(
        F.count("*").alias("selected_normal_count"),
        F.avg("activity_distance").alias("avg_activity_distance"),
        F.max("activity_distance").alias("max_activity_distance"),
        F.min("nearest_candidate_distance_seconds").alias("min_nearest_candidate_distance_seconds"),
    )


def _metric_group_summary(*frames: DataFrame) -> DataFrame:
    return _union_all([frame.select("source_event_id", "report_group", "metric_group", "small_denominator_flag") for frame in frames]).groupBy(
        "source_event_id", "report_group", "metric_group"
    ).agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("small_denominator_flag"), 1).otherwise(0)).alias("small_denominator_count"),
    )


def _top_metrics_by_abs_z(
    exchange_comparison: DataFrame,
    cross_comparison: DataFrame,
    bucket_comparison: DataFrame,
    run_id: str,
    created_at: datetime,
    top_n: int,
) -> DataFrame:
    base = _union_all([exchange_comparison, cross_comparison, bucket_comparison])
    rank_window = Window.partitionBy("source_event_id", "report_group", "metric_group").orderBy(F.desc_nulls_last(F.abs(F.col("z_score_vs_normal"))), F.asc("metric_name"))
    return (
        base.where(~F.col("small_denominator_flag"))
        .withColumn("rank", F.row_number().over(rank_window))
        .where(F.col("rank") <= F.lit(top_n))
        .select(
            F.lit(run_id).alias("run_id"),
            "source_event_id",
            "event_direction",
            "report_group",
            "metric_group",
            "rank",
            "metric_name",
            "exchange",
            "exchange_pair",
            "window_label",
            "from_bucket",
            "to_bucket",
            "event_value",
            "normal_mean",
            "z_score_vs_normal",
            "normal_percentile_rank",
            F.lit(created_at).cast("timestamp").alias("created_at"),
        )
    )


def _top_metrics_by_percentile_extreme(
    exchange_comparison: DataFrame,
    cross_comparison: DataFrame,
    bucket_comparison: DataFrame,
    run_id: str,
    created_at: datetime,
    top_n: int,
) -> DataFrame:
    base = _union_all([exchange_comparison, cross_comparison, bucket_comparison])
    extremity = F.greatest(F.col("normal_percentile_rank"), F.lit(1.0) - F.col("normal_percentile_rank"))
    rank_window = Window.partitionBy("source_event_id", "report_group", "metric_group").orderBy(F.desc_nulls_last(extremity), F.asc("metric_name"))
    return (
        base.where(~F.col("small_denominator_flag"))
        .withColumn("percentile_extremity", extremity)
        .withColumn("rank", F.row_number().over(rank_window))
        .where(F.col("rank") <= F.lit(top_n))
        .select(
            F.lit(run_id).alias("run_id"),
            "source_event_id",
            "event_direction",
            "report_group",
            "metric_group",
            "rank",
            "metric_name",
            "exchange",
            "exchange_pair",
            "window_label",
            "from_bucket",
            "to_bucket",
            "event_value",
            "normal_mean",
            "z_score_vs_normal",
            "normal_percentile_rank",
            "percentile_extremity",
            F.lit(created_at).cast("timestamp").alias("created_at"),
        )
    )


def _status_frame(spark: Any, statuses: Iterable[Any], created_at: datetime) -> DataFrame:
    rows = [
        (
            status.event_id,
            status.event_rank,
            status.direction,
            status.processing_stage,
            status.selected,
            status.exclusion_reason,
            status.normal_selection_count,
            status.direction_balance_note,
            status.row_count_status,
            created_at,
        )
        for status in statuses
    ]
    schema = T.StructType(
        [
            T.StructField("event_id", T.StringType(), nullable=False),
            T.StructField("event_rank", T.IntegerType(), nullable=False),
            T.StructField("direction", T.StringType(), nullable=False),
            T.StructField("processing_stage", T.StringType(), nullable=False),
            T.StructField("selected", T.BooleanType(), nullable=False),
            T.StructField("exclusion_reason", T.StringType(), nullable=True),
            T.StructField("normal_selection_count", T.IntegerType(), nullable=True),
            T.StructField("direction_balance_note", T.StringType(), nullable=True),
            T.StructField("row_count_status", T.StringType(), nullable=True),
            T.StructField("created_at", T.TimestampType(), nullable=False),
        ]
    )
    return spark.createDataFrame(rows, schema)


def _status_with_balance(status: Any, note: str) -> Any:
    return processing_status(
        GroupedMoveEvent(
            event_id=status.event_id,
            event_rank=status.event_rank,
            exchange="",
            symbol="",
            direction=status.direction,
            event_start_ts=datetime(1970, 1, 1),
            event_end_ts=datetime(1970, 1, 1),
            duration_seconds=0,
            base_price=0.0,
            touch_price=0.0,
            move_pct=0.0,
            selected_date="19700101",
            raw_candidate_count=0,
            candidate_group_start_ts=datetime(1970, 1, 1),
            candidate_group_end_ts=datetime(1970, 1, 1),
        ),
        stage=status.processing_stage,
        selected=status.selected,
        exclusion_reason=status.exclusion_reason,
        normal_selection_count=status.normal_selection_count,
        direction_balance_note_value=note,
        row_count_status=status.row_count_status,
    )


def _status_with_row_count(status: Any, row_count_status: str) -> Any:
    return processing_status(
        GroupedMoveEvent(
            event_id=status.event_id,
            event_rank=status.event_rank,
            exchange="",
            symbol="",
            direction=status.direction,
            event_start_ts=datetime(1970, 1, 1),
            event_end_ts=datetime(1970, 1, 1),
            duration_seconds=0,
            base_price=0.0,
            touch_price=0.0,
            move_pct=0.0,
            selected_date="19700101",
            raw_candidate_count=0,
            candidate_group_start_ts=datetime(1970, 1, 1),
            candidate_group_end_ts=datetime(1970, 1, 1),
        ),
        stage=status.processing_stage,
        selected=status.selected,
        exclusion_reason=status.exclusion_reason,
        normal_selection_count=status.normal_selection_count,
        direction_balance_note_value=status.direction_balance_note,
        row_count_status=row_count_status,
    )


def _write_status_summary(spark: Any, statuses: Iterable[Any], path: str) -> None:
    frame = _status_frame(spark, statuses, datetime.now(timezone.utc).replace(tzinfo=None))
    frame.write.mode("errorifexists").parquet(path)


def _validate_count(frame: DataFrame, label: str, expected: int) -> int:
    observed = frame.count()
    LOGGER.info("%s expected vs observed rows: expected=%s observed=%s", label, expected, observed)
    if observed != expected:
        raise RuntimeError(f"{label} row count mismatch: expected={expected}, observed={observed}")
    return observed


def _union_all(frames: list[DataFrame]) -> DataFrame:
    if not frames:
        raise ValueError("At least one DataFrame is required.")
    output = frames[0]
    for frame in frames[1:]:
        output = output.unionByName(frame)
    return output


def _column_or_null_from_alias(frame: DataFrame, alias: str, column: str) -> Any:
    if column in frame.columns:
        return F.col(f"{alias}.{column}")
    return F.lit(None)


def _validate_required_trade_columns(frame: DataFrame) -> None:
    missing = validate_required_columns(frame, REQUIRED_TRADE_MOVE_COLUMNS)
    if missing:
        raise RuntimeError(f"Missing required trade columns: {', '.join(missing)}")


def _validate_required_bbo_columns(frame: DataFrame) -> None:
    required = ("ts_event", "ts_recv", "exchange", "symbol", "bid_price", "bid_qty", "ask_price", "ask_qty")
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise RuntimeError(f"Missing required BBO columns: {', '.join(missing)}")


def _log_partition_coverage(
    label: str,
    expected_exchanges: tuple[str, ...],
    partitions: tuple[ManifestPartition, ...],
) -> None:
    by_date = _coverage_by_date(partitions, expected_exchanges)
    for date, observed in sorted(by_date.items()):
        coverage = exchange_coverage(expected_exchanges, observed)
        LOGGER.info(
            "%s date=%s coverage: expected=%s observed=%s missing=%s",
            label,
            date,
            ",".join(coverage.expected),
            ",".join(coverage.observed),
            ",".join(coverage.missing) or "none",
        )


def _log_spark_runtime_context(spark: Any, run_id: str) -> None:
    context = spark.sparkContext
    LOGGER.info("[PHASE3A_CONF] key=run_id value=%s", run_id)
    LOGGER.info("[PHASE3A_CONF] key=spark.app.id value=%s", context.applicationId)
    LOGGER.info("[PHASE3A_CONF] key=spark.app.name value=%s", context.appName)
    LOGGER.info("[PHASE3A_CONF] key=spark.master value=%s", context.master)


def _partition_count(frame: DataFrame) -> int | str:
    try:
        return frame.rdd.getNumPartitions()
    except Exception as exc:
        return f"unavailable:{type(exc).__name__}"


def _log_df_metrics(
    name: str,
    frame: DataFrame,
    *,
    include_count: bool = False,
    exchange: bool = False,
    symbol: bool = False,
) -> int | None:
    rows = frame.count() if include_count else None
    LOGGER.info(
        "[PHASE3A_DF] name=%s rows=%s partitions=%s schema=%s",
        name,
        rows if rows is not None else "not_counted",
        _partition_count(frame),
        ",".join(frame.columns),
    )
    if exchange and "exchange" in frame.columns:
        values = [row["exchange"] for row in frame.select(F.lower(F.col("exchange")).alias("exchange")).distinct().collect()]
        LOGGER.info("[PHASE3A_DF] name=%s exchanges=%s", name, ",".join(sorted(values)))
    if symbol and "symbol" in frame.columns:
        values = [row["symbol"] for row in frame.select("symbol").distinct().collect()]
        LOGGER.info("[PHASE3A_DF] name=%s symbols=%s", name, ",".join(sorted(str(value) for value in values)))
    return rows


def _parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"Expected boolean value, got {value!r}.")


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
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def _load_job_module(module_name: str, filename: str) -> ModuleType:
    job_file = _script_path()
    if job_file is None:
        raise RuntimeError("Unable to resolve current job path.")
    path = job_file.with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load job module from {path}.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


if __name__ == "__main__":
    main()
