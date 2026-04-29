#!/usr/bin/env python
"""Phase 3A-1 BTC coverage and event-count probe."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
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

from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from quantlab_event_scanner.btc_event_coverage_probe import (  # noqa: E402
    DEFAULT_PROBE_END_DATE,
    DEFAULT_PROBE_HORIZON_SECONDS,
    DEFAULT_PROBE_LOOKBACK_SECONDS,
    DEFAULT_PROBE_MOVE_THRESHOLD_PCT,
    DEFAULT_PROBE_START_DATE,
    DEFAULT_SYMBOL_FAMILY,
    CoverageByDate,
    EventQualityPreview,
    MissingDateDiagnostic,
    ProbeGroupedEvent,
    ProbeRawCandidate,
    RecommendedScope,
    btc_manifest_dates,
    build_coverage_diagnostics,
    build_event_quality_preview,
    default_phase3a_probe_run_id,
    group_probe_candidates,
    recommend_phase3a_scope,
    requested_dates,
    resolve_probe_date_range,
    select_probe_partitions,
)
from quantlab_event_scanner.config import load_config  # noqa: E402
from quantlab_event_scanner.manifest import ManifestPartition  # noqa: E402
from quantlab_event_scanner.manifest import load_manifest_from_s3_with_spark  # noqa: E402
from quantlab_event_scanner.paths import btc_event_coverage_probe_run_path  # noqa: E402
from quantlab_event_scanner.probe import partition_paths  # noqa: E402
from quantlab_event_scanner.trade_move_scan import (  # noqa: E402
    REQUIRED_TRADE_MOVE_COLUMNS,
    build_price_1s,
    scan_move_candidates,
    validate_required_columns,
)


LOGGER = logging.getLogger("quantlab_event_scanner.phase3a_btc_event_coverage_probe")


@contextmanager
def _probe_timer(step: str) -> Any:
    start = time.monotonic()
    try:
        yield
    finally:
        LOGGER.info("[PHASE3A_PROBE_TIMER] step=%s seconds=%.3f", step, time.monotonic() - start)


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config_path = _resolve_path(args.config)
    config = load_config(config_path)
    run_id = args.run_id or default_phase3a_probe_run_id(datetime.now(timezone.utc))
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    output_root = btc_event_coverage_probe_run_path(config, run_id)
    exchange_values = _parse_csv(args.exchanges)
    stream_values = ("trade", "bbo")

    LOGGER.info("Using config: %s", config_path)
    LOGGER.info("Run ID: %s", run_id)
    LOGGER.info("Output root: %s", output_root)
    for key, value in sorted(vars(args).items()):
        LOGGER.info("[PHASE3A_PROBE_CONF] key=%s value=%s", key, value)
    LOGGER.info("[PHASE3A_PROBE_CONF] key=run_id value=%s", run_id)

    spark = _get_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    _log_spark_runtime_context(spark, run_id)

    with _probe_timer("manifest_load"):
        manifest = load_manifest_from_s3_with_spark(spark, config.manifest_path)
        available_btc_dates = btc_manifest_dates(
            manifest.partitions,
            exchanges=exchange_values,
            streams=stream_values,
            symbol_family=args.symbol_family,
        )
        resolved_start_date, resolved_end_date = resolve_probe_date_range(
            args.start_date,
            args.end_date,
            available_btc_dates=available_btc_dates,
        )
        date_values = requested_dates(resolved_start_date, resolved_end_date)
        LOGGER.info(
            "Phase 3A-1 resolved date range: requested_start=%s requested_end=%s "
            "resolved_start=%s resolved_end=%s btc_manifest_min=%s btc_manifest_max=%s "
            "btc_manifest_date_count=%s",
            args.start_date,
            args.end_date,
            resolved_start_date,
            resolved_end_date,
            available_btc_dates[0] if available_btc_dates else None,
            available_btc_dates[-1] if available_btc_dates else None,
            len(available_btc_dates),
        )
        btc_partitions = select_probe_partitions(
            manifest.partitions,
            requested_date_values=date_values,
            exchanges=exchange_values,
            streams=stream_values,
            symbol_family=args.symbol_family,
        )
        trade_partitions = _stream_partitions(btc_partitions, "trade")
        bbo_partitions = _stream_partitions(btc_partitions, "bbo")
        coverage, missing_dates = build_coverage_diagnostics(
            manifest.partitions,
            btc_partitions,
            requested_date_values=date_values,
            exchanges=exchange_values,
        )

    coverage_df = _coverage_frame(spark, coverage, run_id, created_at)
    missing_dates_df = _missing_dates_frame(spark, missing_dates, run_id, created_at)
    event_scan_possible = bool(trade_partitions)
    LOGGER.info(
        "Phase 3A-1 coverage summary: requested_date_count=%s available_date_count=%s "
        "missing_date_count=%s full_trade_bbo_coverage_date_count=%s",
        len(date_values),
        sum(1 for row in coverage if row.is_present_in_btc_manifest),
        len(missing_dates),
        sum(1 for row in coverage if row.has_full_trade_bbo_coverage),
    )
    LOGGER.info(
        "Phase 3A-1 manifest path planning: btc_trade_partitions=%s btc_bbo_partitions=%s",
        len(trade_partitions),
        len(bbo_partitions),
    )

    candidates = _empty_frame(spark, _candidate_schema())
    ambiguous = _empty_frame(spark, _ambiguous_schema())
    raw_candidate_count = 0
    ambiguous_candidate_count = 0

    if trade_partitions:
        with _probe_timer("candidate_scan_timestamp_continuous"):
            trade_frame = _read_partitions(spark, trade_partitions, config.input_root, "BTC trade")
            _validate_required_trade_columns(trade_frame)
            price_1s = build_price_1s(trade_frame)
            if price_1s is None:
                raise RuntimeError("Unable to build 1s price rows for BTC event-count probe.")
            price_1s = price_1s.cache()
            _log_df_metrics("price_1s", price_1s, include_count=True, exchange=True, symbol=True)
            candidates, ambiguous = scan_move_candidates(
                price_1s,
                threshold_pct=args.move_threshold_pct,
                lookahead_seconds=args.horizon_seconds,
            )
            candidates = candidates.cache()
            ambiguous = ambiguous.cache()
            raw_candidate_count = _log_df_metrics("raw_candidates", candidates, include_count=True) or 0
            ambiguous_candidate_count = (
                _log_df_metrics("ambiguous_candidates", ambiguous, include_count=True) or 0
            )
    else:
        LOGGER.warning("No BTC trade partitions available; candidate scan skipped.")

    with _probe_timer("group_and_summarize"):
        probe_candidates = _collect_probe_candidates(candidates, ambiguous)
        grouped_events = group_probe_candidates(probe_candidates, horizon_seconds=args.horizon_seconds)
        quality_previews = build_event_quality_preview(
            grouped_events,
            coverage,
            lookback_seconds=args.lookback_seconds,
        )
        recommended_scope = recommend_phase3a_scope(
            requested_start_date=resolved_start_date,
            requested_end_date=resolved_end_date,
            coverage_by_date=coverage,
            grouped_event_count=len(grouped_events),
            quality_previews=quality_previews,
            event_scan_possible=event_scan_possible,
            default_max_events=args.recommended_max_events,
            default_min_events=args.recommended_min_events,
        )

        raw_candidate_summary = _raw_candidate_summary_frame(
            spark,
            candidates,
            ambiguous,
            run_id,
            created_at,
            raw_candidate_count=raw_candidate_count,
            ambiguous_candidate_count=ambiguous_candidate_count,
        )
        grouped_event_summary = _grouped_event_frame(spark, grouped_events, run_id, created_at)
        event_quality_preview = _quality_preview_frame(spark, quality_previews, run_id, created_at)
        direction_distribution = _direction_distribution_frame(
            spark,
            candidates,
            grouped_events,
            quality_previews,
            run_id,
            created_at,
        )
        recommended_scope_df = _recommended_scope_frame(spark, recommended_scope, run_id, created_at)

    up_count = sum(1 for row in quality_previews if row.quality_status == "quality_pass_preview" and row.direction == "UP")
    down_count = sum(1 for row in quality_previews if row.quality_status == "quality_pass_preview" and row.direction == "DOWN")
    quality_pass_preview_count = sum(1 for row in quality_previews if row.quality_status == "quality_pass_preview")
    LOGGER.info(
        "Phase 3A-1 event summary: raw_candidate_count=%s ambiguous_candidate_count=%s "
        "grouped_event_count=%s quality_pass_preview_count=%s up_count=%s down_count=%s "
        "recommended_scope_status=%s",
        raw_candidate_count,
        ambiguous_candidate_count,
        len(grouped_events),
        quality_pass_preview_count,
        up_count,
        down_count,
        recommended_scope.recommended_scope_status,
    )
    LOGGER.info("Phase 3A-1 recommended scope: %s", recommended_scope)

    with _probe_timer("write_outputs"):
        _write_and_validate(
            coverage_df,
            f"{output_root}/coverage_by_date",
            "coverage_by_date",
            args.sample_size,
            validation_mode=args.validation_mode,
            row_count=len(coverage),
        )
        _write_and_validate(
            missing_dates_df,
            f"{output_root}/missing_dates",
            "missing_dates",
            args.sample_size,
            validation_mode=args.validation_mode,
            row_count=len(missing_dates),
        )
        _write_and_validate(
            raw_candidate_summary,
            f"{output_root}/raw_candidate_summary",
            "raw_candidate_summary",
            args.sample_size,
            validation_mode=args.validation_mode,
        )
        _write_and_validate(
            grouped_event_summary,
            f"{output_root}/grouped_event_summary",
            "grouped_event_summary",
            args.sample_size,
            validation_mode=args.validation_mode,
            row_count=len(grouped_events),
        )
        _write_and_validate(
            event_quality_preview,
            f"{output_root}/event_quality_preview",
            "event_quality_preview",
            args.sample_size,
            validation_mode=args.validation_mode,
            row_count=len(quality_previews),
        )
        _write_and_validate(
            direction_distribution,
            f"{output_root}/direction_distribution",
            "direction_distribution",
            args.sample_size,
            validation_mode=args.validation_mode,
        )
        _write_and_validate(
            recommended_scope_df,
            f"{output_root}/recommended_phase3a_scope",
            "recommended_phase3a_scope",
            args.sample_size,
            validation_mode=args.validation_mode,
            row_count=1,
        )

    LOGGER.info(
        "[PHASE3A_PROBE_OUTPUT] root=%s recommended_scope_status=%s "
        "quality_pass_preview_count=%s",
        output_root,
        recommended_scope.recommended_scope_status,
        quality_pass_preview_count,
    )
    LOGGER.info("Phase 3A-1 BTC coverage/event-count probe complete.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe Phase 3A BTC coverage and event counts.")
    parser.add_argument("--config", default="configs/dev.yaml")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--start-date", default=DEFAULT_PROBE_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_PROBE_END_DATE)
    parser.add_argument("--symbol-family", default=DEFAULT_SYMBOL_FAMILY)
    parser.add_argument("--exchanges", default="binance,bybit,okx")
    parser.add_argument("--horizon-seconds", type=int, default=DEFAULT_PROBE_HORIZON_SECONDS)
    parser.add_argument("--move-threshold-pct", type=float, default=DEFAULT_PROBE_MOVE_THRESHOLD_PCT)
    parser.add_argument("--lookback-seconds", type=int, default=DEFAULT_PROBE_LOOKBACK_SECONDS)
    parser.add_argument("--allow-partial-coverage", type=_parse_bool, default=False)
    parser.add_argument("--validation-mode", choices=("strict", "light"), default="light")
    parser.add_argument("--sample-size", type=int, default=20)
    parser.add_argument("--recommended-max-events", type=int, default=10)
    parser.add_argument("--recommended-min-events", type=int, default=2)
    return parser.parse_args()


def _stream_partitions(
    partitions: Iterable[ManifestPartition],
    stream: str,
) -> tuple[ManifestPartition, ...]:
    return tuple(
        partition
        for partition in partitions
        if partition.stream is not None and partition.stream.lower() == stream
    )


def _read_partitions(
    spark: Any,
    partitions: tuple[ManifestPartition, ...],
    input_root: str,
    label: str,
) -> DataFrame:
    paths = partition_paths(partitions, input_root=input_root)
    LOGGER.info("%s selected parquet path count: %s", label, len(paths))
    for path in paths[:50]:
        LOGGER.info("  %s", path)
    return spark.read.parquet(*paths)


def _collect_probe_candidates(candidates: DataFrame, ambiguous: DataFrame) -> tuple[ProbeRawCandidate, ...]:
    records: list[ProbeRawCandidate] = []
    for row in candidates.orderBy("exchange", "symbol", "direction", "start_ts").collect():
        records.append(
            ProbeRawCandidate(
                exchange=row["exchange"],
                symbol=row["symbol"],
                direction=row["direction"],
                start_ts=row["start_ts"],
                end_ts=row["end_ts"],
                duration_seconds=int(row["duration_seconds"]),
                base_price=float(row["base_price"]),
                touch_price=float(row["hit_price"]),
                move_pct=float(row["move_pct"]),
                is_ambiguous=False,
            )
        )
    for row in ambiguous.orderBy("exchange", "symbol", "start_ts").collect():
        records.append(
            ProbeRawCandidate(
                exchange=row["exchange"],
                symbol=row["symbol"],
                direction="AMBIGUOUS",
                start_ts=row["start_ts"],
                end_ts=row["end_ts"],
                duration_seconds=int(row["duration_seconds"]),
                base_price=float(row["base_price"]),
                touch_price=None,
                move_pct=None,
                is_ambiguous=True,
            )
        )
    return tuple(records)


def _coverage_frame(
    spark: Any,
    rows: Iterable[CoverageByDate],
    run_id: str,
    created_at: datetime,
) -> DataFrame:
    values = [
        (
            run_id,
            row.requested_date,
            row.is_present_in_manifest,
            row.is_present_in_btc_manifest,
            row.has_binance_trade,
            row.has_bybit_trade,
            row.has_okx_trade,
            row.trade_coverage_count,
            row.has_binance_bbo,
            row.has_bybit_bbo,
            row.has_okx_bbo,
            row.bbo_coverage_count,
            row.has_full_trade_coverage,
            row.has_full_bbo_coverage,
            row.has_full_trade_bbo_coverage,
            row.event_scan_possible,
            row.phase3a_quality_possible,
            created_at,
        )
        for row in rows
    ]
    return spark.createDataFrame(values, _coverage_schema())


def _missing_dates_frame(
    spark: Any,
    rows: Iterable[MissingDateDiagnostic],
    run_id: str,
    created_at: datetime,
) -> DataFrame:
    values = [
        (
            run_id,
            row.requested_date,
            row.is_present_in_manifest,
            row.is_present_in_btc_manifest,
            ";".join(row.missing_reasons),
            created_at,
        )
        for row in rows
    ]
    return spark.createDataFrame(values, _missing_dates_schema())


def _raw_candidate_summary_frame(
    spark: Any,
    candidates: DataFrame,
    ambiguous: DataFrame,
    run_id: str,
    created_at: datetime,
    *,
    raw_candidate_count: int,
    ambiguous_candidate_count: int,
) -> DataFrame:
    raw = (
        candidates.select(
            F.date_format("start_ts", "yyyyMMdd").alias("selected_date"),
            F.lower("exchange").alias("exchange"),
            F.col("direction").alias("direction"),
            F.lit(1).cast("long").alias("raw_candidate_count"),
            F.lit(0).cast("long").alias("ambiguous_candidate_count"),
        )
        if candidates.columns
        else _empty_frame(spark, _candidate_summary_base_schema())
    )
    ambiguous_rows = (
        ambiguous.select(
            F.date_format("start_ts", "yyyyMMdd").alias("selected_date"),
            F.lower("exchange").alias("exchange"),
            F.lit("AMBIGUOUS").alias("direction"),
            F.lit(0).cast("long").alias("raw_candidate_count"),
            F.lit(1).cast("long").alias("ambiguous_candidate_count"),
        )
        if ambiguous.columns
        else _empty_frame(spark, _candidate_summary_base_schema())
    )
    detail = (
        raw.unionByName(ambiguous_rows)
        .groupBy("selected_date", "exchange", "direction")
        .agg(
            F.sum("raw_candidate_count").cast("long").alias("raw_candidate_count"),
            F.sum("ambiguous_candidate_count").cast("long").alias("ambiguous_candidate_count"),
        )
        .select(
            F.lit(run_id).alias("run_id"),
            F.lit("date_exchange_direction").alias("summary_level"),
            "selected_date",
            "exchange",
            "direction",
            "raw_candidate_count",
            "ambiguous_candidate_count",
            F.lit(created_at).cast("timestamp").alias("created_at"),
        )
    )
    total = spark.createDataFrame(
        [
            (
                run_id,
                "total",
                None,
                None,
                None,
                int(raw_candidate_count),
                int(ambiguous_candidate_count),
                created_at,
            )
        ],
        _raw_candidate_summary_schema(),
    )
    return detail.unionByName(total)


def _grouped_event_frame(
    spark: Any,
    rows: Iterable[ProbeGroupedEvent],
    run_id: str,
    created_at: datetime,
) -> DataFrame:
    values = [
        (
            run_id,
            row.event_id,
            row.event_rank,
            row.exchange,
            row.symbol,
            row.direction,
            row.event_start_ts,
            row.event_end_ts,
            row.duration_seconds,
            row.move_pct,
            row.selected_date,
            row.raw_candidate_count,
            row.is_ambiguous,
            row.group_start_ts,
            row.group_end_ts,
            row.base_price,
            row.touch_price,
            created_at,
        )
        for row in rows
    ]
    return spark.createDataFrame(values, _grouped_event_schema())


def _quality_preview_frame(
    spark: Any,
    rows: Iterable[EventQualityPreview],
    run_id: str,
    created_at: datetime,
) -> DataFrame:
    values = [
        (
            run_id,
            row.event_id,
            row.direction,
            row.selected_date,
            row.has_full_trade_coverage,
            row.has_full_bbo_coverage,
            row.trade_coverage_count,
            row.bbo_coverage_count,
            row.pre_window_start_ts,
            row.pre_window_end_ts,
            ",".join(row.pre_window_partition_dates),
            row.pre_window_partitions_available,
            row.quality_status,
            row.quality_reason,
            created_at,
        )
        for row in rows
    ]
    return spark.createDataFrame(values, _quality_preview_schema())


def _direction_distribution_frame(
    spark: Any,
    candidates: DataFrame,
    grouped_events: Iterable[ProbeGroupedEvent],
    quality_previews: Iterable[EventQualityPreview],
    run_id: str,
    created_at: datetime,
) -> DataFrame:
    raw_counts = {
        (row["selected_date"], row["direction"]): int(row["count"])
        for row in candidates.select(
            F.date_format("start_ts", "yyyyMMdd").alias("selected_date"),
            "direction",
        )
        .groupBy("selected_date", "direction")
        .count()
        .collect()
    }
    grouped_counts: dict[tuple[str, str], int] = {}
    for event in grouped_events:
        key = (event.selected_date, event.direction)
        grouped_counts[key] = grouped_counts.get(key, 0) + 1
    quality_counts: dict[tuple[str, str], int] = {}
    for preview in quality_previews:
        if preview.quality_status != "quality_pass_preview":
            continue
        key = (preview.selected_date, preview.direction)
        quality_counts[key] = quality_counts.get(key, 0) + 1

    keys = sorted(set(raw_counts) | set(grouped_counts) | set(quality_counts))
    values = [
        (
            run_id,
            date,
            direction,
            raw_counts.get((date, direction), 0),
            grouped_counts.get((date, direction), 0),
            quality_counts.get((date, direction), 0),
            created_at,
        )
        for date, direction in keys
    ]
    return spark.createDataFrame(values, _direction_distribution_schema())


def _recommended_scope_frame(
    spark: Any,
    scope: RecommendedScope,
    run_id: str,
    created_at: datetime,
) -> DataFrame:
    values = [
        (
            run_id,
            scope.requested_start_date,
            scope.requested_end_date,
            ",".join(scope.available_dates),
            ",".join(scope.full_trade_bbo_coverage_dates),
            scope.grouped_event_count,
            scope.quality_pass_preview_count,
            scope.up_count,
            scope.down_count,
            scope.recommended_start_date,
            scope.recommended_end_date,
            scope.recommended_max_events,
            scope.recommended_min_events,
            scope.direction_balance_possible,
            scope.recommended_scope_status,
            scope.notes,
            created_at,
        )
    ]
    return spark.createDataFrame(values, _recommended_scope_schema())


def _write_and_validate(
    frame: DataFrame,
    path: str,
    label: str,
    sample_size: int,
    *,
    validation_mode: str,
    row_count: int | None = None,
) -> int:
    if validation_mode not in {"strict", "light"}:
        raise ValueError("validation_mode must be 'strict' or 'light'.")
    output_count = frame.count() if row_count is None else row_count
    LOGGER.info(
        "[PHASE3A_PROBE_OUTPUT] name=%s path=%s rows=%s validation_mode=%s",
        label,
        path,
        output_count,
        validation_mode,
    )
    LOGGER.info("%s output schema:", label)
    frame.printSchema()
    LOGGER.info("Writing %s: %s", label, path)
    frame.write.mode("errorifexists").parquet(path)
    if validation_mode == "light":
        LOGGER.info("%s light validation complete.", label)
        return output_count
    readback = frame.sparkSession.read.parquet(path)
    readback_count = readback.count()
    LOGGER.info("%s readback count: %s", label, readback_count)
    if readback_count != output_count:
        raise RuntimeError(
            f"{label} readback count mismatch: expected={output_count}, actual={readback_count}"
        )
    _show_df(f"{label} readback sample:", readback, sample_size)
    return output_count


def _coverage_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), nullable=False),
            T.StructField("requested_date", T.StringType(), nullable=False),
            T.StructField("is_present_in_manifest", T.BooleanType(), nullable=False),
            T.StructField("is_present_in_btc_manifest", T.BooleanType(), nullable=False),
            T.StructField("has_binance_trade", T.BooleanType(), nullable=False),
            T.StructField("has_bybit_trade", T.BooleanType(), nullable=False),
            T.StructField("has_okx_trade", T.BooleanType(), nullable=False),
            T.StructField("trade_coverage_count", T.IntegerType(), nullable=False),
            T.StructField("has_binance_bbo", T.BooleanType(), nullable=False),
            T.StructField("has_bybit_bbo", T.BooleanType(), nullable=False),
            T.StructField("has_okx_bbo", T.BooleanType(), nullable=False),
            T.StructField("bbo_coverage_count", T.IntegerType(), nullable=False),
            T.StructField("has_full_trade_coverage", T.BooleanType(), nullable=False),
            T.StructField("has_full_bbo_coverage", T.BooleanType(), nullable=False),
            T.StructField("has_full_trade_bbo_coverage", T.BooleanType(), nullable=False),
            T.StructField("event_scan_possible", T.BooleanType(), nullable=False),
            T.StructField("phase3a_quality_possible", T.BooleanType(), nullable=False),
            T.StructField("created_at", T.TimestampType(), nullable=False),
        ]
    )


def _missing_dates_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), nullable=False),
            T.StructField("requested_date", T.StringType(), nullable=False),
            T.StructField("is_present_in_manifest", T.BooleanType(), nullable=False),
            T.StructField("is_present_in_btc_manifest", T.BooleanType(), nullable=False),
            T.StructField("missing_reasons", T.StringType(), nullable=False),
            T.StructField("created_at", T.TimestampType(), nullable=False),
        ]
    )


def _candidate_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("exchange", T.StringType(), nullable=True),
            T.StructField("symbol", T.StringType(), nullable=True),
            T.StructField("direction", T.StringType(), nullable=True),
            T.StructField("start_ts", T.TimestampType(), nullable=True),
            T.StructField("end_ts", T.TimestampType(), nullable=True),
            T.StructField("duration_seconds", T.LongType(), nullable=True),
            T.StructField("move_pct", T.DoubleType(), nullable=True),
            T.StructField("base_price", T.DoubleType(), nullable=True),
            T.StructField("hit_price", T.DoubleType(), nullable=True),
        ]
    )


def _ambiguous_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("exchange", T.StringType(), nullable=True),
            T.StructField("symbol", T.StringType(), nullable=True),
            T.StructField("direction", T.StringType(), nullable=True),
            T.StructField("start_ts", T.TimestampType(), nullable=True),
            T.StructField("end_ts", T.TimestampType(), nullable=True),
            T.StructField("duration_seconds", T.LongType(), nullable=True),
            T.StructField("base_price", T.DoubleType(), nullable=True),
        ]
    )


def _candidate_summary_base_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("selected_date", T.StringType(), nullable=True),
            T.StructField("exchange", T.StringType(), nullable=True),
            T.StructField("direction", T.StringType(), nullable=True),
            T.StructField("raw_candidate_count", T.LongType(), nullable=False),
            T.StructField("ambiguous_candidate_count", T.LongType(), nullable=False),
        ]
    )


def _raw_candidate_summary_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), nullable=False),
            T.StructField("summary_level", T.StringType(), nullable=False),
            T.StructField("selected_date", T.StringType(), nullable=True),
            T.StructField("exchange", T.StringType(), nullable=True),
            T.StructField("direction", T.StringType(), nullable=True),
            T.StructField("raw_candidate_count", T.LongType(), nullable=False),
            T.StructField("ambiguous_candidate_count", T.LongType(), nullable=False),
            T.StructField("created_at", T.TimestampType(), nullable=False),
        ]
    )


def _grouped_event_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), nullable=False),
            T.StructField("event_id", T.StringType(), nullable=False),
            T.StructField("event_rank", T.IntegerType(), nullable=False),
            T.StructField("exchange", T.StringType(), nullable=False),
            T.StructField("symbol", T.StringType(), nullable=False),
            T.StructField("direction", T.StringType(), nullable=False),
            T.StructField("event_start_ts", T.TimestampType(), nullable=False),
            T.StructField("event_end_ts", T.TimestampType(), nullable=False),
            T.StructField("duration_seconds", T.IntegerType(), nullable=False),
            T.StructField("move_pct", T.DoubleType(), nullable=True),
            T.StructField("selected_date", T.StringType(), nullable=False),
            T.StructField("raw_candidate_count", T.IntegerType(), nullable=False),
            T.StructField("is_ambiguous", T.BooleanType(), nullable=False),
            T.StructField("group_start_ts", T.TimestampType(), nullable=False),
            T.StructField("group_end_ts", T.TimestampType(), nullable=False),
            T.StructField("base_price", T.DoubleType(), nullable=False),
            T.StructField("touch_price", T.DoubleType(), nullable=True),
            T.StructField("created_at", T.TimestampType(), nullable=False),
        ]
    )


def _quality_preview_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), nullable=False),
            T.StructField("event_id", T.StringType(), nullable=False),
            T.StructField("direction", T.StringType(), nullable=False),
            T.StructField("selected_date", T.StringType(), nullable=False),
            T.StructField("has_full_trade_coverage", T.BooleanType(), nullable=False),
            T.StructField("has_full_bbo_coverage", T.BooleanType(), nullable=False),
            T.StructField("trade_coverage_count", T.IntegerType(), nullable=False),
            T.StructField("bbo_coverage_count", T.IntegerType(), nullable=False),
            T.StructField("pre_window_start_ts", T.TimestampType(), nullable=False),
            T.StructField("pre_window_end_ts", T.TimestampType(), nullable=False),
            T.StructField("pre_window_partition_dates", T.StringType(), nullable=False),
            T.StructField("pre_window_partitions_available", T.BooleanType(), nullable=False),
            T.StructField("quality_status", T.StringType(), nullable=False),
            T.StructField("quality_reason", T.StringType(), nullable=False),
            T.StructField("created_at", T.TimestampType(), nullable=False),
        ]
    )


def _direction_distribution_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), nullable=False),
            T.StructField("date", T.StringType(), nullable=False),
            T.StructField("direction", T.StringType(), nullable=False),
            T.StructField("raw_candidate_count", T.IntegerType(), nullable=False),
            T.StructField("grouped_event_count", T.IntegerType(), nullable=False),
            T.StructField("quality_pass_preview_count", T.IntegerType(), nullable=False),
            T.StructField("created_at", T.TimestampType(), nullable=False),
        ]
    )


def _recommended_scope_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), nullable=False),
            T.StructField("requested_start_date", T.StringType(), nullable=False),
            T.StructField("requested_end_date", T.StringType(), nullable=False),
            T.StructField("available_dates", T.StringType(), nullable=False),
            T.StructField("full_trade_bbo_coverage_dates", T.StringType(), nullable=False),
            T.StructField("grouped_event_count", T.IntegerType(), nullable=False),
            T.StructField("quality_pass_preview_count", T.IntegerType(), nullable=False),
            T.StructField("up_count", T.IntegerType(), nullable=False),
            T.StructField("down_count", T.IntegerType(), nullable=False),
            T.StructField("recommended_start_date", T.StringType(), nullable=True),
            T.StructField("recommended_end_date", T.StringType(), nullable=True),
            T.StructField("recommended_max_events", T.IntegerType(), nullable=False),
            T.StructField("recommended_min_events", T.IntegerType(), nullable=False),
            T.StructField("direction_balance_possible", T.BooleanType(), nullable=False),
            T.StructField("recommended_scope_status", T.StringType(), nullable=False),
            T.StructField("notes", T.StringType(), nullable=False),
            T.StructField("created_at", T.TimestampType(), nullable=False),
        ]
    )


def _empty_frame(spark: Any, schema: T.StructType) -> DataFrame:
    return spark.createDataFrame([], schema)


def _validate_required_trade_columns(frame: DataFrame) -> None:
    missing = validate_required_columns(frame, REQUIRED_TRADE_MOVE_COLUMNS)
    if missing:
        raise RuntimeError(f"Missing required trade columns: {', '.join(missing)}")


def _log_spark_runtime_context(spark: Any, run_id: str) -> None:
    context = spark.sparkContext
    LOGGER.info("[PHASE3A_PROBE_CONF] key=run_id value=%s", run_id)
    LOGGER.info("[PHASE3A_PROBE_CONF] key=spark.app.id value=%s", context.applicationId)
    LOGGER.info("[PHASE3A_PROBE_CONF] key=spark.app.name value=%s", context.appName)
    LOGGER.info("[PHASE3A_PROBE_CONF] key=spark.master value=%s", context.master)


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
        "[PHASE3A_PROBE_DF] name=%s rows=%s partitions=%s schema=%s",
        name,
        rows if rows is not None else "not_counted",
        _partition_count(frame),
        ",".join(frame.columns),
    )
    if exchange and "exchange" in frame.columns:
        values = [
            row["exchange"]
            for row in frame.select(F.lower(F.col("exchange")).alias("exchange")).distinct().collect()
        ]
        LOGGER.info("[PHASE3A_PROBE_DF] name=%s exchanges=%s", name, ",".join(sorted(values)))
    if symbol and "symbol" in frame.columns:
        values = [row["symbol"] for row in frame.select("symbol").distinct().collect()]
        LOGGER.info(
            "[PHASE3A_PROBE_DF] name=%s symbols=%s",
            name,
            ",".join(sorted(str(value) for value in values)),
        )
    return rows


def _show_df(label: str, frame: DataFrame, rows: int = 20) -> None:
    LOGGER.info("%s", label)
    sample = frame.take(rows)
    if not sample:
        LOGGER.info("  <empty>")
        return
    for row in sample:
        LOGGER.info("  %s", row.asDict(recursive=True))


def _parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"Expected boolean value, got {value!r}.")


def _parse_csv(value: str) -> tuple[str, ...]:
    parsed = tuple(item.strip().lower() for item in value.split(",") if item.strip())
    if not parsed:
        raise argparse.ArgumentTypeError("CSV value must contain at least one item.")
    return parsed


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


if __name__ == "__main__":
    main()
