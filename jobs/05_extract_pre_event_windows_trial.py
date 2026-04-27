#!/usr/bin/env python
"""Phase 2A trial pre-event window extraction."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
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

from quantlab_event_scanner.config import load_config  # noqa: E402
from quantlab_event_scanner.manifest import ManifestPartition  # noqa: E402
from quantlab_event_scanner.manifest import load_manifest_from_s3_with_spark  # noqa: E402
from quantlab_event_scanner.paths import (  # noqa: E402
    events_map_trial_run_path,
    pre_event_windows_trial_run_path,
)
from quantlab_event_scanner.pre_event_windows import (  # noqa: E402
    default_phase2a_run_id,
    utc_partition_dates_for_window,
    validate_selected_event_count,
)
from quantlab_event_scanner.probe import partition_paths  # noqa: E402
from quantlab_event_scanner.trade_move_scan import (  # noqa: E402
    timestamp_expression,
    validate_required_columns,
)


LOGGER = logging.getLogger("quantlab_event_scanner.phase2a_pre_event_windows")

DEFAULT_SOURCE_EVENT_RUN_ID = "phase1d_20260427T063442Z"
DEFAULT_EVENT_ID = "binance_btcusdt_20260423_down_001"
DEFAULT_LOOKBACK_SECONDS = 300
DEFAULT_STREAMS = "trade"
REQUIRED_COLUMNS = ("ts_event", "ts_recv", "exchange", "symbol", "price", "qty")
OUTPUT_COLUMNS = (
    "run_id",
    "source_event_run_id",
    "event_id",
    "detection_version",
    "is_trial",
    "event_exchange",
    "event_symbol",
    "event_direction",
    "event_start_ts",
    "window_start_ts",
    "window_end_ts",
    "lookback_seconds",
    "exchange",
    "symbol",
    "source_stream",
    "source_partition_date",
    "ts_event",
    "ts_event_ts",
    "ts_recv",
    "ts_recv_ts",
    "seq",
    "price",
    "qty",
    "side",
    "trade_id",
    "seconds_before_event",
    "created_at",
)


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config_path = _resolve_path(args.config)
    config = load_config(config_path)
    run_id = args.run_id or default_phase2a_run_id(datetime.now(timezone.utc))
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    streams = _parse_csv(args.streams)
    source_event_path = events_map_trial_run_path(config, args.source_event_run_id)
    output_path = pre_event_windows_trial_run_path(config, run_id)

    LOGGER.info("Using config: %s", config_path)
    LOGGER.info("Source event run ID: %s", args.source_event_run_id)
    LOGGER.info("Event ID: %s", args.event_id)
    LOGGER.info("Lookback seconds: %s", args.lookback_seconds)
    LOGGER.info("Streams: %s", ", ".join(streams))
    LOGGER.info("Source event path: %s", source_event_path)
    LOGGER.info("Output path: %s", output_path)

    spark = _get_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    LOGGER.info("Spark timezone: %s", spark.conf.get("spark.sql.session.timeZone"))

    events = spark.read.parquet(source_event_path)
    selected_events = events.where(F.col("event_id") == args.event_id)
    selected_event_count = selected_events.count()
    validate_selected_event_count(selected_event_count)
    event = selected_events.first()
    LOGGER.info("Selected event: %s", event.asDict(recursive=True))

    event_start_ts = event["event_start_ts"]
    window_end_ts = event_start_ts
    window_start_ts = event_start_ts - timedelta(seconds=args.lookback_seconds)
    partition_dates = utc_partition_dates_for_window(window_start_ts, window_end_ts)
    LOGGER.info("Window start ts: %s", window_start_ts)
    LOGGER.info("Window end ts: %s", window_end_ts)
    LOGGER.info("Derived partition dates: %s", ", ".join(partition_dates))

    manifest = load_manifest_from_s3_with_spark(spark, config.manifest_path)
    selected_partitions = _select_window_partitions(
        manifest.partitions,
        exchanges=config.exchanges,
        streams=streams,
        partition_dates=partition_dates,
    )
    if not selected_partitions:
        raise RuntimeError(
            "No manifest partitions matched derived window dates, streams, exchanges, and BTC symbols."
        )

    window_frame = _read_window_partitions(
        spark,
        selected_partitions,
        input_root=config.input_root,
        path_log_limit=args.path_log_limit,
    )
    missing = validate_required_columns(window_frame, REQUIRED_COLUMNS)
    if missing:
        raise RuntimeError(f"Missing required columns: {', '.join(missing)}")

    ts_event_expr = timestamp_expression(window_frame, "ts_event")
    ts_recv_expr = timestamp_expression(window_frame, "ts_recv")
    if ts_event_expr is None or ts_recv_expr is None:
        raise RuntimeError("Unable to convert ts_event or ts_recv to timestamp.")

    output = _extract_window(
        window_frame,
        ts_event_expr=ts_event_expr,
        ts_recv_expr=ts_recv_expr,
        run_id=run_id,
        source_event_run_id=args.source_event_run_id,
        event=event,
        window_start_ts=window_start_ts,
        window_end_ts=window_end_ts,
        lookback_seconds=args.lookback_seconds,
        created_at=created_at,
    )
    output_count = output.count()
    LOGGER.info("Extracted pre-event row count: %s", output_count)
    if output_count == 0:
        raise RuntimeError("Extracted pre-event window row count is 0.")

    _show_df("Extracted count by exchange:", output.groupBy("exchange").count().orderBy("exchange"))
    _show_df(
        "Extracted count by exchange + symbol:",
        output.groupBy("exchange", "symbol").count().orderBy("exchange", "symbol"),
    )
    _show_df(
        "Extracted ts_event_ts range:",
        output.agg(F.min("ts_event_ts").alias("min_ts_event_ts"), F.max("ts_event_ts").alias("max_ts_event_ts")),
    )
    _show_df(
        "Extracted seconds_before_event range:",
        output.agg(
            F.min("seconds_before_event").alias("min_seconds_before_event"),
            F.max("seconds_before_event").alias("max_seconds_before_event"),
        ),
    )

    LOGGER.info("Writing pre-event windows trial output: %s", output_path)
    output.write.mode("errorifexists").parquet(output_path)

    readback = spark.read.parquet(output_path)
    LOGGER.info("Readback schema:")
    readback.printSchema()
    readback_count = readback.count()
    LOGGER.info("Readback count: %s", readback_count)
    if readback_count != output_count:
        raise RuntimeError(
            f"Readback count mismatch: expected={output_count}, actual={readback_count}"
        )
    _show_df(
        "Readback sample:",
        readback.orderBy("exchange", "ts_event_ts"),
        args.sample_size,
    )

    LOGGER.info("Phase 2A trial pre-event window extraction complete.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract Phase 2A trial pre-event windows.")
    parser.add_argument("--config", default="configs/dev.yaml")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--source-event-run-id", default=DEFAULT_SOURCE_EVENT_RUN_ID)
    parser.add_argument("--event-id", default=DEFAULT_EVENT_ID)
    parser.add_argument("--lookback-seconds", type=int, default=DEFAULT_LOOKBACK_SECONDS)
    parser.add_argument("--streams", default=DEFAULT_STREAMS)
    parser.add_argument("--sample-size", type=int, default=20)
    parser.add_argument("--path-log-limit", type=int, default=20)
    return parser.parse_args()


def _select_window_partitions(
    partitions: Iterable[ManifestPartition],
    exchanges: tuple[str, ...],
    streams: tuple[str, ...],
    partition_dates: tuple[str, ...],
) -> tuple[ManifestPartition, ...]:
    exchange_set = {exchange.lower() for exchange in exchanges}
    stream_set = set(streams)
    date_set = set(partition_dates)
    return tuple(
        partition
        for partition in partitions
        if partition.available is not False
        and partition.exchange is not None
        and partition.exchange.lower() in exchange_set
        and partition.stream in stream_set
        and partition.symbol is not None
        and "btc" in partition.symbol.lower()
        and partition.date in date_set
    )


def _read_window_partitions(
    spark: Any,
    partitions: tuple[ManifestPartition, ...],
    input_root: str,
    path_log_limit: int,
) -> DataFrame:
    frames: list[DataFrame] = []
    paths_logged = 0
    dates = tuple(sorted({partition.date for partition in partitions if partition.date is not None}))
    for date in dates:
        date_partitions = tuple(partition for partition in partitions if partition.date == date)
        paths = partition_paths(date_partitions, input_root=input_root)
        LOGGER.info("Selected parquet path count for date=%s: %s", date, len(paths))
        for path in paths[: max(0, path_log_limit - paths_logged)]:
            LOGGER.info("  %s", path)
            paths_logged += 1
        frames.append(spark.read.parquet(*paths).withColumn("source_partition_date", F.lit(date)))

    if not frames:
        raise RuntimeError("No parquet paths were produced for selected partitions.")

    frame = frames[0]
    for next_frame in frames[1:]:
        frame = frame.unionByName(next_frame, allowMissingColumns=True)
    return frame


def _extract_window(
    frame: DataFrame,
    ts_event_expr: Any,
    ts_recv_expr: Any,
    run_id: str,
    source_event_run_id: str,
    event: Any,
    window_start_ts: datetime,
    window_end_ts: datetime,
    lookback_seconds: int,
    created_at: datetime,
) -> DataFrame:
    event_start_ts = event["event_start_ts"]
    enriched = (
        frame.withColumn("ts_event_ts", ts_event_expr)
        .withColumn("ts_recv_ts", ts_recv_expr)
        .where(F.col("ts_event_ts") >= F.lit(window_start_ts).cast("timestamp"))
        .where(F.col("ts_event_ts") < F.lit(window_end_ts).cast("timestamp"))
        .withColumn(
            "seconds_before_event",
            F.lit(event_start_ts).cast("timestamp").cast("double") - F.col("ts_event_ts").cast("double"),
        )
    )
    return enriched.select(
        F.lit(run_id).alias("run_id"),
        F.lit(source_event_run_id).alias("source_event_run_id"),
        F.lit(event["event_id"]).alias("event_id"),
        F.lit(event["detection_version"]).alias("detection_version"),
        F.lit(True).alias("is_trial"),
        F.lit(event["exchange"]).alias("event_exchange"),
        F.lit(event["symbol"]).alias("event_symbol"),
        F.lit(event["direction"]).alias("event_direction"),
        F.lit(event_start_ts).cast("timestamp").alias("event_start_ts"),
        F.lit(window_start_ts).cast("timestamp").alias("window_start_ts"),
        F.lit(window_end_ts).cast("timestamp").alias("window_end_ts"),
        F.lit(lookback_seconds).alias("lookback_seconds"),
        F.lower(F.col("exchange")).alias("exchange"),
        F.col("symbol"),
        _column_or_literal(enriched, "stream", "trade").alias("source_stream"),
        F.col("source_partition_date"),
        F.col("ts_event"),
        F.col("ts_event_ts"),
        F.col("ts_recv"),
        F.col("ts_recv_ts"),
        _column_or_null(enriched, "seq").alias("seq"),
        F.col("price"),
        F.col("qty"),
        _column_or_null(enriched, "side").alias("side"),
        _column_or_null(enriched, "trade_id").alias("trade_id"),
        F.col("seconds_before_event"),
        F.lit(created_at).cast("timestamp").alias("created_at"),
    ).select(*OUTPUT_COLUMNS)


def _column_or_null(frame: DataFrame, column: str) -> Any:
    if column in frame.columns:
        return F.col(column)
    return F.lit(None)


def _column_or_literal(frame: DataFrame, column: str, fallback: str) -> Any:
    if column in frame.columns:
        return F.coalesce(F.col(column), F.lit(fallback))
    return F.lit(fallback)


def _show_df(label: str, frame: DataFrame, rows: int = 20) -> None:
    LOGGER.info("%s", label)
    sample = frame.take(rows)
    if not sample:
        LOGGER.info("  <empty>")
        return
    for row in sample:
        LOGGER.info("  %s", row.asDict(recursive=True))


def _parse_csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


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
