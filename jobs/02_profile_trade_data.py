#!/usr/bin/env python
"""Phase 1B BTC trade data profiling.

This job reads coverage-aware BTC trade parquet partitions, logs profiling
summaries, and writes no outputs.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any


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

from quantlab_event_scanner.config import load_config  # noqa: E402
from quantlab_event_scanner.manifest import load_manifest_from_s3_with_spark  # noqa: E402
from quantlab_event_scanner.probe import partition_paths  # noqa: E402
from quantlab_event_scanner.profiling import (  # noqa: E402
    CoverageRow,
    build_coverage_table,
    filter_btc_trade_partitions,
    select_coverage_aware_date,
)


LOGGER = logging.getLogger("quantlab_event_scanner.phase1_trade_profile")

KNOWN_TRADE_COLUMNS = (
    "ts_event",
    "seq",
    "ts_recv",
    "exchange",
    "symbol",
    "stream",
    "stream_version",
    "price",
    "qty",
    "side",
    "trade_id",
)

SAMPLE_COLUMNS = (
    "ts_event",
    "ts_recv",
    "seq",
    "exchange",
    "symbol",
    "price",
    "qty",
    "side",
    "trade_id",
)


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config_path = _resolve_path(args.config)
    config = load_config(config_path)
    LOGGER.info("Using config: %s", config_path)
    LOGGER.info("Input root: %s", config.input_root)
    LOGGER.info("Manifest path: %s", config.manifest_path)
    LOGGER.info("Configured exchanges: %s", ", ".join(config.exchanges))
    LOGGER.info("Configured streams: %s", ", ".join(config.streams))
    LOGGER.info("Output root, not used in Phase 1B: %s", config.output_root)
    LOGGER.info(
        "binance_open_interest_supported: %s",
        config.binance_open_interest_supported,
    )

    spark = _get_spark_session()
    manifest = load_manifest_from_s3_with_spark(spark, config.manifest_path)
    LOGGER.info("Loaded manifest: %s", config.manifest_path)
    LOGGER.info("Manifest partition count: %s", len(manifest.partitions))

    btc_trade_partitions = filter_btc_trade_partitions(manifest.partitions, config.exchanges)
    coverage_rows = build_coverage_table(btc_trade_partitions, config.exchanges)
    _log_coverage_table(coverage_rows, config.exchanges, latest_rows=args.coverage_rows)

    selection = select_coverage_aware_date(manifest.partitions, config.exchanges)
    if selection.selected_date is None or selection.coverage_row is None:
        LOGGER.warning("No BTC trade date has at least 2/%s exchange coverage.", len(config.exchanges))
        LOGGER.warning("Controlled exit: no parquet files will be read and no outputs will be written.")
        return

    selected = selection.partitions
    selected_exchanges = _unique_sorted(partition.exchange for partition in selected)
    selected_symbols = _unique_sorted(partition.symbol for partition in selected)
    _log_selected_coverage(selection.coverage_row, selected_exchanges)

    try:
        paths = partition_paths(selected, input_root=config.input_root)
    except ValueError as exc:
        LOGGER.warning("Controlled exit: %s", exc)
        LOGGER.warning("No parquet files will be read and no outputs will be written.")
        return

    if not paths:
        LOGGER.warning("Controlled exit: selected date produced no parquet paths.")
        return

    LOGGER.info("Selected BTC trade partition count: %s", len(selected))
    LOGGER.info("Selected parquet path count: %s", len(paths))
    LOGGER.info("Selected exchanges: %s", ", ".join(selected_exchanges))
    LOGGER.info("Selected symbols: %s", ", ".join(selected_symbols))
    LOGGER.info("First %s selected parquet paths:", min(args.path_log_limit, len(paths)))
    for path in paths[: args.path_log_limit]:
        LOGGER.info("  %s", path)

    frame = spark.read.parquet(*paths)
    _log_schema_and_sample(frame, args.sample_size)

    total_count = frame.count()
    LOGGER.info("Total row count: %s", total_count)
    if total_count == 0:
        LOGGER.warning("Selected parquet files contain zero rows. Profiling stops here.")
        return

    _log_count_profile(frame)
    _log_timestamp_profile(frame)
    _log_price_qty_profile(frame)
    _log_null_summary(frame)
    _log_side_distribution(frame)
    _log_duplicate_checks(frame)
    _log_sequence_time_ordering(frame)
    _log_per_second_density_and_gaps(frame)

    LOGGER.info("Phase 1B profiling complete. No S3 output was written.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile coverage-aware BTC trade parquet data.")
    parser.add_argument("--config", default="configs/dev.yaml")
    parser.add_argument("--sample-size", type=int, default=20)
    parser.add_argument("--coverage-rows", type=int, default=10)
    parser.add_argument("--path-log-limit", type=int, default=20)
    return parser.parse_args()


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


def _log_coverage_table(
    rows: tuple[CoverageRow, ...],
    exchanges: tuple[str, ...],
    latest_rows: int,
) -> None:
    LOGGER.info("BTC trade coverage table row count: %s", len(rows))
    if not rows:
        LOGGER.warning("No available BTC trade partitions found for configured exchanges.")
        return

    LOGGER.info("Latest %s BTC trade coverage rows:", min(latest_rows, len(rows)))
    for row in sorted(rows, key=lambda item: item.date, reverse=True)[:latest_rows]:
        flags = ", ".join(
            f"has_{exchange.lower()}={'yes' if row.has_exchange(exchange) else 'no'}"
            for exchange in exchanges
        )
        LOGGER.info(
            "date=%s, %s, coverage=%s/%s, missing_exchanges=%s",
            row.date,
            flags,
            row.coverage_count,
            len(exchanges),
            ", ".join(row.missing_exchanges) or "none",
        )


def _log_selected_coverage(row: CoverageRow, selected_exchanges: tuple[str, ...]) -> None:
    LOGGER.info("Coverage-aware selected_date: %s", row.date)
    LOGGER.info("Selected coverage count: %s", row.coverage_count)
    LOGGER.info("Selected exchanges: %s", ", ".join(selected_exchanges))
    LOGGER.info("Missing exchanges: %s", ", ".join(row.missing_exchanges) or "none")


def _log_schema_and_sample(frame: DataFrame, sample_size: int) -> None:
    LOGGER.info("Parquet schema:")
    frame.printSchema()

    available = _available_columns(frame, SAMPLE_COLUMNS)
    missing = _missing_columns(frame, SAMPLE_COLUMNS)
    if missing:
        LOGGER.warning("Missing sample columns: %s", ", ".join(missing))
        LOGGER.info("Available columns: %s", ", ".join(frame.columns))
    if not available:
        LOGGER.warning("No requested sample columns are available; showing all columns.")
        frame.limit(sample_size).show(truncate=False)
        return

    LOGGER.info("Sample rows, limit=%s, columns=%s:", sample_size, ", ".join(available))
    frame.select(*available).limit(sample_size).show(truncate=False)


def _log_count_profile(frame: DataFrame) -> None:
    required = ("exchange", "symbol")
    if not _has_columns(frame, required):
        LOGGER.warning("Skipping count profile; missing columns: %s", _missing_text(frame, required))
        return

    _show_df("Row count by exchange:", frame.groupBy("exchange").count().orderBy("exchange"))
    _show_df(
        "Row count by exchange + symbol:",
        frame.groupBy("exchange", "symbol").count().orderBy("exchange", "symbol"),
    )
    distincts = frame.agg(
        F.countDistinct("exchange").alias("distinct_exchange_count"),
        F.countDistinct("symbol").alias("distinct_symbol_count"),
    ).first()
    LOGGER.info("Distinct exchange count: %s", distincts["distinct_exchange_count"])
    LOGGER.info("Distinct symbol count: %s", distincts["distinct_symbol_count"])


def _log_timestamp_profile(frame: DataFrame) -> None:
    columns = ("ts_event", "ts_recv")
    if not _has_columns(frame, columns):
        LOGGER.warning("Skipping timestamp profile; missing columns: %s", _missing_text(frame, columns))
        return

    ranges = frame.agg(
        F.min("ts_event").alias("min_ts_event"),
        F.max("ts_event").alias("max_ts_event"),
        F.min("ts_recv").alias("min_ts_recv"),
        F.max("ts_recv").alias("max_ts_recv"),
    ).first()
    LOGGER.info("min(ts_event): %s", ranges["min_ts_event"])
    LOGGER.info("max(ts_event): %s", ranges["max_ts_event"])
    LOGGER.info("min(ts_recv): %s", ranges["min_ts_recv"])
    LOGGER.info("max(ts_recv): %s", ranges["max_ts_recv"])

    latency_ms = _latency_millis_expression(frame)
    if latency_ms is None:
        LOGGER.warning("Skipping latency stats; ts_event/ts_recv types are not supported.")
        return

    LOGGER.info(
        "Latency interpretation: historical collector latency only; "
        "not future runtime latency."
    )
    stats = frame.select(latency_ms.alias("latency_ms")).agg(
        F.min("latency_ms").alias("min"),
        F.max("latency_ms").alias("max"),
        F.avg("latency_ms").alias("avg"),
        F.percentile_approx("latency_ms", 0.50, 10000).alias("p50"),
        F.percentile_approx("latency_ms", 0.95, 10000).alias("p95"),
        F.percentile_approx("latency_ms", 0.99, 10000).alias("p99"),
    ).first()
    LOGGER.info(
        "Latency stats ms: min=%s, max=%s, avg=%s, p50=%s, p95=%s, p99=%s",
        stats["min"],
        stats["max"],
        stats["avg"],
        stats["p50"],
        stats["p95"],
        stats["p99"],
    )


def _log_price_qty_profile(frame: DataFrame) -> None:
    for column in ("price", "qty"):
        if column not in frame.columns:
            LOGGER.warning("Skipping %s profile; column missing.", column)
            continue
        stats = frame.agg(
            F.min(column).alias("min"),
            F.max(column).alias("max"),
            F.avg(column).alias("avg"),
            F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias("null_count"),
            F.sum(F.when(F.col(column) <= 0, 1).otherwise(0)).alias("non_positive_count"),
        ).first()
        LOGGER.info(
            "%s profile: min=%s, max=%s, avg=%s, null_count=%s, %s<=0_count=%s",
            column,
            stats["min"],
            stats["max"],
            stats["avg"],
            stats["null_count"],
            column,
            stats["non_positive_count"],
        )

        if "exchange" in frame.columns:
            _show_df(
                f"{column} min/max by exchange:",
                frame.groupBy("exchange").agg(
                    F.min(column).alias(f"{column}_min"),
                    F.max(column).alias(f"{column}_max"),
                ),
            )


def _log_null_summary(frame: DataFrame) -> None:
    existing = [column for column in KNOWN_TRADE_COLUMNS if column in frame.columns]
    missing = [column for column in KNOWN_TRADE_COLUMNS if column not in frame.columns]
    for column in missing:
        LOGGER.warning("Null summary: %s is missing.", column)
    if not existing:
        LOGGER.warning("Skipping null count summary; none of the known columns exist.")
        return

    expressions = [
        F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column)
        for column in existing
    ]
    row = frame.agg(*expressions).first()
    LOGGER.info("Null summary for known columns:")
    for column in existing:
        LOGGER.info("  %s null_count=%s", column, row[column])


def _log_side_distribution(frame: DataFrame) -> None:
    if "side" not in frame.columns:
        LOGGER.warning("Skipping side distribution; side column missing.")
        return

    _show_df("Side distribution globally:", frame.groupBy("side").count().orderBy("side"))
    if "exchange" not in frame.columns:
        LOGGER.warning("Skipping side distribution by exchange; exchange column missing.")
        return
    _show_df(
        "Side distribution by exchange:",
        frame.groupBy("exchange", "side").count().orderBy("exchange", "side"),
    )


def _log_duplicate_checks(frame: DataFrame) -> None:
    trade_id_columns = ("exchange", "symbol", "trade_id")
    if _has_columns(frame, trade_id_columns):
        duplicates = (
            frame.where(F.col("trade_id").isNotNull())
            .groupBy(*trade_id_columns)
            .count()
            .where(F.col("count") > 1)
        )
        _log_duplicate_summary("trade_id duplicate check", duplicates, trade_id_columns)
    else:
        LOGGER.warning(
            "Skipping trade_id duplicate check; missing columns: %s",
            _missing_text(frame, trade_id_columns),
        )

    approximate_columns = ("exchange", "symbol", "ts_event", "price", "qty", "side")
    if _has_columns(frame, approximate_columns):
        duplicates = (
            frame.groupBy(*approximate_columns)
            .count()
            .where(F.col("count") > 1)
        )
        _log_duplicate_summary(
            "Approximate duplicate check",
            duplicates,
            approximate_columns,
        )
    else:
        LOGGER.warning(
            "Skipping approximate duplicate check; missing columns: %s",
            _missing_text(frame, approximate_columns),
        )


def _log_duplicate_summary(
    label: str,
    duplicates: DataFrame,
    order_columns: tuple[str, ...],
) -> None:
    summary = duplicates.agg(
        F.count("*").alias("duplicate_group_count"),
        F.coalesce(F.sum("count"), F.lit(0)).alias("duplicate_row_count"),
    ).first()
    LOGGER.info(
        "%s: duplicate_group_count=%s, duplicate_row_count=%s",
        label,
        summary["duplicate_group_count"],
        summary["duplicate_row_count"],
    )
    _show_df(
        f"{label}, top 20 duplicate examples:",
        duplicates.orderBy(F.desc("count"), *order_columns),
        20,
    )


def _log_sequence_time_ordering(frame: DataFrame) -> None:
    required = ("exchange", "symbol", "seq", "ts_event", "ts_recv")
    if not _has_columns(frame, required):
        LOGGER.warning(
            "Skipping sequence/time ordering check; missing columns: %s",
            _missing_text(frame, required),
        )
        return

    seq_type = _column_type(frame, "seq")
    if not isinstance(seq_type, T.NumericType):
        LOGGER.warning("Skipping sequence/time ordering check; seq type is not numeric: %s", seq_type)
        return

    window = Window.partitionBy("exchange", "symbol").orderBy("seq")
    checked = frame.select(
        "exchange",
        "symbol",
        "ts_event",
        "ts_recv",
        F.lag("ts_event").over(window).alias("prev_ts_event"),
        F.lag("ts_recv").over(window).alias("prev_ts_recv"),
    )
    summary = checked.groupBy("exchange", "symbol").agg(
        F.sum(
            F.when(F.col("prev_ts_event").isNotNull() & (F.col("ts_event") < F.col("prev_ts_event")), 1)
            .otherwise(0)
        ).alias("out_of_order_ts_event_count"),
        F.sum(
            F.when(F.col("prev_ts_recv").isNotNull() & (F.col("ts_recv") < F.col("prev_ts_recv")), 1)
            .otherwise(0)
        ).alias("out_of_order_ts_recv_count"),
    )
    _show_df("Sequence/time ordering sanity by exchange + symbol:", summary)


def _log_per_second_density_and_gaps(frame: DataFrame) -> None:
    required = ("exchange", "symbol", "ts_event")
    if not _has_columns(frame, required):
        LOGGER.warning(
            "Skipping per-second density and gap profile; missing columns: %s",
            _missing_text(frame, required),
        )
        return

    second_expression = _second_bucket_expression(frame, "ts_event")
    if second_expression is None:
        LOGGER.warning("Skipping per-second density and gap profile; unsupported ts_event type.")
        return

    buckets = (
        frame.select("exchange", "symbol", second_expression.alias("second"))
        .where(F.col("second").isNotNull())
        .groupBy("exchange", "symbol", "second")
        .count()
        .withColumnRenamed("count", "trade_count_per_second")
    )

    density = buckets.groupBy("exchange", "symbol").agg(
        F.min("trade_count_per_second").alias("min_trade_count_per_second"),
        F.max("trade_count_per_second").alias("max_trade_count_per_second"),
        F.avg("trade_count_per_second").alias("avg_trade_count_per_second"),
        F.percentile_approx("trade_count_per_second", 0.50, 10000).alias("p50"),
        F.percentile_approx("trade_count_per_second", 0.95, 10000).alias("p95"),
        F.percentile_approx("trade_count_per_second", 0.99, 10000).alias("p99"),
        F.min("second").alias("min_second"),
        F.max("second").alias("max_second"),
        F.count("*").alias("observed_active_second_count"),
    )
    _show_df("Per-second trade density profile:", density)

    window = Window.partitionBy("exchange", "symbol").orderBy("second")
    gaps = buckets.select(
        "exchange",
        "symbol",
        "second",
        F.lag("second").over(window).alias("previous_second"),
    ).withColumn(
        "gap_seconds",
        F.col("second").cast("long") - F.col("previous_second").cast("long"),
    )
    gap_summary = gaps.groupBy("exchange", "symbol").agg(
        F.max("gap_seconds").alias("max_gap_seconds"),
        F.sum(F.when(F.col("gap_seconds") > 1, 1).otherwise(0)).alias("gaps_gt_1s"),
        F.sum(F.when(F.col("gap_seconds") > 5, 1).otherwise(0)).alias("gaps_gt_5s"),
        F.sum(F.when(F.col("gap_seconds") > 10, 1).otherwise(0)).alias("gaps_gt_10s"),
    )
    _show_df("Per-second gap profile:", gap_summary)


def _latency_millis_expression(frame: DataFrame) -> Any | None:
    event_type = _column_type(frame, "ts_event")
    recv_type = _column_type(frame, "ts_recv")
    if event_type is None or recv_type is None:
        return None

    if _is_timestamp_type(event_type) and _is_timestamp_type(recv_type):
        LOGGER.info("Latency assumption: ts_event and ts_recv are Spark timestamp columns.")
        return (F.col("ts_recv").cast("double") - F.col("ts_event").cast("double")) * F.lit(1000.0)

    if isinstance(event_type, T.NumericType) and isinstance(recv_type, T.NumericType):
        divisor = _numeric_timestamp_latency_divisor(frame, "ts_event", "ts_recv")
        if divisor is None:
            return None
        LOGGER.info("Latency assumption: numeric timestamp delta divisor to milliseconds=%s", divisor)
        return (F.col("ts_recv").cast("double") - F.col("ts_event").cast("double")) / F.lit(divisor)

    LOGGER.warning("Unsupported timestamp types: ts_event=%s, ts_recv=%s", event_type, recv_type)
    return None


def _second_bucket_expression(frame: DataFrame, column: str) -> Any | None:
    column_type = _column_type(frame, column)
    if column_type is None:
        return None

    if _is_timestamp_type(column_type):
        return F.date_trunc("second", F.col(column))

    if isinstance(column_type, T.NumericType):
        divisor = _numeric_timestamp_seconds_divisor(frame, column)
        if divisor is None:
            return None
        LOGGER.info("%s bucket assumption: numeric timestamp divisor to seconds=%s", column, divisor)
        epoch_seconds = (F.col(column).cast("double") / F.lit(divisor)).cast("long")
        return F.to_timestamp(F.from_unixtime(epoch_seconds))

    LOGGER.warning("Unsupported %s type for second bucketing: %s", column, column_type)
    return None


def _numeric_timestamp_latency_divisor(
    frame: DataFrame,
    event_column: str,
    recv_column: str,
) -> float | None:
    scale = _numeric_timestamp_scale(frame, event_column)
    recv_scale = _numeric_timestamp_scale(frame, recv_column)
    if scale is None or recv_scale is None:
        return None
    if scale != recv_scale:
        LOGGER.warning(
            "Numeric timestamp scale differs: %s=%s, %s=%s; using ts_event scale.",
            event_column,
            scale,
            recv_column,
            recv_scale,
        )
    return scale / 1000.0


def _numeric_timestamp_seconds_divisor(frame: DataFrame, column: str) -> float | None:
    scale = _numeric_timestamp_scale(frame, column)
    if scale is None:
        return None
    return scale


def _numeric_timestamp_scale(frame: DataFrame, column: str) -> float | None:
    max_abs = frame.agg(F.max(F.abs(F.col(column).cast("double"))).alias("max_abs")).first()["max_abs"]
    if max_abs is None:
        LOGGER.warning("Cannot infer numeric timestamp scale for %s; column is all null.", column)
        return None
    if max_abs >= 1e17:
        LOGGER.info("Numeric timestamp assumption for %s: nanoseconds since epoch.", column)
        return 1_000_000_000.0
    if max_abs >= 1e14:
        LOGGER.info("Numeric timestamp assumption for %s: microseconds since epoch.", column)
        return 1_000_000.0
    if max_abs >= 1e11:
        LOGGER.info("Numeric timestamp assumption for %s: milliseconds since epoch.", column)
        return 1_000.0
    LOGGER.info("Numeric timestamp assumption for %s: seconds since epoch.", column)
    return 1.0


def _show_df(label: str, frame: DataFrame, rows: int = 20) -> None:
    LOGGER.info("%s", label)
    frame.show(rows, truncate=False)


def _available_columns(frame: DataFrame, columns: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(column for column in columns if column in frame.columns)


def _missing_columns(frame: DataFrame, columns: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(column for column in columns if column not in frame.columns)


def _has_columns(frame: DataFrame, columns: tuple[str, ...]) -> bool:
    return all(column in frame.columns for column in columns)


def _missing_text(frame: DataFrame, columns: tuple[str, ...]) -> str:
    return ", ".join(_missing_columns(frame, columns)) or "none"


def _column_type(frame: DataFrame, column: str) -> T.DataType | None:
    for field in frame.schema.fields:
        if field.name == column:
            return field.dataType
    return None


def _is_timestamp_type(data_type: T.DataType) -> bool:
    timestamp_ntz_type = getattr(T, "TimestampNTZType", None)
    timestamp_types = (T.TimestampType,)
    if timestamp_ntz_type is not None:
        timestamp_types = (T.TimestampType, timestamp_ntz_type)
    return isinstance(data_type, timestamp_types)


def _unique_sorted(values: Any) -> tuple[str, ...]:
    return tuple(sorted({value for value in values if isinstance(value, str)}))


if __name__ == "__main__":
    main()
