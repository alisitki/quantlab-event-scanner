#!/usr/bin/env python
"""Phase 1C BTC trade move candidate scan.

This job reads fixed-date BTC trade parquet partitions, builds 1-second OHLCV
rows, logs 60-second +/-1% move candidate summaries, and writes no outputs.
"""

from __future__ import annotations

import argparse
import logging
import sys
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

from pyspark.sql import DataFrame, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from quantlab_event_scanner.config import load_config  # noqa: E402
from quantlab_event_scanner.manifest import load_manifest_from_s3_with_spark  # noqa: E402
from quantlab_event_scanner.probe import partition_paths  # noqa: E402
from quantlab_event_scanner.trade_move_scan import (  # noqa: E402
    REQUIRED_TRADE_MOVE_COLUMNS,
    build_price_1s,
    scan_move_candidates,
    select_fixed_date_partitions,
    validate_required_columns,
)


LOGGER = logging.getLogger("quantlab_event_scanner.phase1_trade_move_candidates")

DEFAULT_DATE = "20260423"
DEFAULT_THRESHOLD_PCT = 1.0
DEFAULT_LOOKAHEAD_SECONDS = 60
SAMPLE_COLUMNS = (
    "exchange",
    "symbol",
    "direction",
    "start_ts",
    "end_ts",
    "duration_seconds",
    "move_pct",
    "base_price",
    "hit_price",
)


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config_path = _resolve_path(args.config)
    config = load_config(config_path)
    selected_exchange_set = {exchange.lower() for exchange in config.exchanges}

    LOGGER.info("Using config: %s", config_path)
    LOGGER.info("Input root: %s", config.input_root)
    LOGGER.info("Manifest path: %s", config.manifest_path)
    LOGGER.info("Output root, not used in Phase 1C: %s", config.output_root)
    LOGGER.info("Selected date: %s", args.date)
    LOGGER.info("Input exchanges: %s", ", ".join(config.exchanges))
    LOGGER.info("Input stream: trade")
    LOGGER.info("Symbol filter: contains btc")
    LOGGER.info("Threshold pct: %s", args.threshold_pct)
    LOGGER.info("Lookahead seconds: %s", args.lookahead_seconds)

    spark = _get_spark_session()
    manifest = load_manifest_from_s3_with_spark(spark, config.manifest_path)
    LOGGER.info("Loaded manifest: %s", config.manifest_path)
    LOGGER.info("Manifest partition count: %s", len(manifest.partitions))

    selected = select_fixed_date_partitions(
        manifest.partitions,
        exchanges=tuple(selected_exchange_set),
        date=args.date,
    )
    selected_exchanges = _unique_sorted(partition.exchange for partition in selected)
    selected_symbols = _unique_sorted(partition.symbol for partition in selected)

    if not selected:
        LOGGER.warning("Controlled exit: no BTC trade partitions found for date=%s.", args.date)
        LOGGER.warning("No parquet files will be read and no outputs will be written.")
        return

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
    LOGGER.info("Parquet schema:")
    frame.printSchema()

    missing = validate_required_columns(frame, REQUIRED_TRADE_MOVE_COLUMNS)
    if missing:
        LOGGER.warning("Missing required columns: %s", ", ".join(missing))
        LOGGER.info("Available columns: %s", ", ".join(frame.columns))
        LOGGER.warning("Controlled exit: required trade columns are missing.")
        LOGGER.warning("No candidates will be scanned and no outputs will be written.")
        return

    total_count = frame.count()
    LOGGER.info("Total trade row count: %s", total_count)
    if total_count == 0:
        LOGGER.warning("Selected parquet files contain zero rows. Candidate scan stops here.")
        return

    price_1s = build_price_1s(frame)
    if price_1s is None:
        LOGGER.warning("Controlled exit: unsupported ts_event type for second bucketing.")
        return

    price_1s_count = price_1s.count()
    LOGGER.info("1s price row count: %s", price_1s_count)
    if price_1s_count == 0:
        LOGGER.warning("No valid 1s price rows were produced. Candidate scan stops here.")
        return

    candidates, ambiguous_candidates = scan_move_candidates(
        price_1s,
        threshold_pct=args.threshold_pct,
        lookahead_seconds=args.lookahead_seconds,
    )

    _log_candidate_summaries(
        candidates,
        ambiguous_candidates,
        sample_size=args.sample_size,
        lookahead_seconds=args.lookahead_seconds,
    )

    LOGGER.info("Phase 1C candidate scan complete. No S3 output was written.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan BTC trade +/-1% move candidates.")
    parser.add_argument("--config", default="configs/dev.yaml")
    parser.add_argument("--date", default=DEFAULT_DATE)
    parser.add_argument("--threshold-pct", type=float, default=DEFAULT_THRESHOLD_PCT)
    parser.add_argument("--lookahead-seconds", type=int, default=DEFAULT_LOOKAHEAD_SECONDS)
    parser.add_argument("--sample-size", type=int, default=20)
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


def _log_candidate_summaries(
    candidates: DataFrame,
    ambiguous_candidates: DataFrame,
    sample_size: int,
    lookahead_seconds: int,
) -> None:
    raw_candidate_count = candidates.count()
    ambiguous_candidate_count = ambiguous_candidates.count()
    LOGGER.info("Raw candidate count total: %s", raw_candidate_count)
    LOGGER.info("Ambiguous candidate count: %s", ambiguous_candidate_count)

    _show_df(
        "Raw candidate count by direction:",
        candidates.groupBy("direction").count().orderBy("direction"),
    )
    _show_df(
        "Candidate count by exchange:",
        candidates.groupBy("exchange").count().orderBy("exchange"),
    )
    _show_df(
        "Candidate count by symbol:",
        candidates.groupBy("symbol").count().orderBy("symbol"),
    )
    _show_df(
        "Candidate count by exchange and direction:",
        candidates.groupBy("exchange", "direction").count().orderBy("exchange", "direction"),
    )

    grouped_candidates = _approx_grouped_candidates(candidates, lookahead_seconds)
    _show_df(
        "Approx grouped candidate count by exchange:",
        grouped_candidates.groupBy("exchange").count().orderBy("exchange"),
    )
    LOGGER.info("Approx grouped candidate count total: %s", grouped_candidates.count())

    _show_df(
        "Sample UP candidates:",
        candidates.where(F.col("direction") == "UP").orderBy("start_ts").select(*SAMPLE_COLUMNS),
        sample_size,
    )
    _show_df(
        "Sample DOWN candidates:",
        candidates.where(F.col("direction") == "DOWN").orderBy("start_ts").select(*SAMPLE_COLUMNS),
        sample_size,
    )

    if raw_candidate_count == 0:
        LOGGER.info("Duration stats seconds: no non-ambiguous candidates.")
        LOGGER.info("Move pct stats: no non-ambiguous candidates.")
        return

    stats = candidates.agg(
        F.min("duration_seconds").alias("duration_min"),
        F.max("duration_seconds").alias("duration_max"),
        F.avg("duration_seconds").alias("duration_avg"),
        F.min("move_pct").alias("move_pct_min"),
        F.max("move_pct").alias("move_pct_max"),
        F.avg("move_pct").alias("move_pct_avg"),
    ).first()
    LOGGER.info(
        "Duration stats seconds: min=%s, max=%s, avg=%s",
        stats["duration_min"],
        stats["duration_max"],
        stats["duration_avg"],
    )
    LOGGER.info(
        "Move pct stats: min=%s, max=%s, avg=%s",
        stats["move_pct_min"],
        stats["move_pct_max"],
        stats["move_pct_avg"],
    )


def _approx_grouped_candidates(candidates: DataFrame, lookahead_seconds: int) -> DataFrame:
    window = Window.partitionBy("exchange", "symbol", "direction").orderBy("start_ts")
    with_previous = candidates.withColumn("previous_start_ts", F.lag("start_ts").over(window))
    return with_previous.where(
        F.col("previous_start_ts").isNull()
        | (
            F.col("start_ts").cast("long") - F.col("previous_start_ts").cast("long")
            > lookahead_seconds
        )
    )


def _show_df(label: str, frame: DataFrame, rows: int = 20) -> None:
    LOGGER.info("%s", label)
    sample = frame.take(rows)
    if not sample:
        LOGGER.info("  <empty>")
        return
    for row in sample:
        LOGGER.info("  %s", row.asDict(recursive=True))


def _unique_sorted(values: Iterable[str | None]) -> tuple[str, ...]:
    return tuple(sorted({value for value in values if value is not None}))


if __name__ == "__main__":
    main()
