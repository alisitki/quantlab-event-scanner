#!/usr/bin/env python
"""Phase 1D trial event map writer.

This job scans BTC trade move candidates, writes raw and grouped trial outputs
to S3, reads them back, and verifies row counts.
"""

from __future__ import annotations

import argparse
import logging
import sys
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

from quantlab_event_scanner.config import load_config  # noqa: E402
from quantlab_event_scanner.event_candidates import format_trial_event_id  # noqa: E402
from quantlab_event_scanner.manifest import load_manifest_from_s3_with_spark  # noqa: E402
from quantlab_event_scanner.paths import (  # noqa: E402
    events_map_trial_run_path,
    raw_candidates_trial_run_path,
)
from quantlab_event_scanner.probe import partition_paths  # noqa: E402
from quantlab_event_scanner.profiling import select_coverage_aware_date  # noqa: E402
from quantlab_event_scanner.trade_move_scan import (  # noqa: E402
    REQUIRED_TRADE_MOVE_COLUMNS,
    build_price_1s,
    scan_move_candidates,
    validate_required_columns,
)


LOGGER = logging.getLogger("quantlab_event_scanner.phase1_trial_event_map")

DEFAULT_THRESHOLD_PCT = 1.0
DEFAULT_HORIZON_SECONDS = 60
DEFAULT_AGGREGATION_SECONDS = 1
DEFAULT_DETECTION_VERSION = "trade_1s_60s_1pct_v1"
RAW_OUTPUT_COLUMNS = (
    "run_id",
    "detection_version",
    "is_trial",
    "exchange",
    "symbol",
    "source_stream",
    "direction",
    "start_ts",
    "end_ts",
    "duration_seconds",
    "base_price",
    "touch_price",
    "move_pct",
    "threshold_pct",
    "horizon_seconds",
    "aggregation_seconds",
    "selected_date",
    "created_at",
)
EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), False),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("detection_version", T.StringType(), False),
        T.StructField("is_trial", T.BooleanType(), False),
        T.StructField("exchange", T.StringType(), False),
        T.StructField("symbol", T.StringType(), False),
        T.StructField("source_stream", T.StringType(), False),
        T.StructField("direction", T.StringType(), False),
        T.StructField("event_start_ts", T.TimestampType(), False),
        T.StructField("event_end_ts", T.TimestampType(), False),
        T.StructField("duration_seconds", T.LongType(), False),
        T.StructField("base_price", T.DoubleType(), False),
        T.StructField("touch_price", T.DoubleType(), False),
        T.StructField("move_pct", T.DoubleType(), False),
        T.StructField("threshold_pct", T.DoubleType(), False),
        T.StructField("horizon_seconds", T.IntegerType(), False),
        T.StructField("aggregation_seconds", T.IntegerType(), False),
        T.StructField("raw_candidate_count", T.LongType(), False),
        T.StructField("candidate_group_start_ts", T.TimestampType(), False),
        T.StructField("candidate_group_end_ts", T.TimestampType(), False),
        T.StructField("selected_date", T.StringType(), False),
        T.StructField("coverage_count", T.IntegerType(), False),
        T.StructField("selected_exchanges", T.ArrayType(T.StringType()), False),
        T.StructField("missing_exchanges", T.ArrayType(T.StringType()), False),
        T.StructField("created_at", T.TimestampType(), False),
    ]
)


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config_path = _resolve_path(args.config)
    config = load_config(config_path)
    run_id = args.run_id or _default_run_id()
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    raw_output_path = raw_candidates_trial_run_path(config, run_id)
    events_output_path = events_map_trial_run_path(config, run_id)

    LOGGER.info("Using config: %s", config_path)
    LOGGER.info("Input root: %s", config.input_root)
    LOGGER.info("Manifest path: %s", config.manifest_path)
    LOGGER.info("Output root: %s", config.output_root)
    LOGGER.info("Run ID: %s", run_id)
    LOGGER.info("Detection version: %s", args.detection_version)
    LOGGER.info("Threshold pct: %s", args.threshold_pct)
    LOGGER.info("Horizon seconds: %s", args.horizon_seconds)
    LOGGER.info("Aggregation seconds: %s", args.aggregation_seconds)
    LOGGER.info("Raw candidates trial output: %s", raw_output_path)
    LOGGER.info("Events map trial output: %s", events_output_path)

    spark = _get_spark_session()
    manifest = load_manifest_from_s3_with_spark(spark, config.manifest_path)
    selection = select_coverage_aware_date(manifest.partitions, config.exchanges)
    if selection.selected_date is None or selection.coverage_row is None:
        LOGGER.warning("No BTC trade date has at least 2/%s exchange coverage.", len(config.exchanges))
        LOGGER.warning("Controlled exit: no parquet files will be read and no outputs will be written.")
        return

    selected = selection.partitions
    selected_exchanges = _unique_sorted(partition.exchange for partition in selected)
    selected_symbols = _unique_sorted(partition.symbol for partition in selected)
    missing_exchanges = selection.coverage_row.missing_exchanges
    LOGGER.info("Selected date: %s", selection.selected_date)
    LOGGER.info("Coverage count: %s", selection.coverage_row.coverage_count)
    LOGGER.info("Selected exchanges: %s", ", ".join(selected_exchanges))
    LOGGER.info("Missing exchanges: %s", ", ".join(missing_exchanges) or "none")
    LOGGER.info("Selected symbols: %s", ", ".join(selected_symbols))

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
        return

    total_count = frame.count()
    LOGGER.info("Total trade row count: %s", total_count)
    if total_count == 0:
        LOGGER.warning("Selected parquet files contain zero rows. Trial write stops here.")
        return

    price_1s = build_price_1s(frame)
    if price_1s is None:
        LOGGER.warning("Controlled exit: unsupported ts_event type for second bucketing.")
        return

    LOGGER.info("Caching 1s price rows for classic cluster execution.")
    price_1s = price_1s.cache()
    price_1s_count = price_1s.count()
    LOGGER.info("1s price row count: %s", price_1s_count)
    if price_1s_count == 0:
        LOGGER.warning("No valid 1s price rows were produced. Trial write stops here.")
        return

    candidates, ambiguous_candidates = scan_move_candidates(
        price_1s,
        threshold_pct=args.threshold_pct,
        lookahead_seconds=args.horizon_seconds,
    )
    LOGGER.info("Caching candidate outputs for classic cluster execution.")
    candidates = candidates.cache()
    ambiguous_candidates = ambiguous_candidates.cache()
    ambiguous_count = ambiguous_candidates.count()
    LOGGER.info("Ambiguous candidate count: %s", ambiguous_count)

    raw_candidates = _prepare_raw_candidates(
        candidates,
        run_id=run_id,
        detection_version=args.detection_version,
        threshold_pct=args.threshold_pct,
        horizon_seconds=args.horizon_seconds,
        aggregation_seconds=args.aggregation_seconds,
        selected_date=selection.selected_date,
        created_at=created_at,
    )
    raw_candidates = raw_candidates.cache()
    raw_candidate_count = raw_candidates.count()
    LOGGER.info("Raw candidate count: %s", raw_candidate_count)

    events = _build_grouped_events(
        spark,
        raw_candidates,
        run_id=run_id,
        detection_version=args.detection_version,
        threshold_pct=args.threshold_pct,
        horizon_seconds=args.horizon_seconds,
        aggregation_seconds=args.aggregation_seconds,
        selected_date=selection.selected_date,
        coverage_count=selection.coverage_row.coverage_count,
        selected_exchanges=selected_exchanges,
        missing_exchanges=missing_exchanges,
        created_at=created_at,
    )
    events = events.cache()
    grouped_event_count = events.count()
    LOGGER.info("Grouped event count: %s", grouped_event_count)

    LOGGER.info("Writing raw candidates trial output: %s", raw_output_path)
    raw_candidates.write.mode("errorifexists").parquet(raw_output_path)
    LOGGER.info("Writing events map trial output: %s", events_output_path)
    events.write.mode("errorifexists").parquet(events_output_path)

    _verify_readback(
        spark,
        label="raw candidates trial",
        path=raw_output_path,
        expected_count=raw_candidate_count,
        sample_size=args.sample_size,
    )
    _verify_readback(
        spark,
        label="events map trial",
        path=events_output_path,
        expected_count=grouped_event_count,
        sample_size=args.sample_size,
    )

    events.unpersist()
    raw_candidates.unpersist()
    ambiguous_candidates.unpersist()
    candidates.unpersist()
    price_1s.unpersist()
    LOGGER.info("Phase 1D trial event map write complete.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write Phase 1D trial event map output.")
    parser.add_argument("--config", default="configs/dev.yaml")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--threshold-pct", type=float, default=DEFAULT_THRESHOLD_PCT)
    parser.add_argument("--horizon-seconds", type=int, default=DEFAULT_HORIZON_SECONDS)
    parser.add_argument("--aggregation-seconds", type=int, default=DEFAULT_AGGREGATION_SECONDS)
    parser.add_argument("--detection-version", default=DEFAULT_DETECTION_VERSION)
    parser.add_argument("--sample-size", type=int, default=20)
    parser.add_argument("--path-log-limit", type=int, default=20)
    return parser.parse_args()


def _prepare_raw_candidates(
    candidates: DataFrame,
    run_id: str,
    detection_version: str,
    threshold_pct: float,
    horizon_seconds: int,
    aggregation_seconds: int,
    selected_date: str,
    created_at: datetime,
) -> DataFrame:
    return candidates.select(
        F.lit(run_id).alias("run_id"),
        F.lit(detection_version).alias("detection_version"),
        F.lit(True).alias("is_trial"),
        F.col("exchange"),
        F.col("symbol"),
        F.lit("trade").alias("source_stream"),
        F.col("direction"),
        F.col("start_ts"),
        F.col("end_ts"),
        F.col("duration_seconds"),
        F.col("base_price"),
        F.col("hit_price").alias("touch_price"),
        F.col("move_pct"),
        F.lit(float(threshold_pct)).alias("threshold_pct"),
        F.lit(int(horizon_seconds)).alias("horizon_seconds"),
        F.lit(int(aggregation_seconds)).alias("aggregation_seconds"),
        F.lit(selected_date).alias("selected_date"),
        F.lit(created_at).cast("timestamp").alias("created_at"),
    ).select(*RAW_OUTPUT_COLUMNS)


def _build_grouped_events(
    spark: Any,
    raw_candidates: DataFrame,
    run_id: str,
    detection_version: str,
    threshold_pct: float,
    horizon_seconds: int,
    aggregation_seconds: int,
    selected_date: str,
    coverage_count: int,
    selected_exchanges: tuple[str, ...],
    missing_exchanges: tuple[str, ...],
    created_at: datetime,
) -> DataFrame:
    rows = (
        raw_candidates.select(
            "exchange",
            "symbol",
            "direction",
            "start_ts",
            "end_ts",
            "duration_seconds",
            "base_price",
            "touch_price",
            "move_pct",
        )
        .orderBy("exchange", "symbol", "direction", "start_ts")
        .collect()
    )
    grouped = _group_candidate_rows(
        rows,
        run_id=run_id,
        detection_version=detection_version,
        threshold_pct=threshold_pct,
        horizon_seconds=horizon_seconds,
        aggregation_seconds=aggregation_seconds,
        selected_date=selected_date,
        coverage_count=coverage_count,
        selected_exchanges=selected_exchanges,
        missing_exchanges=missing_exchanges,
        created_at=created_at,
    )
    return spark.createDataFrame(grouped, schema=EVENT_SCHEMA)


def _group_candidate_rows(
    rows: list[Any],
    run_id: str,
    detection_version: str,
    threshold_pct: float,
    horizon_seconds: int,
    aggregation_seconds: int,
    selected_date: str,
    coverage_count: int,
    selected_exchanges: tuple[str, ...],
    missing_exchanges: tuple[str, ...],
    created_at: datetime,
) -> list[dict[str, Any]]:
    grouped: list[dict[str, Any]] = []
    active: dict[tuple[str, str, str], int] = {}
    group_numbers: dict[tuple[str, str, str], int] = {}

    for row in rows:
        key = (row["exchange"], row["symbol"], row["direction"])
        active_index = active.get(key)
        if active_index is None:
            grouped.append(
                _new_group(
                    row,
                    key,
                    group_number=1,
                    run_id=run_id,
                    detection_version=detection_version,
                    threshold_pct=threshold_pct,
                    horizon_seconds=horizon_seconds,
                    aggregation_seconds=aggregation_seconds,
                    selected_date=selected_date,
                    coverage_count=coverage_count,
                    selected_exchanges=selected_exchanges,
                    missing_exchanges=missing_exchanges,
                    created_at=created_at,
                )
            )
            active[key] = len(grouped) - 1
            group_numbers[key] = 1
            continue

        current = grouped[active_index]
        elapsed = (row["start_ts"] - current["candidate_group_start_ts"]).total_seconds()
        if elapsed > horizon_seconds:
            group_number = group_numbers[key] + 1
            group_numbers[key] = group_number
            grouped.append(
                _new_group(
                    row,
                    key,
                    group_number=group_number,
                    run_id=run_id,
                    detection_version=detection_version,
                    threshold_pct=threshold_pct,
                    horizon_seconds=horizon_seconds,
                    aggregation_seconds=aggregation_seconds,
                    selected_date=selected_date,
                    coverage_count=coverage_count,
                    selected_exchanges=selected_exchanges,
                    missing_exchanges=missing_exchanges,
                    created_at=created_at,
                )
            )
            active[key] = len(grouped) - 1
            continue

        current["raw_candidate_count"] += 1
        current["candidate_group_end_ts"] = row["start_ts"]

    return grouped


def _new_group(
    row: Any,
    key: tuple[str, str, str],
    group_number: int,
    run_id: str,
    detection_version: str,
    threshold_pct: float,
    horizon_seconds: int,
    aggregation_seconds: int,
    selected_date: str,
    coverage_count: int,
    selected_exchanges: tuple[str, ...],
    missing_exchanges: tuple[str, ...],
    created_at: datetime,
) -> dict[str, Any]:
    exchange, symbol, direction = key
    return {
        "event_id": format_trial_event_id(exchange, symbol, selected_date, direction, group_number),
        "run_id": run_id,
        "detection_version": detection_version,
        "is_trial": True,
        "exchange": exchange,
        "symbol": symbol,
        "source_stream": "trade",
        "direction": direction,
        "event_start_ts": row["start_ts"],
        "event_end_ts": row["end_ts"],
        "duration_seconds": int(row["duration_seconds"]),
        "base_price": float(row["base_price"]),
        "touch_price": float(row["touch_price"]),
        "move_pct": float(row["move_pct"]),
        "threshold_pct": float(threshold_pct),
        "horizon_seconds": int(horizon_seconds),
        "aggregation_seconds": int(aggregation_seconds),
        "raw_candidate_count": 1,
        "candidate_group_start_ts": row["start_ts"],
        "candidate_group_end_ts": row["start_ts"],
        "selected_date": selected_date,
        "coverage_count": int(coverage_count),
        "selected_exchanges": list(selected_exchanges),
        "missing_exchanges": list(missing_exchanges),
        "created_at": created_at,
    }


def _verify_readback(
    spark: Any,
    label: str,
    path: str,
    expected_count: int,
    sample_size: int,
) -> None:
    LOGGER.info("Reading back %s output: %s", label, path)
    frame = spark.read.parquet(path)
    LOGGER.info("%s schema:", label)
    frame.printSchema()
    actual_count = frame.count()
    LOGGER.info("%s readback count: %s", label, actual_count)
    if actual_count != expected_count:
        raise RuntimeError(
            f"{label} readback count mismatch: expected={expected_count}, actual={actual_count}"
        )
    _show_df(f"{label} sample:", frame, sample_size)


def _show_df(label: str, frame: DataFrame, rows: int = 20) -> None:
    LOGGER.info("%s", label)
    sample = frame.take(rows)
    if not sample:
        LOGGER.info("  <empty>")
        return
    for row in sample:
        LOGGER.info("  %s", row.asDict(recursive=True))


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


def _default_run_id() -> str:
    return f"phase1d_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


def _unique_sorted(values: Iterable[str | None]) -> tuple[str, ...]:
    return tuple(sorted({value for value in values if value is not None}))


if __name__ == "__main__":
    main()
