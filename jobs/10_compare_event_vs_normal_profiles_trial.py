#!/usr/bin/env python
"""Phase 2F trial event-vs-normal profile comparison report."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
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

from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from quantlab_event_scanner.config import load_config  # noqa: E402
from quantlab_event_scanner.paths import (  # noqa: E402
    normal_profile_reports_trial_run_path,
    pre_event_profile_reports_trial_run_path,
    profile_comparison_reports_trial_run_path,
)
from quantlab_event_scanner.profile_comparison_reports import (  # noqa: E402
    BUCKET_CHANGE_COMPARISON_SOURCE_METRICS,
    BUCKET_CHANGE_COMPARISON_VALUE_FIELDS,
    CROSS_EXCHANGE_MID_DIFF_COMPARISON_METRICS,
    EXCHANGE_PROFILE_COMPARISON_METRICS,
    METRIC_GROUP_CONTEXT,
    METRIC_GROUP_PRICE_DISLOCATION,
    METRIC_GROUP_SIGNAL_CANDIDATE,
    METRIC_GROUP_UNSTABLE,
    SMALL_DENOMINATOR_ABS_THRESHOLD,
    CONTEXT_METRICS,
    PRICE_DISLOCATION_METRICS,
    SIGNAL_CANDIDATE_METRICS,
    default_phase2f_run_id,
    deterministic_exchange_pair_names,
    expected_bucket_change_profile_comparison_rows,
    expected_cross_exchange_mid_diff_comparison_rows,
    expected_exchange_profile_comparison_rows,
    validate_curated_metrics,
)


LOGGER = logging.getLogger("quantlab_event_scanner.phase2f_profile_comparison")

DEFAULT_EVENT_PROFILE_RUN_ID = "phase2d_20260427T090707Z"
DEFAULT_NORMAL_PROFILE_RUN_ID = "phase2e_20260427T093700Z"
DEFAULT_EVENT_ID = "binance_btcusdt_20260423_down_001"

EXPECTED_SOURCE_ROW_COUNTS = {
    "event.exchange_profile": 30,
    "event.cross_exchange_mid_diff": 30,
    "event.bucket_change_profile": 48,
    "normal.exchange_profile": 30,
    "normal.cross_exchange_mid_diff": 30,
    "normal.bucket_change_profile": 48,
}

EVENT_METADATA_COLUMNS = ("event_id", "event_direction", "event_start_ts", "profile_version")
NORMAL_METADATA_COLUMNS = (
    "sample_type",
    "selected_date",
    "event_id",
    "event_direction",
    "event_start_ts",
    "normal_window_start_ts",
    "normal_window_end_ts",
    "normal_anchor_ts",
    "profile_version",
)

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


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config_path = _resolve_path(args.config)
    config = load_config(config_path)
    run_id = args.run_id or default_phase2f_run_id(datetime.now(timezone.utc))
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)

    event_profile_path = pre_event_profile_reports_trial_run_path(
        config,
        args.event_profile_run_id,
    )
    normal_profile_path = normal_profile_reports_trial_run_path(
        config,
        args.normal_profile_run_id,
    )
    output_path = profile_comparison_reports_trial_run_path(config, run_id)

    LOGGER.info("Using config: %s", config_path)
    LOGGER.info("Comparison run ID: %s", run_id)
    LOGGER.info("Event profile run ID: %s", args.event_profile_run_id)
    LOGGER.info("Normal profile run ID: %s", args.normal_profile_run_id)
    LOGGER.info("Event ID: %s", args.event_id)
    LOGGER.info("Event profile path: %s", event_profile_path)
    LOGGER.info("Normal profile path: %s", normal_profile_path)
    LOGGER.info("Output path: %s", output_path)

    spark = _get_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    LOGGER.info("Spark timezone: %s", spark.conf.get("spark.sql.session.timeZone"))

    event_exchange_raw = _read_profile_subtable(
        spark,
        event_profile_path,
        "exchange_profile",
        "event.exchange_profile",
    )
    event_cross_raw = _read_profile_subtable(
        spark,
        event_profile_path,
        "cross_exchange_mid_diff",
        "event.cross_exchange_mid_diff",
    )
    event_bucket_raw = _read_profile_subtable(
        spark,
        event_profile_path,
        "bucket_change_profile",
        "event.bucket_change_profile",
    )
    normal_exchange = _read_profile_subtable(
        spark,
        normal_profile_path,
        "exchange_profile",
        "normal.exchange_profile",
    ).cache()
    normal_cross = _read_profile_subtable(
        spark,
        normal_profile_path,
        "cross_exchange_mid_diff",
        "normal.cross_exchange_mid_diff",
    ).cache()
    normal_bucket = _read_profile_subtable(
        spark,
        normal_profile_path,
        "bucket_change_profile",
        "normal.bucket_change_profile",
    ).cache()

    _validate_columns(event_exchange_raw, "event.exchange_profile", EVENT_METADATA_COLUMNS)
    _validate_columns(event_cross_raw, "event.cross_exchange_mid_diff", EVENT_METADATA_COLUMNS)
    _validate_columns(event_bucket_raw, "event.bucket_change_profile", EVENT_METADATA_COLUMNS)
    _validate_columns(normal_exchange, "normal.exchange_profile", NORMAL_METADATA_COLUMNS)
    _validate_columns(normal_cross, "normal.cross_exchange_mid_diff", NORMAL_METADATA_COLUMNS)
    _validate_columns(normal_bucket, "normal.bucket_change_profile", NORMAL_METADATA_COLUMNS)

    event_exchange = event_exchange_raw.where(F.col("event_id") == args.event_id).cache()
    event_cross = event_cross_raw.where(F.col("event_id") == args.event_id).cache()
    event_bucket = event_bucket_raw.where(F.col("event_id") == args.event_id).cache()
    event_exchange_raw.unpersist()
    event_cross_raw.unpersist()
    event_bucket_raw.unpersist()

    input_frames = (
        ("event.exchange_profile", event_exchange),
        ("event.cross_exchange_mid_diff", event_cross),
        ("event.bucket_change_profile", event_bucket),
        ("normal.exchange_profile", normal_exchange),
        ("normal.cross_exchange_mid_diff", normal_cross),
        ("normal.bucket_change_profile", normal_bucket),
    )
    for label, frame in input_frames:
        count = frame.count()
        LOGGER.info("%s source row count: %s", label, count)
        expected_count = EXPECTED_SOURCE_ROW_COUNTS[label]
        if count != expected_count:
            raise RuntimeError(
                f"{label} source row count mismatch: expected={expected_count}, observed={count}"
            )

    _validate_profile_contract(
        (event_exchange, event_cross, event_bucket),
        (normal_exchange, normal_cross, normal_bucket),
    )
    _log_metadata_summary(event_exchange, normal_exchange)

    exchange_profile_comparison = _build_exchange_profile_comparison(
        event_exchange,
        normal_exchange,
        run_id,
        args.event_profile_run_id,
        args.normal_profile_run_id,
        created_at,
    ).cache()
    cross_exchange_mid_diff_comparison = _build_cross_exchange_mid_diff_comparison(
        event_cross,
        normal_cross,
        run_id,
        args.event_profile_run_id,
        args.normal_profile_run_id,
        created_at,
    ).cache()
    bucket_change_profile_comparison = _build_bucket_change_profile_comparison(
        event_bucket,
        normal_bucket,
        run_id,
        args.event_profile_run_id,
        args.normal_profile_run_id,
        created_at,
    ).cache()

    _validate_metric_group_coverage(exchange_profile_comparison, "exchange_profile_comparison")
    _validate_metric_group_coverage(
        cross_exchange_mid_diff_comparison,
        "cross_exchange_mid_diff_comparison",
    )
    _validate_metric_group_coverage(
        bucket_change_profile_comparison,
        "bucket_change_profile_comparison",
    )

    _validate_expected_row_count(
        exchange_profile_comparison,
        "exchange_profile_comparison",
        expected_exchange_profile_comparison_rows(),
    )
    _validate_expected_row_count(
        cross_exchange_mid_diff_comparison,
        "cross_exchange_mid_diff_comparison",
        expected_cross_exchange_mid_diff_comparison_rows(),
    )
    _validate_expected_row_count(
        bucket_change_profile_comparison,
        "bucket_change_profile_comparison",
        expected_bucket_change_profile_comparison_rows(),
    )

    _write_and_validate(
        exchange_profile_comparison,
        f"{output_path}/exchange_profile_comparison",
        "exchange_profile_comparison",
        args.sample_size,
    )
    _write_and_validate(
        cross_exchange_mid_diff_comparison,
        f"{output_path}/cross_exchange_mid_diff_comparison",
        "cross_exchange_mid_diff_comparison",
        args.sample_size,
    )
    _write_and_validate(
        bucket_change_profile_comparison,
        f"{output_path}/bucket_change_profile_comparison",
        "bucket_change_profile_comparison",
        args.sample_size,
    )

    bucket_change_profile_comparison.unpersist()
    cross_exchange_mid_diff_comparison.unpersist()
    exchange_profile_comparison.unpersist()
    normal_bucket.unpersist()
    normal_cross.unpersist()
    normal_exchange.unpersist()
    event_bucket.unpersist()
    event_cross.unpersist()
    event_exchange.unpersist()
    LOGGER.info("Phase 2F profile comparison trial complete.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare Phase 2D and Phase 2E profile reports.")
    parser.add_argument("--config", default="configs/dev.yaml")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--event-profile-run-id", default=DEFAULT_EVENT_PROFILE_RUN_ID)
    parser.add_argument("--normal-profile-run-id", default=DEFAULT_NORMAL_PROFILE_RUN_ID)
    parser.add_argument("--event-id", default=DEFAULT_EVENT_ID)
    parser.add_argument("--sample-size", type=int, default=20)
    return parser.parse_args()


def _read_profile_subtable(
    spark: Any,
    base_path: str,
    subtable: str,
    label: str,
) -> DataFrame:
    path = f"{base_path}/{subtable}"
    LOGGER.info("%s path: %s", label, path)
    return spark.read.parquet(path)


def _validate_columns(
    frame: DataFrame,
    label: str,
    required_columns: tuple[str, ...],
) -> None:
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise RuntimeError(f"{label} missing required columns: {', '.join(missing)}")


def _validate_profile_contract(
    event_frames: tuple[DataFrame, DataFrame, DataFrame],
    normal_frames: tuple[DataFrame, DataFrame, DataFrame],
) -> None:
    event_contract = _distinct_contract_frame(event_frames, EVENT_METADATA_COLUMNS)
    normal_contract = _distinct_contract_frame(normal_frames, NORMAL_METADATA_COLUMNS)
    event_rows = event_contract.collect()
    normal_rows = normal_contract.collect()
    LOGGER.info(
        "Profile contract row counts: event=%s normal=%s",
        len(event_rows),
        len(normal_rows),
    )
    if len(event_rows) != 1:
        _show_df("Event profile contract rows:", event_contract)
        raise RuntimeError("Event profile metadata is not identical across subtables.")
    if len(normal_rows) != 1:
        _show_df("Normal profile contract rows:", normal_contract)
        raise RuntimeError("Normal profile metadata is not identical across subtables.")

    event_row = event_rows[0]
    normal_row = normal_rows[0]
    if normal_row["sample_type"] != "normal":
        raise RuntimeError(f"normal sample_type mismatch: observed={normal_row['sample_type']}")
    for column in ("event_id", "event_direction", "event_start_ts"):
        if normal_row[column] is not None:
            raise RuntimeError(f"normal.{column} must be null, observed={normal_row[column]}")

    event_version = event_row["profile_version"]
    normal_version = normal_row["profile_version"]
    if event_version != normal_version:
        raise RuntimeError(
            f"profile_version mismatch: event={event_version}, normal={normal_version}"
        )


def _distinct_contract_frame(
    frames: tuple[DataFrame, DataFrame, DataFrame],
    columns: tuple[str, ...],
) -> DataFrame:
    selected = [frame.select(*columns) for frame in frames]
    output = selected[0]
    for frame in selected[1:]:
        output = output.unionByName(frame)
    return output.distinct()


def _log_metadata_summary(event_exchange: DataFrame, normal_exchange: DataFrame) -> None:
    event_summary = event_exchange.select(
        "event_id",
        "event_direction",
        "event_start_ts",
        "profile_version",
    ).distinct()
    normal_summary = normal_exchange.select(
        "sample_type",
        "selected_date",
        "normal_window_start_ts",
        "normal_window_end_ts",
        "normal_anchor_ts",
        "profile_version",
    ).distinct()
    _show_df("Event profile metadata summary:", event_summary)
    _show_df("Normal profile metadata summary:", normal_summary)


def _build_exchange_profile_comparison(
    event_exchange: DataFrame,
    normal_exchange: DataFrame,
    run_id: str,
    event_profile_run_id: str,
    normal_profile_run_id: str,
    created_at: datetime,
) -> DataFrame:
    required = (
        "event_id",
        "event_direction",
        "event_start_ts",
        "profile_version",
        "exchange",
        "symbol",
        "window_mode",
        "window_label",
        *EXCHANGE_PROFILE_COMPARISON_METRICS,
    )
    _validate_columns(event_exchange, "event.exchange_profile", required)
    _validate_columns(normal_exchange, "normal.exchange_profile", required)
    validate_curated_metrics(event_exchange.columns, EXCHANGE_PROFILE_COMPARISON_METRICS)
    validate_curated_metrics(normal_exchange.columns, EXCHANGE_PROFILE_COMPARISON_METRICS)

    event_long = _exchange_profile_long(event_exchange, "event_value")
    normal_long = _exchange_profile_long(normal_exchange, "normal_value")
    _validate_no_unmatched(event_long, normal_long, EXCHANGE_PROFILE_KEYS, "exchange_profile")

    joined = event_long.alias("event").join(
        normal_long.alias("normal"),
        [*EXCHANGE_PROFILE_KEYS, "metric_name"],
        "inner",
    )
    return _select_comparison_columns(
        joined,
        report_group="exchange_profile",
        run_id=run_id,
        event_profile_run_id=event_profile_run_id,
        normal_profile_run_id=normal_profile_run_id,
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


def _exchange_profile_long(frame: DataFrame, value_column: str) -> DataFrame:
    return frame.select(
        F.lower(F.col("exchange")).alias("exchange"),
        F.col("symbol"),
        F.lower(F.col("symbol")).alias("symbol_key"),
        "window_mode",
        "window_label",
        F.col("event_id").alias("source_event_id"),
        "event_direction",
        "event_start_ts",
        "profile_version",
        _column_or_null(frame, "normal_window_start_ts").alias("normal_window_start_ts"),
        _column_or_null(frame, "normal_window_end_ts").alias("normal_window_end_ts"),
        _column_or_null(frame, "normal_anchor_ts").alias("normal_anchor_ts"),
        F.expr(_stack_expr(EXCHANGE_PROFILE_COMPARISON_METRICS, value_column)),
    )


def _build_cross_exchange_mid_diff_comparison(
    event_cross: DataFrame,
    normal_cross: DataFrame,
    run_id: str,
    event_profile_run_id: str,
    normal_profile_run_id: str,
    created_at: datetime,
) -> DataFrame:
    required = (
        "event_id",
        "event_direction",
        "event_start_ts",
        "profile_version",
        "symbol_key",
        "window_mode",
        "window_label",
        "exchange_a",
        "exchange_b",
        *CROSS_EXCHANGE_MID_DIFF_COMPARISON_METRICS,
    )
    _validate_columns(event_cross, "event.cross_exchange_mid_diff", required)
    _validate_columns(normal_cross, "normal.cross_exchange_mid_diff", required)
    validate_curated_metrics(event_cross.columns, CROSS_EXCHANGE_MID_DIFF_COMPARISON_METRICS)
    validate_curated_metrics(normal_cross.columns, CROSS_EXCHANGE_MID_DIFF_COMPARISON_METRICS)

    event_long = _cross_exchange_long(event_cross, "event_value")
    normal_long = _cross_exchange_long(normal_cross, "normal_value")
    _validate_exchange_pair_coverage(event_long, "event.cross_exchange_mid_diff")
    _validate_exchange_pair_coverage(normal_long, "normal.cross_exchange_mid_diff")
    _validate_no_unmatched(event_long, normal_long, CROSS_EXCHANGE_KEYS, "cross_exchange_mid_diff")

    joined = event_long.alias("event").join(
        normal_long.alias("normal"),
        [*CROSS_EXCHANGE_KEYS, "metric_name"],
        "inner",
    )
    return _select_comparison_columns(
        joined,
        report_group="cross_exchange_mid_diff",
        run_id=run_id,
        event_profile_run_id=event_profile_run_id,
        normal_profile_run_id=normal_profile_run_id,
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


def _cross_exchange_long(frame: DataFrame, value_column: str) -> DataFrame:
    normalized = frame.withColumn(
        "exchange_pair",
        F.concat_ws(
            "__",
            F.lower(F.col("exchange_a")),
            F.lower(F.col("exchange_b")),
        ),
    )
    return normalized.select(
        F.lower(F.col("symbol_key")).alias("symbol_key"),
        "window_mode",
        "window_label",
        "exchange_pair",
        F.col("event_id").alias("source_event_id"),
        "event_direction",
        "event_start_ts",
        "profile_version",
        _column_or_null(normalized, "normal_window_start_ts").alias("normal_window_start_ts"),
        _column_or_null(normalized, "normal_window_end_ts").alias("normal_window_end_ts"),
        _column_or_null(normalized, "normal_anchor_ts").alias("normal_anchor_ts"),
        F.expr(_stack_expr(CROSS_EXCHANGE_MID_DIFF_COMPARISON_METRICS, value_column)),
    )


def _build_bucket_change_profile_comparison(
    event_bucket: DataFrame,
    normal_bucket: DataFrame,
    run_id: str,
    event_profile_run_id: str,
    normal_profile_run_id: str,
    created_at: datetime,
) -> DataFrame:
    required = (
        "event_id",
        "event_direction",
        "event_start_ts",
        "profile_version",
        "exchange",
        "symbol",
        "metric_name",
        "from_bucket",
        "to_bucket",
        "from_bucket_value",
        "to_bucket_value",
        *BUCKET_CHANGE_COMPARISON_VALUE_FIELDS,
    )
    _validate_columns(event_bucket, "event.bucket_change_profile", required)
    _validate_columns(normal_bucket, "normal.bucket_change_profile", required)
    validate_curated_metrics(event_bucket.columns, BUCKET_CHANGE_COMPARISON_VALUE_FIELDS)
    validate_curated_metrics(normal_bucket.columns, BUCKET_CHANGE_COMPARISON_VALUE_FIELDS)

    event_filtered = event_bucket.where(
        F.col("metric_name").isin(list(BUCKET_CHANGE_COMPARISON_SOURCE_METRICS))
    )
    normal_filtered = normal_bucket.where(
        F.col("metric_name").isin(list(BUCKET_CHANGE_COMPARISON_SOURCE_METRICS))
    )
    _validate_bucket_source_metric_coverage(event_filtered, "event.bucket_change_profile")
    _validate_bucket_source_metric_coverage(normal_filtered, "normal.bucket_change_profile")

    event_long = _bucket_change_long(event_filtered, "event_value")
    normal_long = _bucket_change_long(normal_filtered, "normal_value")
    _validate_no_unmatched(event_long, normal_long, BUCKET_CHANGE_KEYS, "bucket_change_profile")

    joined = event_long.alias("event").join(
        normal_long.alias("normal"),
        [*BUCKET_CHANGE_KEYS, "metric_name"],
        "inner",
    )
    return _select_comparison_columns(
        joined,
        report_group="bucket_change_profile",
        run_id=run_id,
        event_profile_run_id=event_profile_run_id,
        normal_profile_run_id=normal_profile_run_id,
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


def _bucket_change_long(frame: DataFrame, value_column: str) -> DataFrame:
    return (
        frame.select(
            F.lower(F.col("exchange")).alias("exchange"),
            F.col("symbol"),
            F.lower(F.col("symbol")).alias("symbol_key"),
            F.col("metric_name").alias("source_metric_name"),
            "from_bucket",
            "to_bucket",
            F.col("event_id").alias("source_event_id"),
            "event_direction",
            "event_start_ts",
            "profile_version",
            _column_or_null(frame, "normal_window_start_ts").alias("normal_window_start_ts"),
            _column_or_null(frame, "normal_window_end_ts").alias("normal_window_end_ts"),
            _column_or_null(frame, "normal_anchor_ts").alias("normal_anchor_ts"),
            F.col("from_bucket_value"),
            F.col("to_bucket_value"),
            F.expr(
                _stack_expr(
                    BUCKET_CHANGE_COMPARISON_VALUE_FIELDS,
                    value_column,
                    name_alias="comparison_value_field",
                )
            ),
        )
        .withColumn(
            "metric_name",
            F.concat_ws(".", F.col("source_metric_name"), F.col("comparison_value_field")),
        )
    )


def _select_comparison_columns(
    joined: DataFrame,
    *,
    report_group: str,
    run_id: str,
    event_profile_run_id: str,
    normal_profile_run_id: str,
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
    event_value = F.col("event.event_value").cast("double")
    normal_value = F.col("normal.normal_value").cast("double")
    metric_name = F.col("metric_name")
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
    ratio_unstable = normal_value.isNull() | (
        F.abs(normal_value) <= F.lit(SMALL_DENOMINATOR_ABS_THRESHOLD)
    )
    relative_change_unstable = metric_name.endswith(".relative_change") & (
        event_from_value.isNull()
        | normal_from_value.isNull()
        | (F.abs(event_from_value) <= F.lit(SMALL_DENOMINATOR_ABS_THRESHOLD))
        | (F.abs(normal_from_value) <= F.lit(SMALL_DENOMINATOR_ABS_THRESHOLD))
    )
    return joined.select(
        F.lit(run_id).alias("comparison_run_id"),
        F.lit(event_profile_run_id).alias("event_profile_run_id"),
        F.lit(normal_profile_run_id).alias("normal_profile_run_id"),
        F.lit(report_group).alias("report_group"),
        metric_name.alias("metric_name"),
        _metric_group_column(metric_name).alias("metric_group"),
        F.col("event.source_event_id").alias("source_event_id"),
        F.lit(None).cast("string").alias("normal_event_id"),
        F.lit(None).cast("string").alias("normal_event_direction"),
        F.lit(None).cast("timestamp").alias("normal_event_start_ts"),
        F.col("event.event_direction").alias("event_direction"),
        F.col("event.event_start_ts").alias("event_start_ts"),
        F.col("normal.normal_window_start_ts").alias("normal_window_start_ts"),
        F.col("normal.normal_window_end_ts").alias("normal_window_end_ts"),
        F.col("normal.normal_anchor_ts").alias("normal_anchor_ts"),
        exchange.alias("exchange"),
        symbol.alias("symbol"),
        symbol_key.alias("symbol_key"),
        exchange_pair.alias("exchange_pair"),
        window_mode.alias("window_mode"),
        window_label.alias("window_label"),
        from_bucket.alias("from_bucket"),
        to_bucket.alias("to_bucket"),
        F.col("event.profile_version").alias("profile_version"),
        event_value.alias("event_value"),
        normal_value.alias("normal_value"),
        F.when(
            event_value.isNotNull() & normal_value.isNotNull(),
            event_value - normal_value,
        ).alias("signed_diff"),
        F.when(
            event_value.isNotNull() & normal_value.isNotNull(),
            F.abs(event_value - normal_value),
        ).alias("absolute_diff"),
        F.when(
            event_value.isNotNull() & normal_value.isNotNull() & (normal_value != F.lit(0.0)),
            event_value / normal_value,
        ).alias("ratio"),
        ratio_unstable.alias("ratio_unstable"),
        relative_change_unstable.alias("relative_change_unstable"),
        (ratio_unstable | relative_change_unstable).alias("small_denominator_flag"),
        event_from_value.alias("event_from_bucket_value"),
        normal_from_value.alias("normal_from_bucket_value"),
        event_to_value.alias("event_to_bucket_value"),
        normal_to_value.alias("normal_to_bucket_value"),
        F.lit(created_at).cast("timestamp").alias("created_at"),
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


def _stack_expr(
    metrics: tuple[str, ...],
    value_alias: str,
    *,
    name_alias: str = "metric_name",
) -> str:
    entries = ", ".join(
        f"'{metric}', cast(`{metric}` as double)"
        for metric in metrics
    )
    return f"stack({len(metrics)}, {entries}) AS ({name_alias}, {value_alias})"


def _validate_no_unmatched(
    event_long: DataFrame,
    normal_long: DataFrame,
    keys: tuple[str, ...],
    label: str,
) -> None:
    join_keys = (*keys, "metric_name")
    event_keys = event_long.select(*join_keys).distinct()
    normal_keys = normal_long.select(*join_keys).distinct()
    event_only = event_keys.join(normal_keys, list(join_keys), "left_anti")
    normal_only = normal_keys.join(event_keys, list(join_keys), "left_anti")
    event_only_count = event_only.count()
    normal_only_count = normal_only.count()
    LOGGER.info(
        "%s unmatched key counts: event_only=%s normal_only=%s",
        label,
        event_only_count,
        normal_only_count,
    )
    if event_only_count or normal_only_count:
        _show_df(f"{label} event-only keys:", event_only)
        _show_df(f"{label} normal-only keys:", normal_only)
        raise RuntimeError(f"{label} comparison has unmatched event/normal keys.")


def _validate_exchange_pair_coverage(frame: DataFrame, label: str) -> None:
    expected = set(deterministic_exchange_pair_names())
    observed = {
        row["exchange_pair"]
        for row in frame.select("exchange_pair").distinct().collect()
    }
    LOGGER.info(
        "%s exchange-pair coverage: expected=%s observed=%s",
        label,
        sorted(expected),
        sorted(observed),
    )
    if observed != expected:
        raise RuntimeError(
            f"{label} exchange-pair coverage mismatch: expected={sorted(expected)}, "
            f"observed={sorted(observed)}"
        )


def _validate_bucket_source_metric_coverage(frame: DataFrame, label: str) -> None:
    expected = set(BUCKET_CHANGE_COMPARISON_SOURCE_METRICS)
    observed = {row["metric_name"] for row in frame.select("metric_name").distinct().collect()}
    LOGGER.info(
        "%s bucket source metric coverage: expected=%s observed=%s",
        label,
        sorted(expected),
        sorted(observed),
    )
    if observed != expected:
        raise RuntimeError(
            f"{label} bucket source metric coverage mismatch: expected={sorted(expected)}, "
            f"observed={sorted(observed)}"
        )


def _validate_expected_row_count(frame: DataFrame, label: str, expected_count: int) -> None:
    observed_count = frame.count()
    LOGGER.info(
        "%s expected vs observed output rows: expected=%s observed=%s",
        label,
        expected_count,
        observed_count,
    )
    if observed_count != expected_count:
        raise RuntimeError(
            f"{label} row count mismatch: expected={expected_count}, observed={observed_count}"
        )


def _validate_metric_group_coverage(frame: DataFrame, label: str) -> None:
    null_count = frame.where(F.col("metric_group").isNull()).count()
    _show_df(
        f"{label} metric_group counts:",
        frame.groupBy("metric_group").count().orderBy("metric_group"),
    )
    _show_df(
        f"{label} denominator risk counts:",
        frame.agg(
            F.sum(F.when(F.col("ratio_unstable"), 1).otherwise(0)).alias("ratio_unstable"),
            F.sum(F.when(F.col("relative_change_unstable"), 1).otherwise(0)).alias(
                "relative_change_unstable"
            ),
            F.sum(F.when(F.col("small_denominator_flag"), 1).otherwise(0)).alias(
                "small_denominator_flag"
            ),
        ),
    )
    if null_count:
        _show_df(
            f"{label} metrics with null metric_group:",
            frame.where(F.col("metric_group").isNull()).select("metric_name").distinct(),
        )
        raise RuntimeError(f"{label} has metrics without metric_group: {null_count}")


def _write_and_validate(frame: DataFrame, path: str, label: str, sample_size: int) -> None:
    output_count = frame.count()
    LOGGER.info("%s output row count: %s", label, output_count)
    if output_count == 0:
        raise RuntimeError(f"{label} output row count is 0.")
    LOGGER.info("Writing %s: %s", label, path)
    frame.write.mode("errorifexists").parquet(path)
    readback = frame.sparkSession.read.parquet(path)
    LOGGER.info("%s readback schema:", label)
    readback.printSchema()
    readback_count = readback.count()
    LOGGER.info("%s readback count: %s", label, readback_count)
    if readback_count != output_count:
        raise RuntimeError(
            f"{label} readback count mismatch: expected={output_count}, actual={readback_count}"
        )
    _show_df(f"{label} readback sample:", readback, sample_size)


def _show_df(label: str, frame: DataFrame, rows: int = 20) -> None:
    LOGGER.info("%s", label)
    sample = frame.take(rows)
    if not sample:
        LOGGER.info("  <empty>")
        return
    for row in sample:
        LOGGER.info("  %s", row.asDict(recursive=True))


def _single_value(frame: DataFrame, label: str, column: str) -> Any:
    rows = frame.select(column).distinct().limit(2).collect()
    if len(rows) != 1:
        raise RuntimeError(f"{label}.{column} expected exactly one value, observed={len(rows)}")
    return rows[0][column]


def _validate_single_value(frame: DataFrame, label: str, column: str) -> None:
    _single_value(frame, label, column)


def _column_or_null(frame: DataFrame, column: str) -> Any:
    if column in frame.columns:
        return F.col(column)
    return F.lit(None)


def _resolve_path(value: str) -> Path:
    candidate = Path(value)
    if candidate.exists():
        return candidate
    bases = [Path.cwd()]
    job_file = _script_path()
    if job_file is not None:
        bases.insert(0, job_file.parents[1])
    for base in bases:
        resolved = base / value
        if resolved.exists():
            return resolved
    raise FileNotFoundError(f"Config file not found: {value}")


def _get_spark_session() -> Any:
    try:
        from databricks.connect import DatabricksSession  # type: ignore

        return DatabricksSession.builder.getOrCreate()
    except Exception:
        from pyspark.sql import SparkSession  # type: ignore

        return SparkSession.builder.getOrCreate()


if __name__ == "__main__":
    main()
