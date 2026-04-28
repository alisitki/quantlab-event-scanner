#!/usr/bin/env python
"""Phase 2G trial profile comparison top-diff inspection."""

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

from pyspark.sql import DataFrame, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from quantlab_event_scanner.config import load_config  # noqa: E402
from quantlab_event_scanner.paths import (  # noqa: E402
    profile_comparison_reports_trial_run_path,
    profile_comparison_top_diffs_trial_run_path,
)
from quantlab_event_scanner.profile_comparison_top_diffs import (  # noqa: E402
    TOP_DIFF_RANKING_DEFINITIONS,
    TOP_DIFF_REQUIRED_COLUMNS,
    TOP_DIFF_TIE_BREAKER_COLUMNS,
    default_phase2g_run_id,
    expected_top_diff_rows_for_metric_group,
    expected_top_diff_rows_from_group_counts,
    validate_required_columns,
)


LOGGER = logging.getLogger("quantlab_event_scanner.phase2g_profile_comparison_top_diffs")

DEFAULT_SOURCE_COMPARISON_RUN_ID = "phase2f_20260427T113721Z"
DEFAULT_TOP_N = 20
ABSOLUTE_DIFF_TOLERANCE = 1e-9

SOURCE_SUBTABLES = (
    ("exchange_profile", "exchange_profile_comparison", "exchange_profile_top_diffs", 660),
    (
        "cross_exchange_mid_diff",
        "cross_exchange_mid_diff_comparison",
        "cross_exchange_mid_diff_top_diffs",
        150,
    ),
    (
        "bucket_change_profile",
        "bucket_change_profile_comparison",
        "bucket_change_profile_top_diffs",
        96,
    ),
)

OUTPUT_COLUMNS = (
    "run_id",
    "source_comparison_run_id",
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
    "normal_value",
    "signed_diff",
    "absolute_diff",
    "ratio",
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
    "normal_window_start_ts",
    "normal_window_end_ts",
    "normal_anchor_ts",
    "profile_version",
    "created_at",
)


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config_path = _resolve_path(args.config)
    config = load_config(config_path)
    run_id = args.run_id or default_phase2g_run_id(datetime.now(timezone.utc))
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    source_path = profile_comparison_reports_trial_run_path(
        config,
        args.source_comparison_run_id,
    )
    output_path = profile_comparison_top_diffs_trial_run_path(config, run_id)

    LOGGER.info("Using config: %s", config_path)
    LOGGER.info("Run ID: %s", run_id)
    LOGGER.info("Source comparison run ID: %s", args.source_comparison_run_id)
    LOGGER.info("Top N: %s", args.top_n)
    LOGGER.info("Source comparison path: %s", source_path)
    LOGGER.info("Output path: %s", output_path)

    spark = _get_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    LOGGER.info("Spark timezone: %s", spark.conf.get("spark.sql.session.timeZone"))

    for report_group, source_subtable, output_subtable, expected_source_count in SOURCE_SUBTABLES:
        label = f"{report_group}_top_diffs"
        source = _read_source_subtable(
            spark,
            source_path,
            source_subtable,
            label,
        ).cache()
        _validate_source_contract(
            source,
            label,
            args.source_comparison_run_id,
            expected_source_count,
        )
        _log_metadata_summary(source, label)
        _log_signed_diff_distribution(source, label)
        _log_source_metric_group_counts(source, label, args.top_n)

        top_diffs = _build_top_diffs(
            source,
            run_id=run_id,
            top_n=args.top_n,
            created_at=created_at,
        ).cache()
        _validate_metric_group_output_counts(source, top_diffs, label, args.top_n)
        _log_top_diffs(top_diffs, label, args.top_n)
        _write_and_validate(
            top_diffs,
            f"{output_path}/{output_subtable}",
            output_subtable,
            args.sample_size,
        )
        top_diffs.unpersist()
        source.unpersist()

    LOGGER.info("Phase 2G profile comparison top-diff inspection complete.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect Phase 2F top comparison diffs.")
    parser.add_argument("--config", default="configs/dev.yaml")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--source-comparison-run-id", default=DEFAULT_SOURCE_COMPARISON_RUN_ID)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    parser.add_argument("--sample-size", type=int, default=20)
    return parser.parse_args()


def _read_source_subtable(
    spark: Any,
    source_path: str,
    subtable: str,
    label: str,
) -> DataFrame:
    path = f"{source_path}/{subtable}"
    LOGGER.info("%s source path: %s", label, path)
    return spark.read.parquet(path)


def _validate_source_contract(
    frame: DataFrame,
    label: str,
    source_comparison_run_id: str,
    expected_source_count: int,
) -> None:
    validate_required_columns(frame.columns, TOP_DIFF_REQUIRED_COLUMNS)
    source_count = frame.count()
    LOGGER.info(
        "%s source row count: expected=%s observed=%s",
        label,
        expected_source_count,
        source_count,
    )
    if source_count != expected_source_count:
        raise RuntimeError(
            f"{label} source row count mismatch: expected={expected_source_count}, "
            f"observed={source_count}"
        )
    null_metric_group_count = frame.where(F.col("metric_group").isNull()).count()
    LOGGER.info("%s null metric_group count: %s", label, null_metric_group_count)
    if null_metric_group_count:
        raise RuntimeError(f"{label} has null metric_group rows: {null_metric_group_count}")
    _validate_source_comparison_run_id(frame, label, source_comparison_run_id)
    _validate_absolute_diff_contract(frame, label)


def _validate_source_comparison_run_id(
    frame: DataFrame,
    label: str,
    expected_run_id: str,
) -> None:
    rows = frame.select("comparison_run_id").distinct().limit(2).collect()
    observed = [row["comparison_run_id"] for row in rows]
    LOGGER.info("%s comparison_run_id values: %s", label, observed)
    if observed != [expected_run_id]:
        raise RuntimeError(
            f"{label} comparison_run_id mismatch: expected exactly {expected_run_id}, "
            f"observed={observed}"
        )


def _validate_absolute_diff_contract(frame: DataFrame, label: str) -> None:
    negative_count = frame.where(F.col("absolute_diff").isNotNull() & (F.col("absolute_diff") < 0)).count()
    mismatch_count = frame.where(
        F.col("signed_diff").isNotNull()
        & F.col("absolute_diff").isNotNull()
        & (
            F.abs(F.abs(F.col("signed_diff")) - F.col("absolute_diff"))
            > F.lit(ABSOLUTE_DIFF_TOLERANCE)
        )
    ).count()
    LOGGER.info(
        "%s absolute_diff validation: negative_count=%s mismatch_count=%s",
        label,
        negative_count,
        mismatch_count,
    )
    if negative_count:
        raise RuntimeError(f"{label} has negative absolute_diff rows: {negative_count}")
    if mismatch_count:
        raise RuntimeError(
            f"{label} absolute_diff does not match abs(signed_diff): {mismatch_count}"
        )


def _log_metadata_summary(frame: DataFrame, label: str) -> None:
    summary = frame.select(
        "source_event_id",
        "event_direction",
        "event_start_ts",
        "normal_window_start_ts",
        "normal_window_end_ts",
        "normal_anchor_ts",
        "profile_version",
    ).distinct()
    _show_df(f"{label} metadata summary:", summary)


def _log_signed_diff_distribution(frame: DataFrame, label: str) -> None:
    distribution = frame.agg(
        F.sum(F.when(F.col("signed_diff") > 0, 1).otherwise(0)).alias("positive_count"),
        F.sum(F.when(F.col("signed_diff") < 0, 1).otherwise(0)).alias("negative_count"),
        F.sum(F.when(F.col("signed_diff") == 0, 1).otherwise(0)).alias("zero_count"),
        F.sum(F.when(F.col("signed_diff").isNull(), 1).otherwise(0)).alias("null_count"),
    )
    _show_df(f"{label} signed_diff distribution:", distribution)


def _build_top_diffs(
    frame: DataFrame,
    *,
    run_id: str,
    top_n: int,
    created_at: datetime,
) -> DataFrame:
    outputs: list[DataFrame] = []
    for definition in TOP_DIFF_RANKING_DEFINITIONS:
        outputs.append(
            _rank_top_diffs(
                frame,
                rank_type=definition.rank_type,
                primary_column=definition.primary_column,
                ascending=definition.ascending,
                run_id=run_id,
                top_n=top_n,
                created_at=created_at,
            )
        )
    output = outputs[0]
    for ranked in outputs[1:]:
        output = output.unionByName(ranked)
    return output.select(*OUTPUT_COLUMNS)


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
            "normal_value",
            "signed_diff",
            "absolute_diff",
            "ratio",
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
            "normal_window_start_ts",
            "normal_window_end_ts",
            "normal_anchor_ts",
            "profile_version",
            F.lit(created_at).cast("timestamp").alias("created_at"),
        )
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


def _log_source_metric_group_counts(frame: DataFrame, label: str, top_n: int) -> None:
    rows = (
        frame.groupBy("report_group", "metric_group")
        .count()
        .orderBy("report_group", "metric_group")
        .collect()
    )
    LOGGER.info("%s metric_group source counts:", label)
    for row in rows:
        LOGGER.info(
            "  report_group=%s metric_group=%s source_rows=%s expected_rows_for_group=%s",
            row["report_group"],
            row["metric_group"],
            row["count"],
            expected_top_diff_rows_for_metric_group(row["count"], top_n),
        )


def _validate_metric_group_output_counts(
    source: DataFrame,
    output: DataFrame,
    label: str,
    top_n: int,
) -> None:
    source_rows = (
        source.groupBy("report_group", "metric_group")
        .count()
        .orderBy("report_group", "metric_group")
        .collect()
    )
    output_rows = {
        (row["report_group"], row["metric_group"]): row["count"]
        for row in output.groupBy("report_group", "metric_group").count().collect()
    }
    source_counts = [row["count"] for row in source_rows]
    total_expected = expected_top_diff_rows_from_group_counts(source_counts, top_n)
    _validate_expected_row_count(output, label, total_expected)
    LOGGER.info("%s metric_group output counts:", label)
    for row in source_rows:
        key = (row["report_group"], row["metric_group"])
        expected = expected_top_diff_rows_for_metric_group(row["count"], top_n)
        actual = output_rows.get(key, 0)
        LOGGER.info(
            "  report_group=%s metric_group=%s source_rows=%s "
            "expected_rows_for_group=%s actual_rows_for_group=%s",
            row["report_group"],
            row["metric_group"],
            row["count"],
            expected,
            actual,
        )
        if actual != expected:
            raise RuntimeError(
                f"{label} metric_group row count mismatch for {key}: "
                f"expected={expected}, actual={actual}"
            )


def _log_top_diffs(frame: DataFrame, label: str, top_n: int) -> None:
    metric_group_count = frame.select("metric_group").distinct().count()
    for definition in TOP_DIFF_RANKING_DEFINITIONS:
        sample = (
            frame.where(F.col("rank_type") == definition.rank_type)
            .orderBy("metric_group", "rank")
            .limit(top_n * metric_group_count)
        )
        _show_df(
            f"{label} {definition.rank_type} top {top_n} by metric_group:",
            sample,
            top_n * metric_group_count,
        )


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
