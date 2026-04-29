#!/usr/bin/env python
"""Finalize Phase 3A BTC multi-event trial from persisted comparison reports."""

from __future__ import annotations

import argparse
import importlib.util
import logging
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
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
from pyspark.sql import types as T  # noqa: E402

from quantlab_event_scanner.btc_multi_event_trial import (  # noqa: E402
    DEFAULT_NORMAL_COUNT_PER_EVENT,
    expected_comparison_rows,
    validate_phase3a_source_run_id,
)
from quantlab_event_scanner.config import load_config  # noqa: E402
from quantlab_event_scanner.paths import btc_multi_event_trial_run_path  # noqa: E402
from quantlab_event_scanner.profile_comparison_reports import (  # noqa: E402
    expected_bucket_change_profile_comparison_rows,
    expected_cross_exchange_mid_diff_comparison_rows,
    expected_exchange_profile_comparison_rows,
)


LOGGER = logging.getLogger("quantlab_event_scanner.phase3a_btc_multi_event_finalize")
DEFAULT_TOP_N = 20
DEFAULT_SMALL_OUTPUT_PARTITIONS = 1


@contextmanager
def _phase3a_finalize_timer(step: str) -> Any:
    start = time.monotonic()
    try:
        yield
    finally:
        LOGGER.info("[PHASE3A_FINALIZE_TIMER] step=%s seconds=%.3f", step, time.monotonic() - start)


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    source_run_id = validate_phase3a_source_run_id(args.source_run_id)
    config_path = _resolve_path(args.config)
    config = load_config(config_path)
    output_root = btc_multi_event_trial_run_path(config, source_run_id)
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)

    phase3a = _load_job_module("phase3a_btc_multi_event_trial", "13_build_btc_multi_event_trial.py")

    LOGGER.info("Using config: %s", config_path)
    LOGGER.info("Source run ID: %s", source_run_id)
    LOGGER.info("Output root: %s", output_root)
    for key, value in sorted(vars(args).items()):
        LOGGER.info("[PHASE3A_FINALIZE_CONF] key=%s value=%s", key, value)

    spark = _get_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    _log_spark_runtime_context(spark, source_run_id)

    with _phase3a_finalize_timer("read_source_outputs"):
        events = _read_required(spark, f"{output_root}/events", "events").cache()
        normal_samples = _read_required(spark, f"{output_root}/normal_samples", "normal_samples").cache()
        exchange_comparison = _read_required(
            spark,
            f"{output_root}/comparison_reports/exchange_profile_comparison",
            "comparison_reports.exchange_profile_comparison",
        ).cache()
        cross_comparison = _read_required(
            spark,
            f"{output_root}/comparison_reports/cross_exchange_mid_diff_comparison",
            "comparison_reports.cross_exchange_mid_diff_comparison",
        ).cache()
        bucket_comparison = _read_required(
            spark,
            f"{output_root}/comparison_reports/bucket_change_profile_comparison",
            "comparison_reports.bucket_change_profile_comparison",
        ).cache()

    selected_event_count = _count(events, "events")
    _validate_positive_count(selected_event_count, "events")

    with _phase3a_finalize_timer("source_validation"):
        _validate_count(
            exchange_comparison,
            "comparison.exchange_profile",
            expected_comparison_rows(selected_event_count, expected_exchange_profile_comparison_rows()),
        )
        _validate_count(
            cross_comparison,
            "comparison.cross_exchange_mid_diff",
            expected_comparison_rows(selected_event_count, expected_cross_exchange_mid_diff_comparison_rows()),
        )
        _validate_count(
            bucket_comparison,
            "comparison.bucket_change_profile",
            expected_comparison_rows(selected_event_count, expected_bucket_change_profile_comparison_rows()),
        )
        _validate_normal_selection(normal_samples, args.normal_count_per_event)
        for label, frame in (
            ("exchange_profile_comparison", exchange_comparison),
            ("cross_exchange_mid_diff_comparison", cross_comparison),
            ("bucket_change_profile_comparison", bucket_comparison),
        ):
            _validate_normal_sample_count(frame, label, args.normal_count_per_event)
            _validate_metric_group_coverage(frame, label)
            _validate_absolute_diff(frame, label)

    with _phase3a_finalize_timer("top_diff_build"):
        exchange_top = _small(phase3a._build_top_diffs(exchange_comparison, run_id=source_run_id, top_n=args.top_n, created_at=created_at), args.small_output_partitions).cache()
        cross_top = _small(phase3a._build_top_diffs(cross_comparison, run_id=source_run_id, top_n=args.top_n, created_at=created_at), args.small_output_partitions).cache()
        bucket_top = _small(phase3a._build_top_diffs(bucket_comparison, run_id=source_run_id, top_n=args.top_n, created_at=created_at), args.small_output_partitions).cache()
        rank_type_count = len(phase3a.PHASE3A_TOP_DIFF_RANKINGS)
        _validate_top_diff_count_no_collect(
            exchange_comparison,
            exchange_top,
            "top_diffs.exchange_profile",
            args.top_n,
            rank_type_count,
        )
        _validate_top_diff_count_no_collect(
            cross_comparison,
            cross_top,
            "top_diffs.cross_exchange_mid_diff",
            args.top_n,
            rank_type_count,
        )
        _validate_top_diff_count_no_collect(
            bucket_comparison,
            bucket_top,
            "top_diffs.bucket_change_profile",
            args.top_n,
            rank_type_count,
        )

    with _phase3a_finalize_timer("summary_build"):
        event_counts_by_direction = _small(events.groupBy("direction").count(), args.small_output_partitions)
        status_df = _small(_event_processing_status(events, normal_samples, created_at), args.small_output_partitions).cache()
        exclusion_reasons = _small(status_df.groupBy("processing_stage", "exclusion_reason").count(), args.small_output_partitions)
        normal_selection_quality = _small(phase3a._normal_selection_quality(normal_samples), args.small_output_partitions)
        metric_group_summary = _small(
            phase3a._metric_group_summary(exchange_comparison, cross_comparison, bucket_comparison),
            args.small_output_partitions,
        )
        top_metrics_by_abs_z = _small(
            phase3a._top_metrics_by_abs_z(
                exchange_comparison,
                cross_comparison,
                bucket_comparison,
                source_run_id,
                created_at,
                args.top_n,
            ),
            args.small_output_partitions,
        )
        top_metrics_by_percentile = _small(
            phase3a._top_metrics_by_percentile_extreme(
                exchange_comparison,
                cross_comparison,
                bucket_comparison,
                source_run_id,
                created_at,
                args.top_n,
            ),
            args.small_output_partitions,
        )

    with _phase3a_finalize_timer("write_finalize_outputs"):
        _write_and_validate(exchange_top, f"{output_root}/top_diffs/exchange_profile_top_diffs", "top_diffs.exchange_profile_top_diffs")
        _write_and_validate(cross_top, f"{output_root}/top_diffs/cross_exchange_mid_diff_top_diffs", "top_diffs.cross_exchange_mid_diff_top_diffs")
        _write_and_validate(bucket_top, f"{output_root}/top_diffs/bucket_change_profile_top_diffs", "top_diffs.bucket_change_profile_top_diffs")
        _write_and_validate(event_counts_by_direction, f"{output_root}/summary/event_counts_by_direction", "summary.event_counts_by_direction")
        _write_and_validate(top_metrics_by_abs_z, f"{output_root}/summary/top_metrics_by_abs_z", "summary.top_metrics_by_abs_z")
        _write_and_validate(top_metrics_by_percentile, f"{output_root}/summary/top_metrics_by_percentile_extreme", "summary.top_metrics_by_percentile_extreme")
        _write_and_validate(metric_group_summary, f"{output_root}/summary/metric_group_summary", "summary.metric_group_summary")
        _write_and_validate(exclusion_reasons, f"{output_root}/summary/exclusion_reasons", "summary.exclusion_reasons")
        _write_and_validate(normal_selection_quality, f"{output_root}/summary/normal_selection_quality", "summary.normal_selection_quality")
        _write_and_validate(status_df, f"{output_root}/summary/event_processing_status", "summary.event_processing_status")

    LOGGER.info(
        "[PHASE3A_OUTPUT] root=%s source_run_id=%s selected_event_count=%s stage=finalize_complete",
        output_root,
        source_run_id,
        selected_event_count,
    )
    LOGGER.info("Phase 3A BTC multi-event finalize complete.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Finalize a Phase 3A BTC multi-event trial.")
    parser.add_argument("--config", default="configs/dev.yaml")
    parser.add_argument("--source-run-id", required=True)
    parser.add_argument("--normal-count-per-event", type=int, default=DEFAULT_NORMAL_COUNT_PER_EVENT)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    parser.add_argument("--sample-size", type=int, default=0)
    parser.add_argument("--validation-mode", choices=("strict", "light"), default="light")
    parser.add_argument("--small-output-partitions", type=int, default=DEFAULT_SMALL_OUTPUT_PARTITIONS)
    return parser.parse_args()


def _resolve_path(path_value: str) -> Path:
    candidate = Path(path_value)
    if candidate.exists():
        return candidate
    bases = [Path.cwd()]
    job_file = _script_path()
    if job_file is not None:
        bases.insert(0, job_file.parents[1])
    for base in bases:
        resolved = base / path_value
        if resolved.exists():
            return resolved
    raise FileNotFoundError(f"Config file not found: {path_value}")


def _load_job_module(module_name: str, filename: str) -> ModuleType:
    job_dir = _script_path().parent if _script_path() is not None else Path.cwd() / "jobs"
    module_path = job_dir / filename
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _get_spark_session() -> Any:
    try:
        return spark  # type: ignore[name-defined]  # noqa: F821
    except NameError:
        from pyspark.sql import SparkSession

        return SparkSession.builder.appName("phase3a-btc-multi-event-finalize").getOrCreate()


def _log_spark_runtime_context(spark_session: Any, source_run_id: str) -> None:
    LOGGER.info("[PHASE3A_FINALIZE_CONF] key=source_run_id value=%s", source_run_id)
    for key in ("spark.app.id", "spark.app.name", "spark.master"):
        try:
            LOGGER.info("[PHASE3A_FINALIZE_CONF] key=%s value=%s", key, spark_session.conf.get(key))
        except Exception:
            LOGGER.info("[PHASE3A_FINALIZE_CONF] key=%s value=unavailable", key)


def _read_required(spark_session: Any, path: str, label: str) -> DataFrame:
    LOGGER.info("[PHASE3A_FINALIZE_INPUT] name=%s path=%s", label, path)
    return spark_session.read.parquet(path)


def _count(frame: DataFrame, label: str) -> int:
    count = frame.count()
    LOGGER.info("[PHASE3A_FINALIZE_DF] name=%s rows=%s partitions=%s schema=%s", label, count, frame.rdd.getNumPartitions(), ",".join(frame.columns))
    return count


def _validate_positive_count(count: int, label: str) -> None:
    if count <= 0:
        raise RuntimeError(f"{label} row count is 0.")


def _validate_count(frame: DataFrame, label: str, expected: int) -> int:
    observed = frame.count()
    LOGGER.info("%s expected vs observed rows: expected=%s observed=%s", label, expected, observed)
    if observed != expected:
        raise RuntimeError(f"{label} row count mismatch: expected={expected}, observed={observed}")
    return observed


def _validate_normal_selection(normal_samples: DataFrame, normal_count_per_event: int) -> None:
    invalid = (
        normal_samples.groupBy("matched_event_id")
        .count()
        .where(F.col("count") != F.lit(int(normal_count_per_event)))
        .count()
    )
    LOGGER.info("normal_samples selected normal count invalid event rows: %s", invalid)
    if invalid:
        raise RuntimeError("normal_samples selected normal count validation failed.")


def _validate_normal_sample_count(frame: DataFrame, label: str, normal_count_per_event: int) -> None:
    invalid = frame.where(F.col("normal_sample_count") != F.lit(int(normal_count_per_event))).count()
    LOGGER.info("%s normal_sample_count invalid rows: %s", label, invalid)
    if invalid:
        raise RuntimeError(f"{label} normal_sample_count validation failed.")


def _validate_metric_group_coverage(frame: DataFrame, label: str) -> None:
    null_count = frame.where(F.col("metric_group").isNull()).count()
    LOGGER.info("%s metric_group null count: %s", label, null_count)
    if null_count:
        raise RuntimeError(f"{label} metric_group contains null rows.")


def _validate_absolute_diff(frame: DataFrame, label: str) -> None:
    negative_count = frame.where(F.col("absolute_diff_vs_mean") < F.lit(0.0)).count()
    LOGGER.info("%s absolute_diff_vs_mean negative count: %s", label, negative_count)
    if negative_count:
        raise RuntimeError(f"{label} absolute_diff_vs_mean contains negative rows.")


def _validate_top_diff_count_no_collect(
    source: DataFrame,
    top_diffs: DataFrame,
    label: str,
    top_n: int,
    rank_type_count: int,
) -> None:
    keys = ["source_event_id", "report_group", "metric_group"]
    expected = source.groupBy(*keys).count().select(
        *[F.col(key) for key in keys],
        (F.least(F.col("count"), F.lit(int(top_n))) * F.lit(int(rank_type_count)))
        .cast("long")
        .alias("expected_count"),
    )
    observed = top_diffs.groupBy(*keys).count().select(
        *[F.col(key) for key in keys],
        F.col("count").cast("long").alias("observed_count"),
    )
    mismatch_count = (
        expected.join(observed, keys, "full_outer")
        .where(F.coalesce(F.col("expected_count"), F.lit(-1)) != F.coalesce(F.col("observed_count"), F.lit(-1)))
        .count()
    )
    LOGGER.info("%s top-diff group mismatch count: %s", label, mismatch_count)
    if mismatch_count:
        raise RuntimeError(f"{label} top-diff dynamic row count validation failed.")


def _event_processing_status(events: DataFrame, normal_samples: DataFrame, created_at: datetime) -> DataFrame:
    normal_counts = normal_samples.groupBy("matched_event_id").agg(F.count("*").cast("int").alias("normal_selection_count"))
    return (
        events.select("event_id", "event_rank", "direction")
        .join(normal_counts, F.col("event_id") == F.col("matched_event_id"), "left")
        .select(
            "event_id",
            "event_rank",
            "direction",
            F.lit("selected").alias("processing_stage"),
            F.lit(True).alias("selected"),
            F.lit(None).cast("string").alias("exclusion_reason"),
            F.coalesce(F.col("normal_selection_count"), F.lit(0)).alias("normal_selection_count"),
            F.lit("finalized_from_persisted_comparison_reports").alias("direction_balance_note"),
            F.lit("finalized").alias("row_count_status"),
            F.lit(created_at).cast(T.TimestampType()).alias("created_at"),
        )
    )


def _small(frame: DataFrame, partitions: int) -> DataFrame:
    return frame.coalesce(max(int(partitions), 1))


def _write_and_validate(frame: DataFrame, path: str, label: str) -> int:
    output_count = frame.count()
    LOGGER.info("[PHASE3A_FINALIZE_OUTPUT] name=%s path=%s rows=%s", label, path, output_count)
    if output_count <= 0:
        raise RuntimeError(f"{label} output row count is 0.")
    frame.write.mode("errorifexists").parquet(path)
    readback_count = frame.sparkSession.read.parquet(path).count()
    LOGGER.info("%s readback count: %s", label, readback_count)
    if readback_count != output_count:
        raise RuntimeError(
            f"{label} readback count mismatch: expected={output_count}, actual={readback_count}"
        )
    return output_count


if __name__ == "__main__":
    main()
