#!/usr/bin/env python
"""Phase 2D trial pre-event market snapshot profile report."""

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
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from quantlab_event_scanner.config import load_config  # noqa: E402
from quantlab_event_scanner.paths import (  # noqa: E402
    pre_event_market_snapshots_trial_run_path,
    pre_event_profile_reports_trial_run_path,
)
from quantlab_event_scanner.pre_event_market_snapshots import exchange_coverage  # noqa: E402
from quantlab_event_scanner.pre_event_profile_reports import (  # noqa: E402
    BUCKET_CHANGE_PAIRS,
    PROFILE_VERSION,
    PROFILE_WINDOWS,
    default_phase2d_run_id,
)


LOGGER = logging.getLogger("quantlab_event_scanner.phase2d_pre_event_profile_report")

DEFAULT_SOURCE_SNAPSHOT_RUN_ID = "phase2c_20260427T082248Z"
DEFAULT_EVENT_ID = "binance_btcusdt_20260423_down_001"
DEFAULT_LOOKBACK_SECONDS = 300

REQUIRED_COLUMNS = (
    "event_id",
    "event_direction",
    "event_start_ts",
    "window_start_ts",
    "window_end_ts",
    "lookback_seconds",
    "exchange",
    "symbol",
    "second_before_event",
    "ts_second",
    "trade_count",
    "trade_volume",
    "trade_notional",
    "buy_qty",
    "sell_qty",
    "bbo_update_count",
    "is_bbo_forward_filled",
    "last_trade_price",
    "last_mid_price",
    "last_spread_bps",
    "avg_spread_bps",
    "last_book_imbalance",
    "avg_book_imbalance",
    "bbo_quote_age_seconds",
)

EXCHANGE_PROFILE_COLUMNS = (
    "run_id",
    "source_snapshot_run_id",
    "event_id",
    "event_direction",
    "event_start_ts",
    "profile_version",
    "created_at",
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
)

CROSS_EXCHANGE_COLUMNS = (
    "run_id",
    "source_snapshot_run_id",
    "event_id",
    "event_direction",
    "event_start_ts",
    "profile_version",
    "created_at",
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
)

BUCKET_CHANGE_COLUMNS = (
    "run_id",
    "source_snapshot_run_id",
    "event_id",
    "event_direction",
    "event_start_ts",
    "profile_version",
    "created_at",
    "exchange",
    "symbol",
    "metric_name",
    "from_bucket",
    "to_bucket",
    "from_bucket_value",
    "to_bucket_value",
    "absolute_change",
    "relative_change",
)


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config_path = _resolve_path(args.config)
    config = load_config(config_path)
    run_id = args.run_id or default_phase2d_run_id(datetime.now(timezone.utc))
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    source_path = pre_event_market_snapshots_trial_run_path(
        config,
        args.source_snapshot_run_id,
    )
    output_path = pre_event_profile_reports_trial_run_path(config, run_id)

    LOGGER.info("Using config: %s", config_path)
    LOGGER.info("Source snapshot run ID: %s", args.source_snapshot_run_id)
    LOGGER.info("Event ID: %s", args.event_id)
    LOGGER.info("Lookback seconds: %s", args.lookback_seconds)
    LOGGER.info("Profile version: %s", PROFILE_VERSION)
    LOGGER.info("Source snapshot path: %s", source_path)
    LOGGER.info("Output path: %s", output_path)

    spark = _get_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    LOGGER.info("Spark timezone: %s", spark.conf.get("spark.sql.session.timeZone"))

    snapshot = spark.read.parquet(source_path).where(F.col("event_id") == args.event_id).cache()
    _validate_required_columns(snapshot, REQUIRED_COLUMNS)
    snapshot_count = snapshot.count()
    LOGGER.info("Input snapshot row count: %s", snapshot_count)
    if snapshot_count == 0:
        raise RuntimeError("Selected snapshot row count is 0.")

    metadata = _read_single_metadata(snapshot)
    if int(metadata["lookback_seconds"]) != args.lookback_seconds:
        raise RuntimeError(
            "Lookback seconds mismatch: "
            f"input={metadata['lookback_seconds']}, arg={args.lookback_seconds}"
        )
    LOGGER.info("Event/window metadata summary: %s", metadata)

    _log_exchange_coverage("snapshot input", config.exchanges, _observed_exchanges(snapshot))
    _validate_snapshot_shape(snapshot, config.exchanges, args.lookback_seconds)
    _log_snapshot_profile(snapshot)

    windows = _profile_windows_frame(spark)
    exchange_profile = _build_exchange_profile(
        snapshot,
        windows,
        run_id=run_id,
        source_snapshot_run_id=args.source_snapshot_run_id,
        created_at=created_at,
    ).cache()
    cross_exchange_mid_diff = _build_cross_exchange_mid_diff(
        snapshot,
        windows,
        config.exchanges,
        run_id=run_id,
        source_snapshot_run_id=args.source_snapshot_run_id,
        created_at=created_at,
    ).cache()
    bucket_change_profile = _build_bucket_change_profile(
        spark,
        exchange_profile,
        run_id=run_id,
        source_snapshot_run_id=args.source_snapshot_run_id,
        created_at=created_at,
    ).cache()

    _write_and_validate_report(
        exchange_profile,
        f"{output_path}/exchange_profile",
        "exchange_profile",
        args.sample_size,
    )
    _write_and_validate_report(
        cross_exchange_mid_diff,
        f"{output_path}/cross_exchange_mid_diff",
        "cross_exchange_mid_diff",
        args.sample_size,
    )
    _write_and_validate_report(
        bucket_change_profile,
        f"{output_path}/bucket_change_profile",
        "bucket_change_profile",
        args.sample_size,
    )

    exchange_profile.unpersist()
    cross_exchange_mid_diff.unpersist()
    bucket_change_profile.unpersist()
    snapshot.unpersist()
    LOGGER.info("Phase 2D trial pre-event profile report complete.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile Phase 2C trial market snapshots.")
    parser.add_argument("--config", default="configs/dev.yaml")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--source-snapshot-run-id", default=DEFAULT_SOURCE_SNAPSHOT_RUN_ID)
    parser.add_argument("--event-id", default=DEFAULT_EVENT_ID)
    parser.add_argument("--lookback-seconds", type=int, default=DEFAULT_LOOKBACK_SECONDS)
    parser.add_argument("--sample-size", type=int, default=20)
    return parser.parse_args()


def _validate_required_columns(frame: DataFrame, required_columns: tuple[str, ...]) -> None:
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise RuntimeError(f"Missing required snapshot columns: {', '.join(missing)}")


def _read_single_metadata(frame: DataFrame) -> dict[str, Any]:
    metadata_columns = (
        "event_id",
        "event_direction",
        "event_start_ts",
        "window_start_ts",
        "window_end_ts",
        "lookback_seconds",
    )
    metadata = frame.select(*metadata_columns).distinct()
    metadata_count = metadata.count()
    if metadata_count != 1:
        raise RuntimeError(f"Expected exactly one event/window metadata row, found {metadata_count}.")
    return metadata.first().asDict(recursive=True)


def _observed_exchanges(frame: DataFrame) -> tuple[str, ...]:
    return tuple(
        row["exchange"]
        for row in frame.select(F.lower(F.col("exchange")).alias("exchange"))
        .where(F.col("exchange").isNotNull())
        .distinct()
        .orderBy("exchange")
        .collect()
    )


def _log_exchange_coverage(
    label: str,
    expected_exchanges: tuple[str, ...],
    observed_exchanges: tuple[str, ...],
) -> None:
    coverage = exchange_coverage(expected_exchanges, observed_exchanges)
    LOGGER.info(
        "%s exchange coverage: expected=%s observed=%s missing=%s",
        label,
        ", ".join(coverage.expected) or "none",
        ", ".join(coverage.observed) or "none",
        ", ".join(coverage.missing) or "none",
    )
    if coverage.missing:
        raise RuntimeError(f"Missing required {label} exchange coverage: {coverage.missing}")


def _validate_snapshot_shape(
    snapshot: DataFrame,
    expected_exchanges: tuple[str, ...],
    lookback_seconds: int,
) -> None:
    second_range = snapshot.agg(
        F.min("second_before_event").alias("min_second_before_event"),
        F.max("second_before_event").alias("max_second_before_event"),
    ).first()
    if second_range["min_second_before_event"] != 1:
        raise RuntimeError(
            f"Expected min second_before_event=1, found {second_range['min_second_before_event']}."
        )
    if second_range["max_second_before_event"] != lookback_seconds:
        raise RuntimeError(
            "Expected max second_before_event="
            f"{lookback_seconds}, found {second_range['max_second_before_event']}."
        )

    counts = (
        snapshot.groupBy(F.lower(F.col("exchange")).alias("exchange"), "symbol")
        .count()
        .collect()
    )
    expected_set = {exchange.lower() for exchange in expected_exchanges}
    for row in counts:
        if row["exchange"] in expected_set and row["count"] != lookback_seconds:
            raise RuntimeError(
                "Expected exactly "
                f"{lookback_seconds} seconds for {row['exchange']} {row['symbol']}, "
                f"found {row['count']}."
            )


def _log_snapshot_profile(snapshot: DataFrame) -> None:
    _show_df("Snapshot count by exchange:", snapshot.groupBy("exchange").count().orderBy("exchange"))
    _show_df(
        "Snapshot count by exchange + symbol:",
        snapshot.groupBy("exchange", "symbol").count().orderBy("exchange", "symbol"),
    )
    _show_df(
        "Snapshot second_before_event range:",
        snapshot.agg(
            F.min("second_before_event").alias("min_second_before_event"),
            F.max("second_before_event").alias("max_second_before_event"),
        ),
    )


def _profile_windows_frame(spark: Any) -> DataFrame:
    rows = [
        (
            window.mode,
            window.label,
            window.min_second_before_event,
            window.max_second_before_event,
        )
        for window in PROFILE_WINDOWS
    ]
    return spark.createDataFrame(
        rows,
        (
            "window_mode",
            "window_label",
            "window_min_second_before_event",
            "window_max_second_before_event",
        ),
    )


def _build_exchange_profile(
    snapshot: DataFrame,
    windows: DataFrame,
    *,
    run_id: str,
    source_snapshot_run_id: str,
    created_at: datetime,
) -> DataFrame:
    windowed = (
        snapshot.crossJoin(windows)
        .where(F.col("second_before_event") >= F.col("window_min_second_before_event"))
        .where(F.col("second_before_event") <= F.col("window_max_second_before_event"))
    )
    partition = Window.partitionBy(
        "event_id",
        "exchange",
        "symbol",
        "window_mode",
        "window_label",
    )
    ranked = (
        windowed.withColumn(
            "first_rank",
            F.row_number().over(partition.orderBy(F.col("second_before_event").desc())),
        )
        .withColumn(
            "last_rank",
            F.row_number().over(partition.orderBy(F.col("second_before_event").asc())),
        )
        .withColumn("trade_imbalance_qty", F.col("buy_qty") - F.col("sell_qty"))
    )
    grouped = ranked.groupBy(
        "event_id",
        "event_direction",
        "event_start_ts",
        "exchange",
        "symbol",
        "window_mode",
        "window_label",
        "window_min_second_before_event",
        "window_max_second_before_event",
    ).agg(
        F.count("*").alias("seconds_observed"),
        F.sum(F.when(F.col("trade_count") > 0, 1).otherwise(0)).alias("seconds_with_trades"),
        F.sum(F.when(F.col("bbo_update_count") > 0, 1).otherwise(0)).alias(
            "seconds_with_bbo_update"
        ),
        F.sum(F.when(F.col("is_bbo_forward_filled"), 1).otherwise(0)).alias(
            "seconds_forward_filled_bbo"
        ),
        F.sum("trade_count").alias("trade_count_sum"),
        F.avg("trade_count").alias("trade_count_per_second_avg"),
        F.max("trade_count").alias("trade_count_per_second_max"),
        F.sum("trade_volume").alias("trade_volume_sum"),
        F.sum("trade_notional").alias("trade_notional_sum"),
        F.sum("buy_qty").alias("buy_qty_sum"),
        F.sum("sell_qty").alias("sell_qty_sum"),
        F.sum("trade_imbalance_qty").alias("trade_imbalance_qty_sum"),
        F.avg("trade_imbalance_qty").alias("trade_imbalance_qty_per_second"),
        F.max(F.when(F.col("first_rank") == 1, F.col("last_trade_price"))).alias(
            "first_last_trade_price"
        ),
        F.max(F.when(F.col("last_rank") == 1, F.col("last_trade_price"))).alias(
            "last_last_trade_price"
        ),
        F.min("last_trade_price").alias("min_last_trade_price"),
        F.max("last_trade_price").alias("max_last_trade_price"),
        F.max(F.when(F.col("first_rank") == 1, F.col("last_mid_price"))).alias(
            "first_last_mid_price"
        ),
        F.max(F.when(F.col("last_rank") == 1, F.col("last_mid_price"))).alias(
            "last_last_mid_price"
        ),
        F.avg("avg_spread_bps").alias("avg_spread_bps_mean"),
        F.max("avg_spread_bps").alias("avg_spread_bps_max"),
        F.max(F.when(F.col("last_rank") == 1, F.col("last_spread_bps"))).alias(
            "last_spread_bps_last"
        ),
        F.avg("avg_book_imbalance").alias("avg_book_imbalance_mean"),
        F.max(F.when(F.col("last_rank") == 1, F.col("last_book_imbalance"))).alias(
            "last_book_imbalance_last"
        ),
        F.max("bbo_quote_age_seconds").alias("bbo_quote_age_seconds_max"),
    )
    with_ratios = (
        grouped.withColumn(
            "trade_imbalance_qty_ratio",
            F.when(
                (F.col("buy_qty_sum") + F.col("sell_qty_sum")) != 0,
                F.col("trade_imbalance_qty_sum")
                / (F.col("buy_qty_sum") + F.col("sell_qty_sum")),
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
        F.lit(run_id).alias("run_id"),
        F.lit(source_snapshot_run_id).alias("source_snapshot_run_id"),
        F.col("event_id"),
        F.col("event_direction"),
        F.col("event_start_ts"),
        F.lit(PROFILE_VERSION).alias("profile_version"),
        F.lit(created_at).cast("timestamp").alias("created_at"),
        F.col("exchange"),
        F.col("symbol"),
        F.col("window_mode"),
        F.col("window_label"),
        F.col("window_min_second_before_event"),
        F.col("window_max_second_before_event"),
        *[F.col(column) for column in EXCHANGE_PROFILE_COLUMNS[13:]],
    ).select(*EXCHANGE_PROFILE_COLUMNS)


def _build_cross_exchange_mid_diff(
    snapshot: DataFrame,
    windows: DataFrame,
    expected_exchanges: tuple[str, ...],
    *,
    run_id: str,
    source_snapshot_run_id: str,
    created_at: datetime,
) -> DataFrame:
    pairs = _exchange_pairs_frame(snapshot.sparkSession, expected_exchanges)
    base = snapshot.select(
        "event_id",
        "event_direction",
        "event_start_ts",
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
            (F.col("a.event_id") == F.col("b.event_id"))
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
            F.col("a.event_id").alias("event_id"),
            F.col("a.event_direction").alias("event_direction"),
            F.col("a.event_start_ts").alias("event_start_ts"),
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
            F.when(
                F.col("mid_exchange_b") != 0,
                F.col("mid_diff") / F.col("mid_exchange_b") * F.lit(10000.0),
            ),
        )
        .withColumn("abs_mid_diff_bps", F.abs(F.col("mid_diff_bps")))
    )
    windowed = (
        joined.crossJoin(windows)
        .where(F.col("second_before_event") >= F.col("window_min_second_before_event"))
        .where(F.col("second_before_event") <= F.col("window_max_second_before_event"))
    )
    partition = Window.partitionBy(
        "event_id",
        "symbol_key",
        "window_mode",
        "window_label",
        "exchange_pair",
    )
    ranked = windowed.withColumn(
        "last_rank",
        F.row_number().over(partition.orderBy(F.col("second_before_event").asc())),
    )
    grouped = ranked.groupBy(
        "event_id",
        "event_direction",
        "event_start_ts",
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
        F.max(F.when(F.col("last_rank") == 1, F.col("mid_diff_bps"))).alias(
            "last_mid_diff_bps"
        ),
        F.max(F.when(F.col("last_rank") == 1, F.col("abs_mid_diff_bps"))).alias(
            "last_abs_mid_diff_bps"
        ),
    )
    return grouped.select(
        F.lit(run_id).alias("run_id"),
        F.lit(source_snapshot_run_id).alias("source_snapshot_run_id"),
        F.col("event_id"),
        F.col("event_direction"),
        F.col("event_start_ts"),
        F.lit(PROFILE_VERSION).alias("profile_version"),
        F.lit(created_at).cast("timestamp").alias("created_at"),
        *[F.col(column) for column in CROSS_EXCHANGE_COLUMNS[7:]],
    ).select(*CROSS_EXCHANGE_COLUMNS)


def _exchange_pairs_frame(spark: Any, exchanges: tuple[str, ...]) -> DataFrame:
    lowered = [exchange.lower() for exchange in exchanges]
    rows = [
        (lowered[left], lowered[right], f"{lowered[left]}_{lowered[right]}")
        for left in range(len(lowered))
        for right in range(left + 1, len(lowered))
    ]
    return spark.createDataFrame(rows, ("exchange_a", "exchange_b", "exchange_pair"))


def _build_bucket_change_profile(
    spark: Any,
    exchange_profile: DataFrame,
    *,
    run_id: str,
    source_snapshot_run_id: str,
    created_at: datetime,
) -> DataFrame:
    bucket_pairs = spark.createDataFrame(
        BUCKET_CHANGE_PAIRS,
        ("from_bucket", "to_bucket"),
    )
    bucket_metrics = (
        exchange_profile.where(F.col("window_mode") == "bucket")
        .select(
            "event_id",
            "event_direction",
            "event_start_ts",
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
            & (F.col("from_metric.event_id") == F.col("to_metric.event_id"))
            & (F.col("from_metric.exchange") == F.col("to_metric.exchange"))
            & (F.col("from_metric.symbol") == F.col("to_metric.symbol"))
            & (F.col("from_metric.metric_name") == F.col("to_metric.metric_name")),
            "inner",
        )
        .select(
            F.col("from_metric.event_id").alias("event_id"),
            F.col("from_metric.event_direction").alias("event_direction"),
            F.col("from_metric.event_start_ts").alias("event_start_ts"),
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
        F.lit(run_id).alias("run_id"),
        F.lit(source_snapshot_run_id).alias("source_snapshot_run_id"),
        F.col("event_id"),
        F.col("event_direction"),
        F.col("event_start_ts"),
        F.lit(PROFILE_VERSION).alias("profile_version"),
        F.lit(created_at).cast("timestamp").alias("created_at"),
        *[F.col(column) for column in BUCKET_CHANGE_COLUMNS[7:]],
    ).select(*BUCKET_CHANGE_COLUMNS)


def _write_and_validate_report(
    frame: DataFrame,
    path: str,
    label: str,
    sample_size: int,
) -> None:
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
    _show_df(
        f"{label} readback sample:",
        readback.orderBy(*_sample_order_columns(label)),
        sample_size,
    )


def _sample_order_columns(label: str) -> tuple[str, ...]:
    if label == "exchange_profile":
        return ("exchange", "symbol", "window_mode", "window_label")
    if label == "cross_exchange_mid_diff":
        return ("symbol_key", "exchange_pair", "window_mode", "window_label")
    return ("exchange", "symbol", "metric_name", "from_bucket")


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


if __name__ == "__main__":
    main()
