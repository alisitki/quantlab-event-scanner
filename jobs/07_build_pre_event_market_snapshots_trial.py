#!/usr/bin/env python
"""Phase 2C trial 1-second market snapshot build."""

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
    pre_event_bbo_windows_trial_run_path,
    pre_event_market_snapshots_trial_run_path,
    pre_event_windows_trial_run_path,
)
from quantlab_event_scanner.pre_event_market_snapshots import (  # noqa: E402
    METADATA_COMPATIBILITY_FIELDS,
    default_phase2c_run_id,
    exchange_coverage,
    validate_window_metadata_compatibility,
)


LOGGER = logging.getLogger("quantlab_event_scanner.phase2c_pre_event_market_snapshots")

DEFAULT_SOURCE_TRADE_RUN_ID = "phase2a_20260427T071919Z"
DEFAULT_SOURCE_BBO_RUN_ID = "phase2b_20260427T074205Z"
DEFAULT_EVENT_ID = "binance_btcusdt_20260423_down_001"
DEFAULT_LOOKBACK_SECONDS = 300

TRADE_ZERO_COLUMNS = (
    "trade_count",
    "trade_volume",
    "trade_notional",
    "buy_trade_count",
    "sell_trade_count",
    "buy_qty",
    "sell_qty",
    "buy_notional",
    "sell_notional",
    "trade_imbalance_qty",
)
BBO_LAST_COLUMNS = (
    "last_bid_price",
    "last_ask_price",
    "last_bid_qty",
    "last_ask_qty",
    "last_mid_price",
    "last_spread",
    "last_spread_bps",
    "last_book_imbalance",
)
OUTPUT_COLUMNS = (
    "run_id",
    "source_trade_run_id",
    "source_bbo_run_id",
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
    "second_before_event",
    "ts_second",
    "trade_count",
    "trade_volume",
    "trade_notional",
    "buy_trade_count",
    "sell_trade_count",
    "buy_qty",
    "sell_qty",
    "buy_notional",
    "sell_notional",
    "trade_imbalance_qty",
    "last_trade_price",
    "min_trade_price",
    "max_trade_price",
    "bbo_update_count",
    "has_bbo_update",
    "is_bbo_forward_filled",
    "last_bbo_update_ts",
    "bbo_quote_age_seconds",
    "last_bid_price",
    "last_ask_price",
    "last_bid_qty",
    "last_ask_qty",
    "last_mid_price",
    "last_spread",
    "last_spread_bps",
    "avg_spread_bps",
    "max_spread_bps",
    "last_book_imbalance",
    "avg_book_imbalance",
    "created_at",
)


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config_path = _resolve_path(args.config)
    config = load_config(config_path)
    run_id = args.run_id or default_phase2c_run_id(datetime.now(timezone.utc))
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)

    trade_path = pre_event_windows_trial_run_path(config, args.source_trade_run_id)
    bbo_path = pre_event_bbo_windows_trial_run_path(config, args.source_bbo_run_id)
    output_path = pre_event_market_snapshots_trial_run_path(config, run_id)

    LOGGER.info("Using config: %s", config_path)
    LOGGER.info("Source trade run ID: %s", args.source_trade_run_id)
    LOGGER.info("Source BBO run ID: %s", args.source_bbo_run_id)
    LOGGER.info("Event ID: %s", args.event_id)
    LOGGER.info("Lookback seconds: %s", args.lookback_seconds)
    LOGGER.info("Trade input path: %s", trade_path)
    LOGGER.info("BBO input path: %s", bbo_path)
    LOGGER.info("Output path: %s", output_path)

    spark = _get_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    LOGGER.info("Spark timezone: %s", spark.conf.get("spark.sql.session.timeZone"))

    trade_input = spark.read.parquet(trade_path).where(F.col("event_id") == args.event_id).cache()
    bbo_input = spark.read.parquet(bbo_path).where(F.col("event_id") == args.event_id).cache()
    trade_count = trade_input.count()
    bbo_count = bbo_input.count()
    LOGGER.info("Input trade row count: %s", trade_count)
    LOGGER.info("Input BBO row count: %s", bbo_count)
    if trade_count == 0:
        raise RuntimeError("Selected trade input row count is 0.")
    if bbo_count == 0:
        raise RuntimeError("Selected BBO input row count is 0.")

    trade_metadata = _read_single_metadata(trade_input, "trade")
    bbo_metadata = _read_single_metadata(bbo_input, "bbo")
    validate_window_metadata_compatibility(trade_metadata, bbo_metadata)
    if int(trade_metadata["lookback_seconds"]) != args.lookback_seconds:
        raise RuntimeError(
            "Lookback seconds mismatch: "
            f"input={trade_metadata['lookback_seconds']}, arg={args.lookback_seconds}"
        )
    LOGGER.info("Trade/BBO metadata compatibility summary: %s", trade_metadata)

    _log_exchange_coverage("trade input", config.exchanges, _observed_exchanges(trade_input))
    _log_exchange_coverage("BBO input", config.exchanges, _observed_exchanges(bbo_input))

    exchange_symbols = _expected_exchange_symbols(spark, trade_input, bbo_input, config.exchanges)
    second_grid = _build_second_grid(spark, exchange_symbols, trade_metadata)
    trade_snapshots = _build_trade_snapshots(trade_input)
    bbo_snapshots = _build_bbo_snapshots(bbo_input)

    output = (
        second_grid.join(trade_snapshots, ["exchange", "symbol", "ts_second"], "left")
        .join(bbo_snapshots, ["exchange", "symbol", "ts_second"], "left")
        .transform(_coalesce_empty_trade_seconds)
        .transform(_coalesce_empty_bbo_seconds)
        .select(
            F.lit(run_id).alias("run_id"),
            F.lit(args.source_trade_run_id).alias("source_trade_run_id"),
            F.lit(args.source_bbo_run_id).alias("source_bbo_run_id"),
            F.lit(trade_metadata["source_event_run_id"]).alias("source_event_run_id"),
            F.lit(trade_metadata["event_id"]).alias("event_id"),
            F.lit(trade_metadata["detection_version"]).alias("detection_version"),
            F.lit(True).alias("is_trial"),
            F.lit(trade_metadata["event_exchange"]).alias("event_exchange"),
            F.lit(trade_metadata["event_symbol"]).alias("event_symbol"),
            F.lit(trade_metadata["event_direction"]).alias("event_direction"),
            F.lit(trade_metadata["event_start_ts"]).cast("timestamp").alias("event_start_ts"),
            F.lit(trade_metadata["window_start_ts"]).cast("timestamp").alias("window_start_ts"),
            F.lit(trade_metadata["window_end_ts"]).cast("timestamp").alias("window_end_ts"),
            F.lit(int(trade_metadata["lookback_seconds"])).alias("lookback_seconds"),
            F.col("exchange"),
            F.col("symbol"),
            F.col("second_before_event"),
            F.col("ts_second"),
            *[F.col(column) for column in TRADE_ZERO_COLUMNS],
            F.col("last_trade_price"),
            F.col("min_trade_price"),
            F.col("max_trade_price"),
            F.col("bbo_update_count"),
            F.col("has_bbo_update"),
            F.col("is_bbo_forward_filled"),
            F.col("last_bbo_update_ts"),
            F.col("bbo_quote_age_seconds"),
            *[F.col(column) for column in BBO_LAST_COLUMNS],
            F.col("avg_spread_bps"),
            F.col("max_spread_bps"),
            F.col("avg_book_imbalance"),
            F.lit(created_at).cast("timestamp").alias("created_at"),
        )
        .select(*OUTPUT_COLUMNS)
        .cache()
    )

    output_count = output.count()
    LOGGER.info("Output snapshot row count: %s", output_count)
    if output_count == 0:
        raise RuntimeError("Output snapshot row count is 0.")

    _log_exchange_coverage("snapshot output", config.exchanges, _observed_exchanges(output))
    _log_output_profile(output)
    _log_null_summary(output)
    _log_bbo_fill_summary(output)

    negative_spread_count = output.where(F.col("last_spread") < F.lit(0)).count()
    LOGGER.info("Negative last_spread row count: %s", negative_spread_count)

    LOGGER.info("Writing Phase 2C market snapshots trial output: %s", output_path)
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
        readback.orderBy("exchange", "symbol", "ts_second"),
        args.sample_size,
    )

    output.unpersist()
    trade_input.unpersist()
    bbo_input.unpersist()
    LOGGER.info("Phase 2C trial market snapshot build complete.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Phase 2C trial market snapshots.")
    parser.add_argument("--config", default="configs/dev.yaml")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--source-trade-run-id", default=DEFAULT_SOURCE_TRADE_RUN_ID)
    parser.add_argument("--source-bbo-run-id", default=DEFAULT_SOURCE_BBO_RUN_ID)
    parser.add_argument("--event-id", default=DEFAULT_EVENT_ID)
    parser.add_argument("--lookback-seconds", type=int, default=DEFAULT_LOOKBACK_SECONDS)
    parser.add_argument("--sample-size", type=int, default=20)
    return parser.parse_args()


def _read_single_metadata(frame: DataFrame, label: str) -> dict[str, Any]:
    metadata_columns = (
        *METADATA_COMPATIBILITY_FIELDS,
        "detection_version",
        "event_exchange",
        "event_symbol",
        "event_direction",
    )
    missing = [column for column in metadata_columns if column not in frame.columns]
    if missing:
        raise RuntimeError(f"Missing {label} metadata columns: {', '.join(missing)}")

    metadata = frame.select(*metadata_columns).distinct()
    metadata_count = metadata.count()
    if metadata_count != 1:
        raise RuntimeError(
            f"Expected exactly one {label} metadata row, found {metadata_count}."
        )
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


def _expected_exchange_symbols(
    spark: Any,
    trade_input: DataFrame,
    bbo_input: DataFrame,
    expected_exchanges: tuple[str, ...],
) -> DataFrame:
    expected_set = [exchange.lower() for exchange in expected_exchanges]
    trade_pairs = trade_input.select(F.lower(F.col("exchange")).alias("exchange"), "symbol")
    bbo_pairs = bbo_input.select(F.lower(F.col("exchange")).alias("exchange"), "symbol")
    pairs = (
        trade_pairs.unionByName(bbo_pairs)
        .where(F.col("exchange").isin(expected_set))
        .where(F.col("symbol").isNotNull())
        .distinct()
    )
    pair_count = pairs.count()
    LOGGER.info("Expected exchange + symbol pair count: %s", pair_count)
    if pair_count == 0:
        raise RuntimeError("No exchange + symbol pairs found for snapshot grid.")
    return pairs


def _build_second_grid(
    spark: Any,
    exchange_symbols: DataFrame,
    metadata: dict[str, Any],
) -> DataFrame:
    window_start_ts = metadata["window_start_ts"]
    event_start_ts = metadata["event_start_ts"]
    sequence_frame = spark.sql(
        "SELECT explode(sequence("
        f"timestamp'{window_start_ts}', "
        f"timestamp'{event_start_ts}' - interval 1 second, "
        "interval 1 second)) AS ts_second"
    )
    grid = exchange_symbols.crossJoin(sequence_frame)
    return grid.withColumn(
        "second_before_event",
        (
            F.lit(event_start_ts).cast("timestamp").cast("long")
            - F.col("ts_second").cast("long")
        ).cast("int"),
    )


def _build_trade_snapshots(trade_input: DataFrame) -> DataFrame:
    selected = (
        trade_input.select(
            F.lower(F.col("exchange")).alias("exchange"),
            F.col("symbol"),
            F.date_trunc("second", F.col("ts_event_ts")).alias("ts_second"),
            F.col("ts_event_ts"),
            F.col("seq"),
            F.col("trade_id"),
            F.col("price").cast("double").alias("price"),
            F.col("qty").cast("double").alias("qty"),
            F.lower(F.trim(F.col("side"))).alias("side_norm"),
        )
        .where(F.col("ts_second").isNotNull())
        .where(F.col("price").isNotNull())
        .where(F.col("qty").isNotNull())
    )
    ranked = selected.withColumn(
        "last_rank",
        F.row_number().over(
            Window.partitionBy("exchange", "symbol", "ts_second").orderBy(
                F.col("ts_event_ts").desc(),
                F.col("seq").desc_nulls_last(),
                F.col("trade_id").desc_nulls_last(),
            )
        ),
    ).withColumn("notional", F.col("price") * F.col("qty"))
    buy = F.col("side_norm").isin("buy", "b")
    sell = F.col("side_norm").isin("sell", "s")
    return (
        ranked.groupBy("exchange", "symbol", "ts_second")
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


def _build_bbo_snapshots(bbo_input: DataFrame) -> DataFrame:
    selected = (
        bbo_input.select(
            F.lower(F.col("exchange")).alias("exchange"),
            F.col("symbol"),
            F.date_trunc("second", F.col("ts_event_ts")).alias("ts_second"),
            F.col("ts_event_ts"),
            F.col("ts_recv_ts"),
            F.col("seq"),
            F.col("bid_price").cast("double").alias("bid_price"),
            F.col("ask_price").cast("double").alias("ask_price"),
            F.col("bid_qty").cast("double").alias("bid_qty"),
            F.col("ask_qty").cast("double").alias("ask_qty"),
            F.col("mid_price").cast("double").alias("mid_price"),
            F.col("spread").cast("double").alias("spread"),
            F.col("spread_bps").cast("double").alias("spread_bps"),
            F.col("book_imbalance").cast("double").alias("book_imbalance"),
        )
        .where(F.col("ts_second").isNotNull())
    )
    ranked = selected.withColumn(
        "last_rank",
        F.row_number().over(
            Window.partitionBy("exchange", "symbol", "ts_second").orderBy(
                F.col("ts_event_ts").desc(),
                F.col("seq").desc_nulls_last(),
                F.col("ts_recv_ts").desc_nulls_last(),
            )
        ),
    )
    per_second = ranked.groupBy("exchange", "symbol", "ts_second").agg(
        F.count("*").alias("bbo_update_count"),
        F.max(F.when(F.col("last_rank") == 1, F.col("ts_event_ts"))).alias(
            "last_bbo_update_ts_raw"
        ),
        F.max(F.when(F.col("last_rank") == 1, F.col("bid_price"))).alias("last_bid_price_raw"),
        F.max(F.when(F.col("last_rank") == 1, F.col("ask_price"))).alias("last_ask_price_raw"),
        F.max(F.when(F.col("last_rank") == 1, F.col("bid_qty"))).alias("last_bid_qty_raw"),
        F.max(F.when(F.col("last_rank") == 1, F.col("ask_qty"))).alias("last_ask_qty_raw"),
        F.max(F.when(F.col("last_rank") == 1, F.col("mid_price"))).alias("last_mid_price_raw"),
        F.max(F.when(F.col("last_rank") == 1, F.col("spread"))).alias("last_spread_raw"),
        F.max(F.when(F.col("last_rank") == 1, F.col("spread_bps"))).alias(
            "last_spread_bps_raw"
        ),
        F.avg("spread_bps").alias("avg_spread_bps"),
        F.max("spread_bps").alias("max_spread_bps"),
        F.max(F.when(F.col("last_rank") == 1, F.col("book_imbalance"))).alias(
            "last_book_imbalance_raw"
        ),
        F.avg("book_imbalance").alias("avg_book_imbalance"),
    )
    fill_window = (
        Window.partitionBy("exchange", "symbol")
        .orderBy("ts_second")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return per_second.select(
        "exchange",
        "symbol",
        "ts_second",
        "bbo_update_count",
        "avg_spread_bps",
        "max_spread_bps",
        "avg_book_imbalance",
        F.last("last_bbo_update_ts_raw", True).over(fill_window).alias("last_bbo_update_ts"),
        F.last("last_bid_price_raw", True).over(fill_window).alias("last_bid_price"),
        F.last("last_ask_price_raw", True).over(fill_window).alias("last_ask_price"),
        F.last("last_bid_qty_raw", True).over(fill_window).alias("last_bid_qty"),
        F.last("last_ask_qty_raw", True).over(fill_window).alias("last_ask_qty"),
        F.last("last_mid_price_raw", True).over(fill_window).alias("last_mid_price"),
        F.last("last_spread_raw", True).over(fill_window).alias("last_spread"),
        F.last("last_spread_bps_raw", True).over(fill_window).alias("last_spread_bps"),
        F.last("last_book_imbalance_raw", True).over(fill_window).alias("last_book_imbalance"),
    )


def _coalesce_empty_trade_seconds(frame: DataFrame) -> DataFrame:
    output = frame
    for column in TRADE_ZERO_COLUMNS:
        output = output.withColumn(column, F.coalesce(F.col(column), F.lit(0.0)))
    return output.withColumn("trade_count", F.col("trade_count").cast("long")).withColumn(
        "buy_trade_count", F.col("buy_trade_count").cast("long")
    ).withColumn("sell_trade_count", F.col("sell_trade_count").cast("long"))


def _coalesce_empty_bbo_seconds(frame: DataFrame) -> DataFrame:
    with_count = frame.withColumn(
        "bbo_update_count",
        F.coalesce(F.col("bbo_update_count"), F.lit(0)).cast("long"),
    )
    fill_window = (
        Window.partitionBy("exchange", "symbol")
        .orderBy("ts_second")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    with_filled = with_count
    for column in ("last_bbo_update_ts", *BBO_LAST_COLUMNS):
        with_filled = with_filled.withColumn(column, F.last(column, True).over(fill_window))

    return (
        with_filled.withColumn("has_bbo_update", F.col("bbo_update_count") > F.lit(0))
        .withColumn(
            "is_bbo_forward_filled",
            (F.col("bbo_update_count") == F.lit(0)) & F.col("last_bbo_update_ts").isNotNull(),
        )
        .withColumn(
            "bbo_quote_age_seconds",
            F.when(
                F.col("last_bbo_update_ts").isNotNull(),
                F.col("ts_second").cast("long") - F.col("last_bbo_update_ts").cast("long"),
            ),
        )
    )


def _log_output_profile(output: DataFrame) -> None:
    _show_df("Snapshot count by exchange:", output.groupBy("exchange").count().orderBy("exchange"))
    _show_df(
        "Snapshot count by exchange + symbol:",
        output.groupBy("exchange", "symbol").count().orderBy("exchange", "symbol"),
    )
    _show_df(
        "Snapshot second_before_event range:",
        output.agg(
            F.min("second_before_event").alias("min_second_before_event"),
            F.max("second_before_event").alias("max_second_before_event"),
        ),
    )


def _log_null_summary(output: DataFrame) -> None:
    columns = (
        "event_id",
        "exchange",
        "symbol",
        "ts_second",
        "second_before_event",
        "trade_count",
        "bbo_update_count",
        "has_bbo_update",
        "is_bbo_forward_filled",
        "last_bid_price",
        "last_ask_price",
        "avg_spread_bps",
        "max_spread_bps",
        "avg_book_imbalance",
    )
    expressions = [
        F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column)
        for column in columns
    ]
    row = output.agg(*expressions).first()
    LOGGER.info("Snapshot null summary:")
    for column in columns:
        LOGGER.info("  %s null_count=%s", column, row[column])


def _log_bbo_fill_summary(output: DataFrame) -> None:
    pre_first_update = (
        (F.col("bbo_update_count") == F.lit(0))
        & (F.col("has_bbo_update") == F.lit(False))
        & (F.col("is_bbo_forward_filled") == F.lit(False))
        & F.col("last_bbo_update_ts").isNull()
    )
    _show_df(
        "BBO update / forward-fill summary:",
        output.agg(
            F.sum(F.when(F.col("has_bbo_update"), 1).otherwise(0)).alias("update_seconds"),
            F.sum(F.when(F.col("is_bbo_forward_filled"), 1).otherwise(0)).alias(
                "forward_filled_seconds"
            ),
            F.sum(F.when(pre_first_update, 1).otherwise(0)).alias("pre_first_update_seconds"),
        ),
    )


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
