"""Spark helpers for BTC trade move candidate scans."""

from __future__ import annotations

import logging
from typing import Any, Iterable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .manifest import ManifestPartition
from .profiling import filter_btc_trade_partitions


LOGGER = logging.getLogger("quantlab_event_scanner.trade_move_scan")

REQUIRED_TRADE_MOVE_COLUMNS = ("ts_event", "exchange", "symbol", "price", "qty")


def select_fixed_date_partitions(
    partitions: Iterable[ManifestPartition],
    exchanges: tuple[str, ...],
    date: str,
) -> tuple[ManifestPartition, ...]:
    """Return configured BTC trade partitions for a fixed date."""

    return tuple(
        partition
        for partition in filter_btc_trade_partitions(tuple(partitions), exchanges)
        if partition.date == date
    )


def build_price_1s(frame: DataFrame) -> DataFrame | None:
    """Build 1-second OHLCV trade price rows."""

    second_expression = _second_bucket_expression(frame, "ts_event")
    if second_expression is None:
        return None

    select_columns = [
        F.lower(F.col("exchange")).alias("exchange"),
        F.col("symbol"),
        F.col("ts_event"),
        second_expression.alias("second"),
        F.col("price").cast("double").alias("price"),
        F.col("qty").cast("double").alias("qty"),
    ]
    if "seq" in frame.columns:
        select_columns.append(F.col("seq"))
    if "trade_id" in frame.columns:
        select_columns.append(F.col("trade_id"))

    trades = (
        frame.select(*select_columns)
        .where(F.col("second").isNotNull())
        .where(F.col("exchange").isNotNull())
        .where(F.col("symbol").isNotNull())
        .where(F.col("price").isNotNull() & (F.col("price") > 0))
        .where(F.col("qty").isNotNull())
    )

    asc_order = [F.col("ts_event").asc()]
    desc_order = [F.col("ts_event").desc()]
    if "seq" in trades.columns:
        asc_order.append(F.col("seq").asc_nulls_last())
        desc_order.append(F.col("seq").desc_nulls_last())
    if "trade_id" in trades.columns:
        asc_order.append(F.col("trade_id").asc_nulls_last())
        desc_order.append(F.col("trade_id").desc_nulls_last())

    partition = Window.partitionBy("exchange", "symbol", "second")
    ranked = (
        trades.withColumn("open_rank", F.row_number().over(partition.orderBy(*asc_order)))
        .withColumn("close_rank", F.row_number().over(partition.orderBy(*desc_order)))
    )

    return (
        ranked.groupBy("exchange", "symbol", "second")
        .agg(
            F.max(F.when(F.col("open_rank") == 1, F.col("price"))).alias("open_price"),
            F.max("price").alias("high_price"),
            F.min("price").alias("low_price"),
            F.max(F.when(F.col("close_rank") == 1, F.col("price"))).alias("close_price"),
            F.count("*").alias("trade_count"),
            F.sum("qty").alias("volume"),
        )
        .withColumn("second_epoch", F.col("second").cast("long"))
    )


def scan_move_candidates(
    price_1s: DataFrame,
    threshold_pct: float,
    lookahead_seconds: int,
) -> tuple[DataFrame, DataFrame]:
    """Scan 1-second prices for non-ambiguous and ambiguous move candidates."""

    up_multiplier = 1.0 + threshold_pct / 100.0
    down_multiplier = 1.0 - threshold_pct / 100.0

    base = (
        price_1s.select(
            "exchange",
            "symbol",
            F.col("second").alias("start_ts"),
            F.col("second_epoch").alias("start_epoch"),
            F.col("close_price").alias("base_price"),
            F.explode(
                F.sequence(
                    F.col("second_epoch") + F.lit(1),
                    F.col("second_epoch") + F.lit(lookahead_seconds),
                )
            ).alias("future_epoch_key"),
        )
        .alias("base")
    )
    future = price_1s.alias("future")
    joined = (
        base.join(
            future,
            (F.col("base.exchange") == F.col("future.exchange"))
            & (F.col("base.symbol") == F.col("future.symbol"))
            & (F.col("future.second_epoch") == F.col("base.future_epoch_key")),
            "inner",
        )
        .select(
            F.col("base.exchange").alias("exchange"),
            F.col("base.symbol").alias("symbol"),
            F.col("base.start_ts").alias("start_ts"),
            F.col("base.start_epoch").alias("start_epoch"),
            F.col("base.base_price").alias("base_price"),
            F.col("future.second").alias("future_ts"),
            F.col("future.second_epoch").alias("future_epoch"),
            F.col("future.high_price").alias("future_high_price"),
            F.col("future.low_price").alias("future_low_price"),
        )
        .withColumn("up_hit", F.col("future_high_price") >= F.col("base_price") * up_multiplier)
        .withColumn("down_hit", F.col("future_low_price") <= F.col("base_price") * down_multiplier)
    )

    grouped = (
        joined.groupBy("exchange", "symbol", "start_ts", "start_epoch", "base_price")
        .agg(
            F.min(
                F.when(
                    F.col("up_hit"),
                    F.struct(
                        F.col("future_epoch").alias("hit_epoch"),
                        F.col("future_ts").alias("hit_ts"),
                        F.col("future_high_price").alias("hit_price"),
                    ),
                )
            ).alias("up_hit"),
            F.min(
                F.when(
                    F.col("down_hit"),
                    F.struct(
                        F.col("future_epoch").alias("hit_epoch"),
                        F.col("future_ts").alias("hit_ts"),
                        F.col("future_low_price").alias("hit_price"),
                    ),
                )
            ).alias("down_hit"),
        )
        .where(F.col("up_hit").isNotNull() | F.col("down_hit").isNotNull())
        .withColumn("up_hit_epoch", F.col("up_hit.hit_epoch"))
        .withColumn("down_hit_epoch", F.col("down_hit.hit_epoch"))
        .withColumn(
            "direction",
            F.when(
                F.col("up_hit_epoch").isNotNull()
                & F.col("down_hit_epoch").isNotNull()
                & (F.col("up_hit_epoch") == F.col("down_hit_epoch")),
                F.lit("AMBIGUOUS"),
            )
            .when(F.col("down_hit_epoch").isNull(), F.lit("UP"))
            .when(F.col("up_hit_epoch").isNull(), F.lit("DOWN"))
            .when(F.col("up_hit_epoch") < F.col("down_hit_epoch"), F.lit("UP"))
            .otherwise(F.lit("DOWN")),
        )
        .withColumn("ambiguous", F.col("direction") == F.lit("AMBIGUOUS"))
    )

    resolved = (
        grouped.withColumn(
            "end_ts",
            F.when(F.col("direction") == "UP", F.col("up_hit.hit_ts"))
            .when(F.col("direction") == "DOWN", F.col("down_hit.hit_ts"))
            .otherwise(F.col("up_hit.hit_ts")),
        )
        .withColumn(
            "hit_price",
            F.when(F.col("direction") == "UP", F.col("up_hit.hit_price"))
            .when(F.col("direction") == "DOWN", F.col("down_hit.hit_price"))
            .otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "duration_seconds",
            F.when(F.col("direction") == "UP", F.col("up_hit.hit_epoch"))
            .when(F.col("direction") == "DOWN", F.col("down_hit.hit_epoch"))
            .otherwise(F.col("up_hit.hit_epoch"))
            - F.col("start_epoch"),
        )
        .withColumn(
            "move_pct",
            F.when(
                ~F.col("ambiguous"),
                ((F.col("hit_price") / F.col("base_price")) - F.lit(1.0)) * F.lit(100.0),
            ),
        )
    )

    output_columns = (
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
    candidates = resolved.where(~F.col("ambiguous")).select(*output_columns)
    ambiguous = resolved.where(F.col("ambiguous")).select(
        "exchange",
        "symbol",
        "direction",
        "start_ts",
        "end_ts",
        "duration_seconds",
        "base_price",
    )
    return candidates, ambiguous


def validate_required_columns(frame: DataFrame, required_columns: tuple[str, ...]) -> tuple[str, ...]:
    """Return missing required columns."""

    return tuple(column for column in required_columns if column not in frame.columns)


def timestamp_expression(frame: DataFrame, column: str) -> Any | None:
    """Return a timestamp expression for a timestamp or numeric epoch column."""

    column_type = _column_type(frame, column)
    if column_type is None:
        return None

    if _is_timestamp_type(column_type):
        return F.col(column)

    if isinstance(column_type, T.NumericType):
        divisor = _numeric_timestamp_seconds_divisor(frame, column)
        if divisor is None:
            return None
        LOGGER.info("%s timestamp assumption: numeric timestamp divisor to seconds=%s", column, divisor)
        return (F.col(column).cast("double") / F.lit(divisor)).cast("timestamp")

    LOGGER.warning("Unsupported %s type for timestamp conversion: %s", column, column_type)
    return None


def _second_bucket_expression(frame: DataFrame, column: str) -> Any | None:
    expression = timestamp_expression(frame, column)
    if expression is None:
        return None
    return F.date_trunc("second", expression)


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
