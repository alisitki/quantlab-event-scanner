#!/usr/bin/env python
"""Phase 2E single-job normal-time comparison trial."""

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

from pyspark.sql import DataFrame, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from quantlab_event_scanner.config import load_config  # noqa: E402
from quantlab_event_scanner.manifest import ManifestPartition  # noqa: E402
from quantlab_event_scanner.manifest import load_manifest_from_s3_with_spark  # noqa: E402
from quantlab_event_scanner.normal_time_trial import (  # noqa: E402
    SAMPLE_TYPE_NORMAL,
    build_normal_selection_metadata,
    default_phase2e_run_id,
    exclusion_reason,
    normal_window_for_anchor,
    selected_date_utc_bounds,
)
from quantlab_event_scanner.paths import (  # noqa: E402
    normal_bbo_windows_trial_run_path,
    normal_market_snapshots_trial_run_path,
    normal_profile_reports_trial_run_path,
    normal_trade_windows_trial_run_path,
)
from quantlab_event_scanner.pre_event_market_snapshots import exchange_coverage  # noqa: E402
from quantlab_event_scanner.pre_event_profile_reports import (  # noqa: E402
    BUCKET_CHANGE_PAIRS,
    PROFILE_VERSION,
    PROFILE_WINDOWS,
)
from quantlab_event_scanner.probe import partition_paths  # noqa: E402
from quantlab_event_scanner.trade_move_scan import (  # noqa: E402
    REQUIRED_TRADE_MOVE_COLUMNS,
    build_price_1s,
    scan_move_candidates,
    timestamp_expression,
    validate_required_columns,
)


LOGGER = logging.getLogger("quantlab_event_scanner.phase2e_normal_time_trial")

DEFAULT_DATE = "20260423"
DEFAULT_LOOKBACK_SECONDS = 300
DEFAULT_THRESHOLD_PCT = 1.0
DEFAULT_HORIZON_SECONDS = 60
DEFAULT_PROXIMITY_SECONDS = 1800
DEFAULT_MAX_QUALITY_ATTEMPTS = 20
DEFAULT_ANCHOR_STEP_SECONDS = 60

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
NORMAL_METADATA_COLUMNS = (
    "run_id",
    "sample_type",
    "selected_date",
    "normal_window_start_ts",
    "normal_window_end_ts",
    "normal_anchor_ts",
    "lookback_seconds",
    "selection_reason",
    "excluded_candidate_count",
    "nearest_candidate_distance_seconds",
)


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config_path = _resolve_path(args.config)
    config = load_config(config_path)
    run_id = args.run_id or default_phase2e_run_id(datetime.now(timezone.utc))
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    trade_output_path = normal_trade_windows_trial_run_path(config, run_id)
    bbo_output_path = normal_bbo_windows_trial_run_path(config, run_id)
    snapshot_output_path = normal_market_snapshots_trial_run_path(config, run_id)
    profile_output_path = normal_profile_reports_trial_run_path(config, run_id)

    LOGGER.info("Using config: %s", config_path)
    LOGGER.info("Run ID: %s", run_id)
    LOGGER.info("Selected date target: %s", args.date)
    LOGGER.info("Expected exchanges: %s", ", ".join(config.exchanges))
    LOGGER.info("Lookback seconds: %s", args.lookback_seconds)
    LOGGER.info("Threshold pct: %s", args.threshold_pct)
    LOGGER.info("Horizon seconds: %s", args.horizon_seconds)
    LOGGER.info("Proximity exclusion seconds: %s", args.proximity_seconds)
    LOGGER.info("Max quality attempts: %s", args.max_quality_attempts)
    LOGGER.info("Trade output path: %s", trade_output_path)
    LOGGER.info("BBO output path: %s", bbo_output_path)
    LOGGER.info("Snapshot output path: %s", snapshot_output_path)
    LOGGER.info("Profile output path: %s", profile_output_path)

    spark = _get_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    LOGGER.info("Spark timezone: %s", spark.conf.get("spark.sql.session.timeZone"))

    manifest = load_manifest_from_s3_with_spark(spark, config.manifest_path)
    trade_partitions = _select_partitions(
        manifest.partitions,
        exchanges=config.exchanges,
        stream="trade",
        date=args.date,
    )
    bbo_partitions = _select_partitions(
        manifest.partitions,
        exchanges=config.exchanges,
        stream="bbo",
        date=args.date,
    )
    _log_partition_coverage("trade manifest", config.exchanges, trade_partitions)
    _log_partition_coverage("BBO manifest", config.exchanges, bbo_partitions)

    trade_day = _read_partitions(spark, trade_partitions, config.input_root, "trade").cache()
    bbo_day = _read_partitions(spark, bbo_partitions, config.input_root, "BBO").cache()
    _validate_required_trade_columns(trade_day)
    _validate_required_bbo_columns(bbo_day)

    price_1s = build_price_1s(trade_day)
    if price_1s is None:
        raise RuntimeError("Unable to build 1s price rows for normal window selection.")
    price_1s = price_1s.cache()
    price_1s_count = price_1s.count()
    LOGGER.info("1s price row count for candidate scan: %s", price_1s_count)
    if price_1s_count == 0:
        raise RuntimeError("1s price row count is 0.")

    candidates, ambiguous = scan_move_candidates(
        price_1s,
        threshold_pct=args.threshold_pct,
        lookahead_seconds=args.horizon_seconds,
    )
    candidates = candidates.cache()
    ambiguous = ambiguous.cache()
    candidate_count = candidates.count()
    ambiguous_count = ambiguous.count()
    LOGGER.info("Normal selection raw candidate count: %s", candidate_count)
    LOGGER.info("Normal selection ambiguous candidate count: %s", ambiguous_count)
    candidate_starts = _collect_candidate_starts(candidates)
    LOGGER.info("Collected candidate start timestamp count: %s", len(candidate_starts))

    anchor_candidates = _eligible_anchor_candidates(
        price_1s,
        candidate_starts,
        selected_date=args.date,
        lookback_seconds=args.lookback_seconds,
        horizon_seconds=args.horizon_seconds,
        proximity_seconds=args.proximity_seconds,
        anchor_step_seconds=args.anchor_step_seconds,
    )
    excluded_candidate_count = _excluded_anchor_count
    LOGGER.info("Eligible normal anchor candidate count: %s", len(anchor_candidates))
    LOGGER.info("Excluded normal anchor candidate count: %s", excluded_candidate_count)
    if not anchor_candidates:
        raise RuntimeError("No eligible normal anchor candidates found.")

    accepted = _try_quality_windows(
        trade_day,
        bbo_day,
        config.exchanges,
        args.date,
        run_id,
        created_at,
        anchor_candidates,
        candidate_starts,
        lookback_seconds=args.lookback_seconds,
        max_quality_attempts=args.max_quality_attempts,
        excluded_candidate_count=excluded_candidate_count,
    )
    if accepted is None:
        raise RuntimeError("No eligible normal window passed quality checks.")

    (
        selection_metadata,
        normal_trade,
        normal_bbo,
        normal_snapshot,
        exchange_profile,
        cross_exchange_mid_diff,
        bucket_change_profile,
    ) = accepted

    LOGGER.info(
        "Accepted normal window: start=%s end=%s anchor=%s reason=%s nearest_candidate_distance_seconds=%s",
        selection_metadata.normal_window_start_ts,
        selection_metadata.normal_window_end_ts,
        selection_metadata.normal_anchor_ts,
        selection_metadata.selection_reason,
        selection_metadata.nearest_candidate_distance_seconds,
    )

    _write_and_validate(normal_trade, trade_output_path, "normal_trade_windows", args.sample_size)
    _write_and_validate(normal_bbo, bbo_output_path, "normal_bbo_windows", args.sample_size)
    _write_and_validate(
        normal_snapshot,
        snapshot_output_path,
        "normal_market_snapshots",
        args.sample_size,
    )
    _write_and_validate(
        exchange_profile,
        f"{profile_output_path}/exchange_profile",
        "normal_profile_reports.exchange_profile",
        args.sample_size,
    )
    _write_and_validate(
        cross_exchange_mid_diff,
        f"{profile_output_path}/cross_exchange_mid_diff",
        "normal_profile_reports.cross_exchange_mid_diff",
        args.sample_size,
    )
    _write_and_validate(
        bucket_change_profile,
        f"{profile_output_path}/bucket_change_profile",
        "normal_profile_reports.bucket_change_profile",
        args.sample_size,
    )

    bucket_change_profile.unpersist()
    cross_exchange_mid_diff.unpersist()
    exchange_profile.unpersist()
    normal_snapshot.unpersist()
    normal_bbo.unpersist()
    normal_trade.unpersist()
    ambiguous.unpersist()
    candidates.unpersist()
    price_1s.unpersist()
    bbo_day.unpersist()
    trade_day.unpersist()
    LOGGER.info("Phase 2E normal-time trial complete.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Phase 2E normal-time comparison trial.")
    parser.add_argument("--config", default="configs/dev.yaml")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--date", default=DEFAULT_DATE)
    parser.add_argument("--lookback-seconds", type=int, default=DEFAULT_LOOKBACK_SECONDS)
    parser.add_argument("--threshold-pct", type=float, default=DEFAULT_THRESHOLD_PCT)
    parser.add_argument("--horizon-seconds", type=int, default=DEFAULT_HORIZON_SECONDS)
    parser.add_argument("--proximity-seconds", type=int, default=DEFAULT_PROXIMITY_SECONDS)
    parser.add_argument("--max-quality-attempts", type=int, default=DEFAULT_MAX_QUALITY_ATTEMPTS)
    parser.add_argument("--anchor-step-seconds", type=int, default=DEFAULT_ANCHOR_STEP_SECONDS)
    parser.add_argument("--sample-size", type=int, default=20)
    return parser.parse_args()


def _select_partitions(
    partitions: Iterable[ManifestPartition],
    exchanges: tuple[str, ...],
    stream: str,
    date: str,
) -> tuple[ManifestPartition, ...]:
    exchange_set = {exchange.lower() for exchange in exchanges}
    selected = tuple(
        partition
        for partition in partitions
        if partition.available is not False
        and partition.exchange is not None
        and partition.exchange.lower() in exchange_set
        and partition.stream == stream
        and partition.symbol is not None
        and "btc" in partition.symbol.lower()
        and partition.date == date
    )
    if not selected:
        raise RuntimeError(f"No {stream} partitions selected for date={date}.")
    return selected


def _log_partition_coverage(
    label: str,
    expected_exchanges: tuple[str, ...],
    partitions: tuple[ManifestPartition, ...],
) -> None:
    observed = tuple(sorted({partition.exchange.lower() for partition in partitions if partition.exchange}))
    coverage = exchange_coverage(expected_exchanges, observed)
    LOGGER.info(
        "%s coverage: expected=%s observed=%s missing=%s",
        label,
        ", ".join(coverage.expected) or "none",
        ", ".join(coverage.observed) or "none",
        ", ".join(coverage.missing) or "none",
    )
    if coverage.missing:
        raise RuntimeError(f"Missing required {label} coverage: {coverage.missing}")


def _read_partitions(
    spark: Any,
    partitions: tuple[ManifestPartition, ...],
    input_root: str,
    label: str,
) -> DataFrame:
    paths = partition_paths(partitions, input_root=input_root)
    LOGGER.info("%s selected parquet path count: %s", label, len(paths))
    for path in paths[:20]:
        LOGGER.info("  %s", path)
    return spark.read.parquet(*paths)


def _validate_required_trade_columns(frame: DataFrame) -> None:
    missing = validate_required_columns(frame, REQUIRED_TRADE_MOVE_COLUMNS)
    if missing:
        raise RuntimeError(f"Missing required trade columns: {', '.join(missing)}")


def _validate_required_bbo_columns(frame: DataFrame) -> None:
    required = ("ts_event", "ts_recv", "exchange", "symbol", "bid_price", "bid_qty", "ask_price", "ask_qty")
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise RuntimeError(f"Missing required BBO columns: {', '.join(missing)}")


def _collect_candidate_starts(candidates: DataFrame) -> tuple[datetime, ...]:
    rows = candidates.select("start_ts").distinct().orderBy("start_ts").collect()
    return tuple(row["start_ts"] for row in rows)


_excluded_anchor_count = 0


def _eligible_anchor_candidates(
    price_1s: DataFrame,
    candidate_starts: tuple[datetime, ...],
    *,
    selected_date: str,
    lookback_seconds: int,
    horizon_seconds: int,
    proximity_seconds: int,
    anchor_step_seconds: int,
) -> tuple[datetime, ...]:
    global _excluded_anchor_count
    ranges = price_1s.groupBy("exchange").agg(
        F.min("second").alias("min_second"),
        F.max("second").alias("max_second"),
    ).collect()
    common_start = max(row["min_second"] for row in ranges)
    common_end = min(row["max_second"] for row in ranges)
    date_start, date_end = selected_date_utc_bounds(selected_date)
    date_start = date_start.replace(tzinfo=None)
    date_end = date_end.replace(tzinfo=None)
    first_anchor = max(
        common_start + timedelta(seconds=lookback_seconds),
        date_start + timedelta(seconds=lookback_seconds),
    )
    last_anchor = min(
        common_end - timedelta(seconds=horizon_seconds),
        date_end - timedelta(seconds=horizon_seconds),
    )
    LOGGER.info(
        "Normal anchor bounds: common_start=%s common_end=%s date_start=%s date_end=%s "
        "first_anchor=%s last_anchor=%s",
        common_start,
        common_end,
        date_start,
        date_end,
        first_anchor,
        last_anchor,
    )
    if first_anchor > last_anchor:
        _excluded_anchor_count = 0
        return ()
    anchors: list[datetime] = []
    excluded = 0
    current = first_anchor
    while current <= last_anchor:
        reason = exclusion_reason(
            current,
            candidate_starts,
            lookback_seconds=lookback_seconds,
            horizon_seconds=horizon_seconds,
            proximity_seconds=proximity_seconds,
        )
        if reason is None:
            anchors.append(current)
        else:
            excluded += 1
        current += timedelta(seconds=anchor_step_seconds)
    _excluded_anchor_count = excluded
    return tuple(anchors)


def _try_quality_windows(
    trade_day: DataFrame,
    bbo_day: DataFrame,
    expected_exchanges: tuple[str, ...],
    selected_date: str,
    run_id: str,
    created_at: datetime,
    anchor_candidates: tuple[datetime, ...],
    candidate_starts: tuple[datetime, ...],
    *,
    lookback_seconds: int,
    max_quality_attempts: int,
    excluded_candidate_count: int,
) -> tuple[Any, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame] | None:
    for attempt, anchor_ts in enumerate(anchor_candidates[:max_quality_attempts], start=1):
        window = normal_window_for_anchor(anchor_ts, lookback_seconds)
        metadata = build_normal_selection_metadata(
            selected_date=selected_date,
            window=window,
            lookback_seconds=lookback_seconds,
            selection_reason="earliest_eligible_quality_pass",
            excluded_candidate_count=excluded_candidate_count,
            candidate_start_timestamps=candidate_starts,
        )
        LOGGER.info(
            "Trying normal window attempt=%s start=%s end=%s anchor=%s",
            attempt,
            metadata.normal_window_start_ts,
            metadata.normal_window_end_ts,
            metadata.normal_anchor_ts,
        )
        normal_trade = _extract_normal_trade_window(trade_day, run_id, metadata, created_at).cache()
        normal_bbo = _extract_normal_bbo_window(bbo_day, run_id, metadata, created_at).cache()
        trade_count = normal_trade.count()
        bbo_count = normal_bbo.count()
        if trade_count == 0 or bbo_count == 0:
            LOGGER.warning(
                "Rejected normal window attempt=%s: trade_count=%s bbo_count=%s",
                attempt,
                trade_count,
                bbo_count,
            )
            normal_trade.unpersist()
            normal_bbo.unpersist()
            continue
        if not _has_expected_exchange_counts(normal_trade, expected_exchanges, minimum_count=1):
            LOGGER.warning("Rejected normal window attempt=%s: missing trade exchange rows.", attempt)
            normal_trade.unpersist()
            normal_bbo.unpersist()
            continue
        if not _has_expected_exchange_counts(normal_bbo, expected_exchanges, minimum_count=1):
            LOGGER.warning("Rejected normal window attempt=%s: missing BBO exchange rows.", attempt)
            normal_trade.unpersist()
            normal_bbo.unpersist()
            continue

        normal_snapshot = _build_normal_snapshot(
            normal_trade,
            normal_bbo,
            expected_exchanges,
            run_id,
            metadata,
            created_at,
        ).cache()
        snapshot_count = normal_snapshot.count()
        if snapshot_count != len(expected_exchanges) * lookback_seconds:
            LOGGER.warning(
                "Rejected normal window attempt=%s: snapshot_count=%s",
                attempt,
                snapshot_count,
            )
            normal_snapshot.unpersist()
            normal_trade.unpersist()
            normal_bbo.unpersist()
            continue
        if not _has_expected_exchange_counts(
            normal_snapshot,
            expected_exchanges,
            minimum_count=lookback_seconds,
        ):
            LOGGER.warning("Rejected normal window attempt=%s: snapshot exchange counts invalid.", attempt)
            normal_snapshot.unpersist()
            normal_trade.unpersist()
            normal_bbo.unpersist()
            continue

        _log_normal_window_counts(normal_trade, normal_bbo, normal_snapshot)
        exchange_profile, cross_exchange_mid_diff, bucket_change_profile = _build_normal_profiles(
            normal_snapshot,
            expected_exchanges,
            run_id,
            metadata,
            created_at,
        )
        return (
            metadata,
            normal_trade,
            normal_bbo,
            normal_snapshot,
            exchange_profile.cache(),
            cross_exchange_mid_diff.cache(),
            bucket_change_profile.cache(),
        )
    return None


def _has_expected_exchange_counts(
    frame: DataFrame,
    expected_exchanges: tuple[str, ...],
    *,
    minimum_count: int,
) -> bool:
    counts = {
        row["exchange"]: row["count"]
        for row in frame.groupBy(F.lower(F.col("exchange")).alias("exchange")).count().collect()
    }
    for exchange in expected_exchanges:
        if counts.get(exchange.lower(), 0) < minimum_count:
            return False
    return True


def _metadata_columns(metadata: Any, created_at: datetime) -> tuple[Any, ...]:
    return (
        F.lit(metadata.sample_type).alias("sample_type"),
        F.lit(metadata.selected_date).alias("selected_date"),
        F.lit(metadata.normal_window_start_ts).cast("timestamp").alias("normal_window_start_ts"),
        F.lit(metadata.normal_window_end_ts).cast("timestamp").alias("normal_window_end_ts"),
        F.lit(metadata.normal_anchor_ts).cast("timestamp").alias("normal_anchor_ts"),
        F.lit(metadata.lookback_seconds).alias("lookback_seconds"),
        F.lit(metadata.selection_reason).alias("selection_reason"),
        F.lit(metadata.excluded_candidate_count).alias("excluded_candidate_count"),
        F.lit(metadata.nearest_candidate_distance_seconds).alias(
            "nearest_candidate_distance_seconds"
        ),
        F.lit(created_at).cast("timestamp").alias("created_at"),
    )


def _extract_normal_trade_window(
    trade_day: DataFrame,
    run_id: str,
    metadata: Any,
    created_at: datetime,
) -> DataFrame:
    ts_event_expr = timestamp_expression(trade_day, "ts_event")
    ts_recv_expr = timestamp_expression(trade_day, "ts_recv")
    if ts_event_expr is None or ts_recv_expr is None:
        raise RuntimeError("Unable to convert trade ts_event or ts_recv to timestamp.")
    enriched = (
        trade_day.withColumn("ts_event_ts", ts_event_expr)
        .withColumn("ts_recv_ts", ts_recv_expr)
        .where(F.col("ts_event_ts") >= F.lit(metadata.normal_window_start_ts).cast("timestamp"))
        .where(F.col("ts_event_ts") < F.lit(metadata.normal_window_end_ts).cast("timestamp"))
        .withColumn(
            "second_before_anchor",
            (
                F.lit(metadata.normal_anchor_ts).cast("timestamp").cast("double")
                - F.col("ts_event_ts").cast("double")
            ),
        )
    )
    return enriched.select(
        F.lit(run_id).alias("run_id"),
        *_metadata_columns(metadata, created_at),
        F.lower(F.col("exchange")).alias("exchange"),
        F.col("symbol"),
        F.lit("trade").alias("source_stream"),
        F.col("ts_event"),
        F.col("ts_event_ts"),
        F.col("ts_recv"),
        F.col("ts_recv_ts"),
        _column_or_null(enriched, "seq").alias("seq"),
        F.col("price"),
        F.col("qty"),
        _column_or_null(enriched, "side").alias("side"),
        _column_or_null(enriched, "trade_id").alias("trade_id"),
        F.col("second_before_anchor"),
    )


def _extract_normal_bbo_window(
    bbo_day: DataFrame,
    run_id: str,
    metadata: Any,
    created_at: datetime,
) -> DataFrame:
    ts_event_expr = timestamp_expression(bbo_day, "ts_event")
    ts_recv_expr = timestamp_expression(bbo_day, "ts_recv")
    if ts_event_expr is None or ts_recv_expr is None:
        raise RuntimeError("Unable to convert BBO ts_event or ts_recv to timestamp.")
    enriched = (
        bbo_day.withColumn("ts_event_ts", ts_event_expr)
        .withColumn("ts_recv_ts", ts_recv_expr)
        .where(F.col("ts_event_ts") >= F.lit(metadata.normal_window_start_ts).cast("timestamp"))
        .where(F.col("ts_event_ts") < F.lit(metadata.normal_window_end_ts).cast("timestamp"))
        .withColumn(
            "second_before_anchor",
            (
                F.lit(metadata.normal_anchor_ts).cast("timestamp").cast("double")
                - F.col("ts_event_ts").cast("double")
            ),
        )
        .withColumn("bid_price_num", F.col("bid_price").cast("double"))
        .withColumn("bid_qty_num", F.col("bid_qty").cast("double"))
        .withColumn("ask_price_num", F.col("ask_price").cast("double"))
        .withColumn("ask_qty_num", F.col("ask_qty").cast("double"))
        .withColumn("mid_price", (F.col("bid_price_num") + F.col("ask_price_num")) / F.lit(2.0))
        .withColumn("spread", F.col("ask_price_num") - F.col("bid_price_num"))
        .withColumn(
            "spread_bps",
            F.when(F.col("mid_price") != 0, F.col("spread") / F.col("mid_price") * F.lit(10000.0)),
        )
        .withColumn("book_qty_sum", F.col("bid_qty_num") + F.col("ask_qty_num"))
        .withColumn(
            "book_imbalance",
            F.when(
                F.col("book_qty_sum") != 0,
                (F.col("bid_qty_num") - F.col("ask_qty_num")) / F.col("book_qty_sum"),
            ),
        )
    )
    return enriched.select(
        F.lit(run_id).alias("run_id"),
        *_metadata_columns(metadata, created_at),
        F.lower(F.col("exchange")).alias("exchange"),
        F.col("symbol"),
        F.lit("bbo").alias("source_stream"),
        F.col("ts_event"),
        F.col("ts_event_ts"),
        F.col("ts_recv"),
        F.col("ts_recv_ts"),
        _column_or_null(enriched, "seq").alias("seq"),
        F.col("bid_price"),
        F.col("bid_qty"),
        F.col("ask_price"),
        F.col("ask_qty"),
        F.col("mid_price"),
        F.col("spread"),
        F.col("spread_bps"),
        F.col("book_imbalance"),
        F.col("second_before_anchor"),
    )


def _build_normal_snapshot(
    normal_trade: DataFrame,
    normal_bbo: DataFrame,
    expected_exchanges: tuple[str, ...],
    run_id: str,
    metadata: Any,
    created_at: datetime,
) -> DataFrame:
    spark = normal_trade.sparkSession
    exchange_symbols = (
        normal_trade.select("exchange", "symbol")
        .unionByName(normal_bbo.select("exchange", "symbol"))
        .where(F.col("exchange").isin([exchange.lower() for exchange in expected_exchanges]))
        .distinct()
    )
    second_grid = _build_second_grid(spark, exchange_symbols, metadata)
    trade_snapshots = _build_trade_snapshots(normal_trade)
    bbo_snapshots = _build_bbo_snapshots(normal_bbo)
    output = (
        second_grid.join(trade_snapshots, ["exchange", "symbol", "ts_second"], "left")
        .join(bbo_snapshots, ["exchange", "symbol", "ts_second"], "left")
        .transform(_coalesce_empty_trade_seconds)
        .transform(_coalesce_empty_bbo_seconds)
    )
    return output.select(
        F.lit(run_id).alias("run_id"),
        *_metadata_columns(metadata, created_at),
        F.col("exchange"),
        F.col("symbol"),
        F.col("second_before_anchor"),
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
    )


def _build_second_grid(spark: Any, exchange_symbols: DataFrame, metadata: Any) -> DataFrame:
    start_literal = _timestamp_literal(metadata.normal_window_start_ts)
    end_literal = _timestamp_literal(metadata.normal_window_end_ts)
    sequence_frame = spark.sql(
        "SELECT explode(sequence("
        f"timestamp'{start_literal}', "
        f"timestamp'{end_literal}' - interval 1 second, "
        "interval 1 second)) AS ts_second"
    )
    return exchange_symbols.crossJoin(sequence_frame).withColumn(
        "second_before_anchor",
        (
            F.lit(metadata.normal_anchor_ts).cast("timestamp").cast("long")
            - F.col("ts_second").cast("long")
        ).cast("int"),
    )


def _build_trade_snapshots(normal_trade: DataFrame) -> DataFrame:
    selected = (
        normal_trade.select(
            "exchange",
            "symbol",
            F.date_trunc("second", F.col("ts_event_ts")).alias("ts_second"),
            "ts_event_ts",
            "seq",
            "trade_id",
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


def _build_bbo_snapshots(normal_bbo: DataFrame) -> DataFrame:
    ranked = normal_bbo.withColumn(
        "ts_second",
        F.date_trunc("second", F.col("ts_event_ts")),
    ).withColumn(
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
        F.max(F.when(F.col("last_rank") == 1, F.col("bid_price").cast("double"))).alias(
            "last_bid_price_raw"
        ),
        F.max(F.when(F.col("last_rank") == 1, F.col("ask_price").cast("double"))).alias(
            "last_ask_price_raw"
        ),
        F.max(F.when(F.col("last_rank") == 1, F.col("bid_qty").cast("double"))).alias(
            "last_bid_qty_raw"
        ),
        F.max(F.when(F.col("last_rank") == 1, F.col("ask_qty").cast("double"))).alias(
            "last_ask_qty_raw"
        ),
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


def _build_normal_profiles(
    normal_snapshot: DataFrame,
    expected_exchanges: tuple[str, ...],
    run_id: str,
    metadata: Any,
    created_at: datetime,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    spark = normal_snapshot.sparkSession
    windows = _profile_windows_frame(spark)
    profiled = normal_snapshot.withColumn("second_before_event", F.col("second_before_anchor"))
    exchange_profile = _build_exchange_profile(profiled, windows, run_id, metadata, created_at)
    cross_exchange_mid_diff = _build_cross_exchange_mid_diff(
        profiled,
        windows,
        expected_exchanges,
        run_id,
        metadata,
        created_at,
    )
    bucket_change_profile = _build_bucket_change_profile(
        spark,
        exchange_profile,
        run_id,
        metadata,
        created_at,
    )
    return exchange_profile, cross_exchange_mid_diff, bucket_change_profile


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


def _profile_audit_columns(run_id: str, metadata: Any, created_at: datetime) -> tuple[Any, ...]:
    return (
        F.lit(run_id).alias("run_id"),
        F.lit(run_id).alias("source_snapshot_run_id"),
        F.lit(SAMPLE_TYPE_NORMAL).alias("sample_type"),
        F.lit(metadata.selected_date).alias("selected_date"),
        F.lit(None).cast("string").alias("event_id"),
        F.lit(None).cast("string").alias("event_direction"),
        F.lit(None).cast("timestamp").alias("event_start_ts"),
        F.lit(metadata.normal_window_start_ts).cast("timestamp").alias("normal_window_start_ts"),
        F.lit(metadata.normal_window_end_ts).cast("timestamp").alias("normal_window_end_ts"),
        F.lit(metadata.normal_anchor_ts).cast("timestamp").alias("normal_anchor_ts"),
        F.lit(PROFILE_VERSION).alias("profile_version"),
        F.lit(created_at).cast("timestamp").alias("created_at"),
    )


def _build_exchange_profile(
    snapshot: DataFrame,
    windows: DataFrame,
    run_id: str,
    metadata: Any,
    created_at: datetime,
) -> DataFrame:
    windowed = (
        snapshot.crossJoin(windows)
        .where(F.col("second_before_event") >= F.col("window_min_second_before_event"))
        .where(F.col("second_before_event") <= F.col("window_max_second_before_event"))
    )
    partition = Window.partitionBy("exchange", "symbol", "window_mode", "window_label")
    ranked = (
        windowed.withColumn(
            "first_rank",
            F.row_number().over(partition.orderBy(F.col("second_before_event").desc())),
        )
        .withColumn(
            "last_rank",
            F.row_number().over(partition.orderBy(F.col("second_before_event").asc())),
        )
        .withColumn("imbalance_qty", F.col("buy_qty") - F.col("sell_qty"))
    )
    grouped = ranked.groupBy(
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
        F.sum("imbalance_qty").alias("trade_imbalance_qty_sum"),
        F.avg("imbalance_qty").alias("trade_imbalance_qty_per_second"),
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
        *_profile_audit_columns(run_id, metadata, created_at),
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


def _build_cross_exchange_mid_diff(
    snapshot: DataFrame,
    windows: DataFrame,
    expected_exchanges: tuple[str, ...],
    run_id: str,
    metadata: Any,
    created_at: datetime,
) -> DataFrame:
    pairs = _exchange_pairs_frame(snapshot.sparkSession, expected_exchanges)
    base = snapshot.select(
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
            (F.col("a.symbol_key") == F.col("b.symbol_key"))
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
    partition = Window.partitionBy("symbol_key", "window_mode", "window_label", "exchange_pair")
    ranked = windowed.withColumn(
        "last_rank",
        F.row_number().over(partition.orderBy(F.col("second_before_event").asc())),
    )
    grouped = ranked.groupBy(
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
        *_profile_audit_columns(run_id, metadata, created_at),
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
    run_id: str,
    metadata: Any,
    created_at: datetime,
) -> DataFrame:
    bucket_pairs = spark.createDataFrame(BUCKET_CHANGE_PAIRS, ("from_bucket", "to_bucket"))
    bucket_metrics = exchange_profile.where(F.col("window_mode") == "bucket").select(
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
    joined = (
        bucket_pairs.join(
            bucket_metrics.alias("from_metric"),
            F.col("from_bucket") == F.col("from_metric.bucket_label"),
            "inner",
        )
        .join(
            bucket_metrics.alias("to_metric"),
            (F.col("to_bucket") == F.col("to_metric.bucket_label"))
            & (F.col("from_metric.exchange") == F.col("to_metric.exchange"))
            & (F.col("from_metric.symbol") == F.col("to_metric.symbol"))
            & (F.col("from_metric.metric_name") == F.col("to_metric.metric_name")),
            "inner",
        )
        .select(
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
        *_profile_audit_columns(run_id, metadata, created_at),
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


def _log_normal_window_counts(
    normal_trade: DataFrame,
    normal_bbo: DataFrame,
    normal_snapshot: DataFrame,
) -> None:
    LOGGER.info("Normal trade row count: %s", normal_trade.count())
    _show_df("Normal trade count by exchange:", normal_trade.groupBy("exchange").count().orderBy("exchange"))
    LOGGER.info("Normal BBO row count: %s", normal_bbo.count())
    _show_df("Normal BBO count by exchange:", normal_bbo.groupBy("exchange").count().orderBy("exchange"))
    LOGGER.info("Normal snapshot row count: %s", normal_snapshot.count())
    _show_df(
        "Normal snapshot count by exchange:",
        normal_snapshot.groupBy("exchange").count().orderBy("exchange"),
    )
    _show_df(
        "Normal snapshot second_before_anchor range:",
        normal_snapshot.agg(
            F.min("second_before_anchor").alias("min_second_before_anchor"),
            F.max("second_before_anchor").alias("max_second_before_anchor"),
        ),
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


def _column_or_null(frame: DataFrame, column: str) -> Any:
    if column in frame.columns:
        return F.col(column)
    return F.lit(None)


def _timestamp_literal(value: datetime) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.isoformat(sep=" ")


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
