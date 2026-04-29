#!/usr/bin/env python
"""Build Phase 3B BTC multi-event content review artifacts from Phase 3A outputs."""

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

from quantlab_event_scanner.btc_multi_event_trial import (  # noqa: E402
    DEFAULT_NORMAL_COUNT_PER_EVENT,
    validate_phase3a_source_run_id,
)
from quantlab_event_scanner.config import load_config  # noqa: E402
from quantlab_event_scanner.paths import (  # noqa: E402
    btc_multi_event_review_trial_run_path,
    btc_multi_event_trial_run_path,
)
from quantlab_event_scanner.phase3b_multi_event_review import (  # noqa: E402
    CONTEXT_DOMINANCE_REPORT_COLUMNS,
    DIRECTION_SIGN_CONSISTENCY_REPORT_COLUMNS,
    EVENT_NARRATIVE_REPORT_COLUMNS,
    EXPECTED_SIGN_CONTEXT,
    EXPECTED_SIGN_DIRECTION_NORMALIZED,
    EXPECTED_SIGN_NONE,
    EXPECTED_SIGN_OBSERVED_SIGNED,
    EXPECTED_SIGN_UNSTABLE,
    METRIC_RECURRENT_REPORT_COLUMNS,
    POLICY_CONTEXT_FILTER,
    POLICY_CORE_FEATURE,
    POLICY_DIAGNOSTIC,
    POLICY_EXCLUDE,
    PROFILE_FAMILY_PHASE3A_COMPARISON,
    PROFILE_FAMILY_PHASE3A_TOP_DIFF,
    RECOMMENDED_METRIC_POLICY_COLUMNS,
    REQUIRED_COMPARISON_SUBTABLES,
    REQUIRED_SUMMARY_SUBTABLES,
    REQUIRED_TOP_DIFF_SUBTABLES,
    SUMMARY_REQUIRED_COLUMNS,
    TOP_DIFF_REQUIRED_COLUMNS,
    default_phase3b_review_run_id,
    validate_phase3b_review_run_id,
)
from quantlab_event_scanner.profile_comparison_reports import (  # noqa: E402
    METRIC_GROUP_CONTEXT,
    METRIC_GROUP_PRICE_DISLOCATION,
    METRIC_GROUP_SIGNAL_CANDIDATE,
    METRIC_GROUP_UNSTABLE,
    expected_bucket_change_profile_comparison_rows,
    expected_cross_exchange_mid_diff_comparison_rows,
    expected_exchange_profile_comparison_rows,
)


LOGGER = logging.getLogger("quantlab_event_scanner.phase3b_btc_multi_event_review")
DEFAULT_TOP_K = 20
DEFAULT_SMALL_OUTPUT_PARTITIONS = 1

IDENTITY_COLUMNS = (
    "profile_family",
    "report_group",
    "metric_group",
    "metric_name",
    "exchange",
    "exchange_pair",
    "symbol_key",
    "window_label",
    "from_bucket",
    "to_bucket",
)


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    source_run_id = validate_phase3a_source_run_id(args.source_run_id)
    review_run_id = validate_phase3b_review_run_id(
        args.review_run_id or default_phase3b_review_run_id(datetime.now(timezone.utc))
    )
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    config_path = _resolve_path(args.config)
    config = load_config(config_path)

    source_root = btc_multi_event_trial_run_path(config, source_run_id)
    output_root = btc_multi_event_review_trial_run_path(config, source_run_id, review_run_id)

    LOGGER.info("Using config: %s", config_path)
    LOGGER.info("Source run ID: %s", source_run_id)
    LOGGER.info("Review run ID: %s", review_run_id)
    LOGGER.info("Source root: %s", source_root)
    LOGGER.info("Output root: %s", output_root)
    for key, value in sorted(vars(args).items()):
        LOGGER.info("[PHASE3B_CONF] key=%s value=%s", key, value)

    spark = _get_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    events = _read_required(spark, f"{source_root}/events", "events").cache()
    normal_samples = _read_required(spark, f"{source_root}/normal_samples", "normal_samples").cache()
    event_processing_status = _read_required(
        spark,
        f"{source_root}/summary/event_processing_status",
        "summary.event_processing_status",
    ).cache()

    _validate_columns(event_processing_status, SUMMARY_REQUIRED_COLUMNS, "summary.event_processing_status")

    top_diffs = _read_top_diffs(spark, source_root).cache()
    comparisons = _read_comparisons(spark, source_root).cache()
    _read_summary_contract(spark, source_root)

    source_event_count = events.select("event_id").distinct().count()
    if source_event_count <= 0:
        raise RuntimeError("Phase 3B source events are empty.")

    _validate_positive_count(normal_samples, "normal_samples")
    _validate_normal_selection(event_processing_status, args.normal_count_per_event)
    _validate_comparison_counts(comparisons, source_event_count)
    _validate_positive_count(top_diffs, "top_diffs")

    metric_recurrence = _small(
        _metric_recurrence_report(top_diffs, review_run_id, source_run_id, source_event_count, args.top_k, created_at),
        args.small_output_partitions,
    ).cache()
    direction_sign = _small(
        _direction_sign_consistency_report(comparisons, review_run_id, source_run_id, created_at),
        args.small_output_partitions,
    ).cache()
    context_dominance = _small(
        _context_dominance_report(top_diffs, review_run_id, source_run_id, args.top_k, created_at),
        args.small_output_partitions,
    ).cache()
    event_narrative = _small(
        _event_narrative_report(
            events,
            top_diffs,
            event_processing_status,
            review_run_id,
            source_run_id,
            args.normal_count_per_event,
            created_at,
        ),
        args.small_output_partitions,
    ).cache()
    metric_policy = _small(
        _recommended_metric_policy(metric_recurrence, review_run_id, source_run_id, created_at),
        args.small_output_partitions,
    ).cache()

    _select_contract_columns(metric_recurrence, METRIC_RECURRENT_REPORT_COLUMNS, "metric_recurrence_report")
    _select_contract_columns(direction_sign, DIRECTION_SIGN_CONSISTENCY_REPORT_COLUMNS, "direction_sign_consistency_report")
    _select_contract_columns(context_dominance, CONTEXT_DOMINANCE_REPORT_COLUMNS, "context_dominance_report")
    _select_contract_columns(event_narrative, EVENT_NARRATIVE_REPORT_COLUMNS, "event_narrative_report")
    _select_contract_columns(metric_policy, RECOMMENDED_METRIC_POLICY_COLUMNS, "recommended_metric_policy_v1")

    _write_and_validate(
        metric_recurrence,
        f"{output_root}/metric_recurrence_report",
        "metric_recurrence_report",
    )
    _write_and_validate(
        direction_sign,
        f"{output_root}/direction_sign_consistency_report",
        "direction_sign_consistency_report",
    )
    _write_and_validate(
        context_dominance,
        f"{output_root}/context_dominance_report",
        "context_dominance_report",
    )
    _write_and_validate(
        event_narrative,
        f"{output_root}/event_narrative_report",
        "event_narrative_report",
    )
    _write_and_validate(
        metric_policy,
        f"{output_root}/recommended_metric_policy_v1",
        "recommended_metric_policy_v1",
    )

    if args.sample_size > 0:
        _log_samples(metric_recurrence, "metric_recurrence_report", args.sample_size)
        _log_samples(metric_policy, "recommended_metric_policy_v1", args.sample_size)

    LOGGER.info(
        "[PHASE3B_OUTPUT] root=%s source_run_id=%s review_run_id=%s source_event_count=%s "
        "stage=review_complete",
        output_root,
        source_run_id,
        review_run_id,
        source_event_count,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Review Phase 3A BTC multi-event content.")
    parser.add_argument("--config", default="configs/dev.yaml")
    parser.add_argument("--source-run-id", required=True)
    parser.add_argument("--review-run-id", default=None)
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    parser.add_argument("--normal-count-per-event", type=int, default=DEFAULT_NORMAL_COUNT_PER_EVENT)
    parser.add_argument("--validation-mode", choices=("strict", "light"), default="light")
    parser.add_argument("--small-output-partitions", type=int, default=DEFAULT_SMALL_OUTPUT_PARTITIONS)
    parser.add_argument("--sample-size", type=int, default=20)
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


def _get_spark_session() -> Any:
    try:
        return spark  # type: ignore[name-defined]  # noqa: F821
    except NameError:
        from pyspark.sql import SparkSession

        return SparkSession.builder.appName("phase3b-btc-multi-event-content-review").getOrCreate()


def _read_required(spark_session: Any, path: str, label: str) -> DataFrame:
    LOGGER.info("[PHASE3B_INPUT] name=%s path=%s", label, path)
    frame = spark_session.read.parquet(path)
    LOGGER.info("[PHASE3B_DF] name=%s rows=%s columns=%s", label, frame.count(), ",".join(frame.columns))
    return frame


def _read_top_diffs(spark_session: Any, source_root: str) -> DataFrame:
    frames = []
    for subtable in REQUIRED_TOP_DIFF_SUBTABLES:
        label = subtable.replace("/", ".")
        frame = _read_required(spark_session, f"{source_root}/{subtable}", label)
        _validate_columns(frame, TOP_DIFF_REQUIRED_COLUMNS, label)
        frames.append(frame.withColumn("profile_family", F.lit(PROFILE_FAMILY_PHASE3A_TOP_DIFF)))
    return _union_by_name(frames)


def _read_comparisons(spark_session: Any, source_root: str) -> DataFrame:
    frames = []
    required_columns = tuple(
        column for column in TOP_DIFF_REQUIRED_COLUMNS if column not in ("rank_type", "rank")
    )
    for subtable in REQUIRED_COMPARISON_SUBTABLES:
        label = subtable.replace("/", ".")
        frame = _read_required(spark_session, f"{source_root}/{subtable}", label)
        _validate_columns(frame, required_columns, label)
        frames.append(frame.withColumn("profile_family", F.lit(PROFILE_FAMILY_PHASE3A_COMPARISON)))
    return _union_by_name(frames)


def _read_summary_contract(spark_session: Any, source_root: str) -> None:
    for subtable in REQUIRED_SUMMARY_SUBTABLES:
        _read_required(spark_session, f"{source_root}/{subtable}", subtable.replace("/", "."))


def _metric_recurrence_report(
    top_diffs: DataFrame,
    review_run_id: str,
    source_run_id: str,
    source_event_count: int,
    top_k: int,
    created_at: datetime,
) -> DataFrame:
    base = _with_review_flags(top_diffs.where(F.col("rank") <= F.lit(int(top_k))))
    grouped = base.groupBy(*IDENTITY_COLUMNS).agg(
        F.countDistinct("source_event_id").cast("int").alias("n_events_seen_in_top_k"),
        F.countDistinct(F.when(F.col("event_direction") == "UP", F.col("source_event_id"))).cast("int").alias("n_up_events_seen"),
        F.countDistinct(F.when(F.col("event_direction") == "DOWN", F.col("source_event_id"))).cast("int").alias("n_down_events_seen"),
        F.expr("percentile_approx(cast(rank as double), 0.5)").alias("median_rank"),
        F.expr("percentile_approx(cast(absolute_diff_vs_mean as double), 0.5)").alias("median_abs_diff_vs_mean"),
        F.expr("percentile_approx(abs(cast(z_score_vs_normal as double)), 0.5)").alias("median_abs_z_score"),
        F.expr("percentile_approx(cast(percentile_extremeness as double), 0.5)").alias("median_percentile_extremeness"),
        F.countDistinct(F.when(F.col("denominator_risk_flag"), F.col("source_event_id"))).cast("int").alias("denominator_risk_event_count"),
        F.countDistinct(F.when(F.col("unstable_metric_flag"), F.col("source_event_id"))).cast("int").alias("unstable_event_count"),
        F.sum(F.when(F.col("direction_normalized_sign_value") > 0, 1).otherwise(0)).alias("_normalized_positive_count"),
        F.sum(F.when(F.col("direction_normalized_sign_value") < 0, 1).otherwise(0)).alias("_normalized_negative_count"),
    )
    return (
        grouped.withColumn("review_run_id", F.lit(review_run_id))
        .withColumn("source_run_id", F.lit(source_run_id))
        .withColumn("source_event_count", F.lit(int(source_event_count)))
        .withColumn("rank_scope", F.lit("top_k_by_rank_type"))
        .withColumn("top_k", F.lit(int(top_k)))
        .withColumn(
            "direction_normalized_sign_consistency",
            _sign_label(F.col("_normalized_positive_count"), F.col("_normalized_negative_count")),
        )
        .withColumn("recommendation", _policy_bucket_expr())
        .withColumn("created_at", F.lit(created_at).cast("timestamp"))
        .select(*METRIC_RECURRENT_REPORT_COLUMNS)
    )


def _direction_sign_consistency_report(
    comparisons: DataFrame,
    review_run_id: str,
    source_run_id: str,
    created_at: datetime,
) -> DataFrame:
    base = _with_review_flags(comparisons)
    grouped = base.groupBy(*IDENTITY_COLUMNS).agg(
        F.sum(F.when((F.col("event_direction") == "UP") & (F.col("signed_diff_vs_mean") > 0), 1).otherwise(0)).alias("up_positive_count"),
        F.sum(F.when((F.col("event_direction") == "UP") & (F.col("signed_diff_vs_mean") < 0), 1).otherwise(0)).alias("up_negative_count"),
        F.sum(F.when((F.col("event_direction") == "DOWN") & (F.col("signed_diff_vs_mean") > 0), 1).otherwise(0)).alias("down_positive_count"),
        F.sum(F.when((F.col("event_direction") == "DOWN") & (F.col("signed_diff_vs_mean") < 0), 1).otherwise(0)).alias("down_negative_count"),
        F.sum(F.when(F.col("signed_diff_vs_mean") > 0, 1).otherwise(0)).alias("observed_positive_count"),
        F.sum(F.when(F.col("signed_diff_vs_mean") < 0, 1).otherwise(0)).alias("observed_negative_count"),
        F.sum(F.when(F.col("signed_diff_vs_mean").isNull() | (F.col("signed_diff_vs_mean") == 0), 1).otherwise(0)).alias("observed_zero_count"),
        F.sum(F.when(F.col("direction_normalized_sign_value") > 0, 1).otherwise(0)).alias("_normalized_positive_count"),
        F.sum(F.when(F.col("direction_normalized_sign_value") < 0, 1).otherwise(0)).alias("_normalized_negative_count"),
    )
    non_zero_count = F.col("_normalized_positive_count") + F.col("_normalized_negative_count")
    return (
        grouped.withColumn("review_run_id", F.lit(review_run_id))
        .withColumn("source_run_id", F.lit(source_run_id))
        .withColumn("expected_sign_policy", _expected_sign_policy_expr())
        .withColumn(
            "sign_consistency_rate",
            F.when(non_zero_count == 0, F.lit(None).cast("double")).otherwise(
                F.greatest(F.col("_normalized_positive_count"), F.col("_normalized_negative_count"))
                / non_zero_count
            ),
        )
        .withColumn("direction_normalized_sign", _sign_label(F.col("_normalized_positive_count"), F.col("_normalized_negative_count")))
        .withColumn("notes", F.lit("descriptive_only_not_signal_validation"))
        .withColumn("created_at", F.lit(created_at).cast("timestamp"))
        .select(*DIRECTION_SIGN_CONSISTENCY_REPORT_COLUMNS)
    )


def _context_dominance_report(
    top_diffs: DataFrame,
    review_run_id: str,
    source_run_id: str,
    top_k: int,
    created_at: datetime,
) -> DataFrame:
    base = _with_review_flags(top_diffs.where(F.col("rank") <= F.lit(int(top_k))))
    grouped = base.groupBy("profile_family", "report_group", "rank_type").agg(
        F.sum(F.when(F.col("metric_group") == METRIC_GROUP_CONTEXT, 1).otherwise(0)).alias("context_metric_count"),
        F.sum(F.when(F.col("metric_group") == METRIC_GROUP_SIGNAL_CANDIDATE, 1).otherwise(0)).alias("signal_candidate_count"),
        F.sum(F.when(F.col("metric_group") == METRIC_GROUP_PRICE_DISLOCATION, 1).otherwise(0)).alias("price_dislocation_count"),
        F.sum(F.when(F.col("metric_group") == METRIC_GROUP_UNSTABLE, 1).otherwise(0)).alias("unstable_count"),
        F.sum(F.when(F.col("denominator_risk_flag"), 1).otherwise(0)).alias("denominator_risk_count"),
        F.count("*").alias("_row_count"),
    )
    return (
        grouped.withColumn("review_run_id", F.lit(review_run_id))
        .withColumn("source_run_id", F.lit(source_run_id))
        .withColumn("top_k", F.lit(int(top_k)))
        .withColumn("context_share", F.col("context_metric_count") / F.col("_row_count"))
        .withColumn("unstable_share", F.col("unstable_count") / F.col("_row_count"))
        .withColumn("denominator_risk_share", F.col("denominator_risk_count") / F.col("_row_count"))
        .withColumn("dominant_metric_group", _dominant_metric_group_expr())
        .withColumn("recommended_action", _context_recommended_action_expr())
        .withColumn("created_at", F.lit(created_at).cast("timestamp"))
        .select(*CONTEXT_DOMINANCE_REPORT_COLUMNS)
    )


def _event_narrative_report(
    events: DataFrame,
    top_diffs: DataFrame,
    event_processing_status: DataFrame,
    review_run_id: str,
    source_run_id: str,
    normal_count_per_event: int,
    created_at: datetime,
) -> DataFrame:
    base = _with_review_flags(top_diffs)
    aggregates = base.groupBy("source_event_id").agg(
        F.concat_ws(",", F.sort_array(F.collect_set("exchange"))).alias("dominant_exchange_pattern"),
        F.concat_ws(",", F.sort_array(F.collect_set("exchange_pair"))).alias("cross_exchange_pattern"),
        F.sum(F.when(F.col("metric_name").contains("book") | F.col("metric_name").contains("spread"), 1).otherwise(0)).alias("_book_spread_count"),
        F.sum(F.when(F.col("metric_name").contains("trade_imbalance") | F.col("metric_name").contains("trade_count"), 1).otherwise(0)).alias("_trade_flow_count"),
        F.sum(F.when(F.col("metric_name").contains("return") | F.col("metric_name").contains("mid_diff"), 1).otherwise(0)).alias("_price_return_count"),
        F.sum(F.when(F.col("denominator_risk_flag"), 1).otherwise(0)).alias("_denominator_risk_rows"),
        F.sum(F.when(F.col("unstable_metric_flag"), 1).otherwise(0)).alias("_unstable_rows"),
        F.concat_ws(
            ",",
            F.sort_array(
                F.collect_set(
                    F.when(F.col("window_label").isin("last_10s", "bucket_10_0s"), F.col("window_label"))
                )
            ),
        ).alias("suspected_late_reaction_flags"),
    )
    status = event_processing_status.select("event_id", "normal_selection_count")
    return (
        events.join(aggregates, events.event_id == aggregates.source_event_id, "left")
        .join(status, events.event_id == status.event_id, "left")
        .select(
            F.lit(review_run_id).alias("review_run_id"),
            F.lit(source_run_id).alias("source_run_id"),
            events.event_id.alias("event_id"),
            events.event_rank.alias("event_rank"),
            events.symbol.alias("symbol"),
            events.direction.alias("direction"),
            events.event_start_ts.alias("event_start_ts"),
            events.event_end_ts.alias("event_end_ts"),
            F.coalesce(F.col("dominant_exchange_pattern"), F.lit("not_observed_in_top_diffs")).alias("dominant_exchange_pattern"),
            F.coalesce(F.col("cross_exchange_pattern"), F.lit("not_observed_in_top_diffs")).alias("cross_exchange_pattern"),
            _presence_label(F.col("_book_spread_count")).alias("book_spread_pattern"),
            _presence_label(F.col("_trade_flow_count")).alias("trade_flow_pattern"),
            _presence_label(F.col("_price_return_count")).alias("price_return_pattern"),
            F.when(F.col("normal_selection_count") == F.lit(int(normal_count_per_event)), F.lit("selected_normal_count_valid"))
            .otherwise(F.lit("review_normal_sample_count"))
            .alias("normal_sample_quality"),
            F.format_string(
                "denominator_risk_rows=%s;unstable_rows=%s",
                F.coalesce(F.col("_denominator_risk_rows"), F.lit(0)),
                F.coalesce(F.col("_unstable_rows"), F.lit(0)),
            ).alias("metric_anomalies"),
            F.coalesce(F.col("suspected_late_reaction_flags"), F.lit("")).alias("suspected_late_reaction_flags"),
            F.lit("descriptive_only_not_signal_validation").alias("notes"),
            F.lit(created_at).cast("timestamp").alias("created_at"),
        )
    )


def _recommended_metric_policy(
    recurrence: DataFrame,
    review_run_id: str,
    source_run_id: str,
    created_at: datetime,
) -> DataFrame:
    return (
        recurrence.withColumn("policy_bucket", F.col("recommendation"))
        .withColumn(
            "reason",
            F.when(F.col("metric_group") == METRIC_GROUP_CONTEXT, F.lit("context_metric_for_matching_regime_or_diagnostic"))
            .when(F.col("metric_group") == METRIC_GROUP_UNSTABLE, F.lit("unstable_or_relative_change_metric"))
            .when(F.col("denominator_risk_event_count") > 0, F.lit("denominator_risk_present"))
            .when(F.col("policy_bucket") == POLICY_CORE_FEATURE, F.concat(F.col("metric_group"), F.lit("_candidate_for_future_feature_review")))
            .otherwise(F.lit("review_required")),
        )
        .withColumn("requires_more_data", F.lit(True))
        .withColumn("phase4b_candidate", F.col("policy_bucket") == POLICY_CORE_FEATURE)
        .withColumn(
            "denominator_risk_policy",
            F.when(F.col("denominator_risk_event_count") > 0, F.lit("exclude_primary"))
            .when(F.col("metric_group") == METRIC_GROUP_UNSTABLE, F.lit("appendix_only"))
            .otherwise(F.lit("none_observed")),
        )
        .withColumn("review_run_id", F.lit(review_run_id))
        .withColumn("source_run_id", F.lit(source_run_id))
        .withColumn("created_at", F.lit(created_at).cast("timestamp"))
        .select(*RECOMMENDED_METRIC_POLICY_COLUMNS)
        .dropDuplicates()
    )


def _with_review_flags(frame: DataFrame) -> DataFrame:
    sign_value = (
        F.when(F.col("signed_diff_vs_mean") > 0, F.lit(1))
        .when(F.col("signed_diff_vs_mean") < 0, F.lit(-1))
        .otherwise(F.lit(0))
    )
    return (
        frame.withColumn(
            "denominator_risk_flag",
            F.coalesce(F.col("ratio_unstable"), F.lit(False))
            | F.coalesce(F.col("relative_change_unstable"), F.lit(False))
            | F.coalesce(F.col("small_denominator_flag"), F.lit(False)),
        )
        .withColumn(
            "unstable_metric_flag",
            (F.col("metric_group") == METRIC_GROUP_UNSTABLE)
            | F.col("metric_name").endswith(".relative_change"),
        )
        .withColumn(
            "percentile_extremeness",
            F.greatest(F.col("normal_percentile_rank"), F.lit(1.0) - F.col("normal_percentile_rank")),
        )
        .withColumn(
            "direction_normalized_sign_value",
            F.when(F.col("event_direction") == "DOWN", -sign_value).otherwise(sign_value),
        )
    )


def _expected_sign_policy_expr() -> Any:
    return (
        F.when(F.col("metric_name").contains("abs"), F.lit(EXPECTED_SIGN_NONE))
        .when(
            (F.col("metric_group") == METRIC_GROUP_UNSTABLE)
            | F.col("metric_name").endswith(".relative_change"),
            F.lit(EXPECTED_SIGN_UNSTABLE),
        )
        .when(F.col("metric_group") == METRIC_GROUP_CONTEXT, F.lit(EXPECTED_SIGN_CONTEXT))
        .when(
            (F.col("metric_group") == METRIC_GROUP_PRICE_DISLOCATION)
            & (F.col("metric_name").contains("return_bps") | F.col("metric_name").endswith("mid_diff_bps")),
            F.lit(EXPECTED_SIGN_DIRECTION_NORMALIZED),
        )
        .otherwise(F.lit(EXPECTED_SIGN_OBSERVED_SIGNED))
    )


def _policy_bucket_expr() -> Any:
    return (
        F.when(F.col("unstable_event_count") > 0, F.lit(POLICY_DIAGNOSTIC))
        .when(F.col("denominator_risk_event_count") >= F.col("source_event_count"), F.lit(POLICY_EXCLUDE))
        .when(F.col("denominator_risk_event_count") > 0, F.lit(POLICY_DIAGNOSTIC))
        .when(F.col("metric_group") == METRIC_GROUP_CONTEXT, F.lit(POLICY_CONTEXT_FILTER))
        .when(
            F.col("metric_group").isin(METRIC_GROUP_PRICE_DISLOCATION, METRIC_GROUP_SIGNAL_CANDIDATE),
            F.lit(POLICY_CORE_FEATURE),
        )
        .otherwise(F.lit(POLICY_DIAGNOSTIC))
    )


def _sign_label(positive_count: Any, negative_count: Any) -> Any:
    return (
        F.when((positive_count == 0) & (negative_count == 0), F.lit("none"))
        .when((positive_count > 0) & (negative_count > 0), F.lit("mixed"))
        .when(positive_count > 0, F.lit("positive"))
        .otherwise(F.lit("negative"))
    )


def _dominant_metric_group_expr() -> Any:
    greatest_count = F.greatest(
        F.col("context_metric_count"),
        F.col("signal_candidate_count"),
        F.col("price_dislocation_count"),
        F.col("unstable_count"),
    )
    return (
        F.when(F.col("context_metric_count") == greatest_count, F.lit(METRIC_GROUP_CONTEXT))
        .when(F.col("signal_candidate_count") == greatest_count, F.lit(METRIC_GROUP_SIGNAL_CANDIDATE))
        .when(F.col("price_dislocation_count") == greatest_count, F.lit(METRIC_GROUP_PRICE_DISLOCATION))
        .otherwise(F.lit(METRIC_GROUP_UNSTABLE))
    )


def _context_recommended_action_expr() -> Any:
    return (
        F.when(F.col("context_share") >= F.lit(0.5), F.lit("review_context_as_filter"))
        .when(
            (F.col("unstable_count") > 0) | (F.col("denominator_risk_count") > 0),
            F.lit("demote_unstable_or_denominator_risk"),
        )
        .otherwise(F.lit("review_signal_candidate_content"))
    )


def _presence_label(count_column: Any) -> Any:
    return F.when(F.coalesce(count_column, F.lit(0)) > 0, F.format_string("present_rows=%s", count_column)).otherwise(
        F.lit("not_observed_in_review_rows")
    )


def _validate_normal_selection(event_processing_status: DataFrame, normal_count_per_event: int) -> None:
    invalid = event_processing_status.where(
        F.col("normal_selection_count") != F.lit(int(normal_count_per_event))
    ).count()
    LOGGER.info("Phase 3B normal selection invalid event rows: %s", invalid)
    if invalid:
        raise RuntimeError("Phase 3B source normal selection validation failed.")


def _validate_comparison_counts(comparisons: DataFrame, source_event_count: int) -> None:
    expected_by_group = {
        "exchange_profile": expected_exchange_profile_comparison_rows() * source_event_count,
        "cross_exchange_mid_diff": expected_cross_exchange_mid_diff_comparison_rows() * source_event_count,
        "bucket_change_profile": expected_bucket_change_profile_comparison_rows() * source_event_count,
    }
    for report_group, expected_count in expected_by_group.items():
        observed = comparisons.where(F.col("report_group") == report_group).count()
        LOGGER.info(
            "Phase 3B comparison rows report_group=%s expected=%s observed=%s",
            report_group,
            expected_count,
            observed,
        )
        if observed != expected_count:
            raise RuntimeError(
                f"comparison row count mismatch for {report_group}: "
                f"expected={expected_count}, observed={observed}"
            )


def _validate_positive_count(frame: DataFrame, label: str) -> None:
    count = frame.count()
    LOGGER.info("Phase 3B %s rows: %s", label, count)
    if count <= 0:
        raise RuntimeError(f"{label} row count is 0.")


def _validate_columns(frame: DataFrame, required_columns: tuple[str, ...], label: str) -> None:
    available = set(frame.columns)
    missing = [column for column in required_columns if column not in available]
    if missing:
        raise RuntimeError(f"{label} missing required columns: {', '.join(missing)}")


def _select_contract_columns(frame: DataFrame, columns: tuple[str, ...], label: str) -> DataFrame:
    _validate_columns(frame, columns, label)
    return frame.select(*columns)


def _union_by_name(frames: list[DataFrame]) -> DataFrame:
    if not frames:
        raise ValueError("frames must not be empty.")
    current = frames[0]
    for frame in frames[1:]:
        current = current.unionByName(frame, allowMissingColumns=True)
    return current


def _small(frame: DataFrame, partitions: int) -> DataFrame:
    return frame.coalesce(max(int(partitions), 1))


def _write_and_validate(frame: DataFrame, path: str, label: str) -> int:
    output_count = frame.count()
    LOGGER.info("[PHASE3B_OUTPUT] name=%s path=%s rows=%s", label, path, output_count)
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


def _log_samples(frame: DataFrame, label: str, sample_size: int) -> None:
    LOGGER.info("[PHASE3B_SAMPLE] name=%s rows=%s", label, frame.limit(sample_size).collect())


if __name__ == "__main__":
    main()
