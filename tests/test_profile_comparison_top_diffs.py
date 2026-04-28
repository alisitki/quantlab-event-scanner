from datetime import datetime, timezone

import pytest

from quantlab_event_scanner.profile_comparison_top_diffs import (
    RANK_TOP_ABSOLUTE_DIFF,
    RANK_TOP_SIGNED_NEGATIVE,
    RANK_TOP_SIGNED_POSITIVE,
    TOP_DIFF_RANKING_DEFINITIONS,
    TOP_DIFF_PARTITION_COLUMNS,
    TOP_DIFF_RANK_TYPES,
    TOP_DIFF_REQUIRED_COLUMNS,
    TOP_DIFF_TIE_BREAKER_COLUMNS,
    absolute_diff_is_valid,
    default_phase2g_run_id,
    expected_top_diff_rows,
    expected_top_diff_rows_for_metric_group,
    expected_top_diff_rows_from_group_counts,
    invalid_absolute_diff_rows,
    missing_required_columns,
    signed_diff_distribution,
    validate_required_columns,
)


def test_default_phase2g_run_id_uses_utc_timestamp() -> None:
    now = datetime(2026, 4, 27, 10, 20, 30, tzinfo=timezone.utc)

    assert default_phase2g_run_id(now) == "phase2g_20260427T102030Z"


def test_rank_type_constants_match_approved_names() -> None:
    assert TOP_DIFF_RANK_TYPES == (
        "top_absolute_diff",
        "top_signed_positive",
        "top_signed_negative",
    )
    assert RANK_TOP_ABSOLUTE_DIFF == "top_absolute_diff"
    assert RANK_TOP_SIGNED_POSITIVE == "top_signed_positive"
    assert RANK_TOP_SIGNED_NEGATIVE == "top_signed_negative"


def test_expected_top_diff_rows() -> None:
    assert expected_top_diff_rows() == 60
    assert expected_top_diff_rows(10) == 30
    with pytest.raises(ValueError, match="top_n"):
        expected_top_diff_rows(0)


def test_dynamic_expected_top_diff_rows_handle_small_metric_groups() -> None:
    assert expected_top_diff_rows_for_metric_group(100, top_n=20) == 60
    assert expected_top_diff_rows_for_metric_group(3, top_n=20) == 9
    assert expected_top_diff_rows_from_group_counts((100, 3), top_n=20) == 69
    with pytest.raises(ValueError, match="source_rows"):
        expected_top_diff_rows_for_metric_group(-1)


def test_required_column_validation_fails_on_missing_diff_fields() -> None:
    available = tuple(column for column in TOP_DIFF_REQUIRED_COLUMNS if column != "signed_diff")

    assert missing_required_columns(available) == ("signed_diff",)
    with pytest.raises(ValueError, match="signed_diff"):
        validate_required_columns(available)

    missing_absolute = tuple(
        column for column in TOP_DIFF_REQUIRED_COLUMNS if column != "absolute_diff"
    )
    with pytest.raises(ValueError, match="absolute_diff"):
        validate_required_columns(missing_absolute)


def test_absolute_diff_validation_catches_negative_and_mismatch() -> None:
    assert absolute_diff_is_valid(-3.0, 3.0)
    assert absolute_diff_is_valid(3.0, 3.0000000001)
    assert absolute_diff_is_valid(None, 3.0)
    assert not absolute_diff_is_valid(-3.0, -3.0)
    assert not absolute_diff_is_valid(-3.0, 2.0)
    assert invalid_absolute_diff_rows(((-3.0, 3.0), (-3.0, 2.0))) == ((-3.0, 2.0),)


def test_ranking_definitions_and_tie_breakers_are_deterministic() -> None:
    assert TOP_DIFF_RANKING_DEFINITIONS[0].rank_type == RANK_TOP_ABSOLUTE_DIFF
    assert TOP_DIFF_RANKING_DEFINITIONS[0].primary_column == "absolute_diff"
    assert TOP_DIFF_RANKING_DEFINITIONS[0].ascending is False
    assert TOP_DIFF_RANKING_DEFINITIONS[1].rank_type == RANK_TOP_SIGNED_POSITIVE
    assert TOP_DIFF_RANKING_DEFINITIONS[1].primary_column == "signed_diff"
    assert TOP_DIFF_RANKING_DEFINITIONS[1].ascending is False
    assert TOP_DIFF_RANKING_DEFINITIONS[2].rank_type == RANK_TOP_SIGNED_NEGATIVE
    assert TOP_DIFF_RANKING_DEFINITIONS[2].primary_column == "signed_diff"
    assert TOP_DIFF_RANKING_DEFINITIONS[2].ascending is True
    assert TOP_DIFF_PARTITION_COLUMNS == ("report_group", "metric_group")
    assert TOP_DIFF_TIE_BREAKER_COLUMNS == (
        "report_group",
        "metric_name",
        "exchange",
        "symbol_key",
        "exchange_pair",
        "window_mode",
        "window_label",
        "from_bucket",
        "to_bucket",
    )


def test_signed_diff_distribution_counts_all_buckets() -> None:
    distribution = signed_diff_distribution((2.0, -1.0, 0.0, None, 4.0, -3.0))

    assert distribution.positive_count == 2
    assert distribution.negative_count == 2
    assert distribution.zero_count == 1
    assert distribution.null_count == 1
