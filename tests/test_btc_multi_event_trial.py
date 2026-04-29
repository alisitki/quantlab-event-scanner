from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from quantlab_event_scanner.btc_multi_event_trial import (
    STATUS_SELECTED,
    GroupedMoveEvent,
    RawMoveCandidate,
    default_phase3a_run_id,
    direction_balance_note,
    expected_comparison_rows,
    expected_event_profile_rows,
    expected_event_snapshot_rows,
    expected_normal_profile_rows,
    expected_normal_snapshot_rows,
    expected_top_diff_rows_dynamic,
    group_time_clustered_candidates,
    phase3a_trial_subpath,
    processing_status,
    raw_candidate_start_is_excluded,
    select_direction_balanced_events,
    validate_phase3a_source_run_id,
)


def _candidate(offset: int, direction: str = "DOWN") -> RawMoveCandidate:
    start = datetime(2026, 4, 23, 23, 59, 30, tzinfo=timezone.utc) + timedelta(seconds=offset)
    return RawMoveCandidate(
        exchange="binance",
        symbol="BTCUSDT",
        direction=direction,
        start_ts=start,
        end_ts=start + timedelta(seconds=20),
        duration_seconds=20,
        base_price=100.0,
        touch_price=99.0 if direction == "DOWN" else 101.0,
        move_pct=-1.0 if direction == "DOWN" else 1.0,
    )


def _event(index: int, direction: str) -> GroupedMoveEvent:
    start = datetime(2026, 4, 23, 10, 0, 0) + timedelta(minutes=index)
    return GroupedMoveEvent(
        event_id=f"event_{index:03d}_{direction.lower()}",
        event_rank=index,
        exchange="binance",
        symbol="BTCUSDT",
        direction=direction,
        event_start_ts=start,
        event_end_ts=start + timedelta(seconds=30),
        duration_seconds=30,
        base_price=100.0,
        touch_price=99.0,
        move_pct=-1.0 if direction == "DOWN" else 1.0,
        selected_date="20260423",
        raw_candidate_count=1,
        candidate_group_start_ts=start,
        candidate_group_end_ts=start,
    )


def test_default_phase3a_run_id_uses_utc_timestamp() -> None:
    now = datetime(2026, 4, 28, 10, 20, 30, tzinfo=timezone.utc)

    assert default_phase3a_run_id(now) == "phase3a_20260428T102030Z"


def test_phase3a_finalize_source_run_id_validation_and_paths() -> None:
    assert validate_phase3a_source_run_id(" phase3a_20260428T102030Z ") == "phase3a_20260428T102030Z"
    assert phase3a_trial_subpath(
        "s3://quantlab-research",
        "phase3a_20260428T102030Z",
        "comparison_reports",
        "exchange_profile_comparison",
    ) == (
        "s3://quantlab-research/btc_multi_event_trials/_trial/"
        "run_id=phase3a_20260428T102030Z/comparison_reports/exchange_profile_comparison"
    )

    with pytest.raises(ValueError):
        validate_phase3a_source_run_id("")
    with pytest.raises(ValueError):
        validate_phase3a_source_run_id("phase2j_20260428T102030Z")
    with pytest.raises(ValueError):
        validate_phase3a_source_run_id("phase3a_20260428T102030Z/comparison_reports")


def test_time_cluster_grouping_crosses_date_boundary_and_uses_overlap() -> None:
    groups = group_time_clustered_candidates(
        [_candidate(0), _candidate(59), _candidate(121)],
        horizon_seconds=60,
    )

    assert len(groups) == 2
    assert groups[0].raw_candidate_count == 2
    assert groups[0].selected_date == "20260423"
    assert groups[0].candidate_group_end_ts == datetime(2026, 4, 24, 0, 0, 29)
    assert groups[1].raw_candidate_count == 1


def test_time_cluster_grouping_keeps_directions_separate() -> None:
    groups = group_time_clustered_candidates(
        [_candidate(0, "DOWN"), _candidate(1, "UP")],
        horizon_seconds=60,
    )

    assert {(group.direction, group.raw_candidate_count) for group in groups} == {
        ("DOWN", 1),
        ("UP", 1),
    }


def test_direction_balanced_selection_falls_back_without_failing() -> None:
    events = [_event(1, "DOWN"), _event(2, "DOWN"), _event(3, "DOWN"), _event(4, "UP")]

    selected = select_direction_balanced_events(events, max_events=4)

    assert [event.event_id for event in selected] == [
        "event_001_down",
        "event_002_down",
        "event_003_down",
        "event_004_up",
    ]
    assert direction_balance_note(events, selected).startswith("direction_balance_best_effort")


def test_raw_candidate_start_exclusion_uses_all_raw_starts() -> None:
    anchor = datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc)
    starts = (
        anchor - timedelta(seconds=4000),
        anchor - timedelta(seconds=20),
    )

    assert raw_candidate_start_is_excluded(
        anchor,
        starts,
        lookback_seconds=300,
        horizon_seconds=60,
        exclusion_seconds=1800,
    )
    assert not raw_candidate_start_is_excluded(
        anchor,
        (anchor - timedelta(seconds=4000),),
        lookback_seconds=300,
        horizon_seconds=60,
        exclusion_seconds=1800,
    )


def test_scaled_row_count_helpers() -> None:
    assert expected_event_snapshot_rows(2, 3, 300) == 1800
    assert expected_normal_snapshot_rows(2, 10, 3, 300) == 18000
    assert expected_event_profile_rows(2, 30) == 60
    assert expected_normal_profile_rows(2, 10, 48) == 960
    assert expected_comparison_rows(2, 660) == 1320
    assert expected_top_diff_rows_dynamic((100, 3), top_n=20) == 69

    with pytest.raises(ValueError):
        expected_event_snapshot_rows(-1, 3, 300)


def test_event_processing_status_row_contains_selection_audit() -> None:
    event = _event(1, "DOWN")

    status = processing_status(
        event,
        stage=STATUS_SELECTED,
        selected=True,
        normal_selection_count=10,
        direction_balance_note_value="balanced",
        row_count_status="validated",
    )

    assert status.event_id == event.event_id
    assert status.selected
    assert status.normal_selection_count == 10
    assert status.row_count_status == "validated"


def test_phase3a_finalize_bundle_job_is_declared() -> None:
    bundle = Path("databricks.yml").read_text()

    assert "phase3a_btc_multi_event_finalize_classic" in bundle
    assert "jobs/15_finalize_btc_multi_event_trial.py" in bundle
    assert "--source-run-id" in bundle
