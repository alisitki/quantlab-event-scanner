from datetime import datetime, timedelta, timezone

from quantlab_event_scanner.normal_time_trial import (
    SAMPLE_TYPE_NORMAL,
    build_normal_selection_metadata,
    default_phase2e_run_id,
    exclusion_reason,
    nearest_candidate_distance_seconds,
    normal_window_for_anchor,
    second_before_anchor_values,
    selected_date_utc_bounds,
    select_first_quality_passing_window,
)


def test_default_phase2e_run_id_uses_utc_timestamp() -> None:
    now = datetime(2026, 4, 27, 10, 20, 30, tzinfo=timezone.utc)

    assert default_phase2e_run_id(now) == "phase2e_20260427T102030Z"


def test_normal_window_for_anchor_and_second_values() -> None:
    anchor = datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc)
    window = normal_window_for_anchor(anchor, 300)

    assert window.normal_window_start_ts == anchor - timedelta(seconds=300)
    assert window.normal_window_end_ts == anchor
    assert window.normal_anchor_ts == anchor
    assert second_before_anchor_values(300)[0] == 300
    assert second_before_anchor_values(300)[-1] == 1


def test_selected_date_utc_bounds() -> None:
    start, end = selected_date_utc_bounds("20260423")

    assert start == datetime(2026, 4, 23, 0, 0, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 4, 24, 0, 0, 0, tzinfo=timezone.utc)


def test_exclusion_reason_rejects_anchor_near_candidate() -> None:
    anchor = datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc)
    candidates = (anchor + timedelta(seconds=1700),)

    assert (
        exclusion_reason(
            anchor,
            candidates,
            lookback_seconds=300,
            horizon_seconds=60,
            proximity_seconds=1800,
        )
        == "candidate_within_anchor_proximity"
    )


def test_exclusion_reason_rejects_post_anchor_candidate_when_proximity_allows() -> None:
    anchor = datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc)
    candidates = (anchor + timedelta(seconds=30),)

    assert (
        exclusion_reason(
            anchor,
            candidates,
            lookback_seconds=300,
            horizon_seconds=60,
            proximity_seconds=10,
        )
        == "candidate_inside_post_anchor_horizon"
    )


def test_exclusion_reason_rejects_in_window_candidate_when_proximity_allows() -> None:
    anchor = datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc)
    candidates = (anchor - timedelta(seconds=200),)

    assert (
        exclusion_reason(
            anchor,
            candidates,
            lookback_seconds=300,
            horizon_seconds=60,
            proximity_seconds=10,
        )
        == "candidate_inside_normal_window"
    )


def test_select_first_quality_passing_window_skips_failed_anchors() -> None:
    first = datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc)
    second = datetime(2026, 4, 23, 12, 5, 0, tzinfo=timezone.utc)

    assert select_first_quality_passing_window((second, first), (first,)) == second


def test_normal_metadata_contains_anchor_and_candidate_distance() -> None:
    anchor = datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc)
    window = normal_window_for_anchor(anchor, 300)
    candidates = (anchor - timedelta(seconds=2000), anchor + timedelta(seconds=4000))

    metadata = build_normal_selection_metadata(
        selected_date="20260423",
        window=window,
        lookback_seconds=300,
        selection_reason="earliest_eligible_quality_pass",
        excluded_candidate_count=12,
        candidate_start_timestamps=candidates,
    )

    assert metadata.sample_type == SAMPLE_TYPE_NORMAL
    assert metadata.selected_date == "20260423"
    assert metadata.normal_anchor_ts == anchor
    assert metadata.nearest_candidate_distance_seconds == 2000
    assert nearest_candidate_distance_seconds(anchor, ()) is None
