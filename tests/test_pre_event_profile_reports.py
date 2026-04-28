from datetime import datetime, timezone

from quantlab_event_scanner.pre_event_profile_reports import (
    BUCKET_CHANGE_PAIRS,
    BUCKET_WINDOWS,
    PROFILE_VERSION,
    ProfileWindow,
    absolute_change,
    bucket_seconds_covered,
    default_phase2d_run_id,
    first_second_before_event,
    last_second_before_event,
    mid_diff_bps,
    relative_change,
    return_bps,
    safe_ratio,
)


def test_default_phase2d_run_id_uses_utc_timestamp() -> None:
    now = datetime(2026, 4, 27, 10, 20, 30, tzinfo=timezone.utc)

    assert default_phase2d_run_id(now) == "phase2d_20260427T102030Z"


def test_profile_version_is_stable() -> None:
    assert PROFILE_VERSION == "pre_event_profile_v1"


def test_bucket_window_definitions_cover_exact_ranges() -> None:
    labels = {
        window.label: (window.min_second_before_event, window.max_second_before_event)
        for window in BUCKET_WINDOWS
    }

    assert labels == {
        "bucket_300_120s": (121, 300),
        "bucket_120_60s": (61, 120),
        "bucket_60_30s": (31, 60),
        "bucket_30_10s": (11, 30),
        "bucket_10_0s": (1, 10),
    }
    assert bucket_seconds_covered() == tuple(range(1, 301))


def test_first_last_ordering_uses_max_min_second_before_event() -> None:
    window = ProfileWindow("bucket", "bucket_60_30s", 31, 60)

    assert first_second_before_event(window) == 60
    assert last_second_before_event(window) == 31


def test_safe_ratio_and_return_bps_handle_zero_denominator() -> None:
    assert safe_ratio(2.0, 4.0) == 0.5
    assert safe_ratio(2.0, 0.0) is None
    assert safe_ratio(None, 4.0) is None
    assert return_bps(100.0, 101.0) == 100.00000000000009
    assert return_bps(0.0, 101.0) is None


def test_mid_diff_bps_handles_null_and_zero_denominator() -> None:
    assert mid_diff_bps(101.0, 100.0) == 100.0
    assert mid_diff_bps(None, 100.0) is None
    assert mid_diff_bps(101.0, 0.0) is None


def test_bucket_change_formulas_handle_null_and_zero_denominator() -> None:
    assert absolute_change(2.0, 5.0) == 3.0
    assert absolute_change(None, 5.0) is None
    assert relative_change(2.0, 5.0) == 1.5
    assert relative_change(-2.0, 1.0) == 1.5
    assert relative_change(0.0, 1.0) is None
    assert relative_change(None, 1.0) is None


def test_adjacent_bucket_pairs_are_in_event_approach_order() -> None:
    assert BUCKET_CHANGE_PAIRS == (
        ("bucket_300_120s", "bucket_120_60s"),
        ("bucket_120_60s", "bucket_60_30s"),
        ("bucket_60_30s", "bucket_30_10s"),
        ("bucket_30_10s", "bucket_10_0s"),
    )
