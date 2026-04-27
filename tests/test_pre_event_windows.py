from datetime import datetime, timezone

import pytest

from quantlab_event_scanner.pre_event_windows import (
    default_phase2a_run_id,
    is_in_half_open_window,
    utc_partition_dates_for_window,
    validate_selected_event_count,
)


def test_default_phase2a_run_id_uses_utc_timestamp() -> None:
    now = datetime(2026, 4, 27, 6, 34, 42, tzinfo=timezone.utc)

    assert default_phase2a_run_id(now) == "phase2a_20260427T063442Z"


def test_utc_partition_dates_for_same_day_window() -> None:
    dates = utc_partition_dates_for_window(
        datetime(2026, 4, 23, 10, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 4, 23, 10, 5, 0, tzinfo=timezone.utc),
    )

    assert dates == ("20260423",)


def test_utc_partition_dates_for_midnight_crossing_window() -> None:
    dates = utc_partition_dates_for_window(
        datetime(2026, 4, 23, 23, 58, 0, tzinfo=timezone.utc),
        datetime(2026, 4, 24, 0, 3, 0, tzinfo=timezone.utc),
    )

    assert dates == ("20260423", "20260424")


def test_utc_partition_dates_treats_end_as_exclusive_at_midnight() -> None:
    dates = utc_partition_dates_for_window(
        datetime(2026, 4, 23, 23, 55, 0, tzinfo=timezone.utc),
        datetime(2026, 4, 24, 0, 0, 0, tzinfo=timezone.utc),
    )

    assert dates == ("20260423",)


def test_is_in_half_open_window_respects_boundaries() -> None:
    start = datetime(2026, 4, 23, 10, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 23, 10, 5, 0, tzinfo=timezone.utc)

    assert is_in_half_open_window(start, start, end)
    assert is_in_half_open_window(datetime(2026, 4, 23, 10, 4, 59, tzinfo=timezone.utc), start, end)
    assert not is_in_half_open_window(end, start, end)


def test_validate_selected_event_count_requires_exactly_one() -> None:
    validate_selected_event_count(1)

    with pytest.raises(ValueError, match="Expected exactly one selected event"):
        validate_selected_event_count(0)

    with pytest.raises(ValueError, match="Expected exactly one selected event"):
        validate_selected_event_count(2)
