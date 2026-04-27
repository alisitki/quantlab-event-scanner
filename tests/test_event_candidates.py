from datetime import datetime, timedelta

from quantlab_event_scanner.event_candidates import CandidateRecord
from quantlab_event_scanner.event_candidates import resolve_candidate_direction
from quantlab_event_scanner.event_candidates import format_trial_event_id, group_candidate_records


def test_resolve_candidate_direction_returns_up_for_up_only() -> None:
    resolution = resolve_candidate_direction(up_hit_second=37, down_hit_second=None)

    assert resolution.direction == "UP"
    assert not resolution.ambiguous


def test_resolve_candidate_direction_returns_down_for_down_only() -> None:
    resolution = resolve_candidate_direction(up_hit_second=None, down_hit_second=12)

    assert resolution.direction == "DOWN"
    assert not resolution.ambiguous


def test_resolve_candidate_direction_returns_up_when_up_hits_first() -> None:
    resolution = resolve_candidate_direction(up_hit_second=20, down_hit_second=45)

    assert resolution.direction == "UP"
    assert not resolution.ambiguous


def test_resolve_candidate_direction_returns_down_when_down_hits_first() -> None:
    resolution = resolve_candidate_direction(up_hit_second=45, down_hit_second=20)

    assert resolution.direction == "DOWN"
    assert not resolution.ambiguous


def test_resolve_candidate_direction_marks_same_second_hits_ambiguous() -> None:
    resolution = resolve_candidate_direction(up_hit_second=20, down_hit_second=20)

    assert resolution.direction == "AMBIGUOUS"
    assert resolution.ambiguous


def test_resolve_candidate_direction_returns_none_when_no_hit() -> None:
    resolution = resolve_candidate_direction(up_hit_second=None, down_hit_second=None)

    assert resolution.direction is None
    assert not resolution.ambiguous


def test_format_trial_event_id_is_deterministic() -> None:
    assert (
        format_trial_event_id("Binance", "BTCUSDT", "20260423", "DOWN", 1)
        == "binance_btcusdt_20260423_down_001"
    )


def test_group_candidate_records_collapses_consecutive_candidates() -> None:
    start = datetime(2026, 4, 23, 10, 4, 10)
    records = [
        CandidateRecord("binance", "BTCUSDT", "DOWN", start + timedelta(seconds=offset))
        for offset in range(60)
    ]

    groups = group_candidate_records(records, horizon_seconds=60)

    assert len(groups) == 1
    assert groups[0].raw_candidate_count == 60
    assert groups[0].group_start_ts == start
    assert groups[0].group_end_ts == start + timedelta(seconds=59)


def test_group_candidate_records_splits_after_horizon_from_group_start() -> None:
    start = datetime(2026, 4, 23, 10, 4, 10)
    records = [
        CandidateRecord("binance", "BTCUSDT", "DOWN", start),
        CandidateRecord("binance", "BTCUSDT", "DOWN", start + timedelta(seconds=60)),
        CandidateRecord("binance", "BTCUSDT", "DOWN", start + timedelta(seconds=61)),
    ]

    groups = group_candidate_records(records, horizon_seconds=60)

    assert [group.raw_candidate_count for group in groups] == [2, 1]
    assert [group.group_number for group in groups] == [1, 2]


def test_group_candidate_records_keeps_directions_separate() -> None:
    start = datetime(2026, 4, 23, 10, 4, 10)
    records = [
        CandidateRecord("binance", "BTCUSDT", "DOWN", start),
        CandidateRecord("binance", "BTCUSDT", "UP", start + timedelta(seconds=1)),
    ]

    groups = group_candidate_records(records, horizon_seconds=60)

    assert {(group.direction, group.raw_candidate_count) for group in groups} == {
        ("DOWN", 1),
        ("UP", 1),
    }


def test_group_candidate_records_keeps_exchange_and_symbol_separate() -> None:
    start = datetime(2026, 4, 23, 10, 4, 10)
    records = [
        CandidateRecord("binance", "BTCUSDT", "DOWN", start),
        CandidateRecord("bybit", "BTCUSDT", "DOWN", start + timedelta(seconds=1)),
        CandidateRecord("binance", "BTCUSDC", "DOWN", start + timedelta(seconds=2)),
    ]

    groups = group_candidate_records(records, horizon_seconds=60)

    assert {(group.exchange, group.symbol, group.raw_candidate_count) for group in groups} == {
        ("binance", "BTCUSDT", 1),
        ("bybit", "BTCUSDT", 1),
        ("binance", "BTCUSDC", 1),
    }
