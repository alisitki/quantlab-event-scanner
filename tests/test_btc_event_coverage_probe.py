from datetime import datetime, timedelta, timezone

import pytest

from quantlab_event_scanner.btc_event_coverage_probe import (
    RECOMMENDED_DIRECTION_IMBALANCED,
    RECOMMENDED_NO_EVENT_SCAN,
    RECOMMENDED_NO_QUALITY_EVENTS,
    RECOMMENDED_READY,
    RECOMMENDED_WEAK_MULTI_EVENT,
    EventQualityPreview,
    ProbeGroupedEvent,
    ProbeRawCandidate,
    btc_manifest_dates,
    build_coverage_diagnostics,
    build_event_quality_preview,
    default_phase3a_probe_run_id,
    group_probe_candidates,
    recommend_phase3a_scope,
    recommended_scope_status,
    requested_dates,
    resolve_probe_date_range,
    select_probe_partitions,
)
from quantlab_event_scanner.manifest import ManifestPartition


def _partition(
    date: str,
    *,
    exchange: str = "binance",
    stream: str = "trade",
    symbol: str = "btcusdt",
) -> ManifestPartition:
    return ManifestPartition(
        exchange=exchange,
        stream=stream,
        symbol=symbol,
        date=date,
        path=f"exchange={exchange}/stream={stream}/symbol={symbol}/date={date}/data.parquet",
    )


def _candidate(offset: int, direction: str = "DOWN") -> ProbeRawCandidate:
    start = datetime(2026, 4, 23, 23, 59, 30, tzinfo=timezone.utc) + timedelta(seconds=offset)
    return ProbeRawCandidate(
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


def _event(date: str = "20260423", direction: str = "DOWN") -> ProbeGroupedEvent:
    start = datetime.strptime(f"{date} 12:00:00", "%Y%m%d %H:%M:%S")
    return ProbeGroupedEvent(
        event_id=f"binance_btcusdt_{date}_{direction.lower()}_001",
        event_rank=1,
        exchange="binance",
        symbol="BTCUSDT",
        direction=direction,
        event_start_ts=start,
        event_end_ts=start + timedelta(seconds=20),
        duration_seconds=20,
        base_price=100.0,
        touch_price=99.0,
        move_pct=-1.0,
        selected_date=date,
        raw_candidate_count=1,
        is_ambiguous=False,
        group_start_ts=start,
        group_end_ts=start,
    )


def test_default_probe_run_id_uses_utc_timestamp() -> None:
    now = datetime(2026, 4, 28, 10, 20, 30, tzinfo=timezone.utc)

    assert default_phase3a_probe_run_id(now) == "phase3a_probe_20260428T102030Z"


def test_requested_dates_expands_inclusive_range() -> None:
    assert requested_dates("20260420", "20260422") == ("20260420", "20260421", "20260422")

    with pytest.raises(ValueError):
        requested_dates("20260422", "20260420")


def test_btc_manifest_dates_and_auto_range_resolution() -> None:
    partitions = (
        _partition("20260405", stream="trade", symbol="ethusdt"),
        _partition("20260403", stream="trade", symbol="btcusdt"),
        _partition("20260411", exchange="okx", stream="bbo", symbol="BTC-USDT"),
        _partition("20260412", exchange="coinbase", stream="trade", symbol="btcusdt"),
    )

    dates = btc_manifest_dates(
        partitions,
        exchanges=("binance", "bybit", "okx"),
        streams=("trade", "bbo"),
        symbol_family="btc",
    )

    assert dates == ("20260403", "20260411")
    assert resolve_probe_date_range("auto", "auto", available_btc_dates=dates) == (
        "20260403",
        "20260411",
    )
    assert resolve_probe_date_range("20260401", "auto", available_btc_dates=dates) == (
        "20260401",
        "20260411",
    )


def test_auto_range_resolution_requires_btc_manifest_dates() -> None:
    with pytest.raises(ValueError):
        resolve_probe_date_range("auto", "auto", available_btc_dates=())


def test_btc_specific_manifest_presence_distinguishes_generic_date_presence() -> None:
    partitions = (
        _partition("20260420", stream="trade", symbol="ethusdt"),
        _partition("20260421", stream="trade", symbol="btcusdt"),
        _partition("20260421", exchange="bybit", stream="bbo", symbol="btcusdt"),
    )
    selected = select_probe_partitions(
        partitions,
        requested_date_values=("20260420", "20260421", "20260422"),
        exchanges=("binance", "bybit", "okx"),
        streams=("trade", "bbo"),
    )

    coverage, missing = build_coverage_diagnostics(
        partitions,
        selected,
        requested_date_values=("20260420", "20260421", "20260422"),
        exchanges=("binance", "bybit", "okx"),
    )

    assert len(selected) == 2
    by_date = {row.requested_date: row for row in coverage}
    assert by_date["20260420"].is_present_in_manifest
    assert not by_date["20260420"].is_present_in_btc_manifest
    assert not by_date["20260422"].is_present_in_manifest
    reasons = {row.requested_date: row.missing_reasons for row in missing}
    assert reasons["20260420"] == ("date_missing_btc_streams",)
    assert reasons["20260422"] == ("date_missing_all_streams",)


def test_coverage_event_scan_possible_is_not_quality_possible() -> None:
    partitions = (
        _partition("20260423", exchange="binance", stream="trade"),
        _partition("20260423", exchange="bybit", stream="trade"),
    )
    selected = select_probe_partitions(
        partitions,
        requested_date_values=("20260423",),
        exchanges=("binance", "bybit", "okx"),
    )
    coverage, _ = build_coverage_diagnostics(
        partitions,
        selected,
        requested_date_values=("20260423",),
        exchanges=("binance", "bybit", "okx"),
    )

    assert coverage[0].event_scan_possible
    assert not coverage[0].phase3a_quality_possible


def test_grouping_crosses_date_boundary_without_date_split() -> None:
    groups = group_probe_candidates(
        [_candidate(0), _candidate(59), _candidate(121)],
        horizon_seconds=60,
    )

    assert len(groups) == 2
    assert groups[0].selected_date == "20260423"
    assert groups[0].raw_candidate_count == 2
    assert groups[0].group_end_ts == datetime(2026, 4, 24, 0, 0, 29)


def test_ambiguous_group_quality_preview_does_not_claim_normal_windows() -> None:
    ambiguous = ProbeRawCandidate(
        exchange="binance",
        symbol="BTCUSDT",
        direction="AMBIGUOUS",
        start_ts=datetime(2026, 4, 23, 12, 0, 0),
        end_ts=datetime(2026, 4, 23, 12, 0, 10),
        duration_seconds=10,
        base_price=100.0,
        touch_price=None,
        move_pct=None,
        is_ambiguous=True,
    )
    coverage, _ = build_coverage_diagnostics(
        tuple(_partition("20260423", exchange=exchange, stream=stream) for exchange in ("binance", "bybit", "okx") for stream in ("trade", "bbo")),
        tuple(_partition("20260423", exchange=exchange, stream=stream) for exchange in ("binance", "bybit", "okx") for stream in ("trade", "bbo")),
        requested_date_values=("20260423",),
        exchanges=("binance", "bybit", "okx"),
    )

    preview = build_event_quality_preview(
        group_probe_candidates([ambiguous], horizon_seconds=60),
        coverage,
        lookback_seconds=300,
    )[0]

    assert preview.quality_reason == "ambiguous_event"
    assert preview.quality_status == "ambiguous_event"


def test_quality_preview_reasons_for_missing_and_pass() -> None:
    full_partitions = tuple(
        _partition("20260423", exchange=exchange, stream=stream)
        for exchange in ("binance", "bybit", "okx")
        for stream in ("trade", "bbo")
    )
    full_coverage, _ = build_coverage_diagnostics(
        full_partitions,
        full_partitions,
        requested_date_values=("20260423",),
        exchanges=("binance", "bybit", "okx"),
    )
    full_preview = build_event_quality_preview([_event()], full_coverage, lookback_seconds=300)[0]
    assert full_preview.quality_reason == "quality_pass_preview"

    trade_only = tuple(
        _partition("20260423", exchange=exchange, stream="trade")
        for exchange in ("binance", "bybit", "okx")
    )
    trade_coverage, _ = build_coverage_diagnostics(
        trade_only,
        trade_only,
        requested_date_values=("20260423",),
        exchanges=("binance", "bybit", "okx"),
    )
    trade_preview = build_event_quality_preview([_event()], trade_coverage, lookback_seconds=300)[0]
    assert trade_preview.quality_reason == "missing_bbo_coverage"


def test_pre_window_partition_missing_reason() -> None:
    event = _event("20260423")
    event = ProbeGroupedEvent(
        **{
            **event.__dict__,
            "event_start_ts": datetime(2026, 4, 23, 0, 2, 0),
            "selected_date": "20260423",
        }
    )
    full_partitions = tuple(
        _partition("20260423", exchange=exchange, stream=stream)
        for exchange in ("binance", "bybit", "okx")
        for stream in ("trade", "bbo")
    )
    coverage, _ = build_coverage_diagnostics(
        full_partitions,
        full_partitions,
        requested_date_values=("20260423",),
        exchanges=("binance", "bybit", "okx"),
    )

    preview = build_event_quality_preview([event], coverage, lookback_seconds=300)[0]

    assert preview.pre_window_partition_dates == ("20260422", "20260423")
    assert preview.quality_reason == "pre_window_partition_missing"


def test_recommended_scope_status_values() -> None:
    assert recommended_scope_status(
        event_scan_possible=False,
        quality_pass_preview_count=0,
        up_count=0,
        down_count=0,
    ) == RECOMMENDED_NO_EVENT_SCAN
    assert recommended_scope_status(
        event_scan_possible=True,
        quality_pass_preview_count=0,
        up_count=0,
        down_count=0,
    ) == RECOMMENDED_NO_QUALITY_EVENTS
    assert recommended_scope_status(
        event_scan_possible=True,
        quality_pass_preview_count=1,
        up_count=1,
        down_count=0,
    ) == RECOMMENDED_WEAK_MULTI_EVENT
    assert recommended_scope_status(
        event_scan_possible=True,
        quality_pass_preview_count=3,
        up_count=0,
        down_count=3,
    ) == RECOMMENDED_DIRECTION_IMBALANCED
    assert recommended_scope_status(
        event_scan_possible=True,
        quality_pass_preview_count=3,
        up_count=1,
        down_count=2,
    ) == RECOMMENDED_READY


def test_recommend_scope_notes_normal_window_unverified() -> None:
    coverage, _ = build_coverage_diagnostics(
        (),
        (),
        requested_date_values=("20260423",),
        exchanges=("binance", "bybit", "okx"),
    )
    preview = EventQualityPreview(
        event_id="e1",
        direction="DOWN",
        selected_date="20260423",
        has_full_trade_coverage=True,
        has_full_bbo_coverage=True,
        trade_coverage_count=3,
        bbo_coverage_count=3,
        pre_window_start_ts=datetime(2026, 4, 23, 11, 55),
        pre_window_end_ts=datetime(2026, 4, 23, 11, 59, 59),
        pre_window_partition_dates=("20260423",),
        pre_window_partitions_available=True,
        quality_status="quality_pass_preview",
        quality_reason="quality_pass_preview",
    )

    scope = recommend_phase3a_scope(
        requested_start_date="20260423",
        requested_end_date="20260423",
        coverage_by_date=coverage,
        grouped_event_count=1,
        quality_previews=(preview,),
        event_scan_possible=True,
    )

    assert scope.recommended_scope_status == RECOMMENDED_WEAK_MULTI_EVENT
    assert "normal_window_selection_unverified_by_probe" in scope.notes
