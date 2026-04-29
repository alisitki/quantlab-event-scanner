"""Pure helpers for Phase 3A-1 BTC coverage and event-count probes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

from .manifest import ManifestPartition


DEFAULT_PROBE_START_DATE = "20260420"
DEFAULT_PROBE_END_DATE = "20260425"
DEFAULT_SYMBOL_FAMILY = "btc"
DEFAULT_PROBE_HORIZON_SECONDS = 60
DEFAULT_PROBE_LOOKBACK_SECONDS = 300
DEFAULT_PROBE_MOVE_THRESHOLD_PCT = 1.0

RECOMMENDED_READY = "ready_for_phase3a_trial"
RECOMMENDED_DIRECTION_IMBALANCED = "direction_imbalanced"
RECOMMENDED_WEAK_MULTI_EVENT = "weak_multi_event_coverage"
RECOMMENDED_NO_QUALITY_EVENTS = "no_quality_preview_events"
RECOMMENDED_NO_EVENT_SCAN = "no_event_scan_possible"


@dataclass(frozen=True)
class CoverageByDate:
    requested_date: str
    is_present_in_manifest: bool
    is_present_in_btc_manifest: bool
    has_binance_trade: bool
    has_bybit_trade: bool
    has_okx_trade: bool
    trade_coverage_count: int
    has_binance_bbo: bool
    has_bybit_bbo: bool
    has_okx_bbo: bool
    bbo_coverage_count: int
    has_full_trade_coverage: bool
    has_full_bbo_coverage: bool
    has_full_trade_bbo_coverage: bool
    event_scan_possible: bool
    phase3a_quality_possible: bool


@dataclass(frozen=True)
class MissingDateDiagnostic:
    requested_date: str
    is_present_in_manifest: bool
    is_present_in_btc_manifest: bool
    missing_reasons: tuple[str, ...]


@dataclass(frozen=True)
class ProbeRawCandidate:
    exchange: str
    symbol: str
    direction: str
    start_ts: datetime
    end_ts: datetime
    duration_seconds: int
    base_price: float
    touch_price: float | None
    move_pct: float | None
    is_ambiguous: bool = False


@dataclass(frozen=True)
class ProbeGroupedEvent:
    event_id: str
    event_rank: int
    exchange: str
    symbol: str
    direction: str
    event_start_ts: datetime
    event_end_ts: datetime
    duration_seconds: int
    base_price: float
    touch_price: float | None
    move_pct: float | None
    selected_date: str
    raw_candidate_count: int
    is_ambiguous: bool
    group_start_ts: datetime
    group_end_ts: datetime


@dataclass(frozen=True)
class EventQualityPreview:
    event_id: str
    direction: str
    selected_date: str
    has_full_trade_coverage: bool
    has_full_bbo_coverage: bool
    trade_coverage_count: int
    bbo_coverage_count: int
    pre_window_start_ts: datetime
    pre_window_end_ts: datetime
    pre_window_partition_dates: tuple[str, ...]
    pre_window_partitions_available: bool
    quality_status: str
    quality_reason: str


@dataclass(frozen=True)
class RecommendedScope:
    requested_start_date: str
    requested_end_date: str
    available_dates: tuple[str, ...]
    full_trade_bbo_coverage_dates: tuple[str, ...]
    grouped_event_count: int
    quality_pass_preview_count: int
    up_count: int
    down_count: int
    recommended_start_date: str | None
    recommended_end_date: str | None
    recommended_max_events: int
    recommended_min_events: int
    direction_balance_possible: bool
    recommended_scope_status: str
    notes: str


def default_phase3a_probe_run_id(now: datetime) -> str:
    utc_now = _as_utc(now)
    return f"phase3a_probe_{utc_now.strftime('%Y%m%dT%H%M%SZ')}"


def phase3a_probe_run_path(output_root: str, run_id: str) -> str:
    return f"{output_root.rstrip('/')}/btc_multi_event_trials/_probe/run_id={run_id}"


def requested_dates(start_date: str, end_date: str) -> tuple[str, ...]:
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if end < start:
        raise ValueError("end_date must be on or after start_date.")
    day_count = (end - start).days + 1
    return tuple((start + timedelta(days=offset)).strftime("%Y%m%d") for offset in range(day_count))


def btc_manifest_dates(
    partitions: Iterable[ManifestPartition],
    *,
    exchanges: Iterable[str],
    streams: Iterable[str] = ("trade", "bbo"),
    symbol_family: str = DEFAULT_SYMBOL_FAMILY,
) -> tuple[str, ...]:
    exchange_set = {exchange.lower() for exchange in exchanges}
    stream_set = {stream.lower() for stream in streams}
    symbol_token = symbol_family.lower()
    dates = {
        partition.date
        for partition in partitions
        if partition.available is not False
        and partition.date is not None
        and partition.exchange is not None
        and partition.exchange.lower() in exchange_set
        and partition.stream is not None
        and partition.stream.lower() in stream_set
        and partition.symbol is not None
        and symbol_token in partition.symbol.lower()
    }
    return tuple(sorted(dates))


def resolve_probe_date_range(
    start_date: str,
    end_date: str,
    *,
    available_btc_dates: Iterable[str],
) -> tuple[str, str]:
    dates = tuple(sorted(available_btc_dates))
    if start_date.lower() == "auto" or end_date.lower() == "auto":
        if not dates:
            raise ValueError("Cannot resolve auto date range because no BTC manifest dates were found.")
    resolved_start = dates[0] if start_date.lower() == "auto" else start_date
    resolved_end = dates[-1] if end_date.lower() == "auto" else end_date
    requested_dates(resolved_start, resolved_end)
    return resolved_start, resolved_end


def select_probe_partitions(
    partitions: Iterable[ManifestPartition],
    *,
    requested_date_values: Iterable[str],
    exchanges: Iterable[str],
    streams: Iterable[str] = ("trade", "bbo"),
    symbol_family: str = DEFAULT_SYMBOL_FAMILY,
) -> tuple[ManifestPartition, ...]:
    date_set = set(requested_date_values)
    exchange_set = {exchange.lower() for exchange in exchanges}
    stream_set = {stream.lower() for stream in streams}
    symbol_token = symbol_family.lower()
    return tuple(
        partition
        for partition in partitions
        if partition.available is not False
        and partition.date in date_set
        and partition.exchange is not None
        and partition.exchange.lower() in exchange_set
        and partition.stream is not None
        and partition.stream.lower() in stream_set
        and partition.symbol is not None
        and symbol_token in partition.symbol.lower()
    )


def build_coverage_diagnostics(
    all_partitions: Iterable[ManifestPartition],
    btc_partitions: Iterable[ManifestPartition],
    *,
    requested_date_values: Iterable[str],
    exchanges: Iterable[str],
) -> tuple[tuple[CoverageByDate, ...], tuple[MissingDateDiagnostic, ...]]:
    dates = tuple(requested_date_values)
    exchange_values = tuple(exchange.lower() for exchange in exchanges)
    all_dates = {
        partition.date
        for partition in all_partitions
        if partition.available is not False and partition.date in dates
    }
    btc_by_date_stream: dict[tuple[str, str], set[str]] = {}
    btc_dates: set[str] = set()
    for partition in btc_partitions:
        if partition.date is None or partition.stream is None or partition.exchange is None:
            continue
        stream = partition.stream.lower()
        exchange = partition.exchange.lower()
        if stream not in {"trade", "bbo"} or exchange not in exchange_values:
            continue
        btc_dates.add(partition.date)
        btc_by_date_stream.setdefault((partition.date, stream), set()).add(exchange)

    coverage_rows: list[CoverageByDate] = []
    missing_rows: list[MissingDateDiagnostic] = []
    for date in dates:
        trade_exchanges = btc_by_date_stream.get((date, "trade"), set())
        bbo_exchanges = btc_by_date_stream.get((date, "bbo"), set())
        trade_count = len(trade_exchanges)
        bbo_count = len(bbo_exchanges)
        has_full_trade = trade_count == len(exchange_values)
        has_full_bbo = bbo_count == len(exchange_values)
        has_full_both = has_full_trade and has_full_bbo
        coverage_rows.append(
            CoverageByDate(
                requested_date=date,
                is_present_in_manifest=date in all_dates,
                is_present_in_btc_manifest=date in btc_dates,
                has_binance_trade="binance" in trade_exchanges,
                has_bybit_trade="bybit" in trade_exchanges,
                has_okx_trade="okx" in trade_exchanges,
                trade_coverage_count=trade_count,
                has_binance_bbo="binance" in bbo_exchanges,
                has_bybit_bbo="bybit" in bbo_exchanges,
                has_okx_bbo="okx" in bbo_exchanges,
                bbo_coverage_count=bbo_count,
                has_full_trade_coverage=has_full_trade,
                has_full_bbo_coverage=has_full_bbo,
                has_full_trade_bbo_coverage=has_full_both,
                event_scan_possible=trade_count > 0,
                phase3a_quality_possible=has_full_both,
            )
        )
        reasons = missing_reasons_for_date(
            date,
            is_present_in_manifest=date in all_dates,
            is_present_in_btc_manifest=date in btc_dates,
            trade_exchanges=trade_exchanges,
            bbo_exchanges=bbo_exchanges,
            exchanges=exchange_values,
        )
        if reasons:
            missing_rows.append(
                MissingDateDiagnostic(
                    requested_date=date,
                    is_present_in_manifest=date in all_dates,
                    is_present_in_btc_manifest=date in btc_dates,
                    missing_reasons=reasons,
                )
            )

    return tuple(coverage_rows), tuple(missing_rows)


def missing_reasons_for_date(
    requested_date: str,
    *,
    is_present_in_manifest: bool,
    is_present_in_btc_manifest: bool,
    trade_exchanges: Iterable[str],
    bbo_exchanges: Iterable[str],
    exchanges: Iterable[str],
) -> tuple[str, ...]:
    del requested_date
    if not is_present_in_manifest:
        return ("date_missing_all_streams",)
    if not is_present_in_btc_manifest:
        return ("date_missing_btc_streams",)

    trade_set = {exchange.lower() for exchange in trade_exchanges}
    bbo_set = {exchange.lower() for exchange in bbo_exchanges}
    reasons: list[str] = []
    for exchange in exchanges:
        normalized = exchange.lower()
        if normalized not in trade_set:
            reasons.append(f"missing_{normalized}_trade")
        if normalized not in bbo_set:
            reasons.append(f"missing_{normalized}_bbo")
    return tuple(reasons)


def group_probe_candidates(
    candidates: Iterable[ProbeRawCandidate],
    *,
    horizon_seconds: int,
) -> tuple[ProbeGroupedEvent, ...]:
    if horizon_seconds <= 0:
        raise ValueError("horizon_seconds must be positive.")

    ordered = sorted(
        candidates,
        key=lambda item: (
            item.exchange.lower(),
            item.symbol.lower(),
            item.direction.upper(),
            item.is_ambiguous,
            _as_utc(item.start_ts),
        ),
    )
    grouped: list[dict[str, object]] = []
    active: dict[tuple[str, str, str, bool], int] = {}
    group_numbers: dict[tuple[str, str, str, bool, str], int] = {}

    for candidate in ordered:
        exchange = candidate.exchange.lower()
        symbol = candidate.symbol
        symbol_key = symbol.lower()
        direction = candidate.direction.upper()
        start = _drop_tz(_as_utc(candidate.start_ts))
        end = _drop_tz(_as_utc(candidate.end_ts))
        key = (exchange, symbol_key, direction, candidate.is_ambiguous)
        group_key = (exchange, symbol_key, direction, candidate.is_ambiguous, start.strftime("%Y%m%d"))
        cluster_end = start + timedelta(seconds=horizon_seconds)
        active_index = active.get(key)
        starts_new = active_index is None
        if active_index is not None:
            starts_new = start > grouped[active_index]["_cluster_interval_end_ts"]

        if starts_new:
            group_number = group_numbers.get(group_key, 0) + 1
            group_numbers[group_key] = group_number
            selected_date = start.strftime("%Y%m%d")
            grouped.append(
                {
                    "event_id": _format_probe_event_id(
                        exchange,
                        symbol,
                        selected_date,
                        direction,
                        group_number,
                        candidate.is_ambiguous,
                    ),
                    "event_rank": 0,
                    "exchange": exchange,
                    "symbol": symbol,
                    "direction": direction,
                    "event_start_ts": start,
                    "event_end_ts": end,
                    "duration_seconds": int(candidate.duration_seconds),
                    "base_price": float(candidate.base_price),
                    "touch_price": candidate.touch_price,
                    "move_pct": candidate.move_pct,
                    "selected_date": selected_date,
                    "raw_candidate_count": 1,
                    "is_ambiguous": bool(candidate.is_ambiguous),
                    "group_start_ts": start,
                    "group_end_ts": start,
                    "_cluster_interval_end_ts": cluster_end,
                }
            )
            active[key] = len(grouped) - 1
            continue

        current = grouped[active_index]
        current["raw_candidate_count"] = int(current["raw_candidate_count"]) + 1
        current["group_end_ts"] = start
        if end > current["event_end_ts"]:
            current["event_end_ts"] = end
            current["duration_seconds"] = int((end - current["event_start_ts"]).total_seconds())
        if cluster_end > current["_cluster_interval_end_ts"]:
            current["_cluster_interval_end_ts"] = cluster_end
        if candidate.move_pct is not None and (
            current["move_pct"] is None or abs(float(candidate.move_pct)) > abs(float(current["move_pct"]))
        ):
            current["move_pct"] = float(candidate.move_pct)
            current["touch_price"] = candidate.touch_price

    sorted_rows = sorted(
        grouped,
        key=lambda row: (
            row["event_start_ts"],
            str(row["exchange"]),
            str(row["symbol"]).lower(),
            str(row["direction"]),
            bool(row["is_ambiguous"]),
            str(row["event_id"]),
        ),
    )
    return tuple(
        ProbeGroupedEvent(
            event_rank=index,
            **{
                key: value
                for key, value in row.items()
                if not key.startswith("_") and key != "event_rank"
            },
        )
        for index, row in enumerate(sorted_rows, start=1)
    )


def build_event_quality_preview(
    events: Iterable[ProbeGroupedEvent],
    coverage_by_date: Iterable[CoverageByDate],
    *,
    lookback_seconds: int,
) -> tuple[EventQualityPreview, ...]:
    coverage = {row.requested_date: row for row in coverage_by_date}
    rows: list[EventQualityPreview] = []
    for event in events:
        pre_start = _drop_tz(_as_utc(event.event_start_ts)) - timedelta(seconds=lookback_seconds)
        pre_end = _drop_tz(_as_utc(event.event_start_ts)) - timedelta(microseconds=1)
        pre_dates = tuple(sorted({pre_start.strftime("%Y%m%d"), pre_end.strftime("%Y%m%d")}))
        event_coverage = coverage.get(event.selected_date)
        has_full_trade = bool(event_coverage and event_coverage.has_full_trade_coverage)
        has_full_bbo = bool(event_coverage and event_coverage.has_full_bbo_coverage)
        trade_count = event_coverage.trade_coverage_count if event_coverage else 0
        bbo_count = event_coverage.bbo_coverage_count if event_coverage else 0
        pre_available = all(
            (coverage.get(date) is not None and coverage[date].has_full_trade_bbo_coverage)
            for date in pre_dates
        )
        reason = _quality_reason(
            event,
            has_full_trade=has_full_trade,
            has_full_bbo=has_full_bbo,
            pre_window_partitions_available=pre_available,
            event_coverage_present=event_coverage is not None,
        )
        rows.append(
            EventQualityPreview(
                event_id=event.event_id,
                direction=event.direction,
                selected_date=event.selected_date,
                has_full_trade_coverage=has_full_trade,
                has_full_bbo_coverage=has_full_bbo,
                trade_coverage_count=trade_count,
                bbo_coverage_count=bbo_count,
                pre_window_start_ts=pre_start,
                pre_window_end_ts=pre_end,
                pre_window_partition_dates=pre_dates,
                pre_window_partitions_available=pre_available,
                quality_status=reason,
                quality_reason=reason,
            )
        )
    return tuple(rows)


def recommend_phase3a_scope(
    *,
    requested_start_date: str,
    requested_end_date: str,
    coverage_by_date: Iterable[CoverageByDate],
    grouped_event_count: int,
    quality_previews: Iterable[EventQualityPreview],
    event_scan_possible: bool,
    default_max_events: int = 10,
    default_min_events: int = 2,
) -> RecommendedScope:
    coverage_rows = tuple(coverage_by_date)
    previews = tuple(quality_previews)
    pass_previews = tuple(row for row in previews if row.quality_status == "quality_pass_preview")
    available_dates = tuple(row.requested_date for row in coverage_rows if row.is_present_in_btc_manifest)
    full_dates = tuple(row.requested_date for row in coverage_rows if row.has_full_trade_bbo_coverage)
    pass_dates = tuple(sorted({row.selected_date for row in pass_previews}))
    up_count = sum(1 for row in pass_previews if row.direction == "UP")
    down_count = sum(1 for row in pass_previews if row.direction == "DOWN")
    quality_count = len(pass_previews)
    direction_balance_possible = up_count > 0 and down_count > 0
    status = recommended_scope_status(
        event_scan_possible=event_scan_possible,
        quality_pass_preview_count=quality_count,
        up_count=up_count,
        down_count=down_count,
    )
    notes = "normal_window_selection_unverified_by_probe"
    if status == RECOMMENDED_NO_EVENT_SCAN:
        notes = f"{notes};no_btc_trade_partitions"
    elif status == RECOMMENDED_NO_QUALITY_EVENTS:
        notes = f"{notes};no_accepted_scope"
    return RecommendedScope(
        requested_start_date=requested_start_date,
        requested_end_date=requested_end_date,
        available_dates=available_dates,
        full_trade_bbo_coverage_dates=full_dates,
        grouped_event_count=int(grouped_event_count),
        quality_pass_preview_count=quality_count,
        up_count=up_count,
        down_count=down_count,
        recommended_start_date=pass_dates[0] if pass_dates else None,
        recommended_end_date=pass_dates[-1] if pass_dates else None,
        recommended_max_events=min(default_max_events, quality_count),
        recommended_min_events=min(default_min_events, quality_count),
        direction_balance_possible=direction_balance_possible,
        recommended_scope_status=status,
        notes=notes,
    )


def recommended_scope_status(
    *,
    event_scan_possible: bool,
    quality_pass_preview_count: int,
    up_count: int,
    down_count: int,
) -> str:
    if not event_scan_possible:
        return RECOMMENDED_NO_EVENT_SCAN
    if quality_pass_preview_count == 0:
        return RECOMMENDED_NO_QUALITY_EVENTS
    if quality_pass_preview_count == 1:
        return RECOMMENDED_WEAK_MULTI_EVENT
    if up_count == 0 or down_count == 0:
        return RECOMMENDED_DIRECTION_IMBALANCED
    return RECOMMENDED_READY


def _quality_reason(
    event: ProbeGroupedEvent,
    *,
    has_full_trade: bool,
    has_full_bbo: bool,
    pre_window_partitions_available: bool,
    event_coverage_present: bool,
) -> str:
    if event.is_ambiguous:
        return "ambiguous_event"
    if not event_coverage_present:
        return "missing_date"
    if not has_full_trade:
        return "missing_trade_coverage"
    if not has_full_bbo:
        return "missing_bbo_coverage"
    if not pre_window_partitions_available:
        return "pre_window_partition_missing"
    return "quality_pass_preview"


def _format_probe_event_id(
    exchange: str,
    symbol: str,
    selected_date: str,
    direction: str,
    group_number: int,
    is_ambiguous: bool,
) -> str:
    ambiguity = "_ambiguous" if is_ambiguous else ""
    return (
        f"{exchange.lower()}_{symbol.lower()}_{selected_date}_"
        f"{direction.lower()}{ambiguity}_{group_number:03d}"
    )


def _parse_date(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y%m%d")
    except ValueError as exc:
        raise ValueError(f"Invalid date {value!r}; expected YYYYMMDD.") from exc


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _drop_tz(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)
