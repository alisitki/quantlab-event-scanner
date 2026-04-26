"""Pure helpers for BTC trade data profiling jobs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from .manifest import ManifestPartition


@dataclass(frozen=True)
class CoverageRow:
    """Exchange coverage summary for one manifest date."""

    date: str
    exchange_presence: Mapping[str, bool]
    coverage_count: int
    missing_exchanges: tuple[str, ...]

    def has_exchange(self, exchange: str) -> bool:
        """Return whether an exchange is present for this date."""

        return self.exchange_presence.get(exchange.lower(), False)


@dataclass(frozen=True)
class CoverageSelection:
    """Selected coverage-aware date and matching partitions."""

    selected_date: str | None
    coverage_row: CoverageRow | None
    partitions: tuple[ManifestPartition, ...]


def filter_btc_trade_partitions(
    partitions: Sequence[ManifestPartition],
    exchanges: Sequence[str],
) -> tuple[ManifestPartition, ...]:
    """Return available BTC trade partitions for the configured exchanges."""

    configured_exchanges = {exchange.lower() for exchange in exchanges}
    return tuple(
        partition
        for partition in partitions
        if partition.available is not False
        and partition.exchange is not None
        and partition.exchange.lower() in configured_exchanges
        and partition.stream == "trade"
        and partition.symbol is not None
        and "btc" in partition.symbol.lower()
        and partition.date is not None
    )


def build_coverage_table(
    partitions: Sequence[ManifestPartition],
    exchanges: Sequence[str],
) -> tuple[CoverageRow, ...]:
    """Build date-level exchange coverage rows from selected BTC trade partitions."""

    normalized_exchanges = tuple(exchange.lower() for exchange in exchanges)
    exchanges_by_date: dict[str, set[str]] = {}

    for partition in partitions:
        if partition.date is None or partition.exchange is None:
            continue
        exchange = partition.exchange.lower()
        if exchange not in normalized_exchanges:
            continue
        exchanges_by_date.setdefault(partition.date, set()).add(exchange)

    rows: list[CoverageRow] = []
    for date in sorted(exchanges_by_date):
        present = exchanges_by_date[date]
        exchange_presence = {
            exchange: exchange in present for exchange in normalized_exchanges
        }
        missing = tuple(exchange for exchange in normalized_exchanges if exchange not in present)
        rows.append(
            CoverageRow(
                date=date,
                exchange_presence=exchange_presence,
                coverage_count=len(present),
                missing_exchanges=missing,
            )
        )

    return tuple(rows)


def select_coverage_aware_date(
    partitions: Sequence[ManifestPartition],
    exchanges: Sequence[str],
    min_coverage: int = 2,
) -> CoverageSelection:
    """Select the latest full-coverage date, else latest acceptable partial coverage."""

    selected_partitions = filter_btc_trade_partitions(partitions, exchanges)
    coverage_rows = build_coverage_table(selected_partitions, exchanges)
    full_coverage_count = len(tuple(exchanges))

    full_coverage_rows = [
        row for row in coverage_rows if row.coverage_count == full_coverage_count
    ]
    if full_coverage_rows:
        row = max(full_coverage_rows, key=lambda item: item.date)
        return CoverageSelection(
            selected_date=row.date,
            coverage_row=row,
            partitions=_partitions_for_date(selected_partitions, row.date),
        )

    partial_coverage_rows = [
        row for row in coverage_rows if row.coverage_count >= min_coverage
    ]
    if partial_coverage_rows:
        row = max(partial_coverage_rows, key=lambda item: item.date)
        return CoverageSelection(
            selected_date=row.date,
            coverage_row=row,
            partitions=_partitions_for_date(selected_partitions, row.date),
        )

    return CoverageSelection(selected_date=None, coverage_row=None, partitions=())


def _partitions_for_date(
    partitions: Sequence[ManifestPartition],
    date: str,
) -> tuple[ManifestPartition, ...]:
    return tuple(partition for partition in partitions if partition.date == date)
