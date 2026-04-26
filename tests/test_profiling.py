from quantlab_event_scanner.manifest import ManifestPartition
from quantlab_event_scanner.profiling import (
    build_coverage_table,
    filter_btc_trade_partitions,
    select_coverage_aware_date,
)


EXCHANGES = ("binance", "bybit", "okx")


def test_filter_btc_trade_partitions_respects_exchange_stream_available_and_symbol() -> None:
    selected = filter_btc_trade_partitions(
        (
            _partition("binance", "trade", "BTCUSDT", "20260424"),
            _partition("bybit", "trade", "ETHUSDT", "20260424"),
            _partition("okx", "bbo", "BTC-USDT", "20260424"),
            _partition("coinbase", "trade", "BTC-USD", "20260424"),
            _partition("bybit", "trade", "btcusdt", "20260424", available=False),
            _partition("okx", "trade", "BTC-USDT-SWAP", "20260424"),
        ),
        EXCHANGES,
    )

    assert selected == (
        _partition("binance", "trade", "BTCUSDT", "20260424"),
        _partition("okx", "trade", "BTC-USDT-SWAP", "20260424"),
    )


def test_filter_btc_trade_partitions_preserves_original_symbol_casing() -> None:
    selected = filter_btc_trade_partitions(
        (_partition("bybit", "trade", "BtCUsDt", "20260424"),),
        EXCHANGES,
    )

    assert selected[0].symbol == "BtCUsDt"


def test_select_coverage_aware_date_chooses_latest_full_coverage_date() -> None:
    selection = select_coverage_aware_date(
        (
            _partition("binance", "trade", "btcusdt", "20260424"),
            _partition("bybit", "trade", "BTCUSDT", "20260424"),
            _partition("okx", "trade", "BTC-USDT", "20260424"),
            _partition("bybit", "trade", "BTCUSDT", "20260425"),
            _partition("okx", "trade", "BTC-USDT", "20260425"),
        ),
        EXCHANGES,
    )

    assert selection.selected_date == "20260424"
    assert selection.coverage_row is not None
    assert selection.coverage_row.coverage_count == 3
    assert selection.coverage_row.missing_exchanges == ()


def test_select_coverage_aware_date_falls_back_to_latest_two_of_three() -> None:
    selection = select_coverage_aware_date(
        (
            _partition("binance", "trade", "btcusdt", "20260423"),
            _partition("bybit", "trade", "BTCUSDT", "20260423"),
            _partition("bybit", "trade", "BTCUSDT", "20260424"),
            _partition("okx", "trade", "BTC-USDT", "20260424"),
            _partition("okx", "trade", "BTC-USDT", "20260425"),
        ),
        EXCHANGES,
    )

    assert selection.selected_date == "20260424"
    assert selection.coverage_row is not None
    assert selection.coverage_row.coverage_count == 2
    assert selection.coverage_row.missing_exchanges == ("binance",)


def test_select_coverage_aware_date_returns_empty_when_less_than_two_of_three() -> None:
    selection = select_coverage_aware_date(
        (
            _partition("binance", "trade", "btcusdt", "20260424"),
            _partition("okx", "bbo", "BTC-USDT", "20260424"),
        ),
        EXCHANGES,
    )

    assert selection.selected_date is None
    assert selection.coverage_row is None
    assert selection.partitions == ()


def test_build_coverage_table_reports_missing_exchanges() -> None:
    rows = build_coverage_table(
        (
            _partition("binance", "trade", "btcusdt", "20260424"),
            _partition("okx", "trade", "BTC-USDT", "20260424"),
        ),
        EXCHANGES,
    )

    assert len(rows) == 1
    assert rows[0].date == "20260424"
    assert rows[0].has_exchange("binance")
    assert not rows[0].has_exchange("bybit")
    assert rows[0].has_exchange("okx")
    assert rows[0].coverage_count == 2
    assert rows[0].missing_exchanges == ("bybit",)


def _partition(
    exchange: str,
    stream: str,
    symbol: str,
    date: str,
    available: bool | None = True,
) -> ManifestPartition:
    return ManifestPartition(
        exchange=exchange,
        stream=stream,
        symbol=symbol,
        date=date,
        path=f"exchange={exchange}/stream={stream}/symbol={symbol}/date={date}/data.parquet",
        available=available,
    )
