import pytest

from quantlab_event_scanner.manifest import ManifestMetadata, ManifestPartition
from quantlab_event_scanner.probe import (
    latest_partition_date,
    partition_paths,
    select_btc_trade_partitions,
)


def test_latest_partition_date_uses_max_manifest_date() -> None:
    manifest = ManifestMetadata(
        raw={},
        partitions=(
            ManifestPartition(stream="trade", symbol="btcusdt", date="20260422"),
            ManifestPartition(stream="trade", symbol="btcusdt", date="20260423"),
        ),
    )

    assert latest_partition_date(manifest) == "20260423"


def test_select_btc_trade_partitions_filters_latest_trade_btc_case_insensitive() -> None:
    latest_path = (
        "s3://bucket/exchange=bybit/stream=trade/symbol=BTC-USDT/"
        "date=20260423/data.parquet"
    )
    manifest = ManifestMetadata(
        raw={},
        partitions=(
            ManifestPartition(stream="trade", symbol="ethusdt", date="20260423", path="eth"),
            ManifestPartition(stream="bbo", symbol="btcusdt", date="20260423", path="bbo"),
            ManifestPartition(stream="trade", symbol="btcusdt", date="20260422", path="old"),
            ManifestPartition(
                stream="trade",
                symbol="btcusdt",
                date="20260423",
                path="unavailable",
                available=False,
            ),
            ManifestPartition(
                stream="trade",
                symbol="BTC-USDT",
                date="20260423",
                path=latest_path,
            ),
        ),
    )

    selected = select_btc_trade_partitions(manifest)

    assert selected == (
        ManifestPartition(
            stream="trade",
            symbol="BTC-USDT",
            date="20260423",
            path=latest_path,
        ),
    )
    assert partition_paths(selected) == (latest_path,)


def test_partition_paths_prefixes_relative_artifact_data_keys_with_input_root() -> None:
    paths = partition_paths(
        (
            ManifestPartition(
                stream="trade",
                symbol="btcusdt",
                date="20260423",
                path="exchange=binance/stream=trade/symbol=btcusdt/date=20260423/data.parquet",
            ),
        ),
        input_root="s3://quantlab-compact-stk-euc1",
    )

    assert paths == (
        "s3://quantlab-compact-stk-euc1/exchange=binance/stream=trade/"
        "symbol=btcusdt/date=20260423/data.parquet",
    )


def test_partition_paths_requires_paths_for_selected_partitions() -> None:
    with pytest.raises(ValueError, match="must all include artifact data paths"):
        partition_paths((ManifestPartition(stream="trade", symbol="btcusdt", date="20260423"),))


def test_partition_paths_requires_non_empty_selection() -> None:
    with pytest.raises(ValueError, match="No matching"):
        partition_paths(())
