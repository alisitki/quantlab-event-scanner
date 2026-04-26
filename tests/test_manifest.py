import pytest

from quantlab_event_scanner.manifest import (
    ManifestMetadata,
    load_manifest_from_s3_with_spark,
    parse_manifest_json,
)


def test_parse_manifest_preserves_raw_object() -> None:
    raw = {
        "generated_at": "2026-04-23T00:00:00Z",
        "files": [
            {
                "exchange": "binance",
                "stream": "trade",
                "symbol": "btcusdt",
                "date": "20260423",
                "path": (
                    "s3://quantlab-compact-stk-euc1/exchange=binance/stream=trade/"
                    "symbol=btcusdt/date=20260423/data.parquet"
                ),
            }
        ],
    }

    manifest = parse_manifest_json(raw)

    assert isinstance(manifest, ManifestMetadata)
    assert manifest.raw is raw
    assert manifest.partitions[0].exchange == "binance"
    assert manifest.partitions[0].stream == "trade"
    assert manifest.partitions[0].symbol == "btcusdt"
    assert manifest.partitions[0].date == "20260423"


def test_parse_manifest_tolerates_missing_optional_partition_fields() -> None:
    raw = {"partitions": [{"exchange": "okx"}, {"stream": "funding", "symbol": "BTC-USDT"}]}

    manifest = parse_manifest_json(raw)

    assert len(manifest.partitions) == 2
    assert manifest.partitions[0].exchange == "okx"
    assert manifest.partitions[0].stream is None
    assert manifest.partitions[1].stream == "funding"
    assert manifest.partitions[1].date is None


def test_parse_manifest_extracts_partition_from_path_string() -> None:
    raw = {
        "latest": (
            "s3://quantlab-compact-stk-euc1/exchange=bybit/stream=mark_price/"
            "symbol=btcusdt/date=20260423/data.parquet"
        )
    }

    manifest = parse_manifest_json(raw)

    assert len(manifest.partitions) == 1
    assert manifest.partitions[0].exchange == "bybit"
    assert manifest.partitions[0].stream == "mark_price"
    assert manifest.partitions[0].symbol == "btcusdt"
    assert manifest.partitions[0].date == "20260423"


def test_load_manifest_from_s3_with_spark_is_placeholder() -> None:
    with pytest.raises(NotImplementedError):
        load_manifest_from_s3_with_spark(None, "s3://bucket/compacted/_manifest.json")
