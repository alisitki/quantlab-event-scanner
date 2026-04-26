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


def test_parse_manifest_v2_uses_available_entries_and_data_key_artifacts_only() -> None:
    raw = {
        "schema_version": 1,
        "dates": {
            "20260425": {
                "exchanges": {
                    "binance": {
                        "streams": {
                            "bbo": {
                                "symbols": {
                                    "btcusdt": {
                                        "available": True,
                                        "artifacts": {
                                            "data_key": (
                                                "exchange=binance/stream=bbo/symbol=btcusdt/"
                                                "date=20260425/data.parquet"
                                            ),
                                            "meta_key": (
                                                "exchange=binance/stream=bbo/symbol=btcusdt/"
                                                "date=20260425/meta.json"
                                            ),
                                            "quality_day_key": (
                                                "exchange=binance/stream=bbo/symbol=btcusdt/"
                                                "date=20260425/quality_day.json"
                                            ),
                                        },
                                    }
                                }
                            },
                            "trade": {
                                "symbols": {
                                    "btcusdt": {
                                        "available": False,
                                    }
                                }
                            },
                        }
                    }
                }
            }
        },
    }

    manifest = parse_manifest_json(raw)

    assert len(manifest.partitions) == 2
    available = manifest.partitions[0]
    unavailable = manifest.partitions[1]
    assert available.exchange == "binance"
    assert available.stream == "bbo"
    assert available.symbol == "btcusdt"
    assert available.date == "20260425"
    assert available.available is True
    assert available.path == "exchange=binance/stream=bbo/symbol=btcusdt/date=20260425/data.parquet"
    assert available.artifacts["meta_key"].endswith("/meta.json")
    assert unavailable.stream == "trade"
    assert unavailable.available is False
    assert unavailable.path is None


def test_load_manifest_from_s3_with_spark_uses_fake_spark_text_reader() -> None:
    spark = _FakeSpark(
        [
            (
                '{"files": [{"path": "s3://bucket/exchange=binance/stream=trade/'
                'symbol=btcusdt/date=20260423/data.parquet"}]}'
            ),
        ]
    )

    manifest = load_manifest_from_s3_with_spark(spark, "s3://bucket/_manifest.json")

    assert manifest.partitions[0].exchange == "binance"
    assert manifest.partitions[0].stream == "trade"
    assert manifest.partitions[0].symbol == "btcusdt"
    assert manifest.partitions[0].date == "20260423"
    assert spark.read.path == "s3://bucket/_manifest.json"


def test_load_manifest_from_s3_with_spark_fails_clearly_on_empty_manifest() -> None:
    spark = _FakeSpark([])

    try:
        load_manifest_from_s3_with_spark(spark, "s3://bucket/_manifest.json")
    except ValueError as exc:
        assert "empty or returned no text rows" in str(exc)
    else:
        raise AssertionError("Expected empty manifest to fail.")


class _FakeSpark:
    def __init__(self, lines: list[str]) -> None:
        self.read = _FakeReader(lines)


class _FakeReader:
    def __init__(self, lines: list[str]) -> None:
        self.lines = lines
        self.path: str | None = None

    def text(self, path: str):
        self.path = path
        return _FakeDataFrame(self.lines)


class _FakeDataFrame:
    def __init__(self, lines: list[str]) -> None:
        self.lines = lines

    def collect(self):
        return [_FakeRow(line) for line in self.lines]


class _FakeRow:
    def __init__(self, value: str) -> None:
        self.value = value
