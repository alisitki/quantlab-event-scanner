from quantlab_event_scanner.config import validate_config
from quantlab_event_scanner.paths import (
    compacted_partition_path,
    events_map_path,
    manifest_path,
    normal_time_comparison_path,
    output_path,
    pre_event_windows_path,
    price_1s_path,
)


def _config():
    return validate_config(
        {
            "input_bucket": "quantlab-compact-stk-euc1",
            "input_root": "s3://quantlab-compact-stk-euc1",
            "manifest_path": "s3://quantlab-compact-stk-euc1/compacted/_manifest.json",
            "output_bucket": "quantlab-research",
            "output_root": "s3://quantlab-research",
            "exchanges": ["binance", "bybit", "okx"],
            "streams": ["bbo", "trade", "mark_price", "funding", "open_interest"],
            "binance_open_interest_supported": False,
            "outputs": {
                "price_1s": "s3://quantlab-research/price_1s",
                "events_map": "s3://quantlab-research/events_map",
                "pre_event_windows": "s3://quantlab-research/pre_event_windows",
                "normal_time_comparison": "s3://quantlab-research/normal_time_comparison",
            },
        }
    )


def test_manifest_and_output_paths_come_from_config() -> None:
    config = _config()

    assert manifest_path(config) == "s3://quantlab-compact-stk-euc1/compacted/_manifest.json"
    assert output_path(config, "price_1s") == "s3://quantlab-research/price_1s"
    assert price_1s_path(config) == "s3://quantlab-research/price_1s"
    assert events_map_path(config) == "s3://quantlab-research/events_map"
    assert pre_event_windows_path(config) == "s3://quantlab-research/pre_event_windows"
    assert normal_time_comparison_path(config) == (
        "s3://quantlab-research/normal_time_comparison"
    )


def test_compacted_partition_path_preserves_symbol_casing() -> None:
    config = _config()

    path = compacted_partition_path(
        config,
        exchange="binance",
        stream="trade",
        symbol="btcusdt",
        date="20260423",
    )

    assert path == (
        "s3://quantlab-compact-stk-euc1/exchange=binance/stream=trade/"
        "symbol=btcusdt/date=20260423/data.parquet"
    )


def test_compacted_partition_path_uses_configured_input_root() -> None:
    config = _config()

    path = compacted_partition_path(config, "okx", "bbo", "BTC-USDT", "20260423")

    assert path.startswith(f"{config.input_root}/exchange=okx/")
    assert "symbol=BTC-USDT" in path
