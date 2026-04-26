from pathlib import Path

import pytest
import yaml

from quantlab_event_scanner.config import load_config, validate_config


def _valid_config() -> dict:
    return {
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


def test_load_config_from_yaml(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(_valid_config()), encoding="utf-8")

    config = load_config(config_path)

    assert config.input_bucket == "quantlab-compact-stk-euc1"
    assert config.manifest_path == "s3://quantlab-compact-stk-euc1/compacted/_manifest.json"
    assert config.exchanges == ("binance", "bybit", "okx")
    assert config.streams == ("bbo", "trade", "mark_price", "funding", "open_interest")
    assert config.binance_open_interest_supported is False


def test_validate_config_requires_manifest_path() -> None:
    raw = _valid_config()
    del raw["manifest_path"]

    with pytest.raises(ValueError, match="manifest_path"):
        validate_config(raw)


def test_validate_config_exposes_manifest_path() -> None:
    config = validate_config(_valid_config())

    assert config.manifest_path.endswith("/compacted/_manifest.json")



def test_validate_config_requires_outputs() -> None:
    raw = _valid_config()
    del raw["outputs"]["events_map"]

    with pytest.raises(ValueError, match="events_map"):
        validate_config(raw)
