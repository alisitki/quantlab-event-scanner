"""Configuration loading and validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml


REQUIRED_FIELDS = (
    "input_bucket",
    "input_root",
    "manifest_path",
    "output_bucket",
    "output_root",
    "exchanges",
    "streams",
    "binance_open_interest_supported",
    "stream_semantics",
    "outputs",
)

REQUIRED_OUTPUTS = (
    "price_1s",
    "events_map",
    "pre_event_windows",
    "normal_time_comparison",
)


@dataclass(frozen=True)
class ScannerConfig:
    """Validated scanner configuration."""

    input_bucket: str
    input_root: str
    manifest_path: str
    output_bucket: str
    output_root: str
    exchanges: tuple[str, ...]
    streams: tuple[str, ...]
    binance_open_interest_supported: bool
    stream_semantics: Mapping[str, Mapping[str, str]]
    outputs: Mapping[str, str]
    raw: Mapping[str, Any] = field(repr=False)


def load_config(path: str | Path) -> ScannerConfig:
    """Load and validate a YAML config file."""

    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)

    if loaded is None:
        loaded = {}

    return validate_config(loaded)


def validate_config(config: Mapping[str, Any]) -> ScannerConfig:
    """Validate config shape without importing Spark or touching external systems."""

    if not isinstance(config, Mapping):
        raise TypeError("Config must be a mapping.")

    missing = [field_name for field_name in REQUIRED_FIELDS if field_name not in config]
    if missing:
        raise ValueError(f"Missing required config fields: {', '.join(missing)}")

    outputs = config["outputs"]
    if not isinstance(outputs, Mapping):
        raise TypeError("Config field 'outputs' must be a mapping.")

    missing_outputs = [name for name in REQUIRED_OUTPUTS if name not in outputs]
    if missing_outputs:
        raise ValueError(f"Missing required output paths: {', '.join(missing_outputs)}")

    return ScannerConfig(
        input_bucket=_required_string(config, "input_bucket"),
        input_root=_required_s3_path(config, "input_root"),
        manifest_path=_required_s3_path(config, "manifest_path"),
        output_bucket=_required_string(config, "output_bucket"),
        output_root=_required_s3_path(config, "output_root"),
        exchanges=_required_string_tuple(config, "exchanges"),
        streams=_required_string_tuple(config, "streams"),
        binance_open_interest_supported=_required_bool(
            config, "binance_open_interest_supported"
        ),
        stream_semantics=_required_stream_semantics(config, "stream_semantics"),
        outputs={name: _required_output_path(outputs, name) for name in REQUIRED_OUTPUTS},
        raw=dict(config),
    )


def _required_string(config: Mapping[str, Any], field_name: str) -> str:
    value = config[field_name]
    if not isinstance(value, str) or not value:
        raise TypeError(f"Config field '{field_name}' must be a non-empty string.")
    return value


def _required_s3_path(config: Mapping[str, Any], field_name: str) -> str:
    value = _required_string(config, field_name)
    if not value.startswith("s3://"):
        raise ValueError(f"Config field '{field_name}' must be an S3 URI.")
    return value.rstrip("/")


def _required_output_path(outputs: Mapping[str, Any], name: str) -> str:
    value = outputs[name]
    if not isinstance(value, str) or not value.startswith("s3://"):
        raise ValueError(f"Output path '{name}' must be an S3 URI.")
    return value.rstrip("/")


def _required_string_tuple(config: Mapping[str, Any], field_name: str) -> tuple[str, ...]:
    value = config[field_name]
    if not isinstance(value, list) or not value:
        raise TypeError(f"Config field '{field_name}' must be a non-empty list.")

    if not all(isinstance(item, str) and item for item in value):
        raise TypeError(f"Config field '{field_name}' must contain only non-empty strings.")

    return tuple(value)


def _required_bool(config: Mapping[str, Any], field_name: str) -> bool:
    value = config[field_name]
    if not isinstance(value, bool):
        raise TypeError(f"Config field '{field_name}' must be a boolean.")
    return value


def _required_stream_semantics(
    config: Mapping[str, Any],
    field_name: str,
) -> Mapping[str, Mapping[str, str]]:
    value = config[field_name]
    if not isinstance(value, Mapping):
        raise TypeError(f"Config field '{field_name}' must be a mapping.")

    normalized: dict[str, dict[str, str]] = {}
    for exchange, streams in value.items():
        if not isinstance(exchange, str) or not exchange:
            raise TypeError(f"Config field '{field_name}' must use non-empty string keys.")
        if not isinstance(streams, Mapping):
            raise TypeError(f"Config field '{field_name}.{exchange}' must be a mapping.")

        normalized[exchange] = {}
        for stream, semantic in streams.items():
            if not isinstance(stream, str) or not stream:
                raise TypeError(
                    f"Config field '{field_name}.{exchange}' must use non-empty string keys."
                )
            if not isinstance(semantic, str) or not semantic:
                raise TypeError(
                    f"Config field '{field_name}.{exchange}.{stream}' must be a "
                    "non-empty string."
                )
            normalized[exchange][stream] = semantic

    return normalized
