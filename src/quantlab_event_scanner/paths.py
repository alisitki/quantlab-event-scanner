"""S3 path helpers for scanner inputs and outputs."""

from __future__ import annotations

from .config import ScannerConfig


def manifest_path(config: ScannerConfig) -> str:
    """Return the configured compacted manifest path."""

    return config.manifest_path


def output_path(config: ScannerConfig, name: str) -> str:
    """Return a named configured output path."""

    try:
        return config.outputs[name]
    except KeyError as exc:
        raise KeyError(f"Unknown output path name: {name}") from exc


def price_1s_path(config: ScannerConfig) -> str:
    """Return the configured 1-second price output path."""

    return output_path(config, "price_1s")


def events_map_path(config: ScannerConfig) -> str:
    """Return the configured event map output path."""

    return output_path(config, "events_map")


def pre_event_windows_path(config: ScannerConfig) -> str:
    """Return the configured pre-event windows output path."""

    return output_path(config, "pre_event_windows")


def normal_time_comparison_path(config: ScannerConfig) -> str:
    """Return the configured normal-time comparison output path."""

    return output_path(config, "normal_time_comparison")


def compacted_partition_path(
    config: ScannerConfig,
    exchange: str,
    stream: str,
    symbol: str,
    date: str,
) -> str:
    """Build a compacted partition path while preserving caller-provided values."""

    return (
        f"{config.input_root}/exchange={exchange}/stream={stream}/"
        f"symbol={symbol}/date={date}/data.parquet"
    )
