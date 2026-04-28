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


def events_map_trial_run_path(config: ScannerConfig, run_id: str) -> str:
    """Return the trial event map output path for a run."""

    return f"{events_map_path(config)}/_trial/run_id={run_id}"


def raw_candidates_trial_run_path(config: ScannerConfig, run_id: str) -> str:
    """Return the trial raw candidates output path for a run."""

    return f"{config.output_root}/raw_candidates/_trial/run_id={run_id}"


def pre_event_windows_path(config: ScannerConfig) -> str:
    """Return the configured pre-event windows output path."""

    return output_path(config, "pre_event_windows")


def pre_event_windows_trial_run_path(config: ScannerConfig, run_id: str) -> str:
    """Return the trial pre-event windows output path for a run."""

    return f"{pre_event_windows_path(config)}/_trial/run_id={run_id}"


def pre_event_bbo_windows_trial_run_path(config: ScannerConfig, run_id: str) -> str:
    """Return the trial BBO pre-event windows output path for a run."""

    return f"{config.output_root}/pre_event_bbo_windows/_trial/run_id={run_id}"


def pre_event_market_snapshots_trial_run_path(config: ScannerConfig, run_id: str) -> str:
    """Return the trial 1-second market snapshots output path for a run."""

    return f"{config.output_root}/pre_event_market_snapshots/_trial/run_id={run_id}"


def pre_event_profile_reports_trial_run_path(config: ScannerConfig, run_id: str) -> str:
    """Return the trial pre-event profile reports output path for a run."""

    return f"{config.output_root}/pre_event_profile_reports/_trial/run_id={run_id}"


def normal_trade_windows_trial_run_path(config: ScannerConfig, run_id: str) -> str:
    """Return the trial normal trade windows output path for a run."""

    return f"{config.output_root}/normal_trade_windows/_trial/run_id={run_id}"


def normal_bbo_windows_trial_run_path(config: ScannerConfig, run_id: str) -> str:
    """Return the trial normal BBO windows output path for a run."""

    return f"{config.output_root}/normal_bbo_windows/_trial/run_id={run_id}"


def normal_market_snapshots_trial_run_path(config: ScannerConfig, run_id: str) -> str:
    """Return the trial normal market snapshots output path for a run."""

    return f"{config.output_root}/normal_market_snapshots/_trial/run_id={run_id}"


def normal_profile_reports_trial_run_path(config: ScannerConfig, run_id: str) -> str:
    """Return the trial normal profile reports output path for a run."""

    return f"{config.output_root}/normal_profile_reports/_trial/run_id={run_id}"


def profile_comparison_reports_trial_run_path(config: ScannerConfig, run_id: str) -> str:
    """Return the trial event-vs-normal profile comparison output path for a run."""

    return f"{config.output_root}/profile_comparison_reports/_trial/run_id={run_id}"


def profile_comparison_top_diffs_trial_run_path(config: ScannerConfig, run_id: str) -> str:
    """Return the trial profile comparison top-diff inspection output path for a run."""

    return f"{config.output_root}/profile_comparison_top_diffs/_trial/run_id={run_id}"


def multi_normal_windows_trial_run_path(config: ScannerConfig, run_id: str) -> str:
    """Return the trial multi-normal raw window output path for a run."""

    return f"{config.output_root}/multi_normal_windows/_trial/run_id={run_id}"


def multi_normal_market_snapshots_trial_run_path(config: ScannerConfig, run_id: str) -> str:
    """Return the trial multi-normal market snapshot output path for a run."""

    return f"{config.output_root}/multi_normal_market_snapshots/_trial/run_id={run_id}"


def multi_normal_profile_reports_trial_run_path(config: ScannerConfig, run_id: str) -> str:
    """Return the trial multi-normal profile reports output path for a run."""

    return f"{config.output_root}/multi_normal_profile_reports/_trial/run_id={run_id}"


def multi_normal_comparison_reports_trial_run_path(config: ScannerConfig, run_id: str) -> str:
    """Return the trial multi-normal comparison reports output path for a run."""

    return f"{config.output_root}/multi_normal_comparison_reports/_trial/run_id={run_id}"


def multi_normal_top_diffs_trial_run_path(config: ScannerConfig, run_id: str) -> str:
    """Return the trial multi-normal top-diff inspection output path for a run."""

    return f"{config.output_root}/multi_normal_top_diffs/_trial/run_id={run_id}"


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
