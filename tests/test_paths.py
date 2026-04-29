from quantlab_event_scanner.config import validate_config
from quantlab_event_scanner.paths import (
    btc_event_coverage_probe_run_path,
    btc_multi_event_review_trial_run_path,
    btc_multi_event_trial_run_path,
    compacted_partition_path,
    events_map_path,
    events_map_trial_run_path,
    manifest_path,
    multi_normal_comparison_reports_trial_run_path,
    multi_normal_market_snapshots_trial_run_path,
    multi_normal_profile_reports_trial_run_path,
    multi_normal_top_diffs_trial_run_path,
    multi_normal_windows_trial_run_path,
    normal_bbo_windows_trial_run_path,
    normal_market_snapshots_trial_run_path,
    normal_profile_reports_trial_run_path,
    normal_trade_windows_trial_run_path,
    normal_time_comparison_path,
    output_path,
    pre_event_bbo_windows_trial_run_path,
    pre_event_market_snapshots_trial_run_path,
    pre_event_profile_reports_trial_run_path,
    pre_event_windows_path,
    pre_event_windows_trial_run_path,
    profile_comparison_reports_trial_run_path,
    profile_comparison_top_diffs_trial_run_path,
    price_1s_path,
    raw_candidates_trial_run_path,
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
            "stream_semantics": {
                "binance": {"trade": "agg_trade"},
                "bybit": {"trade": "trade"},
                "okx": {"trade": "trade"},
            },
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


def test_trial_output_paths_are_partitioned_by_run_id() -> None:
    config = _config()

    assert events_map_trial_run_path(config, "phase1d_test") == (
        "s3://quantlab-research/events_map/_trial/run_id=phase1d_test"
    )
    assert raw_candidates_trial_run_path(config, "phase1d_test") == (
        "s3://quantlab-research/raw_candidates/_trial/run_id=phase1d_test"
    )
    assert pre_event_windows_trial_run_path(config, "phase2a_test") == (
        "s3://quantlab-research/pre_event_windows/_trial/run_id=phase2a_test"
    )
    assert pre_event_bbo_windows_trial_run_path(config, "phase2b_test") == (
        "s3://quantlab-research/pre_event_bbo_windows/_trial/run_id=phase2b_test"
    )
    assert pre_event_market_snapshots_trial_run_path(config, "phase2c_test") == (
        "s3://quantlab-research/pre_event_market_snapshots/_trial/run_id=phase2c_test"
    )
    assert pre_event_profile_reports_trial_run_path(config, "phase2d_test") == (
        "s3://quantlab-research/pre_event_profile_reports/_trial/run_id=phase2d_test"
    )
    assert normal_trade_windows_trial_run_path(config, "phase2e_test") == (
        "s3://quantlab-research/normal_trade_windows/_trial/run_id=phase2e_test"
    )
    assert normal_bbo_windows_trial_run_path(config, "phase2e_test") == (
        "s3://quantlab-research/normal_bbo_windows/_trial/run_id=phase2e_test"
    )
    assert normal_market_snapshots_trial_run_path(config, "phase2e_test") == (
        "s3://quantlab-research/normal_market_snapshots/_trial/run_id=phase2e_test"
    )
    assert normal_profile_reports_trial_run_path(config, "phase2e_test") == (
        "s3://quantlab-research/normal_profile_reports/_trial/run_id=phase2e_test"
    )
    assert profile_comparison_reports_trial_run_path(config, "phase2f_test") == (
        "s3://quantlab-research/profile_comparison_reports/_trial/run_id=phase2f_test"
    )
    assert profile_comparison_top_diffs_trial_run_path(config, "phase2g_test") == (
        "s3://quantlab-research/profile_comparison_top_diffs/_trial/run_id=phase2g_test"
    )
    assert multi_normal_windows_trial_run_path(config, "phase2j_test") == (
        "s3://quantlab-research/multi_normal_windows/_trial/run_id=phase2j_test"
    )
    assert multi_normal_market_snapshots_trial_run_path(config, "phase2j_test") == (
        "s3://quantlab-research/multi_normal_market_snapshots/_trial/run_id=phase2j_test"
    )
    assert multi_normal_profile_reports_trial_run_path(config, "phase2j_test") == (
        "s3://quantlab-research/multi_normal_profile_reports/_trial/run_id=phase2j_test"
    )
    assert multi_normal_comparison_reports_trial_run_path(config, "phase2j_test") == (
        "s3://quantlab-research/multi_normal_comparison_reports/_trial/run_id=phase2j_test"
    )
    assert multi_normal_top_diffs_trial_run_path(config, "phase2j_test") == (
        "s3://quantlab-research/multi_normal_top_diffs/_trial/run_id=phase2j_test"
    )
    assert btc_multi_event_trial_run_path(config, "phase3a_test") == (
        "s3://quantlab-research/btc_multi_event_trials/_trial/run_id=phase3a_test"
    )
    assert btc_multi_event_review_trial_run_path(
        config,
        "phase3a_test",
        "phase3b_test",
    ) == (
        "s3://quantlab-research/btc_multi_event_reviews/_trial/"
        "source_run_id=phase3a_test/run_id=phase3b_test"
    )
    assert btc_event_coverage_probe_run_path(config, "phase3a_probe_test") == (
        "s3://quantlab-research/btc_multi_event_trials/_probe/run_id=phase3a_probe_test"
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
