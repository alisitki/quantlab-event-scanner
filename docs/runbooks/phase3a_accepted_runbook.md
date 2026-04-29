# Phase 3A Accepted Runbook

## Accepted Run

- Source run ID: `phase3a_20260429T085638Z`
- Output root:
  `s3://quantlab-research/btc_multi_event_trials/_trial/run_id=phase3a_20260429T085638Z`
- Selected events: `10`

Accepted row counts:

- `exchange_profile_comparison=6600`
- `cross_exchange_mid_diff_comparison=1500`
- `bucket_change_profile_comparison=960`
- `exchange_profile_top_diffs=1800`
- `cross_exchange_mid_diff_top_diffs=1200`
- `bucket_change_profile_top_diffs=1560`
- `event_counts_by_direction=2`
- `metric_group_summary=80`
- `normal_selection_quality=10`
- `event_processing_status=10`

## Bundle Defaults vs Accepted-Scale Overrides

`databricks.yml` keeps Phase 3A task defaults intentionally small/smoke-like:

- `start-date=20260423`
- `end-date=20260423`
- `max-events=5`
- `sample-size=20`
- finalize `source-run-id=phase3a_SOURCE_RUN_ID`

The accepted Phase 3A run used explicit runtime overrides. Do not interpret
bundle defaults as accepted-scale production defaults.

## Accepted Trial Command Shape

Job A:

```bash
databricks bundle run phase3a_btc_multi_event_trial_classic \
  -t dev_classic \
  --profile quantlab-classic \
  --var cluster_id=<classic_cluster_id> \
  --python-params='--config,configs/dev.yaml,--start-date,20260203,--end-date,20260426,--max-events,10,--min-events,2,--normal-count-per-event,10,--lookback-seconds,300,--horizon-seconds,60,--move-threshold-pct,1.0,--exclusion-seconds,1800,--min-normal-anchor-spacing-seconds,1800,--validation-mode,light,--write-raw-windows,false,--allow-partial-coverage,false,--sample-size,0'
```

Job B:

```bash
databricks bundle run phase3a_btc_multi_event_finalize_classic \
  -t dev_classic \
  --profile quantlab-classic \
  --var cluster_id=<classic_cluster_id> \
  --python-params='--config,configs/dev.yaml,--source-run-id,<phase3a_run_id>,--normal-count-per-event,10,--top-n,20,--sample-size,0,--validation-mode,light,--small-output-partitions,1'
```

## Boundary

Phase 3A is a research/trial milestone. It is not production, not ML, not live
trading, and not validated signal proof.
