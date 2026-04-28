# Phase 2J P1 Partition / Extraction Patch Plan

## Context

P0 reduced the Phase 2J controlled runtime from `2h39m` to `20m34s` on the 1-worker classic cluster, then to `10m54s` on the fixed 8-worker Photon classic cluster. The 8-worker benchmark did not scale linearly.

Observed bottlenecks from the 8-worker run:

- `selected_normal_extraction`: `168.953s`
- `bbo_day_read_cache_materialize`: `101.085s`
- `trade_day` partitions: `3`
- `bbo_day` partitions: `5`
- small comparison outputs: `200` partitions
- top-diff outputs: `600` partitions
- `manifest_event_profile_load`: `66.052s`

## P1 Scope

This patch targets physical execution shape only. It does not change event definition, normal-window selection semantics, profile metrics, comparison metrics, output contracts, or S3 output paths.

## Changes

1. Repartition raw day reads after loading trade/BBO parquet paths.
   - Default: `96` partitions.
   - Keys: `exchange`, `symbol`, `ts_event` time bucket.
   - Default bucket width: `60` seconds.
   - Can be disabled with `--raw-day-repartition-partitions 0`.

2. Keep selected-normal extraction as a batch plan, but make output partition count explicit.
   - Default: `200` partitions.
   - Applies to normal trade, BBO, snapshot, and profile DataFrames.
   - Controlled by `--selected-output-partitions`.
   - Trade/BBO extraction joins also use a temporary `time_bucket` equality key plus the existing timestamp range filter, so the batch plan can use the raw-day partition shape more directly.

3. Coalesce small outputs.
   - Default: `1` partition.
   - Applies to comparison outputs and top-diff outputs.
   - Controlled by `--small-output-partitions`.

4. Reduce manifest/event profile load overhead.
   - Keep required non-empty/event-activity checks.
   - Avoid duplicate count and coverage actions in the instrumentation-only log calls.

## New Parameters

- `--raw-day-repartition-partitions`
- `--raw-day-bucket-seconds`
- `--selected-output-partitions`
- `--small-output-partitions`

## Expected Benchmark Signals

The next 8-worker rerun should answer:

- Did `bbo_day_read_cache_materialize` improve now that raw BBO is no longer cached as only `5` partitions?
- Did `selected_normal_extraction` improve or move cost into a more specific timer?
- Did small comparison/top-diff write and validation time drop after coalescing?
- Did `manifest_event_profile_load` drop after removing duplicate instrumentation actions?

## Rerun Boundary

No rerun is part of this patch. The next step is a controlled 8-worker benchmark with the same scope:

- BTC
- `20260423`
- `binance_btcusdt_20260423_down_001`
- `N=10`
- `validation-mode=light`
- fixed 8-worker Photon classic cluster
- auto-termination disabled
