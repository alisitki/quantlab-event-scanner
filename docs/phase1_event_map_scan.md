# Phase 1 Event Map Scan Design

Phase 1 will add the first Databricks/Spark job: an event map scan that reads
compacted S3 inputs, identifies candidate market events, and writes an event map
dataset to S3.

This document is a design checkpoint only. It does not implement Spark logic,
scan S3, create notebooks, add ML, or add trading/execution behavior.

## Goal

Build a Databricks job that produces a compact event map for later extraction of
pre-event windows and normal-time comparisons.

The job must:

- Read compacted metadata from
  `s3://quantlab-compact-stk-euc1/compacted/_manifest.json`.
- Use `configs/dev.yaml` or `configs/prod.yaml` for all S3 roots and output
  paths.
- Respect stream semantics, especially `binance.trade = agg_trade`.
- Write event map output to `s3://quantlab-research/events_map`.
- Avoid local data output entirely.

## Proposed Inputs

The job should load config, parse the manifest, and select relevant compacted
partitions from the configured exchanges and streams:

- Exchanges: `binance`, `bybit`, `okx`
- Streams: `bbo`, `trade`, `mark_price`, `funding`, `open_interest`
- Unsupported pair: `binance` + `open_interest`

The manifest parser should remain schema-tolerant. If the manifest schema has
partition records, use them directly. If it has paths, extract
`exchange/stream/symbol/date` from the path shape already supported by
`manifest.py`.

## Proposed Output

The event map should be a partitioned S3 dataset under the configured
`events_map` output path.

Recommended logical columns:

- `event_id`: deterministic identifier for the event candidate
- `exchange`: source exchange
- `symbol`: source symbol, preserving original casing
- `event_ts`: event timestamp in UTC
- `event_date`: `YYYYMMDD` date derived from `event_ts`
- `event_type`: event classifier name
- `source_streams`: streams used to detect the event
- `trade_semantics`: trade stream meaning, such as `agg_trade` or `trade`
- `score`: numeric event strength
- `metadata`: optional JSON/string field for detector details
- `created_at`: UTC timestamp when the event map row was produced

Recommended partitioning:

- `event_date`
- `exchange`

## Implementation Shape

Phase 1 implementation should add:

- `jobs/01_detect_events.py` as the Databricks job entrypoint.
- A Databricks bundle job resource for the event map scan.
- Small source helpers under `src/quantlab_event_scanner/` only if they are
  independently testable without Spark.
- Local unit tests for config/path/manifest behavior and any pure helper logic.

Spark-specific tests should not require local PySpark. Databricks execution
should be validated through bundle validation first, then a controlled dev run
after the job is explicitly approved.

## Decisions Needed Before Implementation

The following choices should be locked before writing the Spark job:

- Event definition: what market condition qualifies as an event candidate.
- Required source streams for v1: trade-only, bbo+trade, or a broader stream
  combination.
- Time granularity: raw timestamp, 1-second bucket, or another interval.
- Per-symbol scan scope: all manifest symbols or an explicit allowlist.
- Initial date scope: all manifest dates or a bounded dev date range.
- Event score formula and threshold.
- Deduplication rule when multiple exchanges or streams identify nearby events.
- Exact write mode for dev runs: overwrite partition, append, or replace output.

## Phase 1 Non-Goals

Phase 1 should not:

- Extract pre-event windows.
- Build normal-time comparison samples.
- Train or run ML models.
- Place orders or include trading/execution logic.
- Store S3 data locally.
- Create notebooks.
