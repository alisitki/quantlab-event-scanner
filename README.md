# quantlab-event-scanner

Phase 0 repository bootstrap for the QuantLab event scanner.

This repository contains the project skeleton, configuration, tests, Databricks
Asset Bundle jobs, and early Spark profiling/probe code. It does not run Spark
event detection, train ML models, perform trading or execution, create
notebooks, or store data locally.

Input data is expected to live on S3 in future phases. Compacted metadata will be
read from `s3://quantlab-compact-stk-euc1/compacted/_manifest.json` in future
Databricks/Spark workflows. Output data must also be written to S3. The local PC
must not store data outputs.

The Databricks CLI will be required later for bundle validation, deployment, and
job execution:

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run
```

## Phase 0.5 - Databricks CLI authentication

The repository code can exist locally without Databricks CLI authentication.
Databricks CLI authentication is required before running bundle validation,
deployment, or jobs from this machine. Authentication must be performed manually
by the user. Do not commit credentials, tokens, workspace credentials, or
`.databrickscfg`; do not store tokens in this repository.

Install the Databricks CLI on macOS:

```bash
brew tap databricks/tap
brew install databricks
databricks -v
```

Authenticate with the development workspace:

```bash
databricks auth login --host <DATABRICKS_WORKSPACE_URL> --profile quantlab-dev
databricks auth profiles
databricks current-user me --profile quantlab-dev
```

Replace `<DATABRICKS_WORKSPACE_URL>` with the real workspace URL. The preferred
local profile name is `quantlab-dev`. A production profile can be added later as
`quantlab-prod`.

Validate the serverless development bundle after authentication:

```bash
databricks bundle validate -t dev_serverless --profile quantlab-dev
```

Bundle deploy and run commands are not part of Phase 0.5.

Future phases:

1. Phase 1: event map scan
2. Phase 2: pre-event window extraction
3. Phase 3: normal-time comparison

Phase 1 design notes are tracked in
[`docs/phase1_event_map_scan.md`](docs/phase1_event_map_scan.md).

## Phase 1A - Manifest latest BTC trade probe

The first Databricks job is a controlled probe only. It reads the compacted
manifest, finds the latest date, selects `stream=trade` partitions whose symbol
contains `btc`, reads those parquet paths with Spark, logs schema, row count,
and a small sample, then stops.

Manifest v2 consumer semantics are documented in
[`STATE_V2_CONSUMER.md`](STATE_V2_CONSUMER.md): only entries with
`available=true` are read, and parquet read paths come from
`artifacts.data_key`.

It does not calculate z-scores, rolling windows, aggregations, events, features,
or write any S3 output.

This probe can run on either compute model:

- Serverless workflow compute for lightweight probes.
- Classic cluster compute for heavier Spark runs where runtime and node control
  matter.

Validate/deploy/run on the serverless workspace:

```bash
databricks bundle validate -t dev_serverless --profile quantlab-dev
databricks bundle deploy -t dev_serverless --profile quantlab-dev
databricks bundle run phase1_manifest_trade_btc_probe_serverless -t dev_serverless --profile quantlab-dev
```

Validate/deploy/run on the classic workspace:

```bash
databricks clusters start 0426-145152-cv6y6ado --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic
databricks bundle deploy -t dev_classic --profile quantlab-classic
databricks bundle run phase1_manifest_trade_btc_probe_classic -t dev_classic --profile quantlab-classic
databricks clusters delete 0426-145152-cv6y6ado --profile quantlab-classic
```

The classic cluster ID is a development default, not a secret, and can be
overridden with `--var cluster_id=...`. Classic clusters should normally be
terminated after runs to avoid idle cost.

## Phase 1B - BTC trade data profiling

Phase 1B profiles BTC trade data quality and time behavior before event
detection. It reads the manifest through Spark, selects BTC `trade` partitions
with coverage-aware date selection, reads parquet files, logs profiling
summaries, and writes no S3 output.

The date selection rule is:

- Prefer the latest date with full `3/3` coverage across configured exchanges.
- If no full-coverage date exists, use the latest date with at least `2/3`
  coverage.
- If no date has at least `2/3` coverage, log a controlled warning and exit
  without reading parquet files.

Validate/deploy/run on the classic workspace:

```bash
databricks clusters start 0426-145152-cv6y6ado --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic
databricks bundle deploy -t dev_classic --profile quantlab-classic --var cluster_id=0426-145152-cv6y6ado
databricks bundle run phase1_btc_trade_profile -t dev_classic --profile quantlab-classic --var cluster_id=0426-145152-cv6y6ado
databricks clusters delete 0426-145152-cv6y6ado --profile quantlab-classic
```

Verified on 2026-04-26 against `dev_classic`:

- Run result: `TERMINATED SUCCESS`.
- Selected coverage-aware date: `20260423`.
- Selected coverage: `binance`, `bybit`, `okx` (`3/3`).
- Latest manifest dates `20260425` and `20260424` had only `2/3` coverage
  because Binance BTC trade data was missing.
- Total selected row count: `5,578,439`.
- Known trade columns had zero nulls.
- `trade_id` duplicate groups: `0`.
- Sequence/time ordering out-of-order counts: `0` for all selected exchanges.
- No S3 output was written.

## Phase 1C - BTC trade move candidate scan

Phase 1C scans fixed-date BTC trade data for log-only +/-1% move candidates.
It reads `date=20260423`, `stream=trade`, and configured BTC symbols on
`binance`, `bybit`, and `okx`; builds 1-second OHLCV rows; checks whether each
second reaches +1% or -1% within the next 60 seconds; logs raw and approximate
grouped candidate summaries; and writes no S3 output.

Serverless is the default compute path for this phase because the scan reduces
the known `20260423` trade input to roughly 1-second rows before candidate
summary logging. Classic cluster compute remains available as a fallback if
serverless hits access or runtime limits.

Validate/deploy/run on the serverless workspace:

```bash
databricks bundle validate -t dev_serverless --profile quantlab-dev
databricks bundle deploy -t dev_serverless --profile quantlab-dev
databricks bundle run phase1_btc_trade_move_candidates_serverless -t dev_serverless --profile quantlab-dev
```

Fallback validate/deploy/run on the classic workspace:

```bash
databricks clusters start 0426-145152-cv6y6ado --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic
databricks bundle deploy -t dev_classic --profile quantlab-classic --var cluster_id=0426-145152-cv6y6ado
databricks bundle run phase1_btc_trade_move_candidates_classic -t dev_classic --profile quantlab-classic --var cluster_id=0426-145152-cv6y6ado
```

Verified on 2026-04-27 against `dev_serverless`:

- Run result: `TERMINATED SUCCESS`.
- Selected date: `20260423`.
- Selected exchanges: `binance`, `bybit`, `okx`.
- Total selected trade rows: `5,578,439`.
- 1-second price rows: `216,633`.
- Raw candidates: `60`.
- Candidate direction: `DOWN`.
- Ambiguous candidates: `0`.
- Approx grouped candidates: `1`.
- No S3 output was written.

## Phase 1D - Trial event map write

Phase 1D writes the first trial event outputs to S3. It reuses the Phase 1C
trade candidate scan, writes non-ambiguous raw candidates to
`s3://quantlab-research/raw_candidates/_trial/run_id=<run_id>/`, writes grouped
trial events to `s3://quantlab-research/events_map/_trial/run_id=<run_id>/`,
then reads both paths back and verifies row counts, schemas, and samples.

This is still not the production `events_map`; it is a trial write/readback
check. Phase 1D uses classic cluster compute as the primary run path.

Validate/deploy/run on the classic workspace:

```bash
databricks clusters start 0426-145152-cv6y6ado --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic
databricks bundle deploy -t dev_classic --profile quantlab-classic --var cluster_id=0426-145152-cv6y6ado
databricks bundle run phase1_trial_event_map_classic -t dev_classic --profile quantlab-classic --var cluster_id=0426-145152-cv6y6ado
```

Verified on 2026-04-27 against `dev_classic`:

- Run result: `TERMINATED SUCCESS`.
- Run ID: `phase1d_20260427T063442Z`.
- Raw candidates output:
  `s3://quantlab-research/raw_candidates/_trial/run_id=phase1d_20260427T063442Z`.
- Trial event map output:
  `s3://quantlab-research/events_map/_trial/run_id=phase1d_20260427T063442Z`.
- Raw candidates written/read back: `60`.
- Grouped events written/read back: `1`.
- Trial event ID: `binance_btcusdt_20260423_down_001`.
- Selected date: `20260423`; coverage count: `3`.

## Phase 2A - Trial pre-event window extraction

Phase 2A reads one trial event map row, derives the UTC pre-event window, reads
the BTC trade partitions touched by that window, writes row-level trial
pre-event windows to `s3://quantlab-research/pre_event_windows/_trial/`, and
reads the output back for count/schema/sample verification. This phase is
trade-only and runs on classic cluster compute.

Validate/deploy/run on the classic workspace:

```bash
databricks clusters start 0426-145152-cv6y6ado --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic
databricks bundle deploy -t dev_classic --profile quantlab-classic --var cluster_id=0426-145152-cv6y6ado
databricks bundle run phase2a_pre_event_windows_trial_classic -t dev_classic --profile quantlab-classic --var cluster_id=0426-145152-cv6y6ado
```

Verified on 2026-04-27 against `dev_classic`:

- Run result: `TERMINATED SUCCESS`.
- Source event run ID: `phase1d_20260427T063442Z`.
- Source event ID: `binance_btcusdt_20260423_down_001`.
- Output path:
  `s3://quantlab-research/pre_event_windows/_trial/run_id=phase2a_20260427T071919Z`.
- Spark session timezone: `UTC`.
- Window: `2026-04-23 09:59:10 <= ts_event_ts < 2026-04-23 10:04:10`.
- Derived partition dates: `20260423`.
- Extracted rows written/read back: `15,072`.
- Counts by exchange: `binance=3,787`, `bybit=5,518`, `okx=5,767`.
- Output includes `ts_recv_ts`, `seconds_before_event`, and `source_partition_date`.

## Phase 2B - Trial BBO pre-event window extraction

Phase 2B reads the same trial event map row and extracts the matching BTC `bbo`
pre-event window for `binance`, `bybit`, and `okx`. It writes row-level trial
BBO windows to `s3://quantlab-research/pre_event_bbo_windows/_trial/`, includes
basic BBO profile columns, and reads the output back for count/schema/sample
verification.

Validate/deploy/run on the classic workspace:

```bash
databricks clusters start 0426-145152-cv6y6ado --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic
databricks bundle deploy -t dev_classic --profile quantlab-classic --var cluster_id=0426-145152-cv6y6ado
databricks bundle run phase2b_pre_event_bbo_windows_trial_classic -t dev_classic --profile quantlab-classic --var cluster_id=0426-145152-cv6y6ado
```

Verified on 2026-04-27 against `dev_classic`:

- Run result: `TERMINATED SUCCESS`.
- Source event run ID: `phase1d_20260427T063442Z`.
- Source event ID: `binance_btcusdt_20260423_down_001`.
- Output path:
  `s3://quantlab-research/pre_event_bbo_windows/_trial/run_id=phase2b_20260427T074205Z`.
- Spark session timezone: `UTC`.
- Window: `2026-04-23 09:59:10 <= ts_event_ts < 2026-04-23 10:04:10`.
- Derived partition dates: `20260423`.
- Selected BBO exchange coverage: `binance`, `bybit`, `okx`.
- Extracted rows written/read back: `103,067`.
- Counts by exchange: `binance=99,822`, `bybit=922`, `okx=2,323`.
- Null summary for timestamp, BBO, and derived BBO fields: all `0`.
- `spread < 0` rows: `0`.
- Spread range: `0.10` to `13.30`; average spread: `0.1169`.

## Classic Cluster Notes

The current classic cluster is `0426-145152-cv6y6ado`. On 2026-04-27,
`autotermination_minutes` was set to `0` to avoid repeated 8-10 minute startup
delays during active development. Re-enable autotermination or terminate the
cluster manually when the active development block ends. The cluster was
terminated after the successful Phase 2B trial run.

## Local Development

Install the package with development dependencies:

```bash
python -m pip install -e ".[dev]"
```

Run tests:

```bash
pytest
```
