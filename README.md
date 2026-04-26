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

## Local Development

Install the package with development dependencies:

```bash
python -m pip install -e ".[dev]"
```

Run tests:

```bash
pytest
```
