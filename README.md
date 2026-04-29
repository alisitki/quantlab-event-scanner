# quantlab-event-scanner

QuantLab Event Scanner is a Databricks/S3 research pipeline for crypto market
event discovery and descriptive event-vs-normal analysis. The accepted current
milestone is **Phase 3A - BTC 10-event multi-event trial/finalize**.

Accepted Phase 3A run:

- Run ID: `phase3a_20260429T085638Z`.
- Output root:
  `s3://quantlab-research/btc_multi_event_trials/_trial/run_id=phase3a_20260429T085638Z`.
- Selected events: `10`.
- Comparison rows: `exchange_profile_comparison=6600`,
  `cross_exchange_mid_diff_comparison=1500`,
  `bucket_change_profile_comparison=960`.
- Top-diff rows: `exchange_profile_top_diffs=1800`,
  `cross_exchange_mid_diff_top_diffs=1200`,
  `bucket_change_profile_top_diffs=1560`.
- Summary rows: `event_counts_by_direction=2`, `metric_group_summary=80`,
  `normal_selection_quality=10`, `event_processing_status=10`.

This repository is not a production trading system, not an ML system, and not a
live execution system. It does not place orders, integrate Binance Futures order
APIs, run user-data-stream reconciliation, or train/infer ML models. Accepted
outputs are research/trial artifacts, not production datasets or validated
trading signals.

Immediate next step: **Phase 3B - BTC 10-event multi-event content review**.
Phase 3B reads the accepted Phase 3A outputs and produces descriptive review
artifacts. It must not mutate the accepted Phase 3A root and must not claim
statistical significance or production readiness.

Phase 4A decision-time label dataset is the correct future transition toward
decision-time research, but it comes after Phase 3B/3C decisions and is not part
of the current implementation.

Input data lives on S3 under `s3://quantlab-compact-stk-euc1`; compacted
metadata is read from
`s3://quantlab-compact-stk-euc1/compacted/_manifest.json`. Output data is
written to S3 under `s3://quantlab-research`. The local PC must not store data
outputs.

Current project memory and direction:

- `docs/project_state/quantlab_project_state_2026-04-29.md`
- `docs/decision_records/2026-04-29_trading_research_direction.md`
- `docs/phase3b_btc_multi_event_content_review_plan.md`
- `docs/data_universe_2026-04-29.md`
- `docs/roadmap.md`
- `docs/runbooks/phase3a_accepted_runbook.md`
- `docs/runbooks/phase3b_review_runbook.md`

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

Current roadmap:

1. Phase 3B: BTC 10-event multi-event content review.
2. Phase 3C: metric policy and event inventory scope decision.
3. Phase 3D: multi-symbol event inventory / coverage preview.
4. Phase 4A: decision-time label dataset.
5. Phase 4B: decision-time feature dataset.
6. Phase 4C: rule baseline + ML benchmark.
7. Phase 5A: execution simulator / executable backtest.
8. Phase 5B: shadow live signal generation.
9. Phase 6: controlled small-capital production.

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
databricks clusters start <classic_cluster_id> --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic
databricks bundle deploy -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle run phase1_manifest_trade_btc_probe_classic -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
```

Classic runs require the operator to pass `--var cluster_id=<classic_cluster_id>`.
Do not terminate or delete the active development cluster unless explicitly
instructed.

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
databricks clusters start <classic_cluster_id> --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic
databricks bundle deploy -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle run phase1_btc_trade_profile -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
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
databricks clusters start <classic_cluster_id> --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic
databricks bundle deploy -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle run phase1_btc_trade_move_candidates_classic -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
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
databricks clusters start <classic_cluster_id> --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic
databricks bundle deploy -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle run phase1_trial_event_map_classic -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
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
databricks clusters start <classic_cluster_id> --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic
databricks bundle deploy -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle run phase2a_pre_event_windows_trial_classic -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
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
databricks clusters start <classic_cluster_id> --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic
databricks bundle deploy -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle run phase2b_pre_event_bbo_windows_trial_classic -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
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

## Phase 2C - Trial 1-second market snapshots

Phase 2C reads the verified Phase 2A trade window and Phase 2B BBO window for
the same trial event, validates that their event/window metadata match, and
builds a dense 1-second market snapshot panel by `event_id`, `exchange`,
`symbol`, `second_before_event`, and `ts_second`. It writes trial snapshots to
`s3://quantlab-research/pre_event_market_snapshots/_trial/` and reads the output
back for count/schema/sample verification.

This phase is still a profiling/data-shaping step. It does not run ML,
normal-time comparison, lead-lag interpretation, event detection, or production
dataset generation.

Validate/deploy/run on the classic workspace:

```bash
databricks clusters start <classic_cluster_id> --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle deploy -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle run phase2c_pre_event_market_snapshots_trial_classic -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
```

Phase 2C logs input trade/BBO row counts, trade/BBO metadata compatibility,
expected vs observed exchange coverage, output snapshot counts, null summaries,
BBO update/forward-fill/pre-first-update counts, negative spread count, and
readback verification.

BBO `last_*` fields are forward-filled only from quotes observed inside the
window. BBO update-based stats such as `avg_spread_bps`, `max_spread_bps`, and
`avg_book_imbalance` are not forward-filled; they remain null for seconds with
no real BBO update. Pre-first-update seconds have `has_bbo_update=false`,
`is_bbo_forward_filled=false`, and null carried quote fields.

Verified on 2026-04-27 against `dev_classic`:

- Run result: `TERMINATED SUCCESS`.
- Source snapshot run ID: `phase2c_20260427T082248Z`.
- Output path:
  `s3://quantlab-research/pre_event_market_snapshots/_trial/run_id=phase2c_20260427T082248Z`.
- Input trade rows: `15,072`; input BBO rows: `103,067`.
- Output snapshot rows written/read back: `900`.
- Counts by exchange: `binance=300`, `bybit=300`, `okx=300`.
- `second_before_event` range: `1` to `300`.
- BBO update seconds: `871`; forward-filled BBO seconds: `29`.
- Pre-first-update seconds: `0`; negative spread rows: `0`.

## Phase 2D - Trial pre-event profile report

Phase 2D reads the accepted Phase 2C 1-second market snapshot panel and writes
structured trial profile reports to
`s3://quantlab-research/pre_event_profile_reports/_trial/`. It produces three
parquet subtables:

- `exchange_profile/`
- `cross_exchange_mid_diff/`
- `bucket_change_profile/`

This phase is descriptive only. It does not run ML, normal-time comparison,
lead-lag interpretation, event detection, signal labeling, or production
dataset generation.

Validate/deploy/run on the classic workspace:

```bash
databricks clusters start <classic_cluster_id> --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle deploy -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle run phase2d_pre_event_profile_report_trial_classic -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
```

Phase 2D uses both trailing cumulative windows (`last_300s`, `last_120s`,
`last_60s`, `last_30s`, `last_10s`) and non-overlap buckets
(`bucket_300_120s`, `bucket_120_60s`, `bucket_60_30s`, `bucket_30_10s`,
`bucket_10_0s`). Within each window, `first_*` is the oldest row
(`max(second_before_event)`) and `last_*` is the closest-to-event row
(`min(second_before_event)`).

Cross-exchange mid differences use `symbol_key = lower(symbol)`, compare only
seconds where both sides have non-null `last_mid_price`, and calculate
`mid_diff_bps = (mid_exchange_a - mid_exchange_b) / mid_exchange_b * 10000`.
Bucket changes compare adjacent buckets in event-approach order and leave
relative change null when the source bucket value is null or zero.

Verified on 2026-04-27 against `dev_classic`:

- Run result: `TERMINATED SUCCESS`.
- Source snapshot run ID: `phase2c_20260427T082248Z`.
- Output path:
  `s3://quantlab-research/pre_event_profile_reports/_trial/run_id=phase2d_20260427T090707Z`.
- Input snapshot rows: `900`.
- Snapshot exchange coverage: `binance`, `bybit`, `okx`.
- Snapshot count by exchange: `binance=300`, `bybit=300`, `okx=300`.
- `second_before_event` range: `1` to `300`.
- `exchange_profile` rows written/read back: `30`.
- `cross_exchange_mid_diff` rows written/read back: `30`.
- `bucket_change_profile` rows written/read back: `48`.

## Phase 2E - Normal-time comparison trial

Phase 2E auto-selects one healthy BTC normal-time 5-minute window on `20260423`
and runs the normal-time equivalent of the Phase 2A-2D pipeline in one classic
Databricks job. It writes structured trial artifacts to:

- `s3://quantlab-research/normal_trade_windows/_trial/`
- `s3://quantlab-research/normal_bbo_windows/_trial/`
- `s3://quantlab-research/normal_market_snapshots/_trial/`
- `s3://quantlab-research/normal_profile_reports/_trial/`

This phase creates one comparable normal/negative example. It does not claim a
signal, train ML, perform final event-vs-normal comparison, or generalize beyond
one normal window.

Validate/deploy/run on the classic workspace:

```bash
databricks clusters start <classic_cluster_id> --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle deploy -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle run phase2e_normal_time_trial_classic -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
```

Phase 2E selects normal anchors from the common BTC 1-second trade range after
running the same `%1 / 60s` candidate scan used by Phase 1C/1D. It excludes
anchors near all candidate starts on the selected date, not only the known trial
event. For each candidate anchor, `normal_window_end_ts = normal_anchor_ts` and
`normal_window_start_ts = normal_anchor_ts - 300s`; the snapshot uses
`second_before_anchor` with expected range `300..1`.

The job tries eligible normal windows in timestamp order. If an attempt fails
minimum quality checks, it logs the rejection and tries the next candidate. It
accepts only a window with trade/BBO rows for all expected exchanges and a dense
normal snapshot of `3 exchanges x 300 seconds = 900` rows.

Verified on 2026-04-27 against `dev_classic`:

- Run result: `TERMINATED SUCCESS`.
- Run ID: `phase2e_20260427T093700Z`.
- Normal trade output:
  `s3://quantlab-research/normal_trade_windows/_trial/run_id=phase2e_20260427T093700Z`.
- Normal BBO output:
  `s3://quantlab-research/normal_bbo_windows/_trial/run_id=phase2e_20260427T093700Z`.
- Normal snapshot output:
  `s3://quantlab-research/normal_market_snapshots/_trial/run_id=phase2e_20260427T093700Z`.
- Normal profile output:
  `s3://quantlab-research/normal_profile_reports/_trial/run_id=phase2e_20260427T093700Z`.
- Selected window:
  `2026-04-23 00:00:00 <= ts_event_ts < 2026-04-23 00:05:00`.
- Normal anchor: `2026-04-23 00:05:00`.
- Candidate scan: `216,633` 1-second rows, `60` raw candidates,
  `0` ambiguous candidates.
- Eligible anchors: `924`; excluded anchors: `61`; accepted attempt: `1`.
- Nearest candidate distance: `35,950` seconds.
- Normal trade rows written/read back: `28,017`
  (`binance=8,726`, `bybit=10,052`, `okx=9,239`).
- Normal BBO rows written/read back: `162,689`
  (`binance=158,871`, `bybit=1,411`, `okx=2,407`).
- Normal snapshot rows written/read back: `900`
  (`binance=300`, `bybit=300`, `okx=300`).
- `second_before_anchor` range: `1` to `300`.
- `exchange_profile` rows written/read back: `30`.
- `cross_exchange_mid_diff` rows written/read back: `30`.
- `bucket_change_profile` rows written/read back: `48`.

## Phase 2F - Event-vs-normal profile comparison trial

Phase 2F reads the accepted pre-event profile report and accepted normal-time
profile report, then writes curated metric comparison reports to
`s3://quantlab-research/profile_comparison_reports/_trial/`.

Inputs:

- Event profile:
  `s3://quantlab-research/pre_event_profile_reports/_trial/run_id=phase2d_20260427T090707Z`.
- Normal profile:
  `s3://quantlab-research/normal_profile_reports/_trial/run_id=phase2e_20260427T093700Z`.

Outputs:

- `exchange_profile_comparison/`
- `cross_exchange_mid_diff_comparison/`
- `bucket_change_profile_comparison/`

This phase is descriptive only. It does not claim a signal, run ML, compute
statistical significance, produce a production dataset, or make lead-lag
conclusions. `signed_diff = event_value - normal_value` is the primary
comparison field for signed metrics. `absolute_diff = abs(signed_diff)` is the
true unsigned magnitude. `ratio = event_value / normal_value` is included only
as a helper and is null when the normal value is null or zero.

Validate/deploy/run on the classic workspace:

```bash
databricks clusters start <classic_cluster_id> --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle deploy -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle run phase2f_profile_comparison_trial_classic -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
```

Phase 2F uses curated metric sets only. It fails fast if any curated metric is
missing from the relevant source table, if source subtable row counts differ
from the expected trial contract, if metadata is inconsistent across subtables,
or if event/normal join keys do not match. The event profile event id is written
as `source_event_id`; normal event fields are kept separate and null.
Cross-exchange pair names are normalized to deterministic double-underscore
values: `binance__bybit`, `binance__okx`, and `bybit__okx`.

Expected output row counts:

- `exchange_profile_comparison`: `660`.
- `cross_exchange_mid_diff_comparison`: `150`.
- `bucket_change_profile_comparison`: `96`.

Verified on 2026-04-27 against `dev_classic`:

- Run result: `TERMINATED SUCCESS`.
- Accepted run ID: `phase2f_20260427T113721Z`.
- Output path:
  `s3://quantlab-research/profile_comparison_reports/_trial/run_id=phase2f_20260427T113721Z`.
- Event profile run ID: `phase2d_20260427T090707Z`.
- Normal profile run ID: `phase2e_20260427T093700Z`.
- Source profile rows:
  event `exchange_profile=30`, `cross_exchange_mid_diff=30`,
  `bucket_change_profile=48`; normal `exchange_profile=30`,
  `cross_exchange_mid_diff=30`, `bucket_change_profile=48`.
- Profile contract rows: `event=1`, `normal=1`.
- Unmatched join keys: `0` for all three comparison groups.
- Exchange pair coverage:
  `binance__bybit`, `binance__okx`, `bybit__okx`.
- Output schema includes both `signed_diff` and true absolute `absolute_diff`.
- Phase 2I taxonomy columns are present with `metric_group` null count `0`.
- `absolute_diff` validation passed with `negative_count=0` and
  `mismatch_count=0`.
- `exchange_profile_comparison` rows written/read back: `660`.
- `cross_exchange_mid_diff_comparison` rows written/read back: `150`.
- `bucket_change_profile_comparison` rows written/read back: `96`.

## Phase 2G - Profile comparison top-diff inspection trial

Phase 2G reads the accepted Phase 2F comparison report and converts it into
small structured top-diff inspection tables. It logs the ranked rows for quick
human review and writes parquet outputs to
`s3://quantlab-research/profile_comparison_top_diffs/_trial/`.

Input:

- Comparison report:
  `s3://quantlab-research/profile_comparison_reports/_trial/run_id=phase2f_20260427T113721Z`.

Outputs:

- `exchange_profile_top_diffs/`
- `cross_exchange_mid_diff_top_diffs/`
- `bucket_change_profile_top_diffs/`

This phase is inspection only. It does not claim a signal, run ML, compute
statistical significance, produce a production dataset, or make lead-lag
conclusions.

Validate/deploy/run on the classic workspace:

```bash
databricks clusters start <classic_cluster_id> --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle deploy -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle run phase2g_profile_comparison_top_diffs_trial_classic -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
```

Phase 2G emits three ranking views per report group, each with top `20` rows:

- `top_absolute_diff`: `absolute_diff DESC NULLS LAST`.
- `top_signed_positive`: `signed_diff DESC NULLS LAST`.
- `top_signed_negative`: `signed_diff ASC NULLS LAST`.

Ranking uses deterministic tie-breakers after the primary sort:
`report_group`, `metric_name`, `exchange`, `symbol_key`, `exchange_pair`,
`window_mode`, `window_label`, `from_bucket`, and `to_bucket`. Rows are not
deduplicated across rank types.

Phase 2G fails fast if the source comparison run id is not exactly the
configured Phase 2F source run, if source row counts differ from the Phase 2F
contract, if required columns are missing, or if `absolute_diff` is negative or
does not match `abs(signed_diff)`. It also logs signed-diff distribution counts
for positive, negative, zero, and null values per report group.

Before Phase 2I, expected output row counts were fixed at:

- `exchange_profile_top_diffs`: `60`.
- `cross_exchange_mid_diff_top_diffs`: `60`.
- `bucket_change_profile_top_diffs`: `60`.

After Phase 2I, Phase 2G ranks inside `report_group + metric_group`, so output
row counts are dynamic and logged by metric group.

Verified on 2026-04-27 against `dev_classic`:

- Run result: `TERMINATED SUCCESS`.
- Accepted run ID: `phase2g_20260427T113839Z`.
- Source comparison run ID: `phase2f_20260427T113721Z`.
- Output path:
  `s3://quantlab-research/profile_comparison_top_diffs/_trial/run_id=phase2g_20260427T113839Z`.
- Source row counts matched the Phase 2F contract:
  `exchange_profile_comparison=660`, `cross_exchange_mid_diff_comparison=150`,
  `bucket_change_profile_comparison=96`.
- `comparison_run_id` was exactly `phase2f_20260427T113721Z` for all source
  subtables.
- `metric_group` null count: `0` for all source subtables.
- `absolute_diff` validation passed for all source subtables:
  `negative_count=0`, `mismatch_count=0`.
- Dynamic output rows written/read back:
  `exchange_profile_top_diffs=180`, `cross_exchange_mid_diff_top_diffs=120`,
  `bucket_change_profile_top_diffs=156`.

## Phase 2I - Metric curation and class-separated top diffs

Phase 2I refines the Phase 2F/2G comparison layer. It does not add a new
Databricks job. Instead, Phase 2F comparison rows now include metric taxonomy
and denominator-risk flags, and Phase 2G ranks top diffs separately by
`report_group + metric_group`.

Metric groups:

- `context`: coverage, activity, and liquidity-regime metrics.
- `signal_candidate`: imbalance, spread, and book-side metrics.
- `price_dislocation`: return and cross-exchange mid-price dislocation metrics.
- `unstable`: metric-intrinsic unstable metrics, currently `*.relative_change`.

The metric group is the metric's class. It is not changed by row-level
denominator risk. Risk is reported separately with:

- `ratio_unstable`
- `relative_change_unstable`
- `small_denominator_flag`

`SMALL_DENOMINATOR_ABS_THRESHOLD = 1e-9` is a technical near-zero guard, not a
statistical stability threshold. Future phases may introduce metric-specific
thresholds.

Phase 2F comparison outputs also include bucket denominator audit columns:

- `event_from_bucket_value`
- `normal_from_bucket_value`
- `event_to_bucket_value`
- `normal_to_bucket_value`

Phase 2G expected row counts are dynamic after Phase 2I because each metric
group is ranked independently and some groups contain fewer than `top_n` source
rows. Logs include `report_group`, `metric_group`, `source_rows`,
`expected_rows_for_group`, and `actual_rows_for_group`.

Phase 2J design is documented in
`docs/phase2j_multi_normal_window_spec.md`. It specifies `N=10` BTC normal
windows selected by candidate exclusion, 3/3 coverage, 900-row snapshot quality,
minimum anchor spacing, and activity similarity to the event `last_300s`
exchange profile.

Verified on 2026-04-27 against `dev_classic`:

- Phase 2F rerun result: `TERMINATED SUCCESS`.
- Phase 2F run ID: `phase2f_20260427T113721Z`.
- Phase 2F output path:
  `s3://quantlab-research/profile_comparison_reports/_trial/run_id=phase2f_20260427T113721Z`.
- Phase 2F source profile rows remained unchanged:
  event `exchange_profile=30`, `cross_exchange_mid_diff=30`,
  `bucket_change_profile=48`; normal `exchange_profile=30`,
  `cross_exchange_mid_diff=30`, `bucket_change_profile=48`.
- Phase 2F output rows remained unchanged:
  `exchange_profile_comparison=660`, `cross_exchange_mid_diff_comparison=150`,
  `bucket_change_profile_comparison=96`.
- Phase 2F metric-group counts:
  `exchange_profile context=360 price_dislocation=60 signal_candidate=240`;
  `cross_exchange_mid_diff context=30 price_dislocation=120`;
  `bucket_change_profile context=12 signal_candidate=36 unstable=48`.
- Phase 2F denominator risk counts:
  `exchange_profile ratio_unstable=190 relative_change_unstable=0 small_denominator_flag=190`;
  `cross_exchange_mid_diff ratio_unstable=36 relative_change_unstable=0 small_denominator_flag=36`;
  `bucket_change_profile ratio_unstable=24 relative_change_unstable=12 small_denominator_flag=24`.
- Phase 2G rerun result: `TERMINATED SUCCESS`.
- Phase 2G run ID: `phase2g_20260427T113839Z`.
- Phase 2G source comparison run ID: `phase2f_20260427T113721Z`.
- Phase 2G output path:
  `s3://quantlab-research/profile_comparison_top_diffs/_trial/run_id=phase2g_20260427T113839Z`.
- Phase 2G source row counts matched the Phase 2F contract:
  `exchange_profile_comparison=660`, `cross_exchange_mid_diff_comparison=150`,
  `bucket_change_profile_comparison=96`.
- Phase 2G `metric_group` null count: `0` for all source subtables.
- Phase 2G `absolute_diff` validation passed:
  `negative_count=0`, `mismatch_count=0`.
- Phase 2G dynamic output rows written/read back:
  `exchange_profile_top_diffs=180`, `cross_exchange_mid_diff_top_diffs=120`,
  `bucket_change_profile_top_diffs=156`.

## Phase 2J - Multi-normal comparison trial

Phase 2J adds a separate multi-normal trial flow. It keeps the accepted Phase
2F/2G single-normal runs unchanged, selects `N=10` BTC normal windows on
`20260423`, and compares the accepted event profile against the 10-normal
distribution.

Inputs:

- Event profile:
  `s3://quantlab-research/pre_event_profile_reports/_trial/run_id=phase2d_20260427T090707Z`.
- Event ID: `binance_btcusdt_20260423_down_001`.
- Date: `20260423`.
- Exchanges: `binance`, `bybit`, `okx`.

Outputs:

- `s3://quantlab-research/multi_normal_windows/_trial/run_id=<phase2j_run_id>/trade_windows`.
- `s3://quantlab-research/multi_normal_windows/_trial/run_id=<phase2j_run_id>/bbo_windows`.
- `s3://quantlab-research/multi_normal_market_snapshots/_trial/run_id=<phase2j_run_id>`.
- `s3://quantlab-research/multi_normal_profile_reports/_trial/run_id=<phase2j_run_id>`.
- `s3://quantlab-research/multi_normal_comparison_reports/_trial/run_id=<phase2j_run_id>`.
- `s3://quantlab-research/multi_normal_top_diffs/_trial/run_id=<phase2j_run_id>`.

Validate/deploy/run on the classic workspace:

```bash
databricks clusters start <classic_cluster_id> --profile quantlab-classic
databricks bundle validate -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle deploy -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
databricks bundle run phase2j_multi_normal_trial_classic -t dev_classic --profile quantlab-classic --var cluster_id=<classic_cluster_id>
```

Phase 2J excludes anchors against all raw BTC `%1 / 60s` candidate starts on the
date, including ambiguous starts. It logs `price_1s_count`, raw,
non-ambiguous, and ambiguous candidate counts, distinct raw candidate starts,
total enumerated anchors, exclusion counts by reason, quality failure counts by
reason, quality-passing count, spacing-skipped count, and selected normal count.

Final selection order is:

1. Quality pass.
2. `activity_distance ASC`.
3. `nearest_candidate_distance_seconds DESC`.
4. `normal_anchor_ts ASC`.

Activity distance is computed only from event `exchange_profile` rows with
`window_label = last_300s`, across `3 exchanges x 3 activity metrics`:
`trade_count_sum`, `trade_volume_sum`, and `trade_notional_sum`. The score is
`mean(abs(log1p(normal_metric) - log1p(event_metric)))`.

Expected output row counts:

- Multi-normal snapshots: `9000`.
- Multi-normal profiles:
  `exchange_profile=300`, `cross_exchange_mid_diff=300`,
  `bucket_change_profile=480`.
- Distribution comparison:
  `exchange_profile_comparison=660`,
  `cross_exchange_mid_diff_comparison=150`,
  `bucket_change_profile_comparison=96`.

Distribution comparison rows include `normal_mean`, `normal_median`,
`normal_min`, `normal_max`, `normal_p25`, `normal_p75`, `normal_stddev`,
`normal_sample_count`, `normal_non_null_count`,
`normal_zero_or_near_zero_count`, `signed_diff_vs_mean`,
`absolute_diff_vs_mean`, `ratio_vs_mean`, `z_score_vs_normal`, and
`normal_percentile_rank`. Phase 2I `metric_group` and denominator-risk flags are
retained, and top diffs rank inside `report_group + metric_group`.

Verified on 2026-04-27 against `dev_classic`:

- Run result: `TERMINATED SUCCESS`.
- Accepted run ID: `phase2j_20260427T150134Z`.
- Databricks job run ID: `370386900239817`; task run ID:
  `695810238434357`.
- Output paths:
  `s3://quantlab-research/multi_normal_windows/_trial/run_id=phase2j_20260427T150134Z`,
  `s3://quantlab-research/multi_normal_market_snapshots/_trial/run_id=phase2j_20260427T150134Z`,
  `s3://quantlab-research/multi_normal_profile_reports/_trial/run_id=phase2j_20260427T150134Z`,
  `s3://quantlab-research/multi_normal_comparison_reports/_trial/run_id=phase2j_20260427T150134Z`,
  and
  `s3://quantlab-research/multi_normal_top_diffs/_trial/run_id=phase2j_20260427T150134Z`.
- Activity source:
  `window_label=last_300s`, exchanges `binance,bybit,okx`, metrics
  `trade_count_sum,trade_volume_sum,trade_notional_sum`, rows `9`.
- Candidate/anchor audit:
  `price_1s_count=216633`, raw candidates `60`, ambiguous candidates `0`,
  distinct raw candidate starts `60`, total anchors `985`, eligible anchors
  `924`, exclusion counts
  `{'candidate_within_anchor_proximity': 61, 'eligible': 924}`.
- Selection audit:
  quality probes attempted `10`, quality-failed counts `{}`,
  quality-passing candidates `10`, spacing-skipped count `24`, selected normal
  count `10`.
- Multi-normal window readback:
  trade rows `147381`, BBO rows `996060`.
- Snapshot/profile readback:
  snapshots `9000`, `exchange_profile=300`,
  `cross_exchange_mid_diff=300`, `bucket_change_profile=480`.
- Distribution comparison readback:
  `exchange_profile_comparison=660`,
  `cross_exchange_mid_diff_comparison=150`,
  `bucket_change_profile_comparison=96`.
- Comparison validation:
  `normal_sample_count` invalid rows `0`, `metric_group` null count `0`, and
  `absolute_diff_vs_mean` negative count `0`.
- Top-diff readback:
  `exchange_profile_top_diffs=180`,
  `cross_exchange_mid_diff_top_diffs=120`,
  `bucket_change_profile_top_diffs=156`.

## Phase 3A - BTC multi-event trial

Phase 3A is split into two classic jobs to keep the expensive accepted run from
carrying long Spark lineage into final top-diff and summary generation.

Job A, `phase3a_btc_multi_event_trial_classic`, selects BTC `%1 / 60s` grouped
events, selects activity-matched normal windows, writes snapshots, profiles, and
comparison reports, validates those reports, and then stops. It does not build
top diffs or final summary tables.

Job B, `phase3a_btc_multi_event_finalize_classic`, takes
`--source-run-id <phase3a_run_id>`, fresh-reads Job A comparison reports from
`s3://quantlab-research/btc_multi_event_trials/_trial/run_id=<phase3a_run_id>/`,
then writes `top_diffs/` and `summary/` under the same umbrella root.

Accepted retry commands:

```bash
databricks bundle run phase3a_btc_multi_event_trial_classic \
  -t dev_classic \
  --profile quantlab-classic \
  --var cluster_id=<classic_cluster_id> \
  --python-params='--config,configs/dev.yaml,--start-date,20260203,--end-date,20260426,--max-events,10,--min-events,2,--normal-count-per-event,10,--lookback-seconds,300,--horizon-seconds,60,--move-threshold-pct,1.0,--exclusion-seconds,1800,--min-normal-anchor-spacing-seconds,1800,--validation-mode,light,--write-raw-windows,false,--allow-partial-coverage,false,--sample-size,0'

databricks bundle run phase3a_btc_multi_event_finalize_classic \
  -t dev_classic \
  --profile quantlab-classic \
  --var cluster_id=<classic_cluster_id> \
  --python-params='--config,configs/dev.yaml,--source-run-id,<phase3a_run_id>,--normal-count-per-event,10,--top-n,20,--sample-size,0,--validation-mode,light,--small-output-partitions,1'
```

Accepted retry compute should use a large driver, not `m5d.large`: prefer
`r6i.8xlarge` driver with `32 x r6i.2xlarge` workers, Photon enabled, autoscale
off. `r6i.4xlarge` driver is acceptable if `r6i.8xlarge` is unavailable.

Verified accepted retry on 2026-04-29:

- Source run ID: `phase3a_20260429T085638Z`.
- Output root:
  `s3://quantlab-research/btc_multi_event_trials/_trial/run_id=phase3a_20260429T085638Z`.
- Job A run `358829999567665` completed with `selected_event_count=10` and
  `stage=comparison_reports_complete`.
- Job B run `159108292725392` completed with `stage=finalize_complete`.
- Comparison rows validated as exchange `6600`, cross-exchange `1500`, and
  bucket-change `960`.
- Final top diffs wrote exchange `1800`, cross-exchange `1200`, and
  bucket-change `1560` rows.
- Summary outputs wrote `event_counts_by_direction=2`,
  `metric_group_summary=80`, `normal_selection_quality=10`, and
  `event_processing_status=10`.
- Validation checks passed with `normal_sample_count` invalid rows `0`,
  `metric_group` null rows `0`, `absolute_diff_vs_mean` negative rows `0`, and
  top-diff dynamic mismatch count `0`.
- Working compute was a hybrid quota-safe profile: `r6i.4xlarge` on-demand
  driver, `28 x r6i.2xlarge` spot workers, `availability=SPOT`,
  `first_on_demand=1`, Photon enabled.

## Classic Cluster Notes

Classic cluster IDs are supplied at runtime with
`--var cluster_id=<classic_cluster_id>`. Do not hardcode a concrete cluster ID in
bundle job definitions or phase docs. Do not terminate or delete the active
development cluster unless explicitly instructed.

## Local Development

Install the package with development dependencies:

```bash
python -m pip install -e ".[dev]"
```

Run tests:

```bash
pytest
```
