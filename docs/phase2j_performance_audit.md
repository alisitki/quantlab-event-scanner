# Phase 2J Performance Audit

## Run

- job run: `370386900239817`
- task run: `695810238434357`
- run_id: `phase2j_20260427T150134Z`
- cluster: `0426-145152-cv6y6ado`
- final state: `SUCCESS`
- API start: `2026-04-27T15:01:06Z`
- API end: `2026-04-27T17:40:47Z`
- API total duration: `9,581s`, about `2h 39m`

## Duration Reconciliation

The Databricks Jobs API reports a run duration of about `2h 39m`, from
`2026-04-27T15:01:06Z` to `2026-04-27T17:40:47Z`.

The user-observed duration was about `5h`. This audit can reconcile the
Databricks API/task execution interval, but it cannot prove where the remaining
wall-clock gap came from without external operator timing, browser/UI timing, or
other scheduler evidence outside the available run metadata.

The conclusions below therefore treat the `2h 39m` API run as the measured
duration and the `~5h` observation as unresolved wall-clock context.

## Cluster

- node type: `m5d.large`
- worker count: `1`
- autoscale settings: none; fixed `num_workers=1`
- executor count: unavailable from retained metadata
- cores: `2` cores per `m5d.large` node
- memory: `8192 MB` per `m5d.large` node
- local disk: `75 GB`, one local disk per node
- driver node type: `m5d.large`
- runtime: `15.4.x-scala2.12`
- effective runtime: `15.4.x-photon-scala2.12`
- Photon: enabled
- `spark.sql.shuffle.partitions`: unavailable; not logged
- `spark.sql.adaptive.enabled`: unavailable; not logged
- `spark.sql.adaptive.skewJoin.enabled`: unavailable; not logged
- `spark.default.parallelism`: unavailable; not logged

This was effectively a very small one-worker cluster. That is a strong
contributing factor because the job had at most one worker machine available for
Spark work, and the node type provides only two cores. It is not, by itself, the
sole proven root cause because the code structure also creates serial Spark
actions, repeated materialization, and long lineage.

Cluster event metadata during the run also contains repeated
`DRIVER_NOT_RESPONDING` / `DRIVER_HEALTHY` transitions between about
`2026-04-27T15:40Z` and `2026-04-27T17:37Z`. The exact impact of those events is
not provable from the retained metadata.

## Evidence-Backed Findings

- The run used one fixed `m5d.large` worker and no autoscale. This is a strong
  scale constraint for a Spark pipeline with repeated windows, joins, windows,
  and validation actions.
- Raw trade and BBO data are read once per stream/date from three parquet paths
  each, then cached as `trade_day` and `bbo_day` in
  `jobs/12_build_multi_normal_trial.py`. The code does not appear to re-read raw
  trade/BBO parquet once per normal sample.
- The quality probe loop is serial. Each probed candidate builds one trade
  window, one BBO window, and one snapshot, then triggers Spark actions for
  counts and exchange-coverage checks.
- The selected-output build is also per selected normal window. It builds
  trade, BBO, snapshot, and profile DataFrames per selected candidate, caches
  them, and unions the per-window frames afterward. This creates long lineage
  before the later validations and writes.
- `_write_and_validate` performs a pre-write `count()`, parquet write, readback
  `count()`, schema print, and sample `take()` for every output table.
- The accepted run produced small final profile/comparison row counts, but the
  code repeatedly recomputes upstream lineage to validate and write those small
  outputs.
- Log timing shows the comparison/report side dominates the measured run. Profile
  readbacks finished around `15:47:11`, while the first comparison output count
  was not logged until `16:50:59`.

## Evidence-Backed Hypotheses

- The main runtime was probably caused by a combination of very small compute,
  serial per-window Spark work, repeated validation actions, and recomputation
  through long cached/unioned lineage.
- The long comparison interval likely includes lineage recomputation from
  multi-normal profile DataFrames plus comparison validation overhead. Exact
  stage-level proof is unavailable without Spark UI/task metrics.
- Tuning Spark shuffle partitions or AQE could help later, but it should not be
  treated as the first fix until instrumentation confirms partition counts,
  shuffle volume, and executor use.
- The repeated driver health transitions may have added overhead or instability,
  but the audit cannot prove that they caused the long runtime.

## Unavailable Evidence

- Spark UI stage/task metrics were not reachable through the read-only API after
  the cluster was terminated.
- Therefore, exact longest Spark jobs, longest stages, task counts, input sizes,
  shuffle read/write, memory spill, disk spill, skew, max task duration, median
  task duration, and executor utilization are not available.
- The top-stage table below intentionally does not invent stage IDs or metrics.
- Conclusions based on logs and code are labeled as evidence-backed findings or
  evidence-backed hypotheses, not confirmed Spark UI facts.

## Top Bottleneck Stages

Exact Spark UI stage metrics are unavailable.

| stage_id | description | duration | tasks | input | shuffle_read | shuffle_write | spill | max_task | median_task | diagnosis |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| unavailable | Spark UI stage/task data unavailable after cluster termination | unavailable | unavailable | unavailable | unavailable | unavailable | unavailable | unavailable | unavailable | Do not infer stage-level bottlenecks without Spark UI or event-log metrics. |

## Log-Timed Bottleneck Steps

These are run-log timing observations, not Spark UI stage durations.

| step | observed interval | duration | diagnosis |
|---|---:|---:|---|
| Event profile, manifest, first price build | `15:01:34` to `15:02:00` | about `26s` | Setup and initial trade-day materialization were not the main visible bottleneck. |
| Candidate scan | `15:02:00` to `15:02:11` | about `10s` | Candidate scan was short relative to total runtime. |
| Anchor eligibility and activity ranking | `15:02:13` to `15:02:32` | about `19s` | Activity ranking over `924` eligible anchors was not the dominant logged interval. |
| First quality probe | `15:02:32` to `15:03:38` | about `66s` | Per-candidate quality probe is serial and action-heavy. |
| Second quality probe | `15:03:38` to `15:04:37` | about `59s` | Similar serial quality-probe cost. |
| Third probe start to profile validation log | `15:04:37` to `15:32:00` | about `27m 23s` | Missing finer logs; likely includes remaining quality probes and selected-output/profile construction. |
| Profile output write/readbacks | `15:35:31` to `15:47:11` | about `11m 40s` | Multiple write/readback/sample actions over long lineage. |
| Comparison/report interval | `15:47:11` to `17:36:49` | about `1h 49m` | Longest log-backed interval; likely comparison lineage and validation overhead. |
| Top-diff writes/readbacks | `17:36:55` to `17:40:34` | about `3m 39s` | Not the main bottleneck. |

## Executor Utilization

- Were all executors used? unavailable; executor-level Spark UI metrics are not
  retained through the read-only API after cluster termination.
- Any executor idle? unavailable.
- Any skew? unavailable.
- Any GC/spill issue? unavailable.
- What is known: the cluster had one worker, so executor parallelism was
  physically constrained even if Spark scheduled tasks correctly.

## Code Bottlenecks

- repeated reads: no for raw trade/BBO parquet; yes for persisted comparison
  readbacks before top-diff generation
- Python serial loop: yes; quality probing and selected-output construction are
  serial over candidates/windows
- too many actions: yes; many `count()`, grouped `take()`, readback counts, and
  write validations
- low parallelism: yes as a cluster fact; one `m5d.large` worker is a strong
  contributing factor
- shuffle bottleneck: unknown; Spark UI metrics unavailable
- long lineage: yes as a code-backed hypothesis; selected outputs are built from
  cached per-window frames and unioned before repeated validations/writes

Important code paths:

- Raw trade/BBO day reads and cache:
  `jobs/12_build_multi_normal_trial.py`, `_read_partitions(...)` and
  `trade_day = ...cache()`, `bbo_day = ...cache()`.
- Serial quality probes:
  `jobs/12_build_multi_normal_trial.py`, `_select_quality_passed_candidates(...)`.
- Per-selected-window output build:
  `jobs/12_build_multi_normal_trial.py`, `_build_selected_outputs(...)`.
- Snapshot/profile helpers:
  `jobs/09_build_normal_time_trial.py`, `_build_normal_snapshot(...)`,
  `_build_exchange_profile(...)`, `_build_cross_exchange_mid_diff(...)`.
- Write/readback validation:
  `jobs/09_build_normal_time_trial.py`, `_write_and_validate(...)`.

## Action Count Summary

These counts are code-inspection estimates, not measured Spark action counts.
The exact number of Spark jobs/stages triggered by each action is unavailable.

| step | count_actions | collect_actions | writes | readbacks | comments |
|---|---:|---:|---:|---:|---|
| manifest / event profile setup | about `4` | `0` to `1` | `0` | `0` | Three event-profile subtable counts plus event activity count. Manifest load may use `dbutils.fs.head`, not necessarily a Spark action. |
| candidate scan | about `3` | about `2` | `0` | `0` | `price_1s.count()`, candidate/ambiguous counts, raw candidate starts collect, anchor range collect. |
| activity distance | `0` | about `1` | `0` | `0` | Ranking all eligible anchors ends with `ranked.collect()`. |
| normal selection / quality probes | about `60` for `10` passing probes | about `30` for exchange coverage | `0` | `0` | Estimate assumes each passing probe triggers trade count, BBO count, three exchange coverage checks, and snapshot count. |
| trade extraction | included above and in write validations | `0` | `1` | `1` | Final trade window output goes through `_write_and_validate`. |
| BBO extraction | included above and in write validations | `0` | `1` | `1` | Final BBO window output goes through `_write_and_validate`. |
| snapshot build | about `1` validation count plus write validation | `0` | `1` | `1` | Snapshot row-count validation and `_write_and_validate`. |
| profile build | about `3` validation counts plus write validations | `0` | `3` | `3` | Exchange, cross-exchange, and bucket profile outputs. |
| comparison build | about `12` validation counts plus write validations | about `3` grouped samples | `3` | `3` | Comparison row counts, normal sample checks, metric group checks, absolute-diff checks, then writes. |
| top-diff build | about `0` direct count validations before writes | about `6` grouped samples | `3` | `3` | Source/output group samples plus `_write_and_validate`. |
| readback validations | about `12` pre-write counts and `12` readback counts | about `12` sample takes | `12` | `12` | One write/readback sequence for six window/profile outputs, three comparison outputs, and three top-diff outputs. |

## Repeated Read Audit

Raw trade/BBO data appears to be read once per stream/date, not once per normal
sample.

Evidence:

- Trade selected parquet path count: `3`
- BBO selected parquet path count: `3`
- Code reads selected trade partitions into `trade_day` once and selected BBO
  partitions into `bbo_day` once.
- Per-window extraction uses filters over the cached day-level DataFrames.

This is not the critical repeated raw-read bottleneck described in the audit
prompt. The larger issue is repeated Spark actions and repeated derivations from
the day-level cached DataFrames inside serial loops and downstream validations.

## Partition Audit

No code or retained Spark UI metrics record `getNumPartitions()` for the
important DataFrames. Partition counts below are therefore unavailable.

| DataFrame / output | known rows or paths | partition count |
|---|---:|---|
| `trade_raw` / `trade_day` | `3` selected parquet paths | unavailable |
| `bbo_raw` / `bbo_day` | `3` selected parquet paths | unavailable |
| `price_1s` | `216633` rows | unavailable |
| `candidate_df` | `60` non-ambiguous candidates, `0` ambiguous | unavailable |
| `eligible_anchors` | `924` eligible anchors | not a Spark DataFrame after collection |
| `selected_normals` | `10` selected normal windows | not a Spark DataFrame before output build |
| `normal_trade_windows` | `147381` readback rows | unavailable |
| `normal_bbo_windows` | `996060` readback rows | unavailable |
| `multi_normal_market_snapshots` | `9000` readback rows | unavailable |
| `multi_normal_profile_reports.exchange_profile` | `300` readback rows | unavailable |
| `multi_normal_profile_reports.cross_exchange_mid_diff` | `300` readback rows | unavailable |
| `multi_normal_profile_reports.bucket_change_profile` | `480` readback rows | unavailable |
| `multi_normal_comparison_reports.exchange_profile_comparison` | `660` readback rows | unavailable |
| `multi_normal_comparison_reports.cross_exchange_mid_diff_comparison` | `150` readback rows | unavailable |
| `multi_normal_comparison_reports.bucket_change_profile_comparison` | `96` readback rows | unavailable |
| `top_diffs` | `180`, `120`, and `156` rows by report group | unavailable |

## Root Cause Hypothesis

Ranked likely causes:

1. Small compute: one `m5d.large` worker with two cores is a strong contributing
   factor and limits all Spark parallelism.
2. Code structure: serial quality probing and per-selected-window output/profile
   construction trigger many Spark actions and prevent batched processing.
3. Repeated validation and readback actions over long lineage: comparison-side
   validation and writes dominate the log-timed runtime.

The measured evidence does not prove that cluster size alone caused the runtime.
It points to compute size plus code/action/lineage structure.

## Recommended Fixes

P0:

- Add instrumentation before the next rerun: per-step timers, Spark conf logging,
  executor/worker summary, and partition counts for key DataFrames.
- Batch quality probes instead of serial per-candidate Spark actions.
- Build selected normal trade/BBO windows in one batched interval/filter plan
  keyed by `normal_sample_id`.

P1:

- Reduce count/readback/sample actions in `_write_and_validate`.
- Add deliberate materialization, checkpoint, or write/readback boundaries before
  comparison builds.
- Repartition measured-heavy DataFrames by `normal_sample_id`, `exchange`, and
  `symbol` after instrumentation confirms partition shape.

P2:

- Tune `spark.sql.shuffle.partitions`, AQE, and skew settings only after
  code-level bottlenecks and partition metrics are understood.
- Add cleaner phase timing logs around quality, selected-output build, profile
  build, comparison validation, and top-diff build.

## Acceptance Answers

- Was `5h` caused mainly by cluster size or code structure? The measured API run
  was `2h 39m`; the extra wall-clock time is unreconciled. For the measured run,
  the likely cause is both: one-worker compute was a strong contributing factor,
  and code structure created serial Spark work and repeated actions.
- Did the job run mostly serially? Parts of it did. Quality probing and selected
  output construction are serial Python loops. Spark internals may still run
  tasks in parallel, but executor/task-level confirmation is unavailable.
- Which exact stage/step consumed most time? Exact Spark stage is unavailable.
  The longest log-backed step is the comparison/report interval from about
  `15:47:11` to `17:36:49`.
- What are the top three changes needed before scaling beyond BTC/one event?
  Add P0 instrumentation, batch normal-window extraction/probing, and reduce
  repeated validation/readback actions over long lineage.
