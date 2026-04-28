# Phase 2J P0 Performance Patch Plan

## Audit Summary

Accepted Phase 2J run `phase2j_20260427T150134Z` succeeded, but the measured
Databricks API duration was about `2h 39m`, with user-observed wall-clock time
around `5h`. The extra wall-clock gap remains unresolved.

The performance audit found that the fixed one-worker `m5d.large` cluster is a
strong contributing factor, but not the sole proven root cause. Code/log-backed
bottlenecks are serial quality probing, per-window selected output construction,
many Spark actions in validation/readback, and a long comparison/report interval.

## P0 Scope

- Add grep-friendly run, Spark, DataFrame, partition, and timer instrumentation.
- Keep Phase 2J business logic, event definition, normal-window selection rules,
  and metric semantics unchanged.
- Add `strict` / `light` output validation mode.
- Use `light` validation for Phase 2J to avoid repeated readback counts and
  sample takes.
- Batch quality coverage metrics across candidate windows.
- Batch selected normal trade/BBO/snapshot/profile output construction by
  `normal_sample_id`.
- Add a deliberate comparison lineage boundary by reading persisted profile
  outputs before comparison builds.

## Files To Change

- `jobs/12_build_multi_normal_trial.py`
- `jobs/09_build_normal_time_trial.py`
- `databricks.yml`

Reference-only files:

- `docs/phase2j_performance_audit.md`
- `docs/phase2j_multi_normal_window_spec.md`

## Instrumentation Plan

Log format:

- `[PHASE2J_TIMER] step=... seconds=...`
- `[PHASE2J_DF] name=... rows=... partitions=... schema=...`
- `[PHASE2J_CONF] key=... value=...`
- `[PHASE2J_OUTPUT] name=... path=... rows=... validation_mode=...`

Timer coverage:

- manifest/event profile load
- raw trade/BBO path selection
- trade/BBO day read/cache/materialize
- price_1s build
- candidate scan
- anchor eligibility
- activity ranking
- quality probe
- selected normal extraction
- normal trade/BBO output build
- snapshot build
- profile build
- comparison lineage boundary read
- comparison build
- comparison validation
- top-diff build
- write/validate per output table

DataFrame metrics:

- row count only when already needed or explicitly useful for contract checks
- `df.rdd.getNumPartitions()`
- schema summary
- distinct `normal_sample_id`, when present
- exchange and symbol coverage, when present

Spark/runtime context:

- run ID
- Spark app ID/name/master
- Databricks cluster ID/name/runtime/node tags when available
- Photon tag when available
- `spark.sql.shuffle.partitions`
- `spark.sql.adaptive.enabled`
- `spark.sql.adaptive.skewJoin.enabled`
- `spark.default.parallelism`
- executor memory-status count when available

## Validation-Light Plan

Default helper behavior remains `strict`.

`strict` mode:

- pre-write count
- write
- readback schema
- readback count
- readback count equality check
- sample take

`light` mode:

- reuse a previously computed row count when available
- skip pre-write count for large outputs where no row count was already required
- output schema print
- output path and row count log
- write
- skip write-after-readback count
- skip sample take

Phase 2J explicitly uses `validation_mode="light"` by default.

## Batch Quality Probe Plan

Old structure:

- loop candidate by candidate
- extract trade window
- extract BBO window
- build snapshot
- trigger counts and exchange coverage checks

New P0 structure:

- build a candidate window DataFrame for activity-ranked anchors
- interval-join `trade_day` once against all candidate windows
- interval-join `bbo_day` once against all candidate windows
- aggregate trade/BBO/snapshot quality metrics per anchor
- preserve the existing Python selection order, spacing rule, pass/fail reasons,
  and `selected_normal_count=10` contract

Quality metrics preserved:

- trade row count
- BBO row count
- trade exchange coverage
- BBO exchange coverage
- implied snapshot row count
- implied snapshot exchange coverage

## Batch Selected Output Plan

Old structure:

- loop selected normal windows
- build trade/BBO/snapshot/profile per selected window
- union all outputs

New P0 structure:

- create `selected_normals_df`
- interval-join `trade_day` once to build `normal_trade_windows`
- interval-join `bbo_day` once to build `normal_bbo_windows`
- build one multi-normal snapshot plan keyed by `normal_sample_id`
- build exchange, cross-exchange, and bucket profile outputs in batch while
  keeping `normal_sample_id` in window partitions and group keys
- repartition measured-heavy outputs by `normal_sample_id`, `exchange`, and
  `symbol` where those columns exist

Contracts to preserve:

- `normal_trade_windows` schema and row semantics
- `normal_bbo_windows` schema and row semantics
- `multi_normal_market_snapshots = 10 * 3 * 300 = 9000`
- `exchange_profile = 300`
- `cross_exchange_mid_diff = 300`
- `bucket_change_profile = 480`
- comparison output row counts: `660`, `150`, `96`

## Lineage Boundary Plan

After profile outputs are written, comparison inputs are loaded from the persisted
profile output paths:

- `exchange_profile`
- `cross_exchange_mid_diff`
- `bucket_change_profile`

This makes comparison input lineage explicit and prevents comparison builds from
reusing the full selected-output construction lineage. The boundary is logged as
`comparison_profile_lineage_boundary_read`.

## Acceptance Criteria

- Next Phase 2J run logs per-step wall-clock timers.
- Next Phase 2J run logs Spark conf/runtime context.
- Important DataFrames log partition counts.
- `strict` validation behavior remains available.
- Phase 2J runs with `light` validation mode by default.
- Quality probe Spark actions are batched rather than per candidate.
- Selected normal output construction is batched rather than per selected window.
- Comparison input source is explicitly logged as persisted profile readback.
- Output row contracts remain unchanged.
- No accepted old S3 outputs are deleted or overwritten.

## Rerun Plan

Controlled rerun should be executed only after this patch is reviewed:

- same event
- same date
- BTC only
- same selected normal target, `10`
- new `run_id`
- `validation_mode=light`
- instrumentation enabled
- output path under the new `run_id`

Required rerun checks:

- selected normal count equals `10`
- quality pass/fail summary is plausible
- snapshot rows equal `9000`
- profile row contracts remain `300`, `300`, `480`
- comparison row contracts remain `660`, `150`, `96`
- top-diff row contracts remain unchanged
- `metric_group` null count equals `0`
- `absolute_diff_vs_mean` negative count equals `0`
- invalid `normal_sample_count` rows equal `0`

Performance comparison:

- old Phase 2J API duration: about `2h 39m`
- new API duration
- per-step timer table
- comparison/report interval timer

This patch does not execute the controlled rerun by itself.
