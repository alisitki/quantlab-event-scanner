# Phase 2J P0 Performance Patch Result

## Status

Patch implemented and controlled rerun executed.

No cluster terminate/delete operation was performed. The existing cluster was
started for this rerun and left running. `autotermination_minutes` remained `0`.

Operational rule recorded for this project work: do not terminate manually
managed Databricks clusters unless explicitly instructed; when starting a
cluster for these benchmark runs, keep auto-termination disabled
(`autotermination_minutes=0`) and do not enable it implicitly.

## Controlled Rerun

- logical run_id: `phase2j_p0_20260428T072152Z`
- Databricks job run: `571815240926134`
- Databricks task run: `220783994281070`
- cluster: `0426-145152-cv6y6ado`
- job state: `TERMINATED / SUCCESS`
- API duration: `1,233.775s`, about `20m 34s`
- old accepted Phase 2J API duration: `9,581s`, about `2h 39m`
- runtime change: about `7.8x` faster, roughly `87%` lower API runtime

## Changed Files

- `jobs/12_build_multi_normal_trial.py`
- `jobs/09_build_normal_time_trial.py`
- `databricks.yml`
- `docs/phase2j_p0_performance_patch_plan.md`

## Changed Functions

- `jobs/09_build_normal_time_trial.py`
  - `_write_and_validate(...)`
- `jobs/12_build_multi_normal_trial.py`
  - `main()`
  - `_select_quality_passed_candidates(...)`
  - `_build_selected_outputs(...)`
  - `_write_top_diffs(...)`
  - added Phase 2J instrumentation, batch quality, batch selected-output, and
    multi-normal snapshot/profile helpers

## Behavior Before

- Phase 2J quality probing ran candidate by candidate and triggered Spark actions
  per probe.
- Selected normal outputs were built per selected window and unioned.
- `_write_and_validate` always did pre-write count, write, readback count, schema
  print, and sample take.
- Comparison used in-memory profile DataFrame lineage directly.

## Behavior After

- Phase 2J logs `[PHASE2J_TIMER]`, `[PHASE2J_DF]`, `[PHASE2J_CONF]`, and
  `[PHASE2J_OUTPUT]` records.
- Phase 2J defaults to `--validation-mode light`; strict remains available.
- Light validation skips write-after-readback count and sample take, and skips
  pre-write count when no contract count was already required.
- Quality probe metrics are batched across candidate windows.
- Selected normal trade/BBO/snapshot/profile outputs are built in batch keyed by
  `normal_sample_id`.
- Comparison reads persisted profile outputs as an explicit lineage boundary.
- `databricks.yml` explicitly passes `--validation-mode light` for
  `phase2j_multi_normal_trial_classic`.

## Verification

- `python3 -m py_compile jobs/09_build_normal_time_trial.py jobs/12_build_multi_normal_trial.py`
  passed.
- `python3 -m compileall -q src jobs` passed.
- `git diff --check` passed.
- `databricks bundle validate -t dev_classic --profile quantlab-classic --var cluster_id=0426-145152-cv6y6ado`
  passed.
- `pytest` and `ruff` were not run because they are not installed in the local
  Python environment.

## Cluster / Spark Conf

- cluster state after run: `RUNNING`
- autotermination_minutes: `0`
- workers: `1`
- cluster cores: `4`
- cluster memory: `16384 MB`
- runtime: `15.4.x-photon-scala2.12`
- Photon: `true`
- driver node type: `m5d.large`
- scaling: `fixed_size`
- `spark.sql.shuffle.partitions`: `200`
- `spark.sql.adaptive.enabled`: unavailable from Spark conf lookup
- `spark.sql.adaptive.skewJoin.enabled`: `true`
- `spark.default.parallelism`: unavailable from Spark conf lookup
- executor memory-status count: `2`

## Output Contracts

| Contract | Observed |
|---|---:|
| distinct normal samples in outputs | `10` |
| `multi_normal_market_snapshots` rows | `9000` |
| `exchange_profile` rows | `300` |
| `cross_exchange_mid_diff` rows | `300` |
| `bucket_change_profile` rows | `480` |
| `exchange_profile_comparison` rows | `660` |
| `cross_exchange_mid_diff_comparison` rows | `150` |
| `bucket_change_profile_comparison` rows | `96` |
| `exchange_profile_top_diffs` rows | `180` |
| `cross_exchange_mid_diff_top_diffs` rows | `120` |
| `bucket_change_profile_top_diffs` rows | `156` |
| `metric_group` null count | `0` for all comparison outputs |
| `absolute_diff_vs_mean` negative count | `0` for all comparison outputs |
| invalid `normal_sample_count` rows | `0` for all comparison outputs |

## Timer Summary

Retrieved `[PHASE2J_TIMER]` lines:

| Step | Seconds |
|---|---:|
| `selected_normal_extraction` | `273.582` |
| `bbo_day_read_cache_materialize` | `99.706` |
| `manifest_event_profile_load` | `65.931` |
| `trade_day_read_cache_materialize` | `28.728` |
| `comparison_validation` | `27.470` |
| `comparison_build` | `16.543` |
| `top_diff_build_multi_normal_top_diffs.exchange_profile_top_diffs` | `12.897` |
| `write_validate_comparison_exchange_profile` | `12.703` |
| `top_diff_build_multi_normal_top_diffs.bucket_change_profile_top_diffs` | `12.428` |
| `top_diff_build_multi_normal_top_diffs.cross_exchange_mid_diff_top_diffs` | `12.420` |

Other write timers were each below `11s`.

Some expected fine-grained timer lines, including `candidate_scan`,
`anchor_eligibility`, `activity_ranking`, and `quality_probe`, were not present
in the retrieved `jobs get-run-output` log. The available timestamps show a
middle interval between `price_1s` logging around `07:26:21` and selected-output
logging around `07:35:36`, but this report does not assign exact sub-step
durations without the missing timer lines.

## DataFrame / Partition Notes

| DataFrame | Rows | Partitions | Notes |
|---|---:|---:|---|
| `trade_day` | `5,578,439` | `2` | Low partition count relative to raw size. |
| `bbo_day` | `37,147,479` | `3` | Low partition count and largest raw input. |
| `price_1s` | `216,633` | `200` | Expanded to shuffle partition count. |
| `normal_bbo_windows` | not counted | `200` | `10` distinct normal samples. |
| `multi_normal_market_snapshots` | contract count `9000` | `200` | `10` distinct normal samples. |
| profile outputs | contract counts `300/300/480` | `200` | `10` distinct normal samples. |
| comparison input readbacks | not counted | `2` | Deliberate lineage boundary. |
| comparison outputs | contract counts `660/150/96` | `200` | Contract validations passed. |
| top-diff outputs | `180/120/156` | `600` | Small outputs, high partition count. |

## Main Conclusion

- P0 patch improved runtime: yes.
- Old API runtime: about `2h 39m`.
- New P0 controlled rerun API runtime: about `20m 34s`.
- Biggest remaining visible bottleneck: `selected_normal_extraction`
  (`273.582s`), followed by raw BBO materialization (`99.706s`).
- Remaining concern: `trade_day` and `bbo_day` start with very low raw partition
  counts (`2` and `3`), while later small outputs can expand to `200` or `600`
  partitions. Partition tuning should be based on the next benchmark rather than
  guessed.
- Next action: run the same P0 patch on an 8-worker Photon classic cluster for a
  scale benchmark, keeping output contract checks and instrumentation enabled.

## 8-Worker Benchmark

- logical run_id: `phase2j_p0_8w_20260428T075402Z`
- Databricks job run: `373839985585677`
- Databricks task run: `369785315488378`
- cluster: `0426-145152-cv6y6ado`
- worker count: `8`
- job state: `TERMINATED / SUCCESS`
- API duration: `653.740s`, about `10m 54s`
- old accepted Phase 2J API duration: `9,581s`, about `2h 39m`
- P0 1-worker controlled rerun duration: `1,233.775s`, about `20m 34s`
- runtime change vs old accepted run: about `14.7x` faster, roughly `93%` lower
- runtime change vs P0 1-worker run: about `1.9x` faster

The cluster was left `RUNNING` with `num_workers=8` and
`autotermination_minutes=0`.

### 8-Worker Spark / Cluster Summary

- runtime: `15.4.x-photon-scala2.12`
- Photon: `true`
- worker node type: `m5d.large`
- driver node type: `m5d.large`
- scaling: `fixed_size`
- cluster cores: `18`
- cluster memory: `73728 MB`
- executor count from cluster metadata: `8`
- executor memory-status count from job log: `7`
- `spark.sql.shuffle.partitions`: `200`
- `spark.sql.adaptive.enabled`: `true`
- `spark.sql.adaptive.skewJoin.enabled`: `true`
- `spark.default.parallelism`: unavailable from Spark conf lookup

Worker CPU utilization was not available through the Databricks Jobs/Clusters
CLI metadata. A Spark driver proxy metrics request returned `temporarily
unavailable`, so this report does not infer CPU utilization.

### 8-Worker Output Contracts

| Contract | Observed |
|---|---:|
| distinct normal samples in outputs | `10` |
| `multi_normal_market_snapshots` rows | `9000` |
| `exchange_profile` rows | `300` |
| `cross_exchange_mid_diff` rows | `300` |
| `bucket_change_profile` rows | `480` |
| `exchange_profile_comparison` rows | `660` |
| `cross_exchange_mid_diff_comparison` rows | `150` |
| `bucket_change_profile_comparison` rows | `96` |
| `exchange_profile_top_diffs` rows | `180` |
| `cross_exchange_mid_diff_top_diffs` rows | `120` |
| `bucket_change_profile_top_diffs` rows | `156` |
| `metric_group` null count | `0` for all comparison outputs |
| `absolute_diff_vs_mean` negative count | `0` for all comparison outputs |
| invalid `normal_sample_count` rows | `0` for all comparison outputs |

### 8-Worker Timer Summary

| Step | Seconds |
|---|---:|
| `selected_normal_extraction` | `168.953` |
| `bbo_day_read_cache_materialize` | `101.085` |
| `manifest_event_profile_load` | `66.052` |
| `trade_day_read_cache_materialize` | `21.751` |
| `comparison_validation` | `17.280` |
| `comparison_build` | `14.527` |
| `price_1s_build` | `12.003` |
| `top_diff_build_multi_normal_top_diffs.bucket_change_profile_top_diffs` | `11.822` |
| `top_diff_build_multi_normal_top_diffs.cross_exchange_mid_diff_top_diffs` | `11.474` |
| `profile_build` | `9.669` |
| `top_diff_build_multi_normal_top_diffs.exchange_profile_top_diffs` | `9.342` |
| `normal_bbo_output_build` | `8.168` |
| `comparison_profile_lineage_boundary_read` | `7.977` |
| `snapshot_build` | `6.752` |
| `normal_trade_output_build` | `1.347` |

Compared with the P0 1-worker controlled rerun:

| Step | 1 Worker | 8 Workers | Change |
|---|---:|---:|---:|
| total API runtime | `1233.775s` | `653.740s` | `1.9x` faster |
| `selected_normal_extraction` | `273.582s` | `168.953s` | `1.6x` faster |
| `bbo_day_read_cache_materialize` | `99.706s` | `101.085s` | no improvement |
| `trade_day_read_cache_materialize` | `28.728s` | `21.751s` | `1.3x` faster |
| `comparison_build` | `16.543s` | `14.527s` | slight improvement |
| `comparison_validation` | `27.470s` | `17.280s` | `1.6x` faster |

### 8-Worker DataFrame / Partition Notes

| DataFrame | Rows | Partitions | Notes |
|---|---:|---:|---|
| `trade_day` | `5,578,439` | `3` | Still very low relative to 8 workers. |
| `bbo_day` | `37,147,479` | `5` | Still very low and did not speed up. |
| `price_1s` | `216,633` | `200` | Uses shuffle partition count. |
| `normal_trade_windows` | not counted | `200` | `10` distinct normal samples. |
| `normal_bbo_windows` | not counted | `200` | `10` distinct normal samples. |
| `multi_normal_market_snapshots` | contract count `9000` | `200` | Contract passed. |
| profile outputs | contract counts `300/300/480` | `200` | Contract passed. |
| comparison input readbacks | not counted | `15/11/16` | Higher than 1-worker readbacks. |
| comparison outputs | contract counts `660/150/96` | `200` | Contract passed. |
| top-diff outputs | `180/120/156` | `600` | Small outputs, high partition count. |

### 8-Worker Conclusion

- 8-worker benchmark improved total runtime from `20m34s` to `10m54s`.
- Scaling was useful but not linear. Runtime improved about `1.9x` while worker
  count increased from `1` to `8`.
- The strongest remaining bottleneck is still `selected_normal_extraction`.
- Raw BBO materialization did not improve with 8 workers, likely because the
  input started from only `5` partitions in this run.
- Next action should be a P1 partition/extraction patch, focused on raw trade/BBO
  partitioning and selected normal extraction, before larger Phase 3 scale-out.
