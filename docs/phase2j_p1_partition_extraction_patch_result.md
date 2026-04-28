# Phase 2J P1 Partition / Extraction Patch Result

## Run

- run_id: `phase2j_p1_8w_20260428T082634Z`
- Databricks job run: `631833249766377`
- Databricks task run: `448918696730372`
- state: `SUCCESS`
- cluster: `0426-145152-cv6y6ado`
- cluster mode: fixed 8-worker Photon classic
- auto-termination: disabled (`autotermination_minutes=0`)
- API start: `2026-04-28T08:26:37.104Z`
- API end: `2026-04-28T08:34:45.486Z`
- total API runtime: `488.382s` (`8m08s`)

## Comparison

| run | runtime | notes |
| --- | ---: | --- |
| Accepted Phase 2J | `2h39m` | original classic run |
| P0 1-worker controlled rerun | `20m34s` | P0 patch, same small cluster |
| P0 8-worker benchmark | `10m54s` | fixed 8-worker Photon classic |
| P1 8-worker benchmark | `8m08s` | raw-day repartition + extraction bucket join + small output coalesce |

P1 improved the 8-worker runtime by about `166s` versus P0 8-worker (`10m54s` -> `8m08s`), roughly `25%`.

## Spark / Cluster Summary

- Photon: `true`
- runtime: `15.4.x-photon-scala2.12`
- `spark.sql.shuffle.partitions`: `200`
- `spark.sql.adaptive.enabled`: `true`
- `spark.sql.adaptive.skewJoin.enabled`: `true`
- Spark executor memory status count from driver log: `6`
- Databricks cluster metadata after run: `RUNNING`, `num_workers=8`, `autotermination_minutes=0`

Some Databricks cluster usage tags were unavailable inside Spark conf during this run, so worker count is taken from read-only cluster metadata rather than inferred from Spark task metrics.

## Timers

| step | duration_sec | notes |
| --- | ---: | --- |
| selected_normal_extraction | `126.072` | still the largest step, but improved from P0 8-worker `168.953s` |
| comparison_profile_lineage_boundary_read | `58.292` | became a major remaining cost |
| bbo_day_read_cache_materialize | `48.273` | improved from P0 8-worker `101.085s` |
| manifest_event_profile_load | `24.793` | improved from P0 8-worker `66.052s` |
| candidate_scan | `20.246` | moderate remaining cost |
| comparison_build | `19.548` | slightly higher than P0 8-worker `14.527s` |
| trade_day_read_cache_materialize | `12.538` | improved from P0 8-worker `21.751s` |
| activity_ranking | `10.959` | moderate |
| price_1s_build | `9.898` | moderate |
| selected_output_count_validation | `8.300` | validation cost remains visible |
| comparison_validation | `7.710` | improved from P0 8-worker `17.280s` |
| profile_build | `7.190` | small |
| snapshot_build | `4.019` | small |

## DataFrame / Partition Summary

| DataFrame | rows | partitions | notes |
| --- | ---: | ---: | --- |
| trade_day | `5,578,439` | `96` | P1 raw-day repartition applied |
| bbo_day | `37,147,479` | `96` | P1 raw-day repartition applied |
| price_1s | `216,633` | `200` | unchanged |
| candidate_df | `60` | `200` | small but acceptable for scan path |
| quality_candidate_windows | `924` | `10` | one row per quality candidate |
| selected_normals_df | `10` | `10` | selected normal count confirmed |
| normal_trade_windows | not counted | `200` | selected output repartition |
| normal_bbo_windows | not counted | `200` | selected output repartition |
| multi_normal_market_snapshots | not counted | `200` | row contract checked separately |
| exchange_profile | not counted | `200` | row contract checked separately |
| cross_exchange_mid_diff | not counted | `200` | row contract checked separately |
| bucket_change_profile | not counted | `200` | row contract checked separately |
| comparison_input_exchange_profile | not counted | `12` | persisted profile readback |
| comparison_input_cross_exchange_mid_diff | not counted | `11` | persisted profile readback |
| comparison_input_bucket_change_profile | not counted | `15` | persisted profile readback |
| exchange_profile_comparison | not counted | `1` | small output coalesce |
| cross_exchange_mid_diff_comparison | not counted | `1` | small output coalesce |
| bucket_change_profile_comparison | not counted | `1` | small output coalesce |
| exchange_profile_top_diffs | `180` | `1` | small output coalesce |
| cross_exchange_mid_diff_top_diffs | `120` | `1` | small output coalesce |
| bucket_change_profile_top_diffs | `156` | `1` | small output coalesce |

## Output Contracts

| contract | observed |
| --- | ---: |
| selected_normal_count | `10` |
| multi_normal_market_snapshots | `9000` |
| exchange_profile | `300` |
| cross_exchange_mid_diff | `300` |
| bucket_change_profile | `480` |
| exchange_profile_comparison | `660` |
| cross_exchange_mid_diff_comparison | `150` |
| bucket_change_profile_comparison | `96` |
| exchange_profile_top_diffs | `180` |
| cross_exchange_mid_diff_top_diffs | `120` |
| bucket_change_profile_top_diffs | `156` |
| metric_group null count | `0` |
| absolute_diff_vs_mean negative count | `0` |
| normal_sample_count invalid rows | `0` |

## Conclusion

P1 improved runtime materially but did not eliminate the main bottleneck. The most important wins were:

- `bbo_day_read_cache_materialize`: `101.085s` -> `48.273s`
- `manifest_event_profile_load`: `66.052s` -> `24.793s`
- `selected_normal_extraction`: `168.953s` -> `126.072s`
- `comparison_validation`: `17.280s` -> `7.710s`

The biggest remaining bottleneck is still `selected_normal_extraction`, followed by `comparison_profile_lineage_boundary_read`. The next patch should focus on reducing profile readback/materialization cost and splitting selected extraction timers further into trade extraction, BBO extraction, snapshot, and profile materialization actions so the remaining `126s` can be attributed more precisely.

Phase 3 scale-out should still wait until the remaining selected extraction and comparison-boundary costs are understood.
