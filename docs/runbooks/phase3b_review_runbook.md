# Phase 3B Review Runbook

## Purpose

Phase 3B reads accepted Phase 3A BTC 10-event artifacts and writes separate
content-review artifacts. It does not rerun Phase 3A, does not mutate the
accepted Phase 3A root, and does not claim statistical significance or validated
trading signals.

## Source

Accepted Phase 3A source run:

- `source_run_id=phase3a_20260429T085638Z`
- Source root:
  `s3://quantlab-research/btc_multi_event_trials/_trial/run_id=phase3a_20260429T085638Z`

## Output

Recommended Phase 3B output root:

`s3://quantlab-research/btc_multi_event_reviews/_trial/source_run_id=<source_run_id>/run_id=<phase3b_run_id>/`

Expected subtables:

- `metric_recurrence_report/`
- `direction_sign_consistency_report/`
- `context_dominance_report/`
- `event_narrative_report/`
- `recommended_metric_policy_v1/`

## Bundle Defaults

`databricks.yml` uses a placeholder source run id:

- `--source-run-id phase3a_SOURCE_RUN_ID`
- `--top-k 20`
- `--normal-count-per-event 10`
- `--sample-size 20`
- `--validation-mode light`
- `--small-output-partitions 1`

Do not hardcode the accepted source run id in bundle defaults.

## Compute Profile

Project compute policy is recorded in `docs/compute_policy_2026-04-29.md`.
Phase 3B should not need the max quota profile because it reads persisted Phase
3A parquet outputs and writes small review tables.

Recommended Phase 3B starting profile:

- Driver: `r6i.xlarge` or `r6i.2xlarge`
- Workers: `2-4 x r6i.xlarge` or `2-4 x r6i.2xlarge`
- Photon: enabled
- Autoscale: not required

If the priority is shortest wall-clock time over cost, a larger cluster inside
the current quota is acceptable. Record the actual driver, worker type, worker
count, Photon setting, spot/on-demand mix, and runtime in the accepted review
result.

## Manual Accepted-Source Command Shape

Run manually only after bundle validation/deploy and cluster selection:

```bash
databricks bundle run phase3b_btc_multi_event_content_review_classic \
  -t dev_classic \
  --profile quantlab-classic \
  --var cluster_id=<classic_cluster_id> \
  --python-params='--config,configs/dev.yaml,--source-run-id,phase3a_20260429T085638Z,--top-k,20,--normal-count-per-event,10,--sample-size,0,--validation-mode,light,--small-output-partitions,1'
```

Optional explicit review run id:

```bash
--python-params='--config,configs/dev.yaml,--source-run-id,phase3a_20260429T085638Z,--review-run-id,phase3b_<YYYYMMDDTHHMMSSZ>,--top-k,20,--normal-count-per-event,10,--sample-size,0,--validation-mode,light,--small-output-partitions,1'
```

## Post-Run Documentation

After the manual Databricks run completes and row counts are verified, add a
human-readable accepted review summary:

- `docs/phase3b_btc_multi_event_content_review_result.md`

That document should remain descriptive and should not claim production
readiness, statistical significance, or validated trading signals.
