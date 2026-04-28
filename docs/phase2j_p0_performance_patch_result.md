# Phase 2J P0 Performance Patch Result

## Status

Patch implemented. Controlled Databricks rerun was not executed in this step.

Reason: the patch changes the bundle/job code path and would produce new S3
trial outputs under a new `run_id`. The rerun should be started deliberately
after review using the manually managed existing cluster. No cluster start,
terminate, or delete operation was performed.

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

## Controlled Rerun

Not executed.

Recommended rerun shape:

- existing cluster: `0426-145152-cv6y6ado`
- job: `phase2j_multi_normal_trial_classic`
- new `run_id`
- same BTC event/date/scope
- `validation_mode=light`

The rerun result should populate the timer table and output contract comparison
described in `docs/phase2j_p0_performance_patch_plan.md`.
