# Phase 2J - Multi-Normal Window Trial Spec

Phase 2J extends the normal-time side from one BTC normal window to `N=10`
activity-matched normal windows. The accepted Databricks run is
`phase2j_20260427T150134Z`.

## Inputs

- Date: `20260423`.
- Symbol scope: BTC only.
- Exchanges: `binance`, `bybit`, `okx`.
- Window length: `300` seconds.
- Candidate exclusion scan: existing `%1 / 60s` scan over all BTC candidates on
  the date, including ambiguous candidate starts.
- Event activity reference:
  `pre_event_profile_reports/_trial/run_id=phase2d_20260427T090707Z`.

## Eligibility

A normal window candidate is eligible only when all conditions hold:

- Anchor is at least `1800` seconds away from every `%1 / 60s` candidate start.
- No candidate starts inside `[normal_window_start_ts, normal_window_end_ts)`.
- No candidate starts in `(normal_anchor_ts, normal_anchor_ts + 60s]`.
- Trade and BBO coverage is complete for all three exchanges.
- Extracted snapshot has exactly `3 exchanges x 300 seconds = 900` rows.
- Candidate anchor is at least `1800` seconds away from every already selected
  normal anchor.

## Activity Similarity

Activity matching uses only `exchange_profile` rows with
`window_label = last_300s`.

For each eligible quality-passing normal window, compare against the event
`last_300s` profile by exchange using:

- `trade_count_sum`
- `trade_volume_sum`
- `trade_notional_sum`

Distance is:

`mean(abs(log1p(normal_metric) - log1p(event_metric)))`

The mean is computed across all expected exchanges and all three metrics.

Selection order:

1. Lower activity-distance score.
2. Larger nearest-candidate distance.
3. Earlier `normal_anchor_ts`.

Select the first `10` windows after applying the minimum anchor-spacing rule.

## Outputs

Phase 2J writes trial-only outputs using the same schemas as Phase 2E, but with
multi-normal metadata per selected window:

- `multi_normal_windows/_trial/run_id=<phase2j_run_id>/trade_windows/`
- `multi_normal_windows/_trial/run_id=<phase2j_run_id>/bbo_windows/`
- `multi_normal_market_snapshots/_trial/run_id=<phase2j_run_id>/`
- `multi_normal_profile_reports/_trial/run_id=<phase2j_run_id>/`
- `multi_normal_comparison_reports/_trial/run_id=<phase2j_run_id>/`
- `multi_normal_top_diffs/_trial/run_id=<phase2j_run_id>/`

Required additional metadata:

- `normal_sample_id`
- `normal_sample_rank`
- `activity_distance`
- `nearest_candidate_distance_seconds`
- `selected_normal_count`
- `min_anchor_spacing_seconds`

Comparison rows collapse the `10` normal samples into a distribution per
event/profile key and metric:

- `event_value`
- `normal_mean`
- `normal_median`
- `normal_min`
- `normal_max`
- `normal_p25`
- `normal_p75`
- `normal_stddev`
- `normal_sample_count`
- `normal_non_null_count`
- `normal_zero_or_near_zero_count`
- `signed_diff_vs_mean`
- `absolute_diff_vs_mean`
- `ratio_vs_mean`
- `z_score_vs_normal`
- `normal_percentile_rank`

Top diffs rank within `report_group + metric_group`.

## Accepted Run

Accepted on 2026-04-27 against `dev_classic`:

- Run ID: `phase2j_20260427T150134Z`.
- Databricks job run ID: `370386900239817`.
- Task run ID: `695810238434357`.
- Result: `TERMINATED SUCCESS`.
- Selected normal count: `10`.
- Activity source rows: `9`
  (`last_300s`, `3 exchanges x 3 activity metrics`).
- Candidate audit:
  `price_1s_count=216633`, raw candidates `60`, ambiguous candidates `0`,
  distinct raw starts `60`, total anchors `985`, eligible anchors `924`,
  exclusion counts
  `{'candidate_within_anchor_proximity': 61, 'eligible': 924}`.
- Selection audit:
  quality probes attempted `10`, quality-failed counts `{}`,
  quality-passing candidates `10`, spacing-skipped count `24`.
- Readback counts:
  snapshots `9000`; profiles `exchange_profile=300`,
  `cross_exchange_mid_diff=300`, `bucket_change_profile=480`; comparisons
  `exchange_profile_comparison=660`,
  `cross_exchange_mid_diff_comparison=150`,
  `bucket_change_profile_comparison=96`; top diffs
  `exchange_profile_top_diffs=180`,
  `cross_exchange_mid_diff_top_diffs=120`,
  `bucket_change_profile_top_diffs=156`.
- Validation:
  `normal_sample_count` invalid rows `0`, `metric_group` null count `0`, and
  `absolute_diff_vs_mean` negative count `0`.

## Non-Goals

- No ML.
- No production dataset.
- No statistical significance.
- No signal claim.
- No multi-event expansion.
