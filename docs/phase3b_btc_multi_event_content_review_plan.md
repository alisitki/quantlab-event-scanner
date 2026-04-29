# Phase 3B BTC Multi-Event Content Review Plan

## Summary

Phase 3B reviews the accepted Phase 3A BTC 10-event output content. It reads
existing Phase 3A artifacts only and writes separate Phase 3B review artifacts.
It must not rerun Phase 3A and must not mutate the accepted Phase 3A root.

Input:

`s3://quantlab-research/btc_multi_event_trials/_trial/run_id=phase3a_20260429T085638Z`

Recommended output root:

`s3://quantlab-research/btc_multi_event_reviews/_trial/source_run_id=<source_run_id>/run_id=<phase3b_run_id>/`

## Questions

- How many events repeat price-dislocation metrics?
- How many events show OKX book imbalance?
- Is spread widening present?
- Do signs change by UP/DOWN direction?
- Do context metrics still dominate?
- What remains after unstable metrics are excluded?
- Does a metric appear before the event or only after the move has started?
- Are normal samples comparable?
- Does the same trace appear on the Binance executable side?
- Does cross-exchange behavior appear in pairs involving Binance or only away
  from Binance?

## Deliverables

Structured Phase 3B artifacts:

- `metric_recurrence_report/`
- `direction_sign_consistency_report/`
- `context_dominance_report/`
- `event_narrative_report/`
- `recommended_metric_policy_v1/`

Accepted-run human review, after the Databricks review job is run manually:

- `docs/phase3b_btc_multi_event_content_review_result.md`

Manual run instructions belong in:

- `docs/runbooks/phase3b_review_runbook.md`

## Artifact Contract

`metric_recurrence_report/` columns:

- `review_run_id`
- `source_run_id`
- `source_event_count`
- `profile_family`
- `report_group`
- `metric_group`
- `metric_name`
- `exchange`
- `exchange_pair`
- `symbol_key`
- `window_label`
- `from_bucket`
- `to_bucket`
- `rank_scope`
- `top_k`
- `n_events_seen_in_top_k`
- `n_up_events_seen`
- `n_down_events_seen`
- `median_rank`
- `median_abs_diff_vs_mean`
- `median_abs_z_score`
- `median_percentile_extremeness`
- `denominator_risk_event_count`
- `unstable_event_count`
- `direction_normalized_sign_consistency`
- `recommendation`
- `created_at`

`direction_sign_consistency_report/` columns:

- `review_run_id`
- `source_run_id`
- `profile_family`
- `report_group`
- `metric_group`
- `metric_name`
- `exchange`
- `exchange_pair`
- `symbol_key`
- `window_label`
- `from_bucket`
- `to_bucket`
- `expected_sign_policy`
- `up_positive_count`
- `up_negative_count`
- `down_positive_count`
- `down_negative_count`
- `observed_positive_count`
- `observed_negative_count`
- `observed_zero_count`
- `sign_consistency_rate`
- `direction_normalized_sign`
- `notes`
- `created_at`

`context_dominance_report/` columns:

- `review_run_id`
- `source_run_id`
- `profile_family`
- `report_group`
- `rank_type`
- `top_k`
- `context_metric_count`
- `signal_candidate_count`
- `price_dislocation_count`
- `unstable_count`
- `denominator_risk_count`
- `context_share`
- `unstable_share`
- `denominator_risk_share`
- `dominant_metric_group`
- `recommended_action`
- `created_at`

`event_narrative_report/` columns:

- `review_run_id`
- `source_run_id`
- `event_id`
- `event_rank`
- `symbol`
- `direction`
- `event_start_ts`
- `event_end_ts`
- `dominant_exchange_pattern`
- `cross_exchange_pattern`
- `book_spread_pattern`
- `trade_flow_pattern`
- `price_return_pattern`
- `normal_sample_quality`
- `metric_anomalies`
- `suspected_late_reaction_flags`
- `notes`
- `created_at`

`recommended_metric_policy_v1/` columns:

- `review_run_id`
- `source_run_id`
- `metric_name`
- `metric_group`
- `profile_family`
- `exchange`
- `exchange_pair`
- `window_label`
- `policy_bucket`
- `reason`
- `requires_more_data`
- `phase4b_candidate`
- `denominator_risk_policy`
- `created_at`

Allowed `policy_bucket` values:

- `core_feature`
- `context_filter`
- `diagnostic`
- `exclude`

## Method

Phase 3B should use the existing metric taxonomy from
`src/quantlab_event_scanner/profile_comparison_reports.py`.

The review job should detect:

- Recurrence across the 10 events by counting distinct events per metric/window
  inside top-k rows.
- UP/DOWN sign behavior by joining event direction and signed comparison values.
- Context dominance by computing metric-group shares inside ranked top-k rows.
- Unstable/relative-change metrics and denominator-risk rows, then demoting
  them away from the primary feature shortlist.
- Event-proximate behavior by separating earlier windows from `last_10s`,
  `last_30s`, `bucket_30_10s`, and `bucket_10_0s`.
- Binance executable relevance by distinguishing Binance exchange rows and
  cross-exchange pairs that include Binance from pairs that do not.

## Non-Goals

Phase 3B does not:

- Claim statistical significance.
- Claim production readiness.
- Claim any metric is a validated trading signal.
- Train or run ML.
- Build Phase 4A labels or Phase 4B features.
- Implement live trading, Binance API integration, order management, or user
  data stream reconciliation.
- Write into the accepted Phase 3A source root.
