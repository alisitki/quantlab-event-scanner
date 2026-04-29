# QuantLab Project State - 2026-04-29

## Current Status

QuantLab Event Scanner is a research/data-discovery pipeline, not a production
trading system. The accepted current milestone is Phase 3A BTC 10-event
multi-event trial/finalize.

Accepted Phase 3A run:

- `run_id=phase3a_20260429T085638Z`
- Source/output root:
  `s3://quantlab-research/btc_multi_event_trials/_trial/run_id=phase3a_20260429T085638Z`
- `selected_event_count=10`
- Comparison rows:
  - `exchange_profile_comparison=6600`
  - `cross_exchange_mid_diff_comparison=1500`
  - `bucket_change_profile_comparison=960`
- Top-diff rows:
  - `exchange_profile_top_diffs=1800`
  - `cross_exchange_mid_diff_top_diffs=1200`
  - `bucket_change_profile_top_diffs=1560`
- Summary rows:
  - `event_counts_by_direction=2`
  - `metric_group_summary=80`
  - `normal_selection_quality=10`
  - `event_processing_status=10`

Phase 3A accepted outputs are trial/research artifacts. They are not production
datasets and do not validate any metric as a trading signal.

## Immediate Next Phase

Phase 3B is the next approved phase: BTC 10-event multi-event content review.
It reads the accepted Phase 3A root and writes separate review artifacts under:

`s3://quantlab-research/btc_multi_event_reviews/_trial/source_run_id=<source_run_id>/run_id=<phase3b_run_id>/`

Phase 3B must not mutate or extend the accepted Phase 3A root.

## Preserved Direction

The Event Scanner remains the research/data discovery layer. The longer-term
research target moves from event-centric descriptive analysis to decision-time
prediction:

Every symbol / every decision timestamp -> as-of-safe features -> future 60s
executable long/short label -> rule or ML scoring -> executable backtest ->
shadow live -> controlled production.

Phase 4A decision-time labels are the correct future transition, but they are
not part of the current Phase 3B implementation.

## Current Boundaries

This repo does not currently implement:

- Live trading.
- Order management.
- Binance Futures API/order execution.
- User data stream reconciliation.
- ML training or ML inference.
- Phase 4A label datasets.
- Phase 4B feature store.
- Production artifact promotion.
- Statistical significance claims.

## Metric Policy

- `context` metrics: normal matching, regime filter, diagnostics.
- `signal_candidate` metrics: feature candidates only.
- `price_dislocation` metrics: core cross-exchange feature candidates.
- Stable return metrics: core price/return candidates.
- Book/spread metrics: core liquidity/microstructure candidates.
- `unstable` metrics: appendix/diagnostic only.
- Denominator-risk metrics: excluded from the primary signal shortlist.
- `relative_change` metrics: cautious use; excluded from primary shortlist when
  denominator risk exists.
