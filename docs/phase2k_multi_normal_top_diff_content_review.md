# Phase 2K Multi-Normal Top-Diff Content Review

## Source

- Comparison run id: `phase2j_p1_8w_20260428T082634Z`
- Top-diff run id: `phase2j_p1_8w_20260428T082634Z`
- Comparison path:
  `s3://quantlab-research/multi_normal_comparison_reports/_trial/run_id=phase2j_p1_8w_20260428T082634Z/`
- Top-diff path:
  `s3://quantlab-research/multi_normal_top_diffs/_trial/run_id=phase2j_p1_8w_20260428T082634Z/`
- Accepted performance baseline: Phase 2J P1 completed in `8m08s` on fixed
  8-worker Photon classic, with output contracts preserved.

The review read the existing S3 outputs only. It did not create a new S3
output, Databricks production job, ML artifact, production dataset, lead-lag
result, or business-logic change.

## Scope

- Symbol scope: BTC only (`BTCUSDT`).
- Event scope: one event, `binance_btcusdt_20260423_down_001`.
- Event direction: `DOWN`.
- Event start: `2026-04-23 10:04:10`.
- Normal scope: 10 activity-selected normal windows on `20260423`.
- Review type: descriptive content review only.

Rows read:

| table group | comparison rows | top-diff rows |
|---|---:|---:|
| exchange profile | 660 | 180 |
| cross-exchange mid diff | 150 | 120 |
| bucket change profile | 96 | 156 |
| total | 906 | 456 |

`phase2k_abs_z_score` in the tables below is a Phase 2K review ranking over
the existing comparison rows by `abs(z_score_vs_normal)`. It is not a new
pipeline output.

## Executive Summary

- Context/activity metrics still produce the largest outliers. OKX
  `trade_count_per_second_max` reaches `z=19.22` in `bucket_120_60s`, so raw
  global ranking is still activity-dominated.
- The 10-normal comparison nevertheless makes non-context rows readable:
  stable price/return rows and OKX book/spread rows stand out without relying
  on `relative_change` or denominator-risk flags.
- Cross-exchange price dislocation is strongest in `bucket_120_60s`, especially
  `bybit__okx` and `binance__okx` `last_abs_mid_diff_bps` at about `z=3`.
  Nearer `bucket_30_10s` / `last_30s` price-dislocation rows are present but
  less extreme.
- The clearest signal-candidate content is event-proximate: negative returns
  across exchanges in `bucket_30_10s` / `last_30s`, plus OKX book imbalance
  flipping negative and OKX average spread widening in the same band.
- Strict denominator-flagged rows are mostly zero or null comparisons. The
  large unstable rows are `*.relative_change` rows and should remain appendix
  material, not Phase 3A shortlist metrics.

## Context Metrics Review

Context still dominates if the rows are ranked globally by absolute z-score or
by raw unit differences. The largest context rows are activity-level rows, not
price-dislocation or order-book signal rows. That means context should be used
as a market regime and matching-control layer, not as a signal claim.

The important improvement versus the Phase 2H single-normal read is that
metric-group-separated review is now usable: context no longer prevents looking
at price-dislocation and stable signal-candidate rows.

| metric_group | rank_type | exchange | exchange_pair | window_label | metric_name | event_value | normal_mean | normal_median | normal_p25 | normal_p75 | normal_stddev | signed_diff_vs_mean | absolute_diff_vs_mean | z_score_vs_normal | normal_percentile_rank | ratio_unstable | relative_change_unstable | small_denominator_flag |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| context | phase2k_abs_z_score | okx |  | bucket_120_60s | trade_count_per_second_max | 1111 | 130.3 | 113 | 100 | 151 | 51.0164 | 980.7 | 980.7 | 19.2232 | 1 | false | false | false |
| context | phase2k_abs_z_score | okx |  | last_120s | trade_count_per_second_max | 1111 | 179.8 | 151 | 141 | 196 | 64.4357 | 931.2 | 931.2 | 14.4516 | 1 | false | false | false |
| context | phase2k_abs_z_score | okx |  | bucket_120_60s | trade_count_per_second_avg | 39.1167 | 16.1767 | 15.65 | 14.8167 | 17.2833 | 3.06713 | 22.94 | 22.94 | 7.4793 | 1 | false | false | false |
| context | phase2k_abs_z_score | okx |  | bucket_120_60s | trade_count_sum | 2347 | 970.6 | 939 | 889 | 1037 | 184.028 | 1376.4 | 1376.4 | 7.4793 | 1 | false | false | false |
| context | phase2k_abs_z_score | okx |  | last_120s | trade_count_sum | 3378 | 1926.7 | 2022 | 1737 | 2108 | 227.901 | 1451.3 | 1451.3 | 6.36811 | 1 | false | false | false |
| context | phase2k_abs_z_score | okx |  | last_120s | trade_count_per_second_avg | 28.15 | 16.0558 | 16.85 | 14.475 | 17.5667 | 1.89918 | 12.0942 | 12.0942 | 6.36811 | 1 | false | false | false |
| context | phase2k_abs_z_score | okx |  | bucket_120_60s | trade_volume_sum | 10224.9 | 3163.5 | 2950.07 | 2841.78 | 3887.48 | 1249.49 | 7061.43 | 7061.43 | 5.65143 | 1 | false | false | false |
| context | phase2k_abs_z_score | okx |  | bucket_120_60s | trade_notional_sum | 7.9410e+08 | 2.4624e+08 | 2.3087e+08 | 2.2148e+08 | 3.0328e+08 | 9.7313e+07 | 5.4786e+08 | 5.4786e+08 | 5.62983 | 1 | false | false | false |
| context | phase2k_abs_z_score | bybit |  | bucket_30_10s | trade_count_sum | 915 | 301.9 | 235 | 161 | 403 | 164.758 | 613.1 | 613.1 | 3.72121 | 1 | false | false | false |
| context | phase2k_abs_z_score | bybit |  | bucket_30_10s | trade_count_per_second_avg | 45.75 | 15.095 | 11.75 | 8.05 | 20.15 | 8.2379 | 30.655 | 30.655 | 3.72121 | 1 | false | false | false |
| context | phase2k_abs_z_score | bybit |  | last_120s | trade_count_sum | 3090 | 1946 | 1911 | 1738 | 2280 | 329.597 | 1144 | 1144 | 3.4709 | 1 | false | false | false |
| context | phase2k_abs_z_score | bybit |  | last_120s | trade_count_per_second_avg | 25.75 | 16.2167 | 15.925 | 14.4833 | 19 | 2.74664 | 9.53333 | 9.53333 | 3.4709 | 1 | false | false | false |
| context | phase2k_abs_z_score | bybit |  | last_30s | trade_count_sum | 1078 | 389 | 347 | 229 | 465 | 209.458 | 689 | 689 | 3.28944 | 1 | false | false | false |
| context | phase2k_abs_z_score | bybit |  | last_30s | trade_count_per_second_avg | 35.9333 | 12.9667 | 11.5667 | 7.63333 | 15.5 | 6.98193 | 22.9667 | 22.9667 | 3.28944 | 1 | false | false | false |
| context | phase2k_abs_z_score | bybit |  | bucket_30_10s | trade_volume_sum | 53.508 | 11.2723 | 7.503 | 4.838 | 11.984 | 13.1892 | 42.2357 | 42.2357 | 3.2023 | 1 | false | false | false |
| context | phase2k_abs_z_score | bybit |  | bucket_30_10s | trade_notional_sum | 4.1549e+06 | 878677 | 580203 | 377301 | 933263 | 1.0294e+06 | 3.2762e+06 | 3.2762e+06 | 3.18263 | 1 | false | false | false |
| context | phase2k_abs_z_score | bybit |  | last_30s | trade_volume_sum | 69.688 | 14.5334 | 8.496 | 5.913 | 13.457 | 18.9791 | 55.1546 | 55.1546 | 2.90607 | 1 | false | false | false |
| context | phase2k_abs_z_score | bybit |  | last_30s | trade_notional_sum | 5.4108e+06 | 1.1331e+06 | 656988 | 462647 | 1.0543e+06 | 1.4815e+06 | 4.2777e+06 | 4.2777e+06 | 2.88739 | 1 | false | false | false |
| context | phase2k_abs_z_score | bybit |  | last_60s | trade_volume_sum | 115.565 | 31.5893 | 18.829 | 13.015 | 40.393 | 29.8705 | 83.9757 | 83.9757 | 2.81132 | 1 | false | false | false |
| context | phase2k_abs_z_score | bybit |  | last_60s | trade_notional_sum | 8.9740e+06 | 2.4637e+06 | 1.4685e+06 | 1.0115e+06 | 3.1632e+06 | 2.3338e+06 | 6.5103e+06 | 6.5103e+06 | 2.78963 | 1 | false | false | false |

Context conclusion:

- Yes, context still dominates the most extreme rows.
- The domination is concentrated in OKX `bucket_120_60s` / `last_120s` and
  Bybit `bucket_30_10s` / trailing short windows.
- These rows should remain Phase 3A controls and regime descriptors.
- They should not be interpreted as pre-event signal rows.

## Price Dislocation Review

The strongest cross-exchange dislocation rows are in `bucket_120_60s`, not the
closest `bucket_30_10s` band. `bybit__okx` and `binance__okx`
`last_abs_mid_diff_bps` both sit at the top of the reviewed price table with
normal percentile rank `1`. The `bucket_30_10s` and `last_30s` rows are still
visible, especially `bybit__okx` `max_abs_mid_diff_bps`, but they are less
extreme than the 120-60s last-diff rows.

| metric_group | rank_type | exchange | exchange_pair | window_label | metric_name | event_value | normal_mean | normal_median | normal_p25 | normal_p75 | normal_stddev | signed_diff_vs_mean | absolute_diff_vs_mean | z_score_vs_normal | normal_percentile_rank | ratio_unstable | relative_change_unstable | small_denominator_flag |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| price_dislocation | phase2k_abs_z_score |  | bybit__okx | bucket_120_60s | last_abs_mid_diff_bps | 1.1328 | 0.35032 | 0.255697 | 0.167257 | 0.372026 | 0.259238 | 0.782475 | 0.782475 | 3.01837 | 1 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | binance__okx | bucket_120_60s | last_abs_mid_diff_bps | 1.48036 | 0.42182 | 0.308782 | 0.128144 | 0.524179 | 0.360504 | 1.05854 | 1.05854 | 2.93627 | 1 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | binance__okx | bucket_120_60s | last_mid_diff_bps | -1.48036 | -0.147188 | -0.128144 | -0.386448 | 0.16677 | 0.550993 | -1.33317 | 1.33317 | -2.41957 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | bybit__okx | bucket_120_60s | last_mid_diff_bps | -1.1328 | -0.0816925 | -0.232666 | -0.283302 | -0.128144 | 0.442887 | -1.0511 | 1.0511 | -2.3733 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | bybit__okx | last_30s | max_abs_mid_diff_bps | 2.26621 | 1.22193 | 0.93725 | 0.762902 | 1.53327 | 0.610444 | 1.04429 | 1.04429 | 1.7107 | 0.9 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | bybit__okx | bucket_30_10s | max_abs_mid_diff_bps | 2.26621 | 1.22193 | 0.93725 | 0.762902 | 1.53327 | 0.610444 | 1.04429 | 1.04429 | 1.7107 | 0.9 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | bybit__okx | last_60s | max_abs_mid_diff_bps | 2.26621 | 1.26889 | 0.999635 | 0.762902 | 1.53327 | 0.647789 | 0.997326 | 0.997326 | 1.53958 | 0.9 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | binance__bybit | bucket_60_30s | last_abs_mid_diff_bps | 0.0257487 | 0.670297 | 0.450406 | 0.359039 | 0.794503 | 0.449769 | -0.644549 | 0.644549 | -1.43307 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | bybit__okx | bucket_30_10s | avg_mid_diff_bps | -1.18815 | -0.0957642 | -0.484666 | -0.712115 | 0.369414 | 0.829751 | -1.09238 | 1.09238 | -1.31652 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | binance__okx | last_30s | last_abs_mid_diff_bps | 0.141692 | 0.704304 | 0.604972 | 0.499615 | 0.977616 | 0.439002 | -0.562612 | 0.562612 | -1.28157 | 0.1 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | binance__okx | last_60s | last_abs_mid_diff_bps | 0.141692 | 0.704304 | 0.604972 | 0.499615 | 0.977616 | 0.439002 | -0.562612 | 0.562612 | -1.28157 | 0.1 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | binance__okx | last_120s | last_abs_mid_diff_bps | 0.141692 | 0.704304 | 0.604972 | 0.499615 | 0.977616 | 0.439002 | -0.562612 | 0.562612 | -1.28157 | 0.1 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | binance__okx | bucket_120_60s | avg_mid_diff_bps | -0.657431 | -0.108353 | -0.139516 | -0.290917 | 0.271024 | 0.485292 | -0.549078 | 0.549078 | -1.13144 | 0.1 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | bybit__okx | last_120s | max_abs_mid_diff_bps | 2.26621 | 1.54057 | 1.24811 | 1.11505 | 2.09181 | 0.685886 | 0.725642 | 0.725642 | 1.05796 | 0.9 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | bybit__okx | last_30s | avg_mid_diff_bps | -1.02096 | -0.112041 | -0.462876 | -0.762025 | 0.255649 | 0.867148 | -0.90892 | 0.90892 | -1.04817 | 0.1 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | binance__bybit | bucket_30_10s | max_abs_mid_diff_bps | 1.53262 | 1.07826 | 0.948848 | 0.836236 | 1.43831 | 0.563184 | 0.454353 | 0.454353 | 0.806757 | 0.8 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | binance__bybit | bucket_120_60s | max_abs_mid_diff_bps | 0.939952 | 1.61855 | 1.25275 | 0.953153 | 1.96351 | 0.843973 | -0.678603 | 0.678603 | -0.804058 | 0.2 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | bybit__okx | bucket_60_30s | last_abs_mid_diff_bps | 0.270354 | 0.748325 | 0.488988 | 0.294726 | 0.772993 | 0.596683 | -0.477971 | 0.477971 | -0.801047 | 0.2 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | binance__bybit | last_30s | max_abs_mid_diff_bps | 1.53262 | 1.0847 | 1.01322 | 0.836236 | 1.43831 | 0.561907 | 0.447916 | 0.447916 | 0.797135 | 0.8 | false | false | false |
| price_dislocation | phase2k_abs_z_score |  | bybit__okx | last_60s | avg_mid_diff_bps | -0.748218 | -0.121771 | -0.5053 | -0.640021 | 0.167665 | 0.786975 | -0.626447 | 0.626447 | -0.796018 | 0.2 | false | false | false |

Price-dislocation conclusion:

- Keep cross-exchange mid-price dislocation for Phase 3A.
- Prioritize `last_abs_mid_diff_bps` and `last_mid_diff_bps` on
  `bybit__okx` and `binance__okx`, especially `bucket_120_60s`.
- Keep `bybit__okx` `max_abs_mid_diff_bps` in `bucket_30_10s` / `last_30s` as
  a secondary candidate.
- Do not claim this proves event prediction. This is one BTC DOWN event.

## Signal Candidate Review

The requested signal-candidate review includes return metrics. In the current
source taxonomy, `last_mid_return_bps` and `last_trade_return_bps` are
classified as `price_dislocation`, so the table keeps the source
`metric_group` value.

The strongest rows are negative event-proximate returns across exchanges in
`bucket_30_10s` and `last_30s`. Among true `signal_candidate` rows, the clearest
pattern is OKX-specific: `avg_book_imbalance_mean` turns sharply negative in
`bucket_30_10s` and remains negative in `last_30s`; `avg_spread_bps_mean` is
wider than normal in the same near-event region. Binance and Bybit show some
negative `bucket_30_10s` last-book imbalance, but the average book-imbalance
evidence is weaker and less consistent than OKX.

| metric_group | rank_type | exchange | exchange_pair | window_label | metric_name | event_value | normal_mean | normal_median | normal_p25 | normal_p75 | normal_stddev | signed_diff_vs_mean | absolute_diff_vs_mean | z_score_vs_normal | normal_percentile_rank | ratio_unstable | relative_change_unstable | small_denominator_flag |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| price_dislocation | phase2k_abs_z_score | bybit |  | bucket_30_10s | last_trade_return_bps | -7.71173 | 0.201036 | 0.0639094 | -0.884774 | 1.53233 | 1.78279 | -7.91276 | 7.91276 | -4.43841 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score | okx |  | last_30s | last_trade_return_bps | -5.48433 | 0.555964 | 0.663963 | -1.08219 | 0.945896 | 1.6103 | -6.04029 | 6.04029 | -3.75104 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score | bybit |  | bucket_30_10s | last_mid_return_bps | -6.21831 | 0.527079 | 0.0766913 | -0.871952 | 1.74278 | 1.93791 | -6.74539 | 6.74539 | -3.48076 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score | binance |  | last_30s | last_trade_return_bps | -5.40721 | 0.598588 | 0.830845 | -1.45597 | 1.78761 | 1.80271 | -6.0058 | 6.0058 | -3.33154 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score | binance |  | last_30s | last_mid_return_bps | -5.39434 | 0.601164 | 0.818062 | -1.45597 | 1.78761 | 1.80061 | -5.9955 | 5.9955 | -3.3297 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score | binance |  | bucket_30_10s | last_trade_return_bps | -5.69044 | 0.603785 | 0.436585 | -1.06421 | 1.78108 | 1.90215 | -6.29423 | 6.29423 | -3.30901 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score | binance |  | bucket_30_10s | last_mid_return_bps | -5.69045 | 0.599937 | 0.436585 | -1.06421 | 1.78108 | 1.90197 | -6.29038 | 6.29038 | -3.30729 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score | okx |  | bucket_30_10s | last_trade_return_bps | -5.52295 | 0.532359 | 0.0128212 | -1.06931 | 1.72991 | 1.87577 | -6.05531 | 6.05531 | -3.22817 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score | okx |  | bucket_30_10s | last_mid_return_bps | -5.52295 | 0.528502 | 0 | -1.08219 | 1.72991 | 1.87552 | -6.05145 | 6.05145 | -3.22654 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score | bybit |  | last_30s | last_mid_return_bps | -5.58747 | 0.818963 | 0.67744 | -1.44811 | 2.72987 | 2.01705 | -6.40643 | 6.40643 | -3.17614 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score | okx |  | last_30s | last_mid_return_bps | -5.4972 | 0.591681 | 0.676732 | -1.08219 | 2.35608 | 1.99968 | -6.08888 | 6.08888 | -3.04493 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score | okx |  | last_60s | last_trade_return_bps | -6.03757 | 2.86625 | 2.90887 | -2.07966 | 4.7076 | 3.52881 | -8.90383 | 8.90383 | -2.52318 | 0 | false | false | false |
| signal_candidate | phase2k_abs_z_score | okx |  | bucket_30_10s | avg_book_imbalance_mean | -0.530481 | 0.10158 | -0.0358516 | -0.111211 | 0.317468 | 0.281183 | -0.632061 | 0.632061 | -2.24787 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score | okx |  | last_60s | last_mid_return_bps | -6.03758 | 2.09528 | 2.50141 | -2.07966 | 4.99993 | 3.89551 | -8.13286 | 8.13286 | -2.08775 | 0 | false | false | false |
| signal_candidate | phase2k_abs_z_score | okx |  | last_30s | avg_spread_bps_mean | 0.0183443 | 0.0144486 | 0.0129917 | 0.0128227 | 0.0157296 | 0.00198686 | 0.00389574 | 0.00389574 | 1.96075 | 1 | false | false | false |
| price_dislocation | phase2k_abs_z_score | bybit |  | last_60s | last_mid_return_bps | -5.6518 | 2.18541 | 2.12948 | -1.32227 | 5.19179 | 4.0602 | -7.83721 | 7.83721 | -1.93025 | 0 | false | false | false |
| signal_candidate | phase2k_abs_z_score | okx |  | bucket_30_10s | avg_spread_bps_mean | 0.0205927 | 0.0152218 | 0.0130218 | 0.0128218 | 0.0169331 | 0.00295247 | 0.00537092 | 0.00537092 | 1.81913 | 0.9 | false | false | false |
| signal_candidate | phase2k_abs_z_score | okx |  | last_30s | avg_book_imbalance_mean | -0.34802 | 0.0676821 | 0.0109178 | -0.110665 | 0.204928 | 0.234091 | -0.415702 | 0.415702 | -1.77581 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score | binance |  | last_60s | last_trade_return_bps | -5.20133 | 2.12474 | 2.24476 | -1.64312 | 4.56619 | 4.16684 | -7.32607 | 7.32607 | -1.75818 | 0 | false | false | false |
| price_dislocation | phase2k_abs_z_score | binance |  | last_60s | last_mid_return_bps | -5.18845 | 2.12218 | 2.24475 | -1.64312 | 4.56619 | 4.16423 | -7.31063 | 7.31063 | -1.75558 | 0 | false | false | false |

Focused observations:

- OKX `avg_book_imbalance_mean` moves from positive in `bucket_120_60s`
  (`event=0.248`, `z=1.16`) and `bucket_60_30s` (`event=0.358`, `z=1.16`) to
  strongly negative in `bucket_30_10s` (`event=-0.530`, `normal_mean=0.102`,
  `z=-2.25`, percentile `0`).
- OKX `last_30s` `avg_book_imbalance_mean` remains negative
  (`event=-0.348`, `normal_mean=0.068`, `z=-1.78`, percentile `0`).
- OKX `avg_spread_bps_mean` widens in `bucket_30_10s` (`z=1.82`) and
  `last_30s` (`z=1.96`, percentile `1`).
- Binance `bucket_30_10s` average book imbalance is also negative
  (`event=-0.391`, `z=-1.07`), but weaker than OKX.
- Bybit `bucket_30_10s` average book imbalance is near normal
  (`event=-0.007`, `z=-0.02`), although last-book imbalance is negative
  (`z=-1.00`).
- `trade_imbalance_qty_ratio` and `trade_imbalance_qty_per_second` are not
  useful here: they are null or zero with denominator-risk flags.

## Unstable Metrics Review

There are two separate instability concepts:

- Intrinsic unstable metric names: `*.relative_change`.
- Row-level denominator-risk flags:
  `ratio_unstable`, `relative_change_unstable`, and `small_denominator_flag`.

The largest unstable rows come from `*.relative_change` rows. They can look
large, but should not enter the main Phase 3A shortlist because their
interpretation depends heavily on bucket denominators.

| metric_group | rank_type | exchange | exchange_pair | window_label | metric_name | event_value | normal_mean | normal_median | normal_p25 | normal_p75 | normal_stddev | signed_diff_vs_mean | absolute_diff_vs_mean | z_score_vs_normal | normal_percentile_rank | ratio_unstable | relative_change_unstable | small_denominator_flag |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| unstable | phase2k_abs_z_score | okx |  | bucket_300_120s->bucket_120_60s | trade_count_per_second.relative_change | 1.94726 | -0.0926543 | -0.147991 | -0.281326 | 0.0851809 | 0.213196 | 2.03991 | 2.03991 | 9.56825 | 1 | false | false | false |
| unstable | phase2k_abs_z_score | binance |  | bucket_300_120s->bucket_120_60s | avg_book_imbalance.relative_change | 18.0082 | -0.0722582 | -0.848518 | -1.48819 | 1.86529 | 2.01188 | 18.0804 | 18.0804 | 8.98683 | 1 | false | false | false |
| unstable | phase2k_abs_z_score | bybit |  | bucket_30_10s->bucket_10_0s | avg_book_imbalance.relative_change | 43.3474 | 2.28996 | -0.499309 | -0.906195 | 1.26383 | 7.72391 | 41.0574 | 41.0574 | 5.31562 | 1 | false | false | false |
| unstable | phase2k_abs_z_score | okx |  | bucket_60_30s->bucket_30_10s | trade_count_per_second.relative_change | 1.44602 | 0.0513594 | -0.135762 | -0.173295 | 0.461661 | 0.509499 | 1.39466 | 1.39466 | 2.73732 | 1 | false | false | false |
| unstable | phase2k_abs_z_score | bybit |  | bucket_60_30s->bucket_30_10s | trade_count_per_second.relative_change | 1.00365 | -0.0081438 | -0.0571429 | -0.340164 | 0.393509 | 0.390321 | 1.01179 | 1.01179 | 2.59221 | 1 | false | false | false |
| unstable | phase2k_abs_z_score | binance |  | bucket_60_30s->bucket_30_10s | trade_count_per_second.relative_change | 1.16367 | -0.0658783 | -0.145882 | -0.297468 | 0.253497 | 0.484349 | 1.22955 | 1.22955 | 2.53856 | 1 | false | false | false |
| unstable | phase2k_abs_z_score | bybit |  | bucket_300_120s->bucket_120_60s | trade_count_per_second.relative_change | 0.639621 | -0.0144239 | 0.0454819 | -0.273012 | 0.22175 | 0.29526 | 0.654045 | 0.654045 | 2.21515 | 1 | false | false | false |
| unstable | phase2k_abs_z_score | binance |  | bucket_300_120s->bucket_120_60s | trade_count_per_second.relative_change | 0.419434 | -0.0996036 | -0.106815 | -0.24696 | 0.0545039 | 0.246876 | 0.519037 | 0.519037 | 2.10242 | 1 | false | false | false |
| unstable | phase2k_abs_z_score | okx |  | bucket_60_30s->bucket_30_10s | avg_spread_bps.relative_change | 0.385876 | 0.0275656 | -0.0254324 | -0.0407295 | 0.0844857 | 0.208695 | 0.35831 | 0.35831 | 1.71691 | 0.9 | false | false | false |
| unstable | phase2k_abs_z_score | okx |  | bucket_120_60s->bucket_60_30s | trade_count_per_second.relative_change | -0.700043 | 0.138907 | -0.0819348 | -0.147357 | 0.124601 | 0.532745 | -0.83895 | 0.83895 | -1.57477 | 0 | false | false | false |
| unstable | phase2k_abs_z_score | okx |  | bucket_300_120s->bucket_120_60s | avg_spread_bps.relative_change | -0.129355 | 0.0421955 | -0.00871128 | -0.0384778 | 0.11531 | 0.112356 | -0.17155 | 0.17155 | -1.52684 | 0 | false | false | false |
| unstable | phase2k_abs_z_score | okx |  | bucket_30_10s->bucket_10_0s | avg_spread_bps.relative_change | -0.327548 | -0.126781 | -0.130595 | -0.213221 | -4.0590e-16 | 0.147832 | -0.200768 | 0.200768 | -1.35808 | 0.2 | false | false | false |
| unstable | phase2k_abs_z_score | binance |  | bucket_300_120s->bucket_120_60s | avg_spread_bps.relative_change | 0.0454459 | -0.0239091 | -0.039788 | -0.0427665 | 0.00604524 | 0.0510866 | 0.069355 | 0.069355 | 1.3576 | 0.9 | false | false | false |
| unstable | phase2k_abs_z_score | binance |  | bucket_30_10s->bucket_10_0s | avg_book_imbalance.relative_change | 1.23461 | -0.337196 | -0.310405 | -1.3349 | 0.567212 | 1.23341 | 1.57181 | 1.57181 | 1.27436 | 0.9 | false | false | false |
| unstable | phase2k_abs_z_score | okx |  | bucket_60_30s->bucket_30_10s | avg_book_imbalance.relative_change | -2.4822 | 0.133422 | 0.551467 | -1.42591 | 2.68307 | 2.55 | -2.61563 | 2.61563 | -1.02574 | 0.2 | false | false | false |
| unstable | phase2k_abs_z_score | bybit |  | bucket_30_10s->bucket_10_0s | trade_count_per_second.relative_change | -0.643716 | -0.397675 | -0.554585 | -0.692308 | -0.15528 | 0.298133 | -0.246041 | 0.246041 | -0.825273 | 0.3 | false | false | false |
| unstable | phase2k_abs_z_score | okx |  | bucket_30_10s->bucket_10_0s | trade_count_per_second.relative_change | -0.634146 | -0.441905 | -0.382151 | -0.673267 | -0.331808 | 0.244637 | -0.192241 | 0.192241 | -0.785824 | 0.3 | false | false | false |
| unstable | phase2k_abs_z_score | binance |  | bucket_30_10s->bucket_10_0s | trade_count_per_second.relative_change | -0.546135 | -0.324829 | -0.396694 | -0.483444 | -0.173077 | 0.303195 | -0.221305 | 0.221305 | -0.729911 | 0.2 | false | false | false |
| unstable | phase2k_abs_z_score | binance |  | bucket_120_60s->bucket_60_30s | avg_spread_bps.relative_change | -0.0380692 | 0.0601403 | -0.0100501 | -0.0317369 | 0.141518 | 0.154872 | -0.0982096 | 0.0982096 | -0.634133 | 0.1 | false | false | false |
| unstable | phase2k_abs_z_score | binance |  | bucket_120_60s->bucket_60_30s | trade_count_per_second.relative_change | -0.426213 | 0.379484 | -0.153949 | -0.255339 | 0.320442 | 1.27263 | -0.805697 | 0.805697 | -0.633098 | 0.1 | false | false | false |

Strict denominator-risk flags:

| metric_group | metric_name | flagged rows | note |
|---|---|---:|---|
| context | `bbo_quote_age_seconds_max` | 20 | all absolute differences are `0` |
| context | `buy_qty_sum` | 30 | all absolute differences are `0` |
| context | `seconds_forward_filled_bbo` | 20 | all absolute differences are `0` |
| context | `sell_qty_sum` | 30 | all absolute differences are `0` |
| signal_candidate | `trade_imbalance_qty_per_second` | 30 | all absolute differences are `0` |
| signal_candidate | `trade_imbalance_qty_per_second.absolute_change` | 12 | all absolute differences are `0` |
| signal_candidate | `trade_imbalance_qty_ratio` | 30 | null ratio values |
| signal_candidate | `trade_imbalance_qty_sum` | 30 | all absolute differences are `0` |
| unstable | `trade_imbalance_qty_per_second.relative_change` | 12 | null relative-change values |

Unstable conclusion:

- Large `*.relative_change` rows are not reliable Phase 3A shortlist metrics.
- Strictly flagged denominator-risk rows do not contain meaningful nonzero
  differences in this run; they mostly warn that denominator-sensitive trade
  imbalance fields are structurally zero or null.
- Keep these rows in an appendix or diagnostic layer.

## Candidate Shortlist For Phase 3

Keep these as descriptive Phase 3A candidates:

- `last_mid_return_bps` and `last_trade_return_bps` on `bucket_30_10s` and
  `last_30s`, across Binance, Bybit, and OKX.
- OKX `avg_book_imbalance_mean` on `bucket_30_10s` and `last_30s`.
- OKX `avg_spread_bps_mean` on `bucket_30_10s` and `last_30s`.
- `bybit__okx` and `binance__okx` `last_abs_mid_diff_bps` /
  `last_mid_diff_bps` on `bucket_120_60s`.
- `bybit__okx` `max_abs_mid_diff_bps` on `bucket_30_10s` and `last_30s` as a
  secondary price-dislocation candidate.
- Context controls: `trade_count_sum`, `trade_volume_sum`,
  `trade_notional_sum`, `trade_count_per_second_avg`,
  `trade_count_per_second_max`, `seconds_with_trades`, and
  `seconds_with_bbo_update`.

## Metrics To Deprioritize

Deprioritize these for the main Phase 3A signal shortlist:

- `trade_notional_sum`, `trade_volume_sum`, `trade_count_sum`, and
  `trade_count_per_second_*` as direct signal metrics. Keep them as context and
  matching controls.
- `trade_imbalance_qty_ratio`, `trade_imbalance_qty_per_second`, and
  `trade_imbalance_qty_sum` in the current output form, because they are
  zero/null and denominator-flagged in this run.
- All `*.relative_change` rows, including
  `trade_count_per_second.relative_change`,
  `avg_book_imbalance.relative_change`,
  `avg_spread_bps.relative_change`, and
  `trade_imbalance_qty_per_second.relative_change`.
- `buy_qty_sum`, `sell_qty_sum`, `bbo_quote_age_seconds_max`, and
  `seconds_forward_filled_bbo` where denominator flags indicate zero or
  near-zero denominators.

## What This Does Not Prove

- This does not prove a signal.
- This does not prove lead-lag.
- This does not prove statistical significance.
- This does not train or evaluate ML.
- This does not generalize beyond one BTC DOWN event on `20260423`.
- This does not prove the normal-window selection is final for scale-out.

## Recommendation

Proceed to Phase 3A, but with a narrowed metric policy:

- Scale BTC to many events with N normal windows per event.
- Keep context/activity metrics as controls and matching diagnostics.
- Promote stable return, cross-exchange dislocation, OKX book-imbalance, and
  OKX spread metrics into the Phase 3A descriptive candidate set.
- Keep ratio, denominator-risk, and `*.relative_change` metrics out of the main
  shortlist and report them only as appendix diagnostics.

No additional Phase 2J pipeline work is required before Phase 3A based on this
content review. Normal-window selection can be revisited later if the multi-event
Phase 3A review remains globally dominated by context metrics.
