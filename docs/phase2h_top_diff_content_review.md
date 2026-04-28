# Phase 2H - Top-Diff Content Review

Source top-diff run: `phase2g_20260427T111155Z`

Source path: `s3://quantlab-research/profile_comparison_top_diffs/_trial/run_id=phase2g_20260427T111155Z`

Status: historical Phase 2H content review. The accepted Phase 2I rerun
supersedes this source for future references:
`phase2g_20260427T113839Z`, sourced from accepted comparison run
`phase2f_20260427T113721Z`.

This is an inspection note only. It does not claim a signal, compute statistical
significance, run ML, produce a production dataset, or make a lead-lag
conclusion.

The tables below show the Phase 2G top-diff rows requested for each report group
and rank type. Values are rounded for review readability.

PHASE2H_SOURCE_RUN_ID=phase2g_20260427T111155Z
PHASE2H_SOURCE_PATH=s3://quantlab-research/profile_comparison_top_diffs/_trial/run_id=phase2g_20260427T111155Z

## exchange_profile_top_diffs count=60

### exchange_profile_top_diffs / top_absolute_diff rows=20
| report_group | rank_type | rank | exchange | exchange_pair | window_label | metric_name | event_value | normal_value | signed_diff | absolute_diff | ratio |
|---|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|
| exchange_profile | top_absolute_diff | 1 | okx |  | bucket_300_120s | trade_notional_sum | 5.044e+08 | 1.822e+09 | -1.317e+09 | 1.317e+09 | 0.276869 |
| exchange_profile | top_absolute_diff | 2 | okx |  | last_300s | trade_notional_sum | 1.549e+09 | 2.410e+09 | -8.614e+08 | 8.614e+08 | 0.642587 |
| exchange_profile | top_absolute_diff | 3 | okx |  | last_120s | trade_notional_sum | 1.044e+09 | 5.881e+08 | 4.561e+08 | 4.561e+08 | 1.7755 |
| exchange_profile | top_absolute_diff | 4 | okx |  | bucket_120_60s | trade_notional_sum | 7.941e+08 | 3.606e+08 | 4.335e+08 | 4.335e+08 | 2.2019 |
| exchange_profile | top_absolute_diff | 5 | okx |  | bucket_60_30s | trade_notional_sum | 1.713e+08 | 9.512e+07 | 7.614e+07 | 7.614e+07 | 1.8005 |
| exchange_profile | top_absolute_diff | 6 | okx |  | bucket_30_10s | trade_notional_sum | 3.923e+07 | 9.684e+07 | -5.761e+07 | 5.761e+07 | 0.405140 |
| exchange_profile | top_absolute_diff | 7 | okx |  | last_30s | trade_notional_sum | 7.888e+07 | 1.324e+08 | -5.352e+07 | 5.352e+07 | 0.595788 |
| exchange_profile | top_absolute_diff | 8 | binance |  | last_300s | trade_notional_sum | 2.121e+07 | 4.503e+07 | -2.382e+07 | 2.382e+07 | 0.471080 |
| exchange_profile | top_absolute_diff | 9 | okx |  | last_60s | trade_notional_sum | 2.501e+08 | 2.275e+08 | 2.262e+07 | 2.262e+07 | 1.0994 |
| exchange_profile | top_absolute_diff | 10 | binance |  | bucket_300_120s | trade_notional_sum | 1.185e+07 | 3.104e+07 | -1.919e+07 | 1.919e+07 | 0.381769 |
| exchange_profile | top_absolute_diff | 11 | bybit |  | bucket_300_120s | trade_notional_sum | 3.632e+06 | 1.966e+07 | -1.603e+07 | 1.603e+07 | 0.184729 |
| exchange_profile | top_absolute_diff | 12 | bybit |  | last_300s | trade_notional_sum | 1.685e+07 | 2.560e+07 | -8.743e+06 | 8.743e+06 | 0.658453 |
| exchange_profile | top_absolute_diff | 13 | bybit |  | last_120s | trade_notional_sum | 1.322e+07 | 5.934e+06 | 7.288e+06 | 7.288e+06 | 2.2282 |
| exchange_profile | top_absolute_diff | 14 | bybit |  | last_60s | trade_notional_sum | 8.974e+06 | 3.298e+06 | 5.676e+06 | 5.676e+06 | 2.7212 |
| exchange_profile | top_absolute_diff | 15 | binance |  | last_120s | trade_notional_sum | 9.360e+06 | 1.398e+07 | -4.623e+06 | 4.623e+06 | 0.669360 |
| exchange_profile | top_absolute_diff | 16 | okx |  | bucket_10_0s | trade_notional_sum | 3.964e+07 | 3.555e+07 | 4.092e+06 | 4.092e+06 | 1.1151 |
| exchange_profile | top_absolute_diff | 17 | okx |  | last_10s | trade_notional_sum | 3.964e+07 | 3.555e+07 | 4.092e+06 | 4.092e+06 | 1.1151 |
| exchange_profile | top_absolute_diff | 18 | bybit |  | last_30s | trade_notional_sum | 5.411e+06 | 1.625e+06 | 3.786e+06 | 3.786e+06 | 3.3296 |
| exchange_profile | top_absolute_diff | 19 | bybit |  | bucket_30_10s | trade_notional_sum | 4.155e+06 | 821103.79 | 3.334e+06 | 3.334e+06 | 5.0601 |
| exchange_profile | top_absolute_diff | 20 | binance |  | last_30s | trade_notional_sum | 1.740e+06 | 4.211e+06 | -2.472e+06 | 2.472e+06 | 0.413133 |

### exchange_profile_top_diffs / top_signed_positive rows=20
| report_group | rank_type | rank | exchange | exchange_pair | window_label | metric_name | event_value | normal_value | signed_diff | absolute_diff | ratio |
|---|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|
| exchange_profile | top_signed_positive | 1 | okx |  | last_120s | trade_notional_sum | 1.044e+09 | 5.881e+08 | 4.561e+08 | 4.561e+08 | 1.7755 |
| exchange_profile | top_signed_positive | 2 | okx |  | bucket_120_60s | trade_notional_sum | 7.941e+08 | 3.606e+08 | 4.335e+08 | 4.335e+08 | 2.2019 |
| exchange_profile | top_signed_positive | 3 | okx |  | bucket_60_30s | trade_notional_sum | 1.713e+08 | 9.512e+07 | 7.614e+07 | 7.614e+07 | 1.8005 |
| exchange_profile | top_signed_positive | 4 | okx |  | last_60s | trade_notional_sum | 2.501e+08 | 2.275e+08 | 2.262e+07 | 2.262e+07 | 1.0994 |
| exchange_profile | top_signed_positive | 5 | bybit |  | last_120s | trade_notional_sum | 1.322e+07 | 5.934e+06 | 7.288e+06 | 7.288e+06 | 2.2282 |
| exchange_profile | top_signed_positive | 6 | bybit |  | last_60s | trade_notional_sum | 8.974e+06 | 3.298e+06 | 5.676e+06 | 5.676e+06 | 2.7212 |
| exchange_profile | top_signed_positive | 7 | okx |  | bucket_10_0s | trade_notional_sum | 3.964e+07 | 3.555e+07 | 4.092e+06 | 4.092e+06 | 1.1151 |
| exchange_profile | top_signed_positive | 8 | okx |  | last_10s | trade_notional_sum | 3.964e+07 | 3.555e+07 | 4.092e+06 | 4.092e+06 | 1.1151 |
| exchange_profile | top_signed_positive | 9 | bybit |  | last_30s | trade_notional_sum | 5.411e+06 | 1.625e+06 | 3.786e+06 | 3.786e+06 | 3.3296 |
| exchange_profile | top_signed_positive | 10 | bybit |  | bucket_30_10s | trade_notional_sum | 4.155e+06 | 821103.79 | 3.334e+06 | 3.334e+06 | 5.0601 |
| exchange_profile | top_signed_positive | 11 | bybit |  | bucket_60_30s | trade_notional_sum | 3.563e+06 | 1.673e+06 | 1.890e+06 | 1.890e+06 | 2.1302 |
| exchange_profile | top_signed_positive | 12 | bybit |  | bucket_120_60s | trade_notional_sum | 4.248e+06 | 2.636e+06 | 1.612e+06 | 1.612e+06 | 1.6114 |
| exchange_profile | top_signed_positive | 13 | bybit |  | bucket_10_0s | trade_notional_sum | 1.256e+06 | 803973.80 | 451960.40 | 451960.40 | 1.5622 |
| exchange_profile | top_signed_positive | 14 | bybit |  | last_10s | trade_notional_sum | 1.256e+06 | 803973.80 | 451960.40 | 451960.40 | 1.5622 |
| exchange_profile | top_signed_positive | 15 | binance |  | bucket_60_30s | trade_notional_sum | 1.715e+06 | 1.575e+06 | 139903.30 | 139903.30 | 1.0888 |
| exchange_profile | top_signed_positive | 16 | okx |  | last_120s | trade_volume_sum | 13445.46 | 7508.16 | 5937.30 | 5937.30 | 1.7908 |
| exchange_profile | top_signed_positive | 17 | okx |  | bucket_120_60s | trade_volume_sum | 10224.93 | 4603.73 | 5621.20 | 5621.20 | 2.2210 |
| exchange_profile | top_signed_positive | 18 | okx |  | bucket_120_60s | trade_count_sum | 2347.00 | 1288.00 | 1059.00 | 1059.00 | 1.8222 |
| exchange_profile | top_signed_positive | 19 | okx |  | last_120s | trade_count_sum | 3378.00 | 2378.00 | 1000.00 | 1000.00 | 1.4205 |
| exchange_profile | top_signed_positive | 20 | okx |  | bucket_60_30s | trade_volume_sum | 2204.66 | 1213.99 | 990.6700 | 990.6700 | 1.8160 |

### exchange_profile_top_diffs / top_signed_negative rows=20
| report_group | rank_type | rank | exchange | exchange_pair | window_label | metric_name | event_value | normal_value | signed_diff | absolute_diff | ratio |
|---|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|
| exchange_profile | top_signed_negative | 1 | okx |  | bucket_300_120s | trade_notional_sum | 5.044e+08 | 1.822e+09 | -1.317e+09 | 1.317e+09 | 0.276869 |
| exchange_profile | top_signed_negative | 2 | okx |  | last_300s | trade_notional_sum | 1.549e+09 | 2.410e+09 | -8.614e+08 | 8.614e+08 | 0.642587 |
| exchange_profile | top_signed_negative | 3 | okx |  | bucket_30_10s | trade_notional_sum | 3.923e+07 | 9.684e+07 | -5.761e+07 | 5.761e+07 | 0.405140 |
| exchange_profile | top_signed_negative | 4 | okx |  | last_30s | trade_notional_sum | 7.888e+07 | 1.324e+08 | -5.352e+07 | 5.352e+07 | 0.595788 |
| exchange_profile | top_signed_negative | 5 | binance |  | last_300s | trade_notional_sum | 2.121e+07 | 4.503e+07 | -2.382e+07 | 2.382e+07 | 0.471080 |
| exchange_profile | top_signed_negative | 6 | binance |  | bucket_300_120s | trade_notional_sum | 1.185e+07 | 3.104e+07 | -1.919e+07 | 1.919e+07 | 0.381769 |
| exchange_profile | top_signed_negative | 7 | bybit |  | bucket_300_120s | trade_notional_sum | 3.632e+06 | 1.966e+07 | -1.603e+07 | 1.603e+07 | 0.184729 |
| exchange_profile | top_signed_negative | 8 | bybit |  | last_300s | trade_notional_sum | 1.685e+07 | 2.560e+07 | -8.743e+06 | 8.743e+06 | 0.658453 |
| exchange_profile | top_signed_negative | 9 | binance |  | last_120s | trade_notional_sum | 9.360e+06 | 1.398e+07 | -4.623e+06 | 4.623e+06 | 0.669360 |
| exchange_profile | top_signed_negative | 10 | binance |  | last_30s | trade_notional_sum | 1.740e+06 | 4.211e+06 | -2.472e+06 | 2.472e+06 | 0.413133 |
| exchange_profile | top_signed_negative | 11 | binance |  | last_60s | trade_notional_sum | 3.455e+06 | 5.786e+06 | -2.332e+06 | 2.332e+06 | 0.597054 |
| exchange_profile | top_signed_negative | 12 | binance |  | bucket_120_60s | trade_notional_sum | 5.905e+06 | 8.197e+06 | -2.292e+06 | 2.292e+06 | 0.720405 |
| exchange_profile | top_signed_negative | 13 | binance |  | bucket_30_10s | trade_notional_sum | 1.434e+06 | 3.120e+06 | -1.686e+06 | 1.686e+06 | 0.459637 |
| exchange_profile | top_signed_negative | 14 | binance |  | bucket_10_0s | trade_notional_sum | 305700.68 | 1.091e+06 | -785479.45 | 785479.45 | 0.280156 |
| exchange_profile | top_signed_negative | 15 | binance |  | last_10s | trade_notional_sum | 305700.68 | 1.091e+06 | -785479.45 | 785479.45 | 0.280156 |
| exchange_profile | top_signed_negative | 16 | okx |  | bucket_300_120s | trade_volume_sum | 6499.42 | 23282.23 | -16782.81 | 16782.81 | 0.279158 |
| exchange_profile | top_signed_negative | 17 | okx |  | last_300s | trade_volume_sum | 19944.88 | 30790.39 | -10845.51 | 10845.51 | 0.647763 |
| exchange_profile | top_signed_negative | 18 | binance |  | last_300s | trade_count_sum | 3787.00 | 8726.00 | -4939.00 | 4939.00 | 0.433990 |
| exchange_profile | top_signed_negative | 19 | bybit |  | bucket_300_120s | trade_count_sum | 2428.00 | 7362.00 | -4934.00 | 4934.00 | 0.329802 |
| exchange_profile | top_signed_negative | 20 | bybit |  | last_300s | trade_count_sum | 5518.00 | 10052.00 | -4534.00 | 4534.00 | 0.548945 |

## cross_exchange_mid_diff_top_diffs count=60

### cross_exchange_mid_diff_top_diffs / top_absolute_diff rows=20
| report_group | rank_type | rank | exchange | exchange_pair | window_label | metric_name | event_value | normal_value | signed_diff | absolute_diff | ratio |
|---|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|
| cross_exchange_mid_diff | top_absolute_diff | 1 |  | bybit__okx | bucket_120_60s | last_mid_diff_bps | -1.1328 | 0.408434 | -1.5412 | 1.5412 | -2.7735 |
| cross_exchange_mid_diff | top_absolute_diff | 2 |  | bybit__okx | bucket_30_10s | max_abs_mid_diff_bps | 2.2662 | 0.778855 | 1.4874 | 1.4874 | 2.9097 |
| cross_exchange_mid_diff | top_absolute_diff | 3 |  | bybit__okx | last_30s | max_abs_mid_diff_bps | 2.2662 | 0.778855 | 1.4874 | 1.4874 | 2.9097 |
| cross_exchange_mid_diff | top_absolute_diff | 4 |  | bybit__okx | bucket_30_10s | avg_mid_diff_bps | -1.1881 | -0.012131 | -1.1760 | 1.1760 | 97.9426 |
| cross_exchange_mid_diff | top_absolute_diff | 5 |  | binance__okx | bucket_120_60s | last_abs_mid_diff_bps | 1.4804 | 0.331853 | 1.1485 | 1.1485 | 4.4609 |
| cross_exchange_mid_diff | top_absolute_diff | 6 |  | binance__okx | bucket_120_60s | last_mid_diff_bps | -1.4804 | -0.331853 | -1.1485 | 1.1485 | 4.4609 |
| cross_exchange_mid_diff | top_absolute_diff | 7 |  | bybit__okx | last_120s | max_abs_mid_diff_bps | 2.2662 | 1.1487 | 1.1175 | 1.1175 | 1.9728 |
| cross_exchange_mid_diff | top_absolute_diff | 8 |  | bybit__okx | last_60s | max_abs_mid_diff_bps | 2.2662 | 1.1487 | 1.1175 | 1.1175 | 1.9728 |
| cross_exchange_mid_diff | top_absolute_diff | 9 |  | bybit__okx | bucket_60_30s | avg_mid_diff_bps | -0.475476 | 0.592004 | -1.0675 | 1.0675 | -0.803163 |
| cross_exchange_mid_diff | top_absolute_diff | 10 |  | binance__bybit | bucket_30_10s | avg_mid_diff_bps | 0.572459 | -0.435358 | 1.0078 | 1.0078 | -1.3149 |
| cross_exchange_mid_diff | top_absolute_diff | 11 |  | bybit__okx | last_60s | avg_mid_diff_bps | -0.748218 | 0.258333 | -1.0066 | 1.0066 | -2.8963 |
| cross_exchange_mid_diff | top_absolute_diff | 12 |  | binance__okx | bucket_30_10s | max_abs_mid_diff_bps | 1.0945 | 2.0555 | -0.960995 | 0.960995 | 0.532470 |
| cross_exchange_mid_diff | top_absolute_diff | 13 |  | binance__okx | last_30s | max_abs_mid_diff_bps | 1.0945 | 2.0555 | -0.960995 | 0.960995 | 0.532470 |
| cross_exchange_mid_diff | top_absolute_diff | 14 |  | binance__okx | last_60s | max_abs_mid_diff_bps | 1.0945 | 2.0555 | -0.960995 | 0.960995 | 0.532470 |
| cross_exchange_mid_diff | top_absolute_diff | 15 |  | binance__bybit | bucket_300_120s | max_abs_mid_diff_bps | 0.992191 | 1.9490 | -0.956775 | 0.956775 | 0.509086 |
| cross_exchange_mid_diff | top_absolute_diff | 16 |  | bybit__okx | last_30s | avg_mid_diff_bps | -1.0210 | -0.075337 | -0.945623 | 0.945623 | 13.5519 |
| cross_exchange_mid_diff | top_absolute_diff | 17 |  | binance__bybit | bucket_60_30s | avg_mid_diff_bps | 0.127468 | -0.780212 | 0.907680 | 0.907680 | -0.163376 |
| cross_exchange_mid_diff | top_absolute_diff | 18 |  | binance__bybit | bucket_30_10s | last_mid_diff_bps | 0.553941 | -0.331997 | 0.885938 | 0.885938 | -1.6685 |
| cross_exchange_mid_diff | top_absolute_diff | 19 |  | binance__bybit | last_60s | avg_mid_diff_bps | 0.315531 | -0.567788 | 0.883319 | 0.883319 | -0.555721 |
| cross_exchange_mid_diff | top_absolute_diff | 20 |  | binance__bybit | last_30s | avg_mid_diff_bps | 0.503595 | -0.355363 | 0.858958 | 0.858958 | -1.4171 |

### cross_exchange_mid_diff_top_diffs / top_signed_positive rows=20
| report_group | rank_type | rank | exchange | exchange_pair | window_label | metric_name | event_value | normal_value | signed_diff | absolute_diff | ratio |
|---|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|
| cross_exchange_mid_diff | top_signed_positive | 1 |  | bybit__okx | bucket_30_10s | max_abs_mid_diff_bps | 2.2662 | 0.778855 | 1.4874 | 1.4874 | 2.9097 |
| cross_exchange_mid_diff | top_signed_positive | 2 |  | bybit__okx | last_30s | max_abs_mid_diff_bps | 2.2662 | 0.778855 | 1.4874 | 1.4874 | 2.9097 |
| cross_exchange_mid_diff | top_signed_positive | 3 |  | binance__okx | bucket_120_60s | last_abs_mid_diff_bps | 1.4804 | 0.331853 | 1.1485 | 1.1485 | 4.4609 |
| cross_exchange_mid_diff | top_signed_positive | 4 |  | bybit__okx | last_120s | max_abs_mid_diff_bps | 2.2662 | 1.1487 | 1.1175 | 1.1175 | 1.9728 |
| cross_exchange_mid_diff | top_signed_positive | 5 |  | bybit__okx | last_60s | max_abs_mid_diff_bps | 2.2662 | 1.1487 | 1.1175 | 1.1175 | 1.9728 |
| cross_exchange_mid_diff | top_signed_positive | 6 |  | binance__bybit | bucket_30_10s | avg_mid_diff_bps | 0.572459 | -0.435358 | 1.0078 | 1.0078 | -1.3149 |
| cross_exchange_mid_diff | top_signed_positive | 7 |  | binance__bybit | bucket_60_30s | avg_mid_diff_bps | 0.127468 | -0.780212 | 0.907680 | 0.907680 | -0.163376 |
| cross_exchange_mid_diff | top_signed_positive | 8 |  | binance__bybit | bucket_30_10s | last_mid_diff_bps | 0.553941 | -0.331997 | 0.885938 | 0.885938 | -1.6685 |
| cross_exchange_mid_diff | top_signed_positive | 9 |  | binance__bybit | last_60s | avg_mid_diff_bps | 0.315531 | -0.567788 | 0.883319 | 0.883319 | -0.555721 |
| cross_exchange_mid_diff | top_signed_positive | 10 |  | binance__bybit | last_30s | avg_mid_diff_bps | 0.503595 | -0.355363 | 0.858958 | 0.858958 | -1.4171 |
| cross_exchange_mid_diff | top_signed_positive | 11 |  | bybit__okx | bucket_120_60s | last_abs_mid_diff_bps | 1.1328 | 0.408434 | 0.724361 | 0.724361 | 2.7735 |
| cross_exchange_mid_diff | top_signed_positive | 12 |  | binance__bybit | last_120s | avg_mid_diff_bps | 0.114076 | -0.524943 | 0.639019 | 0.639019 | -0.217310 |
| cross_exchange_mid_diff | top_signed_positive | 13 |  | binance__okx | bucket_300_120s | last_mid_diff_bps | 0.025767 | -0.612950 | 0.638717 | 0.638717 | -0.042038 |
| cross_exchange_mid_diff | top_signed_positive | 14 |  | bybit__okx | bucket_120_60s | max_abs_mid_diff_bps | 1.5200 | 0.945075 | 0.574955 | 0.574955 | 1.6084 |
| cross_exchange_mid_diff | top_signed_positive | 15 |  | binance__bybit | bucket_10_0s | avg_mid_diff_bps | 0.365866 | -0.195374 | 0.561240 | 0.561240 | -1.8726 |
| cross_exchange_mid_diff | top_signed_positive | 16 |  | binance__bybit | last_10s | avg_mid_diff_bps | 0.365866 | -0.195374 | 0.561240 | 0.561240 | -1.8726 |
| cross_exchange_mid_diff | top_signed_positive | 17 |  | bybit__okx | bucket_30_10s | last_abs_mid_diff_bps | 0.966083 | 0.421363 | 0.544720 | 0.544720 | 2.2928 |
| cross_exchange_mid_diff | top_signed_positive | 18 |  | binance__bybit | bucket_60_30s | last_mid_diff_bps | 0.025749 | -0.459566 | 0.485315 | 0.485315 | -0.056028 |
| cross_exchange_mid_diff | top_signed_positive | 19 |  | binance__bybit | bucket_120_60s | avg_mid_diff_bps | -0.087380 | -0.482099 | 0.394719 | 0.394719 | 0.181249 |
| cross_exchange_mid_diff | top_signed_positive | 20 |  | binance__bybit | bucket_120_60s | last_mid_diff_bps | -0.347602 | -0.740257 | 0.392656 | 0.392656 | 0.469569 |

### cross_exchange_mid_diff_top_diffs / top_signed_negative rows=20
| report_group | rank_type | rank | exchange | exchange_pair | window_label | metric_name | event_value | normal_value | signed_diff | absolute_diff | ratio |
|---|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|
| cross_exchange_mid_diff | top_signed_negative | 1 |  | bybit__okx | bucket_120_60s | last_mid_diff_bps | -1.1328 | 0.408434 | -1.5412 | 1.5412 | -2.7735 |
| cross_exchange_mid_diff | top_signed_negative | 2 |  | bybit__okx | bucket_30_10s | avg_mid_diff_bps | -1.1881 | -0.012131 | -1.1760 | 1.1760 | 97.9426 |
| cross_exchange_mid_diff | top_signed_negative | 3 |  | binance__okx | bucket_120_60s | last_mid_diff_bps | -1.4804 | -0.331853 | -1.1485 | 1.1485 | 4.4609 |
| cross_exchange_mid_diff | top_signed_negative | 4 |  | bybit__okx | bucket_60_30s | avg_mid_diff_bps | -0.475476 | 0.592004 | -1.0675 | 1.0675 | -0.803163 |
| cross_exchange_mid_diff | top_signed_negative | 5 |  | bybit__okx | last_60s | avg_mid_diff_bps | -0.748218 | 0.258333 | -1.0066 | 1.0066 | -2.8963 |
| cross_exchange_mid_diff | top_signed_negative | 6 |  | binance__okx | bucket_30_10s | max_abs_mid_diff_bps | 1.0945 | 2.0555 | -0.960995 | 0.960995 | 0.532470 |
| cross_exchange_mid_diff | top_signed_negative | 7 |  | binance__okx | last_30s | max_abs_mid_diff_bps | 1.0945 | 2.0555 | -0.960995 | 0.960995 | 0.532470 |
| cross_exchange_mid_diff | top_signed_negative | 8 |  | binance__okx | last_60s | max_abs_mid_diff_bps | 1.0945 | 2.0555 | -0.960995 | 0.960995 | 0.532470 |
| cross_exchange_mid_diff | top_signed_negative | 9 |  | binance__bybit | bucket_300_120s | max_abs_mid_diff_bps | 0.992191 | 1.9490 | -0.956775 | 0.956775 | 0.509086 |
| cross_exchange_mid_diff | top_signed_negative | 10 |  | bybit__okx | last_30s | avg_mid_diff_bps | -1.0210 | -0.075337 | -0.945623 | 0.945623 | 13.5519 |
| cross_exchange_mid_diff | top_signed_negative | 11 |  | bybit__okx | last_120s | avg_mid_diff_bps | -0.659133 | 0.196281 | -0.855414 | 0.855414 | -3.3581 |
| cross_exchange_mid_diff | top_signed_negative | 12 |  | bybit__okx | bucket_120_60s | avg_mid_diff_bps | -0.570047 | 0.134229 | -0.704276 | 0.704276 | -4.2468 |
| cross_exchange_mid_diff | top_signed_negative | 13 |  | binance__bybit | bucket_30_10s | max_abs_mid_diff_bps | 1.5326 | 2.2086 | -0.676025 | 0.676025 | 0.693918 |
| cross_exchange_mid_diff | top_signed_negative | 14 |  | binance__bybit | last_120s | max_abs_mid_diff_bps | 1.5326 | 2.2086 | -0.676025 | 0.676025 | 0.693918 |
| cross_exchange_mid_diff | top_signed_negative | 15 |  | binance__bybit | last_300s | max_abs_mid_diff_bps | 1.5326 | 2.2086 | -0.676025 | 0.676025 | 0.693918 |
| cross_exchange_mid_diff | top_signed_negative | 16 |  | binance__bybit | last_30s | max_abs_mid_diff_bps | 1.5326 | 2.2086 | -0.676025 | 0.676025 | 0.693918 |
| cross_exchange_mid_diff | top_signed_negative | 17 |  | binance__bybit | last_60s | max_abs_mid_diff_bps | 1.5326 | 2.2086 | -0.676025 | 0.676025 | 0.693918 |
| cross_exchange_mid_diff | top_signed_negative | 18 |  | bybit__okx | last_300s | avg_mid_diff_bps | -0.623610 | -0.025568 | -0.598042 | 0.598042 | 24.3899 |
| cross_exchange_mid_diff | top_signed_negative | 19 |  | bybit__okx | bucket_300_120s | max_abs_mid_diff_bps | 1.3789 | 1.9702 | -0.591267 | 0.591267 | 0.699897 |
| cross_exchange_mid_diff | top_signed_negative | 20 |  | binance__okx | bucket_300_120s | last_abs_mid_diff_bps | 0.025767 | 0.612950 | -0.587183 | 0.587183 | 0.042038 |

## bucket_change_profile_top_diffs count=60

### bucket_change_profile_top_diffs / top_absolute_diff rows=20
| report_group | rank_type | rank | exchange | exchange_pair | window_label | metric_name | event_value | normal_value | signed_diff | absolute_diff | ratio |
|---|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|
| bucket_change_profile | top_absolute_diff | 1 | bybit |  |  | trade_count_per_second.absolute_change | -29.4500 | 15.7500 | -45.2000 | 45.2000 | -1.8698 |
| bucket_change_profile | top_absolute_diff | 2 | bybit |  |  | avg_book_imbalance.relative_change | 43.3474 | -0.932297 | 44.2796 | 44.2796 | -46.4952 |
| bucket_change_profile | top_absolute_diff | 3 | okx |  |  | trade_count_per_second.absolute_change | 25.8444 | -16.6500 | 42.4944 | 42.4944 | -1.5522 |
| bucket_change_profile | top_absolute_diff | 4 | bybit |  |  | trade_count_per_second.absolute_change | 22.9167 | -6.5500 | 29.4667 | 29.4667 | -3.4987 |
| bucket_change_profile | top_absolute_diff | 5 | bybit |  |  | trade_count_per_second.absolute_change | 8.6278 | -17.1167 | 25.7444 | 25.7444 | -0.504057 |
| bucket_change_profile | top_absolute_diff | 6 | okx |  |  | trade_count_per_second.absolute_change | -18.2000 | 5.7500 | -23.9500 | 23.9500 | -3.1652 |
| bucket_change_profile | top_absolute_diff | 7 | okx |  |  | trade_count_per_second.absolute_change | -27.3833 | -6.9000 | -20.4833 | 20.4833 | 3.9686 |
| bucket_change_profile | top_absolute_diff | 8 | binance |  |  | trade_count_per_second.absolute_change | 4.7722 | -13.5667 | 18.3389 | 18.3389 | -0.351761 |
| bucket_change_profile | top_absolute_diff | 9 | binance |  |  | avg_book_imbalance.relative_change | 18.0082 | 0.129575 | 17.8786 | 17.8786 | 138.9793 |
| bucket_change_profile | top_absolute_diff | 10 | okx |  |  | trade_count_per_second.absolute_change | 16.9667 | 5.2833 | 11.6833 | 11.6833 | 3.2114 |
| bucket_change_profile | top_absolute_diff | 11 | binance |  |  | trade_count_per_second.absolute_change | -10.9500 | -4.4000 | -6.5500 | 6.5500 | 2.4886 |
| bucket_change_profile | top_absolute_diff | 12 | bybit |  |  | avg_book_imbalance.relative_change | 0.304073 | -4.0371 | 4.3411 | 4.3411 | -0.075320 |
| bucket_change_profile | top_absolute_diff | 13 | bybit |  |  | trade_count_per_second.absolute_change | 0.716667 | -2.0833 | 2.8000 | 2.8000 | -0.344000 |
| bucket_change_profile | top_absolute_diff | 14 | binance |  |  | avg_book_imbalance.relative_change | -2.0728 | 0.639978 | -2.7128 | 2.7128 | -3.2389 |
| bucket_change_profile | top_absolute_diff | 15 | okx |  |  | trade_count_per_second.relative_change | 1.9473 | -0.436817 | 2.3841 | 2.3841 | -4.4578 |
| bucket_change_profile | top_absolute_diff | 16 | binance |  |  | trade_count_per_second.absolute_change | 10.7833 | 8.4667 | 2.3167 | 2.3167 | 1.2736 |
| bucket_change_profile | top_absolute_diff | 17 | bybit |  |  | trade_count_per_second.relative_change | -0.643716 | 1.0396 | -1.6833 | 1.6833 | -0.619193 |
| bucket_change_profile | top_absolute_diff | 18 | okx |  |  | avg_book_imbalance.relative_change | 1.0319 | -0.331541 | 1.3634 | 1.3634 | -3.1123 |
| bucket_change_profile | top_absolute_diff | 19 | bybit |  |  | trade_count_per_second.relative_change | 1.0036 | -0.301843 | 1.3055 | 1.3055 | -3.3251 |
| bucket_change_profile | top_absolute_diff | 20 | binance |  |  | avg_book_imbalance.relative_change | 1.2346 | 0.076358 | 1.1583 | 1.1583 | 16.1688 |

### bucket_change_profile_top_diffs / top_signed_positive rows=20
| report_group | rank_type | rank | exchange | exchange_pair | window_label | metric_name | event_value | normal_value | signed_diff | absolute_diff | ratio |
|---|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|
| bucket_change_profile | top_signed_positive | 1 | bybit |  |  | avg_book_imbalance.relative_change | 43.3474 | -0.932297 | 44.2796 | 44.2796 | -46.4952 |
| bucket_change_profile | top_signed_positive | 2 | okx |  |  | trade_count_per_second.absolute_change | 25.8444 | -16.6500 | 42.4944 | 42.4944 | -1.5522 |
| bucket_change_profile | top_signed_positive | 3 | bybit |  |  | trade_count_per_second.absolute_change | 22.9167 | -6.5500 | 29.4667 | 29.4667 | -3.4987 |
| bucket_change_profile | top_signed_positive | 4 | bybit |  |  | trade_count_per_second.absolute_change | 8.6278 | -17.1167 | 25.7444 | 25.7444 | -0.504057 |
| bucket_change_profile | top_signed_positive | 5 | binance |  |  | trade_count_per_second.absolute_change | 4.7722 | -13.5667 | 18.3389 | 18.3389 | -0.351761 |
| bucket_change_profile | top_signed_positive | 6 | binance |  |  | avg_book_imbalance.relative_change | 18.0082 | 0.129575 | 17.8786 | 17.8786 | 138.9793 |
| bucket_change_profile | top_signed_positive | 7 | okx |  |  | trade_count_per_second.absolute_change | 16.9667 | 5.2833 | 11.6833 | 11.6833 | 3.2114 |
| bucket_change_profile | top_signed_positive | 8 | bybit |  |  | avg_book_imbalance.relative_change | 0.304073 | -4.0371 | 4.3411 | 4.3411 | -0.075320 |
| bucket_change_profile | top_signed_positive | 9 | bybit |  |  | trade_count_per_second.absolute_change | 0.716667 | -2.0833 | 2.8000 | 2.8000 | -0.344000 |
| bucket_change_profile | top_signed_positive | 10 | okx |  |  | trade_count_per_second.relative_change | 1.9473 | -0.436817 | 2.3841 | 2.3841 | -4.4578 |
| bucket_change_profile | top_signed_positive | 11 | binance |  |  | trade_count_per_second.absolute_change | 10.7833 | 8.4667 | 2.3167 | 2.3167 | 1.2736 |
| bucket_change_profile | top_signed_positive | 12 | okx |  |  | avg_book_imbalance.relative_change | 1.0319 | -0.331541 | 1.3634 | 1.3634 | -3.1123 |
| bucket_change_profile | top_signed_positive | 13 | bybit |  |  | trade_count_per_second.relative_change | 1.0036 | -0.301843 | 1.3055 | 1.3055 | -3.3251 |
| bucket_change_profile | top_signed_positive | 14 | binance |  |  | avg_book_imbalance.relative_change | 1.2346 | 0.076358 | 1.1583 | 1.1583 | 16.1688 |
| bucket_change_profile | top_signed_positive | 15 | okx |  |  | trade_count_per_second.relative_change | 1.4460 | 0.362700 | 1.0833 | 1.0833 | 3.9868 |
| bucket_change_profile | top_signed_positive | 16 | bybit |  |  | trade_count_per_second.relative_change | 0.639621 | -0.418500 | 1.0581 | 1.0581 | -1.5284 |
| bucket_change_profile | top_signed_positive | 17 | okx |  |  | avg_book_imbalance.relative_change | 0.440950 | -0.616301 | 1.0573 | 1.0573 | -0.715479 |
| bucket_change_profile | top_signed_positive | 18 | binance |  |  | avg_book_imbalance.relative_change | -1.7662 | -2.6533 | 0.887017 | 0.887017 | 0.665688 |
| bucket_change_profile | top_signed_positive | 19 | binance |  |  | trade_count_per_second.relative_change | 0.419434 | -0.386882 | 0.806316 | 0.806316 | -1.0841 |
| bucket_change_profile | top_signed_positive | 20 | okx |  |  | avg_book_imbalance.absolute_change | 0.547384 | -0.105865 | 0.653249 | 0.653249 | -5.1706 |

### bucket_change_profile_top_diffs / top_signed_negative rows=20
| report_group | rank_type | rank | exchange | exchange_pair | window_label | metric_name | event_value | normal_value | signed_diff | absolute_diff | ratio |
|---|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|
| bucket_change_profile | top_signed_negative | 1 | bybit |  |  | trade_count_per_second.absolute_change | -29.4500 | 15.7500 | -45.2000 | 45.2000 | -1.8698 |
| bucket_change_profile | top_signed_negative | 2 | okx |  |  | trade_count_per_second.absolute_change | -18.2000 | 5.7500 | -23.9500 | 23.9500 | -3.1652 |
| bucket_change_profile | top_signed_negative | 3 | okx |  |  | trade_count_per_second.absolute_change | -27.3833 | -6.9000 | -20.4833 | 20.4833 | 3.9686 |
| bucket_change_profile | top_signed_negative | 4 | binance |  |  | trade_count_per_second.absolute_change | -10.9500 | -4.4000 | -6.5500 | 6.5500 | 2.4886 |
| bucket_change_profile | top_signed_negative | 5 | binance |  |  | avg_book_imbalance.relative_change | -2.0728 | 0.639978 | -2.7128 | 2.7128 | -3.2389 |
| bucket_change_profile | top_signed_negative | 6 | bybit |  |  | trade_count_per_second.relative_change | -0.643716 | 1.0396 | -1.6833 | 1.6833 | -0.619193 |
| bucket_change_profile | top_signed_negative | 7 | okx |  |  | trade_count_per_second.relative_change | -0.634146 | 0.289673 | -0.923819 | 0.923819 | -2.1892 |
| bucket_change_profile | top_signed_negative | 8 | bybit |  |  | avg_book_imbalance.relative_change | -2.3187 | -1.5216 | -0.797121 | 0.797121 | 1.5239 |
| bucket_change_profile | top_signed_negative | 9 | binance |  |  | trade_count_per_second.absolute_change | -6.8833 | -6.2667 | -0.616667 | 0.616667 | 1.0984 |
| bucket_change_profile | top_signed_negative | 10 | binance |  |  | avg_spread_bps.relative_change | -0.027784 | 0.373341 | -0.401126 | 0.401126 | -0.074421 |
| bucket_change_profile | top_signed_negative | 11 | okx |  |  | avg_book_imbalance.absolute_change | -0.888381 | -0.487371 | -0.401010 | 0.401010 | 1.8228 |
| bucket_change_profile | top_signed_negative | 12 | okx |  |  | trade_count_per_second.relative_change | -0.700043 | -0.321429 | -0.378614 | 0.378614 | 2.1779 |
| bucket_change_profile | top_signed_negative | 13 | binance |  |  | trade_count_per_second.relative_change | -0.546135 | -0.185654 | -0.360481 | 0.360481 | 2.9417 |
| bucket_change_profile | top_signed_negative | 14 | binance |  |  | avg_book_imbalance.absolute_change | -0.273220 | 0.086221 | -0.359442 | 0.359442 | -3.1688 |
| bucket_change_profile | top_signed_negative | 15 | okx |  |  | avg_spread_bps.relative_change | -0.327548 | -0.016175 | -0.311373 | 0.311373 | 20.2502 |
| bucket_change_profile | top_signed_negative | 16 | bybit |  |  | avg_book_imbalance.absolute_change | 0.067479 | 0.218133 | -0.150653 | 0.150653 | 0.309350 |
| bucket_change_profile | top_signed_negative | 17 | binance |  |  | trade_count_per_second.relative_change | -0.426213 | -0.291473 | -0.134740 | 0.134740 | 1.4623 |
| bucket_change_profile | top_signed_negative | 18 | okx |  |  | avg_book_imbalance.absolute_change | 0.115208 | 0.182931 | -0.067723 | 0.067723 | 0.629788 |
| bucket_change_profile | top_signed_negative | 19 | binance |  |  | avg_spread_bps.relative_change | -0.038069 | 0.012561 | -0.050631 | 0.050631 | -3.0307 |
| bucket_change_profile | top_signed_negative | 20 | okx |  |  | avg_spread_bps.relative_change | -0.129355 | -0.091400 | -0.037955 | 0.037955 | 1.4153 |

PHASE2H_COMPLETE
