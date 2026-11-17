### Experiment `smoke`

- Commit `b8e6cad8ae10`, image `sha256:b0aaea1c5310`, config hash `61f223621be4`.
- Repetitions 1 (+0 warm-up), timeout 600 s, retried artifacts 24.

| Dataset | Workload | Variant | ok/total | Query s median (min–max) | Speedup vs B0 | Paired speedup median (min–max) | Shuffle-stage task max/median | Slowest shuffle task s | Shuffle write MiB | Spill MiB (mem/disk) | Final plan |
|---|---|---|---|---|---|---|---|---|---|---|---|
| s15k_control | w1_join_agg | B0 | 1/1 | 9.29 (9.29–9.29) | 1.00 | 1.00 (1.00–1.00) | 5.82 | 1.10 | 2.8 | 0.0/0.0 | sort_merge_join |
| s15k_control | w1_join_agg | B1T | 1/1 | 9.55 (9.55–9.55) | 0.97 | 0.97 (0.97–0.97) | 7.08 | 1.05 | 2.8 | 0.0/0.0 | sort_merge_join |
| s15k_control | w1_join_agg | B2 | 1/1 | 9.22 (9.22–9.22) | 1.01 | 1.01 (1.01–1.01) | 3.85 | 0.83 | 1.7 | 0.0/0.0 | broadcast_hash_join |
| s15k_control | w1_join_agg | B3 | 1/1 | 12.81 (12.81–12.81) | 0.72 | 0.72 (0.72–0.72) | 6.14 | 1.32 | 3.8 | 0.0/0.0 | sort_merge_join |
| s15k_control | w2_agg | B0 | 1/1 | 9.04 (9.04–9.04) | 1.00 | 1.00 (1.00–1.00) | 4.48 | 0.94 | 1.5 | 0.0/0.0 | n/a |
| s15k_control | w2_agg | B1T | 1/1 | 8.72 (8.72–8.72) | 1.04 | 1.04 (1.04–1.04) | 3.98 | 0.65 | 1.5 | 0.0/0.0 | n/a |
| s15k_skew80 | w1_join_agg | B0 | 1/1 | 8.56 (8.56–8.56) | 1.00 | 1.00 (1.00–1.00) | 6.00 | 0.51 | 2.7 | 0.0/0.0 | sort_merge_join |
| s15k_skew80 | w1_join_agg | B1T | 1/1 | 10.18 (10.18–10.18) | 0.84 | 0.84 (0.84–0.84) | 5.02 | 1.07 | 2.7 | 0.0/0.0 | sort_merge_join |
| s15k_skew80 | w1_join_agg | B2 | 1/1 | 8.49 (8.49–8.49) | 1.01 | 1.01 (1.01–1.01) | 3.58 | 0.58 | 1.5 | 0.0/0.0 | broadcast_hash_join |
| s15k_skew80 | w1_join_agg | B3 | 1/1 | 9.82 (9.82–9.82) | 0.87 | 0.87 (0.87–0.87) | 6.21 | 0.51 | 3.1 | 0.0/0.0 | sort_merge_join |
| s15k_skew80 | w2_agg | B0 | 1/1 | 8.53 (8.53–8.53) | 1.00 | 1.00 (1.00–1.00) | 3.55 | 0.63 | 1.5 | 0.0/0.0 | n/a |
| s15k_skew80 | w2_agg | B1T | 1/1 | 8.29 (8.29–8.29) | 1.03 | 1.03 (1.03–1.03) | 5.53 | 0.89 | 1.5 | 0.0/0.0 | n/a |
