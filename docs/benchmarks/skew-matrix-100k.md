### Experiment `matrix-100k`

- Commit `b8e6cad8ae10`, image `sha256:b0aaea1c5310`, config hash `1697af3ced3f`.
- Repetitions 3 (+1 warm-up), timeout 1200 s, retried artifacts 0.

| Dataset | Workload | Variant | ok/total | Query s median (min–max) | Speedup vs B0 | Paired speedup median (min–max) | Shuffle-stage task max/median | Slowest shuffle task s | Shuffle write MiB | Spill MiB (mem/disk) | Final plan |
|---|---|---|---|---|---|---|---|---|---|---|---|
| m105k_control | w1_join_agg | B0 | 3/3 | 13.32 (11.06–14.23) | 1.00 | 1.00 (1.00–1.00) | 7.03 | 1.91 | 17.5 | 0.0/0.0 | sort_merge_join |
| m105k_control | w1_join_agg | B1 | 3/3 | 15.03 (14.16–22.40) | 0.89 | 0.74 (0.64–0.94) | 5.85 | 2.33 | 17.5 | 0.0/0.0 | sort_merge_join |
| m105k_control | w1_join_agg | B1T | 3/3 | 12.67 (10.73–19.75) | 1.05 | 0.87 (0.72–1.24) | 8.29 | 2.23 | 17.5 | 0.0/0.0 | sort_merge_join |
| m105k_control | w1_join_agg | B2 | 3/3 | 9.54 (9.46–14.51) | 1.40 | 1.16 (0.98–1.41) | 3.83 | 0.68 | 10.1 | 0.0/0.0 | broadcast_hash_join |
| m105k_control | w1_join_agg | B3 | 3/3 | 13.25 (12.49–20.30) | 1.00 | 0.83 (0.70–1.07) | 8.56 | 1.78 | 20.6 | 0.0/0.0 | sort_merge_join |
| m105k_control | w1_join_agg | B4_64 | 3/3 | 14.59 (13.64–20.90) | 0.91 | 0.76 (0.68–0.98) | 13.32 | 0.97 | 18.4 | 0.0/0.0 | sort_merge_join |
| m105k_control | w1_join_agg | P0 | 3/3 | 11.25 (7.60–11.56) | 1.18 | 1.26 (1.15–1.46) | 1.11 | 1.54 | 10.0 | 0.0/0.0 | broadcast_hash_join, aqe_shuffle_read |
| m105k_skew80 | w1_join_agg | B0 | 3/3 | 9.69 (8.91–12.29) | 1.00 | 1.00 (1.00–1.00) | 6.92 | 0.48 | 17.1 | 0.0/0.0 | sort_merge_join |
| m105k_skew80 | w1_join_agg | B1 | 3/3 | 12.49 (12.34–12.97) | 0.78 | 0.79 (0.71–0.95) | 8.57 | 1.89 | 17.1 | 0.0/0.0 | sort_merge_join |
| m105k_skew80 | w1_join_agg | B1T | 3/3 | 12.88 (12.60–14.22) | 0.75 | 0.71 (0.68–0.95) | 8.94 | 1.91 | 17.1 | 0.0/0.0 | sort_merge_join |
| m105k_skew80 | w1_join_agg | B2 | 3/3 | 12.16 (10.81–12.18) | 0.80 | 0.90 (0.73–1.01) | 5.09 | 1.16 | 9.5 | 0.0/0.0 | broadcast_hash_join |
| m105k_skew80 | w1_join_agg | B3 | 3/3 | 13.05 (11.38–15.11) | 0.74 | 0.81 (0.68–0.85) | 6.13 | 0.71 | 19.0 | 0.0/0.0 | sort_merge_join |
| m105k_skew80 | w1_join_agg | B4_64 | 3/3 | 16.44 (12.47–17.77) | 0.59 | 0.75 (0.50–0.78) | 8.62 | 0.99 | 18.1 | 0.0/0.0 | sort_merge_join |
| m105k_skew80 | w1_join_agg | P0 | 3/3 | 9.30 (8.67–10.14) | 1.04 | 1.04 (0.88–1.42) | 1.06 | 1.40 | 9.4 | 0.0/0.0 | broadcast_hash_join, aqe_shuffle_read |
