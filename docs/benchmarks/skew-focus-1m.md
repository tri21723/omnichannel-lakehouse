### Experiment `focus-1m`

- Commit `db8dd559e5c8`, image `sha256:d19acf3fdfcc`, config hash `ebc4aac9fcb6`.
- Repetitions 7 (+1 warm-up), timeout 1800 s, retried artifacts 0.

| Dataset | Workload | Variant | ok/total | Query s median (min–max) | Speedup vs B0 | Paired speedup median (min–max) | Shuffle-stage task max/median | Slowest shuffle task s | Shuffle write MiB | Spill MiB (mem/disk) | Final plan |
|---|---|---|---|---|---|---|---|---|---|---|---|
| l1m_skew80 | w1_join_agg | B0 | 7/7 | 13.03 (10.20–14.28) | 1.00 | 1.00 (1.00–1.00) | 13.31 | 1.90 | 155.3 | 0.0/0.0 | sort_merge_join |
| l1m_skew80 | w1_join_agg | B1 | 7/7 | 13.20 (12.21–15.20) | 0.99 | 0.94 (0.83–0.99) | 14.12 | 2.08 | 155.3 | 0.0/0.0 | sort_merge_join |
| l1m_skew80 | w1_join_agg | B1T | 7/7 | 16.37 (13.78–20.21) | 0.80 | 0.80 (0.51–0.95) | 13.88 | 2.12 | 155.5 | 0.0/0.0 | sort_merge_join, skew_join, aqe_shuffle_read |
| l1m_skew80 | w1_join_agg | B2 | 7/7 | 12.10 (10.59–18.64) | 1.08 | 0.96 (0.75–1.18) | 3.97 | 1.17 | 84.5 | 0.0/0.0 | broadcast_hash_join |
| l1m_skew80 | w1_join_agg | B3 | 7/7 | 15.85 (13.23–18.91) | 0.82 | 0.81 (0.65–0.88) | 6.33 | 1.57 | 165.5 | 0.0/0.0 | sort_merge_join |
| l1m_skew80 | w1_join_agg | P0 | 7/7 | 10.23 (9.65–14.53) | 1.27 | 1.25 (0.89–1.40) | 1.24 | 2.11 | 84.4 | 0.0/0.0 | broadcast_hash_join, aqe_shuffle_read |
