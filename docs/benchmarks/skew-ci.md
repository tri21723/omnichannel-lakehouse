### Experiment `ci`

- Commit `b8e6cad8ae10`, image `sha256:b0aaea1c5310`, config hash `d1c7b477127a`.
- Repetitions 1 (+0 warm-up), timeout 900 s, retried artifacts 0.

| Dataset | Workload | Variant | ok/total | Query s median (min–max) | Speedup vs B0 | Paired speedup median (min–max) | Shuffle-stage task max/median | Slowest shuffle task s | Shuffle write MiB | Spill MiB (mem/disk) | Final plan |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ci_control | w1_join_agg | B0 | 1/1 | 10.54 (10.54–10.54) | 1.00 | 1.00 (1.00–1.00) | 6.12 | 1.47 | 2.7 | 0.0/0.0 | sort_merge_join |
| ci_control | w1_join_agg | B1T | 1/1 | 11.07 (11.07–11.07) | 0.95 | 0.95 (0.95–0.95) | 7.59 | 1.68 | 2.7 | 0.0/0.0 | sort_merge_join |
| ci_control | w1_join_agg | B2 | 1/1 | 9.10 (9.10–9.10) | 1.16 | 1.16 (1.16–1.16) | 4.12 | 0.76 | 1.7 | 0.0/0.0 | broadcast_hash_join |
| ci_control | w1_join_agg | B3 | 1/1 | 13.21 (13.21–13.21) | 0.80 | 0.80 (0.80–0.80) | 6.49 | 1.56 | 3.7 | 0.0/0.0 | sort_merge_join |
| ci_control | w2_agg | B0 | 1/1 | 8.44 (8.44–8.44) | 1.00 | 1.00 (1.00–1.00) | 2.91 | 0.57 | 1.5 | 0.0/0.0 | n/a |
| ci_control | w2_agg | B1T | 1/1 | 8.53 (8.53–8.53) | 0.99 | 0.99 (0.99–0.99) | 5.43 | 0.89 | 1.5 | 0.0/0.0 | n/a |
| ci_skew80 | w1_join_agg | B0 | 1/1 | 9.74 (9.74–9.74) | 1.00 | 1.00 (1.00–1.00) | 6.27 | 1.31 | 2.6 | 0.0/0.0 | sort_merge_join |
| ci_skew80 | w1_join_agg | B1T | 1/1 | 10.26 (10.26–10.26) | 0.95 | 0.95 (0.95–0.95) | 7.24 | 1.43 | 2.6 | 0.0/0.0 | sort_merge_join |
| ci_skew80 | w1_join_agg | B2 | 1/1 | 9.44 (9.44–9.44) | 1.03 | 1.03 (1.03–1.03) | 3.64 | 0.71 | 1.5 | 0.0/0.0 | broadcast_hash_join |
| ci_skew80 | w1_join_agg | B3 | 1/1 | 12.10 (12.10–12.10) | 0.81 | 0.81 (0.81–0.81) | 11.20 | 2.15 | 3.1 | 0.0/0.0 | sort_merge_join |
| ci_skew80 | w2_agg | B0 | 1/1 | 8.16 (8.16–8.16) | 1.00 | 1.00 (1.00–1.00) | 4.83 | 0.60 | 1.4 | 0.0/0.0 | n/a |
| ci_skew80 | w2_agg | B1T | 1/1 | 7.67 (7.67–7.67) | 1.06 | 1.06 (1.06–1.06) | 4.57 | 0.72 | 1.4 | 0.0/0.0 | n/a |
