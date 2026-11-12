# Batch/load validation — 2026-11-12

Validated against the local PostgreSQL + MinIO core stack with the non-root generator
credentials. Host tests used Python 3.12; container execution used Python 3.11.11.

## Automated checks

- `make format check`: all linters and 23 tests passed.
- `make config-core`: Compose validates.
- `make build-generator`: image builds from pinned runtime/dependencies.
- `make init-core`: TEMPORARY privilege bootstrap succeeds without deleting data.
- `make gen-sources verify-sources`: completed twice for the identical config.
- Second load reports changed=0 in all five relational tables.

Dataset run_id: `b1e64afaff0176de180a`.

| Source | Physical JSONL rows | Unique orders | Duplicate rows |
| --- | ---: | ---: | ---: |
| Amazon | 3,120 | 3,000 | 120 |
| Lazada | 3,120 | 3,000 | 120 |
| Shopee | 3,120 | 3,000 | 120 |
| TikTok Shop | 3,120 | 3,000 | 120 |
| Walmart | 3,120 | 3,000 | 120 |
| Total | 15,600 | 15,000 | 600 |

| Walmart table | Reconciled rows | Changed on second load |
| --- | ---: | ---: |
| customers | 200 | 0 |
| products | 20 | 0 |
| orders | 3,000 | 0 |
| order_items | 3,000 | 0 |
| returns | 300 | 0 |

S3 verification uses boto3 to stream every object, recompute SHA-256/bytes/row count,
compare the complete object inventory for the selected run and verify the completion
manifest. SQL verification uses temporary staging and compares every exported column
against matching source rows; it does not upsert business data.

Unit tests independently measure duplicate/skew/return counts, byte-identical replay,
channel schema drift, timezone/currency behavior, exact amount arithmetic, config
round-trip identity, corruption detection, publication ordering and failed uploads.

Runtime logs included:

```text
{"event": "reconciliation_passed", "remote": true, "database": true, "run_id": "b1e64afaff0176de180a"}
```

No credentials or full synthetic payloads are included in this report. Raw data remains
in MinIO and the generator named volume, not Git. This dataset is ready for the first
Bronze pipeline. Million-row performance, stream late/dedup/burst and CDC remain
unimplemented/unmeasured; the entire original Phase 2 checklist is not marked complete.
