# Bronze validation — Phase 4A

## Baseline checked 2026-11-13

- make check: 27 tests pass; lint pass.
- make up-batch, verify-runtime, smoke-core, verify-lakehouse, verify-sources: pass.
- Dataset b1e64afaff0176de180a: 15,600 JSONL rows; PostgreSQL Walmart
  customers=200, products=20, orders=3000, order_items=3000, returns=300.
- Existing snapshots remain readable; source verification reports changed=0.
- Host: 11GiB RAM, 8.5GiB available before jobs, 84GiB free workspace disk.
- Small reproducible config: spark/tests/fixtures/bronze.yaml (25 orders/day,
  3 days, all channels, duplicate/drift, chunks of 10). Run ID is derived by generator.

Hosted GitHub Actions remains unverified due to reported account billing lock.
No Phase 4A runtime success is claimed by baseline evidence.
