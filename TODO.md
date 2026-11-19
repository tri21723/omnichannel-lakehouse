# TODO

Cập nhật: **2026-11-19**.

Phase 8 (CDC với Debezium) đã hoàn thành local (`v0.8-cdc`):
[checklist đã lưu](docs/phase8-checklist.md) · [kiểm chứng](docs/cdc-validation.md) ·
[ADR-0017](docs/adr/0017-cdc-debezium.md) · [bàn giao Phase 9](docs/phase9-handoff.md).

## Tiếp theo

- [ ] Lập kế hoạch chi tiết Phase 9 (DataHub) dựa trên [bàn giao Phase 9](docs/phase9-handoff.md) và
  `docs/plan.md`; chưa triển khai.
- [x] `Debezium CDC` xanh trên `f589277` ([44574829186](https://github.com/tri21723/omnichannel-lakehouse/actions/runs/44574829186), 22/22 bước, ~11,5 phút); lần đầu fail khi tải jar
  (Maven Central từ chối IP runner) → thêm mirror Google + pin SHA-256.
