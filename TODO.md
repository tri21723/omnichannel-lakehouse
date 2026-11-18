# TODO

Cập nhật: **2026-11-18**.

Phase 6 (Query layer với Trino + DBeaver) đã hoàn thành local (`v0.6-query`):
[checklist đã lưu](docs/phase6-checklist.md) · [kiểm chứng](docs/query-validation.md) ·
[ADR-0015](docs/adr/0015-trino-query-layer.md) · [DBeaver](docs/dbeaver.md) ·
[bàn giao Phase 7](docs/phase7-handoff.md).

## Tiếp theo

- [ ] Lập kế hoạch chi tiết Phase 7 (Kafka + Flink) dựa trên [bàn giao Phase 7](docs/phase7-handoff.md)
  và `docs/plan.md`; chưa triển khai.
- [x] DBeaver kết nối và truy vấn thành công (ảnh trong `docs/images/phase6/`).
- [x] Hosted `Scaffold checks` xanh trên `a11d318`:
  [run 44355559275](https://github.com/tri21723/omnichannel-lakehouse/actions/runs/44355559275).
- [ ] Chạy thủ công trên GitHub: `Trino query layer`, `Airflow orchestration e2e`,
  `Skew benchmark smoke`; ghi run URL/commit (hiện chỉ có bằng chứng local).
