# TODO

Cập nhật: **2026-11-18**.

Phase 6 (Query layer với Trino + DBeaver) đã hoàn thành local (`v0.6-query`):
[checklist đã lưu](docs/phase6-checklist.md) · [kiểm chứng](docs/query-validation.md) ·
[ADR-0015](docs/adr/0015-trino-query-layer.md) · [DBeaver](docs/dbeaver.md) ·
[bàn giao Phase 7](docs/phase7-handoff.md).

## Tiếp theo

- [ ] Lập kế hoạch chi tiết Phase 7 (Kafka + Flink) dựa trên [bàn giao Phase 7](docs/phase7-handoff.md)
  và `docs/plan.md`; chưa triển khai.
- [ ] (Người dùng) Kết nối DBeaver theo [docs/dbeaver.md](docs/dbeaver.md) và gửi ảnh chụp để bổ sung bằng chứng Q3.
- [ ] Chạy thủ công trên GitHub: `Trino query layer`, `Airflow orchestration e2e`,
  `Skew benchmark smoke`; ghi run URL/commit (hiện chỉ có bằng chứng local).
