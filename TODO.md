# TODO

Cập nhật: **2026-11-18**.

Phase 5 (Orchestration với Airflow) đã hoàn thành local (`v0.5-orchestration`):
[checklist đã lưu](docs/phase5-checklist.md) · [kiểm chứng](docs/orchestration-validation.md) ·
[ADR-0013](docs/adr/0013-airflow-orchestration.md) · [bàn giao Phase 6](docs/phase6-handoff.md).

## Tiếp theo

- [ ] Lập kế hoạch chi tiết Phase 6 (Trino + DBeaver) dựa trên [bàn giao Phase 6](docs/phase6-handoff.md)
  và `docs/plan.md`; chưa triển khai.
- [ ] Chụp ảnh Graph/Gantt của `batch_daily` trong UI (http://localhost:8081) để bổ sung bằng chứng.
- [ ] Chạy hosted CI: `Scaffold checks` (job `dags`), `Airflow orchestration e2e`,
  `Skew benchmark smoke`; ghi run URL/commit (hiện chỉ có bằng chứng local).
- [ ] (Tùy chọn) `maintenance_iceberg` dry-run sau khi có ADR retention snapshot.
