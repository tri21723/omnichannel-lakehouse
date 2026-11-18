# TODO

Cập nhật: **2026-11-18**.

Phase 5 (Orchestration với Airflow) đã hoàn thành local (`v0.5-orchestration`):
[checklist đã lưu](docs/phase5-checklist.md) · [kiểm chứng](docs/orchestration-validation.md) ·
[ADR-0013](docs/adr/0013-airflow-orchestration.md) · [bàn giao Phase 6](docs/phase6-handoff.md).

## Tiếp theo

- [ ] Lập kế hoạch chi tiết Phase 6 (Trino + DBeaver) dựa trên [bàn giao Phase 6](docs/phase6-handoff.md)
  và `docs/plan.md`; chưa triển khai.
- [x] Hosted `Scaffold checks` (job `check` + `dags`) xanh trên `b528ae3`:
  [run 44335729476](https://github.com/tri21723/omnichannel-lakehouse/actions/runs/44335729476).
- [ ] Chạy thủ công `Airflow orchestration e2e` và `Skew benchmark smoke` trên GitHub; ghi run URL.
