# TODO

Cập nhật: **2026-11-18**.

Phase 4F (Performance & Data Skew Benchmark) đã hoàn thành và đóng Phase 4:
[checklist đã lưu](docs/phase4f-checklist.md) · [kết quả benchmark](docs/benchmarks/skew.md) ·
[ADR-0012](docs/adr/0012-skew-benchmark.md) · [bàn giao Phase 5](docs/phase5-handoff.md).

## Tiếp theo

- [ ] Lập kế hoạch chi tiết Phase 5 (Airflow, LocalExecutor, `parallelism=1`) dựa trên
  [bàn giao Phase 5](docs/phase5-handoff.md) và `docs/plan.md`; chưa triển khai.
- [ ] Chạy thủ công workflow `Skew benchmark smoke` trên hosted CI và ghi run URL/commit
  (hiện chỉ có bằng chứng local).
