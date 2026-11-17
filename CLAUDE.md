# CLAUDE.md

Claude Code đọc file này tự động. Nguồn quy tắc chính là `AGENTS.md` (import bên dưới);
file này chỉ bổ sung ghi chú riêng cho Claude Code, không lặp lại quy tắc.

@AGENTS.md

## Trạng thái hiện tại

- Phase đang làm: xem `TODO.md` (checklist phase hiện hành) và `docs/implementation-status.md`.
- Tag đã có: `v0.3-phase3`, `v0.4a-bronze` … `v0.4e-warehouse`. Checklist phase cũ lưu ở
  `docs/phase*-checklist.md`; bằng chứng kiểm chứng ở `docs/*-validation.md`.
- Hai dataset phải cùng tồn tại: small fixture `186ae107d0793d2ee237` (375 orders) và
  default reference `b1e64afaff0176de180a` (15.000 orders). Không ghi đè/xoá để "sửa" sai lệch.
- `docs/plan.md` là plan gốc, giữ nguyên; điểm khác biệt được chốt trong `docs/adr/`
  (ADR mới nhất quyết định khi mâu thuẫn với plan).

## Ghi chú cho Claude Code

- Repo nằm trên `/mnt/d` (WSL) nên I/O chậm: tránh `find`/`grep` toàn repo khi không cần,
  bỏ qua `.venv/`, `.local-data/`, `__pycache__/`, `.ruff_cache/`.
- `.local-data/` và `.env` chứa dữ liệu/credential local: không đọc nội dung `.env`, không
  in secret ra output, không commit.
- Kiểm tra nhanh không cần Docker: `make check` (lint + unit test). Các target
  `smoke-*`, `integration-*`, `test-*-recovery`, `test-review-regressions` cần
  `make up-batch` và chạy tuần tự (không chạy song song nhiều Spark driver).
- Target chưa triển khai (`up-orch`, `up-query`, `up-stream`, `up-cdc`, `up-gov`,
  `gen-stream`, `e2e`) cố ý `exit 2`.
- Khi review: đối chiếu với DoD ở `AGENTS.md` §8, checklist trong `TODO.md`, và các
  contract trong ADR liên quan (idempotency, ledger/snapshot pin, DECIMAL, PII, censoring).
- Báo cáo và commit message theo phong cách sẵn có (tài liệu tiếng Việt, commit tiếng Anh
  theo Conventional Commits).
