# CLAUDE.md

Claude Code đọc file này tự động. Nguồn quy tắc chính là `AGENTS.md` (import bên dưới);
file này chỉ bổ sung ghi chú riêng cho Claude Code, không lặp lại quy tắc.

@AGENTS.md

## Trạng thái hiện tại

- Phase đang làm: xem `TODO.md` (checklist phase hiện hành) và `docs/implementation-status.md`.
- Tag đã có: `v0.3-phase3`, `v0.4a-bronze` … `v0.4f-benchmark`, `v0.5-orchestration`, `v0.5.1-orchestration`, `v0.6-query`, `v0.7-stream`, `v0.8-cdc`, `v0.9-governance`. Checklist phase cũ lưu ở
  `docs/phase*-checklist.md`; bằng chứng kiểm chứng ở `docs/*-validation.md`.
- Các dataset phải cùng tồn tại: small fixture `186ae107d0793d2ee237` (375 orders), default
  reference `b1e64afaff0176de180a` (15.000 orders) và orchestration e2e `5f552019ee41c24af95f`
  (300 orders, nạp qua Airflow). Không ghi đè/xoá để "sửa" sai lệch.
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
- DataHub (`make up-gov`, ADR-0018, UI 127.0.0.1:9002, GMS không có port host) nặng ~3,5 GiB: chỉ chạy
  cùng core + Kafka (+ Airflow khi thử lịch ingestion), không cùng Spark/Flink/Trino. Mật khẩu UI nằm ở
  `.local-data/governance/user.props` (không in ra). Cài CLI DataHub từ `datahub/requirements.lock`.
  Sau khi đổi recipe/lineage/metadata: `make gov-refresh gov-assert`; `make gov-idempotency` phải giữ
  nguyên inventory. Thứ tự task không phải lineage (không phát `inputDatajobs`). UI còn hiện cạnh cũ
  sau khi sửa lineage → `make restart-gov` (cache lineage của GMS).
- CDC (`make up-cdc`, ADR-0017): Kafka Connect không có port host; thử CDC chỉ trên dòng có tiền tố
  riêng (không bao giờ đụng 3 dataset batch), kiểm bằng `make cdc-batch-guard`. Không đổi schema
  `walmart.orders` (Bronze JDBC batch đọc `SELECT *`). Sau khi build lại image Spark phải
  `make up-batch` để worker dùng code mới. Không xoá slot `dbz_walmart` khi chưa hỏi.
- Streaming (`make up-stream`): Kafka 127.0.0.1:9092 (container: `kafka:29092`), kafka-ui 8085,
  Flink 8082; không bật cùng batch/orch/query khi đo Flink. Job Flink viết Java (ADR-0016), đổi
  `flink/jobs` cần `make build-stream`. Mỗi stream run chỉ publish một lần (seed mới để chạy lại);
  nghiệm thu: `make stream-acceptance`. Hằng số watermark ở `Rules.java` và
  `generators/stream/config.py` phải khớp (có test).
- Trino (`make up-query`, 127.0.0.1:8083) chỉ đọc; đổi `docker/query/trino/etc` cần
  `make restart-query` + `make smoke-query`. Không nới `iceberg.security=read_only`.
- Airflow (`make up-orch`, UI 127.0.0.1:8081) cũng là Spark driver; mọi Spark driver (make hoặc
  Airflow) đi qua khóa `/runtime/locks/spark-driver.lock`. Đổi DAG: chỉ cần lưu file (bind mount);
  đổi `spark/`: `make build-orch`. `make test-dags` chạy test DAG trong image Airflow.
- Khi review: đối chiếu với DoD ở `AGENTS.md` §8, checklist trong `TODO.md`, và các
  contract trong ADR liên quan (idempotency, ledger/snapshot pin, DECIMAL, PII, censoring).
- Báo cáo và commit message theo phong cách sẵn có (tài liệu tiếng Việt, commit tiếng Anh
  theo Conventional Commits).
