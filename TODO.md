# TODO — Phase 4E: Xây dựng Data Warehouse Export (`dp4_warehouse`)

Cập nhật: **2026-11-16**. Trạng thái: **Đang triển khai Phase 4E (`P4E-01`..`P4E-06` hoàn tất — 22/46 checklist)**.

Mục tiêu: triển khai job `dp4_warehouse` thuộc phạm vi `dp4` (theo [ADR-0003](docs/adr/0003-data-contracts.md),
[AGENTS.md](AGENTS.md) và [plan](docs/plan.md)), tiêu thụ bất biến một completed Gold star-schema
publication (`/runtime/gold/publication/ledger.json`) theo `snapshot-id`, xuất bản đồng bộ 8 bảng
star schema (`dim_date`, `dim_channel`, `dim_customer`, `dim_product`, `dim_seller`, `fact_orders`,
`fact_order_items`, `fact_returns`) trong cửa sổ trượt 30 ngày `[run_date - 29 days, run_date + 1 day)` UTC
sang PostgreSQL database `sources`, schema `warehouse.*` bằng một giao dịch ACID nguyên tử
(`BEGIN ... COMMIT` / `ROLLBACK`), bảo toàn kiểu `NUMERIC(18,2)` / `currency` / ranh giới PII,
cắt bỏ sạch dữ liệu trượt ra ngoài cửa sổ 30 ngày, và kiểm chứng trên cả hai ma trận dữ liệu
(`186ae107d0793d2ee237` và `b1e64afaff0176de180a`).

Tham chiếu: [plan](docs/plan.md), [data model](docs/data-model.md),
[bàn giao Phase 4D/4E](docs/features-warehouse-handoff.md), [Gold contract](sql/iceberg/gold_contract.sql),
[ADR-0003](docs/adr/0003-data-contracts.md), [ADR-0009](docs/adr/0009-gold-star-schema.md),
[ADR-0010](docs/adr/0010-features-and-return-labels.md).
Lịch sử: [checklist Phase 4C](docs/phase4c-checklist.md), [bằng chứng Gold](docs/gold-validation.md) (`v0.4c-gold`),
[checklist Phase 4D](docs/phase4d-checklist.md), [bằng chứng DP3](docs/dp3-validation.md) (`v0.4d-features`).

## 1. Baseline và các điểm rà soát kiến trúc quan trọng

Gold star schema (`v0.4c-gold`) và Feature Store / Return Labels (`v0.4d-features`) đã hoàn tất kiểm chứng local
với 138 unit tests pass. Hai dataset độc lập cùng tồn tại trong `lh.gold.*`:
- Small fixture (`186ae107d0793d2ee237`): `fact_orders=375`, `fact_order_items=375`, `dim_customer=25`, `dim_product=88`, `dim_seller=88`, `fact_returns=30`, `dim_date=15`, `dim_channel=5`.
- Default reference (`b1e64afaff0176de180a`): `fact_orders=15,000`, `fact_order_items=15,000`, `dim_customer=1,000`, `dim_product=100`, `dim_seller=100`, `fact_returns=1,500`, `dim_date=17`, `dim_channel=5`.

Các điểm chuẩn hóa kiến trúc cho Phase 4E:
1. **Ranh giới trách nhiệm của `dp4_warehouse` (`ADR-0003`):**
   - `dp2` sở hữu các bảng `lh.gold.{dim_*,fact_*}` trên Iceberg và `/runtime/gold/publication/ledger.json`.
   - `dp4_warehouse` chỉ đọc (read-only) 8 bảng `lh.gold.{dim_*,fact_*}` từ snapshot được pin trong `/runtime/gold/publication/ledger.json` và xuất sang PostgreSQL schema `warehouse.*`; tuyệt đối không ghi ngược vào Iceberg hay sửa ledger của `dp2`/`dp3`.
   - `dp4_warehouse` quản lý publication/export ledger riêng tại `/runtime/warehouse/export/ledger.json` (kèm bản lưu bất biến `export-<sha256>.json` và `attempt.json`).
2. **Bảo mật Least-Privilege và Giao dịch ACID nguyên tử trên Driver (`docker/core/roles.psql`, `grants.psql`):**
   - Trong PostgreSQL `sources`, role `spark_writer` là chủ sở hữu (`AUTHORIZATION spark_writer`) của schema `warehouse`, và default privileges đã tự động cấp quyền `SELECT` trên mọi bảng trong `warehouse.*` cho `trino_ro` (phục vụ Trino Phase 6 và DBeaver).
   - Giống mô hình của `dp1_bronze` (`spark/lib/bronze_jdbc.py`), chỉ container driver (`spark-submit`) được nhận biến môi trường `SPARK_WRITER_DB_PASSWORD` và `WAREHOUSE_JDBC_URL`; các worker (`spark-worker1`, `spark-worker2`) không giữ mật khẩu DB.
   - Không dùng `df.write.jdbc` phân tán qua nhiều kết nối độc lập (vì nếu lỗi giữa chừng sẽ để lại dữ liệu dở dang giữa 8 bảng). Thay vào đó, sau khi Spark đọc/lọc/kiểm tra từ Gold, driver mở một kết nối JVM JDBC duy nhất (`conn.setAutoCommit(False)`) để thực thi DDL và thay thế dữ liệu của `_dataset_run_id` trên cả 8 bảng `warehouse.*` trong **một giao dịch PostgreSQL duy nhất (`COMMIT` / `ROLLBACK`)**.
3. **Cửa sổ trượt 30 ngày (`[run_date - 29 days, run_date + 1 day)` UTC — `docs/data-model.md`):**
   - Với ngày chạy `run_date` (`DATE` UTC, mặc định là ngày hoạt động lớn nhất của run trong Gold hoặc truyền qua `--run-date YYYY-MM-DD`), cửa sổ xuất dữ liệu là `[W_start, W_end)` với `W_start = run_date - 29 days 00:00:00 UTC` và `W_end = run_date + 1 day 00:00:00 UTC`.
   - `fact_orders`: chọn các đơn hàng của `_dataset_run_id` có `order_ts >= W_start AND order_ts < W_end`.
   - `fact_order_items`: chọn các dòng hàng có `order_ts >= W_start AND order_ts < W_end` thuộc các đơn hàng trong cửa sổ.
   - `fact_returns`: chọn các dòng hoàn trả có `return_ts >= W_start AND return_ts < W_end` và thuộc các đơn hàng hợp lệ trong `warehouse.fact_orders`.
   - Mọi bản ghi cũ của `_dataset_run_id` trong `warehouse.fact_*` nằm ngoài cửa sổ `[W_start, W_end)` hoặc đã bị retract ở Gold phải bị xóa sạch trong cùng giao dịch khi trượt `run_date` tiến lên.
4. **Ràng buộc khóa chính/khóa ngoại vật lý và Multi-Dataset Coexistence trong `warehouse.*`:**
   - Surrogate keys của `dim_customer`, `dim_product`, `dim_seller`, `fact_orders`, `fact_order_items`, `fact_returns` đã được băm kèm `_dataset_run_id`.
   - Với `warehouse.dim_date` và `warehouse.dim_channel`, khóa chính vật lý được định nghĩa là `(_dataset_run_id, date_key)` và `(_dataset_run_id, channel_key)` để cả hai dataset (`186ae107d0793d2ee237` và `b1e64afaff0176de180a`) cùng tồn tại trong `warehouse.*` mà không xung đột khóa và vẫn đảm bảo 100% `FOREIGN KEY` constraints cấp cơ sở dữ liệu.
5. **Bảo toàn `NUMERIC(18,2)`, `currency` và ranh giới PII:**
   - Mọi cột tiền tệ trong PostgreSQL `warehouse.*` dùng `NUMERIC(18,2)` (`CHECK (>= 0)`), giữ nguyên `currency` gốc (`USD`, `VND`).
   - `warehouse.dim_customer` chỉ lưu `name_hash`, `email_hash`, `_pii_key_version`, tuyệt đối không có PII rõ.

Ngoài phạm vi Phase 4E: benchmark skew quy mô lớn (Phase 4F), Airflow (Phase 5), Trino (Phase 6), streaming/CDC.

## 2. Thứ tự và mốc bàn giao

| Task | Phụ thuộc | Đầu ra | Công sức dự kiến |
| --- | --- | --- | --- |
| P4E-01 | — | Baseline Gold/Postgres, ADR-0011, physical DDL `sql/postgres/003_warehouse.sql` & env wiring | 2–3 giờ |
| P4E-02 | 01 | Reader Gold publication cố định theo snapshot-id và bộ lọc cửa sổ trượt 30 ngày | 2–3 giờ |
| P4E-03 | 02 | Writer JVM JDBC transactional (`spark_writer`) đồng bộ 8 bảng `warehouse.*` trong 1 ACID tx | 4–5 giờ |
| P4E-04 | 03 | Export ledger `/runtime/warehouse/export/ledger.json`, skip idempotent và fault injection rollback | 3–4 giờ |
| P4E-05 | 04 | Đối soát hai chiều Gold Iceberg ↔ PostgreSQL `warehouse.*` (`NUMERIC(18,2)`, FK, `trino_ro`) | 3–4 giờ |
| P4E-06 | 05 | Kiểm thử trượt cửa sổ (window pruning), multi-dataset coexistence, recovery & restart regression | 2–3 giờ |
| P4E-07 | 06 | Makefile, CI allowlist/workflow, runbook, `docs/warehouse-validation.md` và tag `v0.4e-warehouse` | 2–3 giờ |

Tổng **18–25 giờ công**, là ước lượng lập kế hoạch, không phải deadline.

## 3. Checklist triển khai chi tiết

### P4E-01 — Baseline Gold/Postgres, ADR-0011, physical DDL và cấu hình kết nối

- [x] Chạy kiểm tra baseline: `make check`, `make smoke-core`, `make verify-gold`, `make verify-features`, `make verify-labels`.
- [x] Soạn thảo `docs/adr/0011-warehouse-export.md` chốt các quyết định thiết kế cho `dp4_warehouse`:
  - Ranh giới đọc bất biến từ `/runtime/gold/publication/ledger.json` theo `snapshot-id` và ledger riêng tại `/runtime/warehouse/export/ledger.json`.
  - Định nghĩa chính xác cửa sổ trượt 30 ngày `[run_date - 29 days, run_date + 1 day)` UTC cho `fact_orders`, `fact_order_items`, `fact_returns` và cách đồng bộ 5 bảng `dim_*`.
  - Cơ chế ghi transactional qua một kết nối JVM JDBC duy nhất trên driver bằng role `spark_writer` (`conn.setAutoCommit(False)`), đảm bảo rollback 100% nếu bất kỳ bảng nào lỗi.
  - Thiết kế khóa chính/khóa ngoại vật lý trong PostgreSQL `warehouse.*` hỗ trợ multi-dataset coexistence (`_dataset_run_id`).
- [x] Viết `sql/postgres/003_warehouse.sql` định nghĩa DDL idempotent (`CREATE TABLE IF NOT EXISTS warehouse.<table_name>`) cho 8 bảng:
  - `warehouse.dim_date`, `warehouse.dim_channel`, `warehouse.dim_customer`, `warehouse.dim_product`, `warehouse.dim_seller`
  - `warehouse.fact_orders`, `warehouse.fact_order_items`, `warehouse.fact_returns`
  - Đầy đủ `PRIMARY KEY`, `FOREIGN KEY`, `CHECK` constraints cho các cột tiền tệ `NUMERIC(18,2) >= 0`, và indexes trên `(_dataset_run_id, order_ts)`, `(_dataset_run_id, return_ts)`, `(channel, currency)`.
- [x] Cập nhật `docker/compose.batch.yml` (chỉ thêm `SPARK_WRITER_DB_PASSWORD` và `WAREHOUSE_JDBC_URL` vào service `spark-submit`, không thêm vào workers) và `.env.example` (nếu cần), kèm unit test kiểm tra cấu hình môi trường.

**Đạt khi:** ADR-0011, DDL `sql/postgres/003_warehouse.sql` (pass `sqlfluff`) và cấu hình driver JDBC sẵn sàng.
**Commit gợi ý:** `docs(adr): define Phase 4E warehouse export contract and Postgres DDL`.

### P4E-02 — Reader Gold publication theo snapshot-id và bộ lọc cửa sổ trượt 30 ngày

- [x] Tạo `spark/lib/warehouse_input.py` đọc và xác thực `/runtime/gold/publication/ledger.json` (`status == "completed"`, khớp `publication-<sha256>.json`, đủ 8 bảng Gold star schema).
- [x] Đọc 8 bảng `lh.gold.{dim_date,dim_channel,dim_customer,dim_product,dim_seller,fact_orders,fact_order_items,fact_returns}` bằng `option("snapshot-id", ...)`, đối soát summary (`rows`, `h1`, `h2`) khớp với ledger trước khi lọc theo `_dataset_run_id`.
- [x] Xây dựng hàm tính cửa sổ 30 ngày `compute_export_window(run_date)` trả về `[W_start, W_end)` UTC (`W_start = run_date - 29 days 00:00:00 UTC`, `W_end = run_date + 1 day 00:00:00 UTC`) và lọc các bảng `fact_orders`, `fact_order_items`, `fact_returns` của `_dataset_run_id` theo đúng cửa sổ.
- [x] Viết unit tests (`spark/tests/test_warehouse_input.py`) kiểm chứng: từ chối ledger chưa completed/sai digest, tính đúng biên 30 ngày `[run_date - 29 days, run_date + 1 day)`, loại bỏ các dòng ngoài cửa sổ `[W_start, W_end)` và cô lập giữa các `_dataset_run_id`.

**Đạt khi:** `dp4_warehouse` đọc đúng snapshot Gold đã pin và cắt chính xác lát cắt 30 ngày theo `run_date`.
**Commit gợi ý:** `feat(warehouse): read pinned Gold snapshots and slice 30-day window`.

### P4E-03 — Transactional JVM JDBC Writer (`spark_writer`) và job `dp4_warehouse`

- [x] Tạo `spark/lib/warehouse_jdbc.py` quản lý kết nối JVM JDBC (`user="spark_writer"`, `SPARK_WRITER_DB_PASSWORD`, `WAREHOUSE_JDBC_URL`, `setAutoCommit(False)`, timezone `UTC`) và khởi tạo schema/bảng từ `sql/postgres/003_warehouse.sql`.
- [x] Thực thi thay thế nguyên tử (transactional slice replacement) cho `_dataset_run_id` bên trong một giao dịch duy nhất:
  - Xóa dữ liệu cũ của `_dataset_run_id` theo thứ tự khóa ngoại ngược (`fact_returns` → `fact_order_items` → `fact_orders` → `dim_product` / `dim_customer` / `dim_seller` / `dim_channel` / `dim_date`) hoặc xóa các dòng ngoài cửa sổ / upsert idempotent.
  - Ghi batch (`PreparedStatement.addBatch` / `executeBatch`) theo thứ tự khóa ngoại xuôi (`dim_*` trước, `fact_orders` → `fact_order_items` → `fact_returns` sau), giữ chính xác kiểu `BigDecimal` (`NUMERIC(18,2)`), `Timestamp` UTC, `Date`, `Boolean`.
  - Chỉ gọi `conn.commit()` sau khi cả 8 bảng đã ghi và kiểm tra số dòng thành công; nếu có bất kỳ lỗi nào thì `conn.rollback()` ngay lập tức.
- [x] Tạo entrypoint `spark/jobs/dp4_warehouse/main.py` hỗ trợ CLI: `--run-id`, `--run-date` (tùy chọn; mặc định lấy ngày lớn nhất của run trong Gold), `--publication-id`, `--mode full|incremental`, `--force`, `--verify`.
- [x] Viết unit tests (`spark/tests/test_warehouse_jdbc.py`) kiểm chứng thứ tự bảng FK, ánh xạ kiểu dữ liệu `DECIMAL(18,2)` / `TIMESTAMPTZ`, và hành vi rollback khi xảy ra ngoại lệ trước `commit()`.

**Đạt khi:** `dp4_warehouse` đồng bộ cả 8 bảng sang PostgreSQL `warehouse.*` trong một giao dịch ACID duy nhất bằng role `spark_writer`.
**Commit gợi ý:** `feat(warehouse): export 30-day Gold star schema to Postgres transactionally`.

### P4E-04 — Export ledger `/runtime/warehouse/export`, skip idempotent và fault recovery

- [x] Tạo `spark/lib/warehouse_publish.py` sử dụng POSIX `writer_lock` tại `/runtime/warehouse/export`, quản lý `attempt.json`, `ledger.json` và bản lưu bất biến `export-<sha256>.json`.
- [x] Lưu đầy đủ thông tin đối soát trong `ledger.json`: `status`, `runs[<run_id>]` (`gold_publication_id`, `run_date`, `window_start`, `window_end`, `tables`, `updated_at`), và thống kê số dòng / tổng tiền theo từng bảng trong `warehouse.*`.
- [x] Nếu gặp đúng tổ hợp `(_dataset_run_id, _gold_publication_id, run_date)` đã `completed` và kiểm tra read-back từ PostgreSQL `warehouse.*` khớp 100% với ledger, cho phép skip an toàn (`skipped=True`).
- [x] Hỗ trợ tiêm lỗi (`fail_after="dimensions" | "facts" | "completion"`) để kiểm thử:
  - Khi lỗi tại `dimensions` hoặc `facts`: giao dịch PostgreSQL được `ROLLBACK` hoàn toàn (trạng thái các bảng `warehouse.*` trong PostgreSQL giữ nguyên như trước khi chạy), `attempt.json` ghi `failed`, `ledger.json` giữ nguyên từng byte.
  - Khi chạy lại không tiêm lỗi: phục hồi và commit thành công.
- [x] Viết unit tests (`spark/tests/test_warehouse_publish.py`) cho cơ chế lock, skip idempotent, phát hiện thay đổi `_gold_publication_id` / `run_date`, và bảo toàn ledger khi lỗi.

**Đạt khi:** Quá trình export sang PostgreSQL có ledger độc lập, idempotent khi chạy lại và rollback sạch sẽ khi gặp lỗi giữa chừng.
**Commit gợi ý:** `feat(warehouse): manage export ledger idempotent skip and transactional rollback`.

### P4E-05 — Verifier read-only và đối soát hai chiều Gold Iceberg ↔ PostgreSQL `warehouse.*`

- [x] Tích hợp chế độ `--verify` read-only trong `dp4_warehouse` đọc `/runtime/warehouse/export/ledger.json` và truy vấn PostgreSQL `warehouse.*` để xác nhận số dòng, tổng tiền và ràng buộc mà không ghi lại dữ liệu.
- [x] Tạo `spark/jobs/dp4_warehouse/reconcile.py` và `spark/jobs/dp4_warehouse/dq_test.py` kiểm chứng toàn diện:
  - **Row count & Key reconciliation:** số dòng của từng bảng trong `warehouse.{dim_*,fact_*}` khớp 100% với lát cắt `[W_start, W_end)` trong `lh.gold.{dim_*,fact_*}` cho từng `_dataset_run_id`.
  - **Exact monetary reconciliation (`NUMERIC(18,2)`):** tổng `gross_amount`, `discount_amount`, `net_amount` (`fact_orders`), `gross_line_amount`, `net_line_amount` (`fact_order_items`), và `refund_amount` (`fact_returns`) theo từng `(channel, currency)` trong PostgreSQL `warehouse.*` khớp tuyệt đối đến từng xu (`0.00`) với Gold Iceberg.
  - **Referential integrity & PII guard:** 0 orphan foreign keys giữa `fact_*` và `dim_*` trong PostgreSQL; 0 cột PII rõ (`full_name`, `email`, `_raw`); role `trino_ro` đọc được (`SELECT`) toàn bộ 8 bảng `warehouse.*` nhưng bị từ chối mọi lệnh ghi (`INSERT`/`UPDATE`/`DELETE`).

**Đạt khi:** Đối soát hai chiều giữa Iceberg Gold và PostgreSQL `warehouse.*` khớp tuyệt đối về số dòng, khóa và tiền tệ `NUMERIC(18,2)`.
**Commit gợi ý:** `feat(warehouse): add Gold-to-Postgres reconciliation and read-only verifier`.

### P4E-06 — Kiểm thử trượt cửa sổ 30 ngày, multi-dataset coexistence, recovery và regression

- [x] Viết `spark/jobs/dp4_warehouse/recovery_test.py` kiểm chứng trên môi trường thực:
  - **Sliding-window pruning test:** xuất dữ liệu với `run_date = 2026-01-17` (bao phủ toàn bộ đơn `2026-01-01..2026-01-03`), sau đó chạy với `run_date = 2026-02-01` (cửa sổ `[2026-01-03, 2026-02-02)` cắt bỏ ngày `2026-01-01` và `2026-01-02`), xác minh các đơn của `2026-01-01..2026-01-02` bị xóa sạch khỏi `warehouse.fact_*`, rồi khôi phục lại `run_date = 2026-01-17`.
  - **Transactional rollback & recovery test:** tiêm lỗi `fail_after="dimensions"`, `"facts"`, `"completion"`, chứng minh PostgreSQL `warehouse.*` tự động rollback giữ nguyên số dòng đã commit trước đó và `ledger.json` không bị hỏng; chạy lại phục hồi thành công.
- [x] Chạy và đối soát toàn diện trên cả 2 dataset cùng tồn tại trong `warehouse.*`:
  - Small fixture (`186ae107d0793d2ee237`): `375` orders, `375` order_items, `30` returns, `25` customers, `88` products, `88` sellers.
  - Default reference (`b1e64afaff0176de180a`): `15,000` orders, `15,000` order_items, `1,500` returns, `1,000` customers, `100` products, `100` sellers.
  - Tổng cộng trong PostgreSQL `warehouse.*`: `15,375` orders, `15,375` order_items, `1,530` returns, `1,025` customers, `188` products, `188` sellers.
- [x] Thực hiện `make restart-core` và `make restart-lakehouse`, sau đó chạy `make verify-warehouse`, `make reconcile-warehouse` cùng toàn bộ chuỗi regression (`make smoke-core`, `make verify-sources`, `make verify-lakehouse`, `make smoke-bronze`, `make verify-silver`, `make verify-gold`, `make verify-features`, `make verify-labels`).

**Đạt khi:** Cả 2 dataset cùng tồn tại trong PostgreSQL `warehouse.*`, vượt qua bài kiểm tra trượt cửa sổ 30 ngày, transactional rollback, và bền vững sau khi restart.
**Commit gợi ý:** `test(warehouse): verify 30-day window pruning rollback recovery and coexistence`.

### P4E-07 — Makefile, CI, tài liệu và bàn giao sang Phase 4F

- [ ] Mở rộng `Makefile`:
  - Hỗ trợ `JOB=dp4_warehouse` trong `spark-submit`.
  - Thêm các target: `smoke-warehouse`, `verify-warehouse`, `integration-warehouse`, `integration-warehouse-reference`, `reconcile-warehouse`, `test-warehouse-dq`, `test-warehouse-recovery` (giữ nguyên toàn bộ target cũ).
- [ ] Cập nhật `docker/batch/ci_events.py` (allowlist các structured event của `dp4_warehouse`) và cập nhật manual workflow `.github/workflows/gold-dp3.yml` (hoặc thêm `.github/workflows/warehouse.yml`) chạy kiểm thử end-to-end với small fixture và `make down` không xóa volume.
- [ ] Cập nhật `docs/runbook.md`, `docs/data-model.md`, `docs/implementation-status.md`, `README.md`, `docs/features-warehouse-handoff.md` và viết báo cáo bằng chứng `docs/warehouse-validation.md`.
- [ ] Rà soát toàn bộ checkbox trong `TODO.md` theo bằng chứng thực tế, lưu trữ vào `docs/phase4e-checklist.md`, commit theo Conventional Commits và gắn tag `v0.4e-warehouse` khi đạt đủ local DoD.

**Đạt khi:** Đầy đủ công cụ `make`, CI allowlist, runbook, báo cáo kiểm chứng `docs/warehouse-validation.md` và gắn tag `v0.4e-warehouse`.
**Commit gợi ý:** `docs(warehouse): publish Phase 4E validation evidence and close warehouse export`.

## 4. Điều kiện đóng Phase 4E

- [ ] 8 bảng `warehouse.{dim_date,dim_channel,dim_customer,dim_product,dim_seller,fact_orders,fact_order_items,fact_returns}` trong PostgreSQL `sources` có đầy đủ khóa chính, khóa ngoại, kiểm tra `NUMERIC(18,2)` và quyền đọc cho `trino_ro`.
- [ ] Input được pin chặt chẽ từ completed Gold publication ledger (`/runtime/gold/publication/ledger.json`) theo `snapshot-id`; publication của `dp4_warehouse` tách riêng tại `/runtime/warehouse/export/ledger.json`.
- [ ] Cửa sổ trượt 30 ngày `[run_date - 29 days, run_date + 1 day)` UTC hoạt động chính xác: giữ đúng dữ liệu trong cửa sổ và cắt bỏ sạch dữ liệu trượt ra ngoài cửa sổ.
- [ ] Ghi transactional nguyên tử bằng یک kết nối JVM JDBC (`spark_writer`): nếu lỗi giữa chừng thì `ROLLBACK` 100%, không để lại dữ liệu dở dang giữa các bảng.
- [ ] Multi-dataset coexistence (`186ae107d0793d2ee237` và `b1e64afaff0176de180a`), đối soát hai chiều Gold ↔ Postgres khớp từng xu (`0.00`), replay idempotent, recovery và restart persistence đều xanh.
- [ ] `make check` và toàn bộ regression `core`/`sources`/`lakehouse`/`bronze`/`silver`/`gold`/`dp3` đều xanh; tài liệu, ADR-0011, DDL và runbook cập nhật đầy đủ.

## 5. Quy tắc thực hiện

1. Mọi checkbox để trống cho đến khi có implementation và bằng chứng kiểm tra tương ứng.
2. Sau mỗi task nhỏ: chạy kiểm tra liên quan và `make check`, cập nhật `TODO.md`/evidence, rồi tạo commit theo Conventional Commits ngay; không chờ dồn cuối phase.
3. Mục không áp dụng phải ghi rõ `N/A` kèm lý do; mục bị chặn giữ nguyên `[ ]` và ghi rõ blocker.
4. Không đổi version/port đã pin, không xóa volume/bucket/DB, không expire các snapshot đang được pin/ledger tham chiếu.
5. Không commit `.env`, secret, HMAC key, DB password, raw PII hay file dữ liệu runtime.
6. Mọi quyết định thiết kế về cửa sổ 30 ngày, giao dịch JDBC và khóa vật lý trong `warehouse.*` phải được chốt trong ADR-0011 trước khi viết code phụ thuộc.

Sau 4E: **Phase 4F** (benchmark data skew & đóng toàn bộ Phase 4) → **Phase 5** (Airflow orchestration) → **Phase 6** (Trino + DBeaver federated query).
