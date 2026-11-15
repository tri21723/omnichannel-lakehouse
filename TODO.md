# TODO — Phase 4C: Xây dựng Gold dimensions và facts

Cập nhật: **2026-11-15**. Trạng thái: **Đã lập kế hoạch; chưa bắt đầu triển khai (0/72 checklist)**.

Mục tiêu: triển khai phần Gold dimensions và facts thuộc phạm vi `dp2` (theo [ADR-0003](docs/adr/0003-data-contracts.md)),
tiêu thụ nhất quán một completed Silver publication theo snapshot ID, tạo star schema
chuẩn trên Iceberg (`lh.gold`), bảo toàn kiểu `DECIMAL(18,2)` / `currency` / ranh giới PII,
hỗ trợ recompute idempotent khi nguồn Silver hiệu chỉnh và đối soát toàn diện 5 kênh.

Tham chiếu: [plan](docs/plan.md), [data model](docs/data-model.md),
[bàn giao Gold](docs/gold-handoff.md), [Silver contract](sql/iceberg/silver_contract.sql),
[ADR-0003](docs/adr/0003-data-contracts.md), [ADR-0008](docs/adr/0008-silver-contract.md).
Lịch sử: [checklist Phase 4B](docs/phase4b-checklist.md),
[bằng chứng Silver](docs/silver-validation.md), tag `v0.4b-silver`.

## 1. Baseline và phạm vi

Silver đã hoàn tất kiểm chứng local (`v0.4b-silver`): 5 entity (`orders`, `order_items`,
`customers`, `products`, `returns`) trên 5 kênh (`walmart`, `amazon`, `tiktok_shop`,
`shopee`, `lazada`), 98 unit tests pass. Hai dataset độc lập cùng tồn tại trong Silver
trên 3 ngày logic (`2026-01-01`..`2026-01-03`):
- Small fixture (`186ae107d0793d2ee237`): `orders=375`, `order_items=375`, `customers=25`, `products=88`, `returns=30`.
- Default reference (`b1e64afaff0176de180a`): `orders=15,000`, `order_items=15,000`, `customers=1,000`, `products=100`, `returns=1,500`.

Phạm vi Phase 4C:
- Chốt ADR-0009 và physical Iceberg DDL (`sql/iceberg/gold_contract.sql`) cho 8 bảng Gold:
  `dim_date`, `dim_channel`, `dim_customer`, `dim_product`, `dim_seller`,
  `fact_orders`, `fact_order_items`, `fact_returns`.
- Đọc đầu vào từ một completed Silver publication (`/runtime/silver/publication/ledger.json`
  và bản lưu bất biến `publication-<sha256>.json`), dùng `option("snapshot-id", ...)`
  cho cả 5 bảng `lh.silver.<entity>` và lọc theo `_dataset_run_id` được chọn rõ ràng.
- Thiết kế surrogate keys deterministic, grain, FK joins luôn kèm `channel`, xử lý
  `dim_seller` dẫn xuất ID-only từ `seller_id` (không tự bịa `seller_name`/`country`),
  giữ nguyên `currency` gốc và ranh giới PII (`name_hash`, `email_hash`, `_pii_key_version`).
- Ghi atomic từng bảng và quản lý Gold publication ledger (`/runtime/gold/publication/ledger.json`),
  đảm bảo recompute từ Silver publication mới thay thế sạch trạng thái của dataset run đó
  (kể cả khi dòng cũ bị retract thành invalid/rỗng) mà không nhân đôi khoá hay ảnh hưởng run khác.

Ngoài phạm vi: `dp3_features`, `dp3_return_labels` (Phase 4D), `dp4_warehouse` export sang
PostgreSQL `warehouse` (Phase 4E), benchmark skew quy mô lớn, Airflow (Phase 5),
Trino (Phase 6), streaming/CDC, DataHub và AWS. Không đổi version/port đã pin.

## 2. Thứ tự và mốc bàn giao

| Task | Phụ thuộc | Đầu ra | Công sức dự kiến |
| --- | --- | --- | --- |
| P4C-01 | — | Baseline Silver, kiểm chứng snapshot và fixture Gold | 1–2 giờ |
| P4C-02 | 01 | ADR-0009, star-schema contract, surrogate keys và DDL | 3–5 giờ |
| P4C-03 | 02 | Reader Silver publication theo snapshot cố định và run isolation | 3–4 giờ |
| P4C-04 | 03 | Xây dựng 5 bảng chiều (`dim_date`, `dim_channel`, `dim_customer`, `dim_product`, `dim_seller`) | 4–6 giờ |
| P4C-05 | 04 | Xây dựng 3 bảng fact (`fact_orders`, `fact_order_items`, `fact_returns`) và DQ tiền tệ | 4–6 giờ |
| P4C-06 | 05 | Gold publication ledger, recompute/retraction và phục hồi lỗi | 4–6 giờ |
| P4C-07 | 06 | Verifier read-only, đối soát 2 dataset (small + default) và regression | 3–5 giờ |
| P4C-08 | 07 | Make/CI/runbook, báo cáo validation và bàn giao Phase 4D/4E | 2–4 giờ |

Tổng **24–38 giờ công**, là ước lượng lập kế hoạch, không phải deadline.
Mốc A: contract và reader (`01–03`); mốc B: dimensions và facts (`04–05`);
mốc C: ledger/recovery, đối soát đủ 5 kênh và đóng local DoD (`06–08`).

## 3. Checklist triển khai

### P4C-01 — Baseline Silver và fixture kiểm thử

- [ ] Đọc `AGENTS.md`, `docs/gold-handoff.md`, các ADR hiện có và kiểm tra `git status`.
- [ ] Chạy `make check`, `make verify-runtime`, `make verify-silver` và `make reconcile-silver`
  cho cả `186ae107d0793d2ee237` (small) và `b1e64afaff0176de180a` (default); ghi nhận baseline.
- [ ] Xác minh `/runtime/silver/publication/ledger.json` có `status=completed`, bản lưu
  `publication-<identity>.json` khớp SHA-256 của `json.dumps(ledger, sort_keys=True).encode()`
  và cả 5 bảng `lh.silver.<entity>` đọc được bằng `snapshot-id`.
- [ ] Chuẩn bị fixture unit/local test cho Gold: đơn hàng nhiều dòng, hoàn trả một phần/nhiều lần,
  hoàn trả khác ngày đặt hàng (`return_ts > order_ts`), nhiều tiền tệ (`USD`, `VND`),
  cùng `order_id`/`customer_id`/`product_id`/`seller_id` ở 2 kênh khác nhau và bản sửa Silver retract dòng cũ.
- [ ] Đảm bảo mọi thử nghiệm mutation/recovery dùng `_dataset_run_id` hoặc thư mục ledger riêng,
  không làm bẩn dữ liệu Silver đã publish hoặc xóa volume.

**Đạt khi:** Baseline Silver được xác nhận nguyên vẹn và có bộ test cases Gold độc lập.
**Commit gợi ý:** `test(gold): establish Silver publication baseline and Gold fixtures`.

### P4C-02 — Chốt contract Gold, surrogate keys và DDL

- [ ] Viết `docs/adr/0009-gold-star-schema.md` và cập nhật `docs/data-model.md`,
  `sql/iceberg/gold_contract.sql` cho 8 bảng trong namespace `lh.gold`.
- [ ] Chốt ranh giới entrypoint thuộc `dp2` (mở rộng `dp2_silver` hoặc module `dp2_gold` dùng chung
  thư viện `spark/lib/gold_*.py`) và tham số CLI: `--run-id`, `--publication-id` (tuỳ chọn,
  mặc định pin từ `ledger.json` hiện hành), `--mode full|incremental`, `--verify`.
- [ ] Chốt quy tắc sinh surrogate keys deterministic (ví dụ SHA-256 canonical theo `_dataset_run_id`
  + `channel` + natural key cho `customer_key`, `product_key`, `seller_key`, `channel_key`;
  `date_key` dạng `INT` `YYYYMMDD` hoặc `DATE` UTC) để chạy lại luôn cho cùng khoá.
- [ ] Chốt thiết kế `dim_seller`: dẫn xuất ID-only (`seller_key`, `channel`, `seller_id`,
  audit fields) từ tập hợp `seller_id` hợp lệ trong `silver.products` và `silver.order_items`;
  tuyệt đối không tự suy diễn `seller_name` hay `country`.
- [ ] Chốt thiết kế `dim_date` (UTC calendar attributes: `date_key`, `calendar_date`, `year`,
  `quarter`, `month`, `day_of_month`, `day_of_week`, `is_weekend`) bao phủ đầy đủ mọi ngày
  xuất hiện trong `order_ts`, `first_order_ts` và `return_ts` của dataset run.
- [ ] Chốt thiết kế `dim_channel` (5 kênh chuẩn kèm thuộc tính tĩnh có kiểm soát như
  `channel_key`, `channel`, `source_Format`/` ingestion_mode` hoặc metadata được định nghĩa rõ).
- [ ] Chốt grain và measures cho 3 bảng fact:
  - `fact_orders`: 1 dòng / `(channel, order_id)`; giữ `gross_amount`, `discount_amount`,
    `net_amount` (`total_amount`), `item_count`, `total_qty`, `currency`, `status`, `is_deleted`.
  - `fact_order_items`: 1 dòng / `(channel, order_id, line_no)`; giữ `qty`, `unit_price`,
    `discount`, `gross_line_amount` (`qty * unit_price`), `net_line_amount` (`qty * unit_price - discount`), `currency`.
  - `fact_returns`: 1 dòng / `(channel, return_id, return_line_no)`; liên kết về order line,
    `order_date_key`, `return_date_key`, `refund_amount`, `currency`, `reason`.
- [ ] Chốt cách tránh nhân bản số liệu (fan-out/chasm trap) khi kết hợp order facts và return facts:
  tổng hợp hoàn trả về đúng grain trước khi đối soát hoặc giữ tách biệt ở `fact_returns`.
- [ ] Chốt các cột audit/provenance Gold (`_dataset_run_id`, `_silver_publication_id`,
  `_silver_snapshot`, `_record_hash`, `_policy_version`) và cấu hình bảng Iceberg v2 / Parquet / ZSTD / partitioning.

**Đạt khi:** Schema, surrogate keys, công thức tiền tệ và cơ chế pin publication được định nghĩa rõ ràng trước khi viết code transform.
**Commit gợi ý:** `docs(adr): define Gold star schema and publication contract`.

### P4C-03 — Reader Silver publication cố định và cô lập dataset run

- [ ] Tạo `spark/lib/gold_input.py` đọc `/runtime/silver/publication/ledger.json`, kiểm tra
  `status == "completed"`, đầy đủ 15 bảng Silver và tính `publication_id` đúng chuẩn
  `hashlib.sha256(json.dumps(ledger, sort_keys=True).encode()).hexdigest()`.
- [ ] Xác minh file bất biến `publication-<publication_id>.json` tồn tại và khớp nội dung;
  từ chối đọc từ `attempt.json` hoặc trạng thái `running`/`failed`.
- [ ] Pin input cho Gold attempt; nếu Silver publish thêm slice mới trong lúc Gold đang chạy,
  Gold vẫn chỉ đọc đúng bộ `snapshot-id` của `publication_id` đã pin.
- [ ] Đọc 5 bảng `lh.silver.{orders,order_items,customers,products,returns}` bằng
  `option("snapshot-id", ...)`; xử lý `snapshot = None` thành DataFrame rỗng có kiểu chỉ khi
  `rows == "0"`, và báo lỗi rõ nếu snapshot bị thiếu/expire.
- [ ] Đối soát summary (`rows`, `h1`, `h2`) của từng bảng Silver đọc từ snapshot khớp với
  `evidence["content"]` trong ledger trước khi lọc theo `_dataset_run_id`.
- [ ] Lọc chặt chẽ theo `_dataset_run_id == args.run_id`; từ chối nếu `_dataset_run_id` không
  có bất kỳ slice nào trong `ledger["slices"]` (trừ trường hợp toàn bộ slice của run đó đều là
  retraction/empty slice hợp lệ đã được ghi nhận trong `ledger["slices"]`).
- [ ] Viết unit/integration test cho các trường hợp: ledger chưa completed, sai hash archive,
  snapshot không tồn tại, Silver cập nhật bảng sau khi Gold đã pin, và cô lập giữa small run với default run.

**Đạt khi:** Đầu vào Gold bất biến theo một Silver publication đã hoàn tất và không bao giờ trộn lẫn hai dataset run.
**Commit gợi ý:** `feat(gold): read pinned Silver publication snapshots by dataset run`.

### P4C-04 — Xây dựng 5 bảng chiều (`dim_*`)

- [ ] Xây dựng `dim_channel` và `dim_date` deterministic theo phạm vi ngày thực tế của run
  (bao gồm cả ngày của `order_ts`, `first_order_ts`, `return_ts`, kể cả khi `return_ts` rơi vào sau `run_date`).
- [ ] Xây dựng `dim_customer` từ `lh.silver.customers`: giữ nguyên `name_hash`, `email_hash`,
  `_pii_key_version`, `country`, `first_order_ts`, `updated_ts`; đảm bảo không có `full_name`/`email` rõ.
- [ ] Xây dựng `dim_product` từ `lh.silver.products`: giữ `title`, `category`, `brand` (nullable),
  `seller_id`, `seller_key`, `updated_ts`.
- [ ] Xây dựng `dim_seller` dẫn xuất từ tập hợp `(channel, seller_id)` của `products` và `order_items`
  trong run; kiểm tra tính duy nhất của `( _dataset_run_id, channel, seller_id )` và `seller_key`.
- [ ] Đảm bảo mọi khoá tự nhiên và surrogate key đều phân tách theo `channel` (cùng ID gốc ở
  2 kênh khác nhau phải sinh ra 2 dòng dimension với 2 surrogate key khác nhau).
- [ ] Kiểm tra không có NULL trên surrogate keys, natural keys hoặc cột bắt buộc; kiểm tra
  chính sách current-state profile (không nhân bản lịch sử SCD2 ngoài phạm vi v1).
- [ ] Viết unit test kiểm chứng schema, surrogate keys, `dim_date` boundary (bao gồm ngày hoàn trả muộn)
  và tính bảo mật PII của `dim_customer`.

**Đạt khi:** 5 bảng `dim_*` tạo ra tập khoá duy nhất, đầy đủ để join 100% với các bảng fact.
**Commit gợi ý:** `feat(gold): build deterministic dimensions for date channel customer product and seller`.

### P4C-05 — Xây dựng 3 bảng sự kiện (`fact_*`) và đối soát tiền tệ

- [ ] Xây dựng `fact_order_items` từ `lh.silver.order_items` join với `lh.silver.orders` (để lấy
  `order_ts`, `order_date_key`, `customer_key`, `currency`, `status`) và join `dim_product`, `dim_seller`, `dim_channel`.
- [ ] Tính chính xác bằng `DECIMAL(18,2)`: `gross_line_amount = qty * unit_price`,
  `discount_amount = discount`, `net_line_amount = qty * unit_price - discount`; không dùng float.
- [ ] Xây dựng `fact_orders` từ `lh.silver.orders` kết hợp tổng hợp ở grain `(channel, order_id)`
  từ `fact_order_items` (`item_count`, `total_qty`, `gross_amount`, `discount_amount`, `net_amount`),
  đối soát `net_amount == orders.total_amount`.
- [ ] Xây dựng `fact_returns` từ `lh.silver.returns` join với `fact_order_items` / `orders` theo
  `(_dataset_run_id, channel, order_id, line_no)`: gắn `return_date_key` (từ `return_ts` UTC thực tế),
  `order_date_key`, `customer_key`, `product_key`, `seller_key`, `currency`, `refund_amount`, `reason`.
- [ ] Không suy diễn trường số lượng hoàn trả (`return_qty` = N/A vì nguồn không cung cấp);
  kiểm tra tổng `refund_amount` theo `(channel, order_id, line_no)` không vượt quá `net_line_amount`.
- [ ] Đảm bảo khi một đơn hàng có nhiều lần hoàn trả trên cùng/khác dòng, việc join tổng hợp
  không làm nhân đôi doanh thu đơn hàng (`fact_orders`) hay giá trị dòng hàng (`fact_order_items`).
- [ ] Bảo toàn `currency` gốc theo từng kênh (ví dụ `VND` cho Lazada, `USD` cho các kênh khác);
  mọi kiểm tra đối soát tổng tiền đều nhóm theo `(channel, currency)`.
- [ ] Viết unit test kiểm chứng công thức tính tiền, trường hợp hoàn trả nhiều dòng/nhiều ngày
  và kiểm tra không thất thoát hay nhân bản dòng khi join với 5 bảng `dim_*`.

**Đạt khi:** 3 bảng `fact_*` khớp 100% số dòng, khoá ngoại và tổng tiền `DECIMAL(18,2)` với Silver.
**Commit gợi ý:** `feat(gold): build order item and return fact tables with decimal reconciliation`.

### P4C-06 — Gold publication ledger, recompute/retraction và phục hồi lỗi

- [ ] Tạo `spark/lib/gold_publish.py` sử dụng POSIX `writer_lock` tại `/runtime/gold/publication`,
  quản lý `attempt.json`, `ledger.json` và bản lưu bất biến `publication-<id>.json`.
- [ ] Ghi idempotent từng bảng trong `lh.gold.*` theo phạm vi `_dataset_run_id` bằng `MERGE INTO`
  (delete mọi dòng cũ của `_dataset_run_id` đó + insert tập dòng mới của run đó), giữ nguyên
  dữ liệu của các `_dataset_run_id` khác đang cùng tồn tại trong bảng.
- [ ] Nếu `mode` (`full` hoặc `incremental`) gặp đúng cặp `(_dataset_run_id, _silver_publication_id)`
  đã được publish completed và read-back tất cả 8 bảng Gold đều khớp summary, cho phép skip an toàn.
- [ ] Khi Silver có publication mới (do nạp thêm ngày/kênh hoặc `--correction` làm thay đổi/retract
  dòng cũ hoặc tạo slice rỗng), Gold recompute toàn bộ `_dataset_run_id` từ snapshot Silver mới,
  xoá sạch các dòng fact/dim không còn hiệu lực của run đó mà không để lại dữ liệu mồ côi.
- [ ] Từ chối publish nếu phát hiện natural key hoặc surrogate key bị trùng giữa hai `_dataset_run_id` khác nhau.
- [ ] Kiểm tra read-back schema, key uniqueness, referential integrity (mọi FK trong 3 bảng fact
  đều tồn tại trong 5 bảng dim của cùng `_dataset_run_id`), PII guard và tổng tiền trước khi ghi `ledger.json`.
- [ ] Hỗ trợ tiêm lỗi (`fail_after="dim" | "fact" | "completion"`) để kiểm thử partial publish:
  khi lỗi xảy ra, `attempt.json` chuyển sang `failed`, `ledger.json` cũ giữ nguyên từng byte,
  và lần chạy lại phục hồi thành công từ trạng thái đã commit mà không nhân đôi dữ liệu.

**Đạt khi:** Gold publish có tính atomic theo ledger cho từng dataset run, chịu lỗi tốt và xử lý đúng retraction từ Silver.
**Commit gợi ý:** `feat(gold): publish replay-safe Gold tables with ledger and recovery`.

### P4C-07 — Verifier read-only, đối soát đa kênh và regression

- [ ] Tạo verifier read-only (`--verify` và `spark/jobs/dp2_silver/gold_verify.py` hoặc tương đương)
  đọc các snapshot trong `/runtime/gold/publication/ledger.json` mà không ghi lại dữ liệu.
- [ ] Kiểm chứng toàn diện trên small fixture (`186ae107d0793d2ee237`):
  `fact_orders=375`, `fact_order_items=375`, `dim_customer=25`, `dim_product=88`, `fact_returns=30`,
  `dim_channel=5`, cùng số lượng `dim_seller` và `dim_date` khớp tập khoá duy nhất từ Silver.
- [ ] Kiểm chứng toàn diện trên default reference dataset (`b1e64afaff0176de180a`):
  `fact_orders=15,000` (3.000/kênh), `fact_order_items=15,000`, `dim_customer=1,000`,
  `dim_product=100`, `fact_returns=1,500`, `dim_channel=5`.
- [ ] Xác minh cả hai dataset (`186ae107d0793d2ee237` và `b1e64afaff0176de180a`) cùng tồn tại
  trong các bảng `lh.gold.*` (`total fact_orders = 15,375`), đọc độc lập theo `_dataset_run_id`
  và tổng tiền/hoàn trả theo `(channel, currency)` khớp tuyệt đối giữa Gold, Silver và Bronze authoritative.
- [ ] Kiểm thử chạy lại (replay) 2 lần liên tiếp cho cả `full` và `incremental`: số dòng,
  surrogate keys và business hashes không đổi.
- [ ] Kiểm thử kịch bản nguồn Silver thay đổi (correction retract một đơn hàng và cập nhật profile):
  Gold cập nhật đúng fact/dim và giữ nguyên các thực thể dùng chung bởi ngày khác.
- [ ] Restart `lakekeeper`, `spark-master`, `spark-worker1`, `spark-worker2` rồi chạy verifier
  read-only trước mọi thao tác ghi; chạy lại regression `make smoke-core`, `make verify-sources`,
  `make verify-lakehouse`, `make verify-bronze`, `make verify-silver` và `make reconcile-silver`.
- [ ] Ghi nhận thời gian chạy, số dòng, số file/bytes của các bảng Gold; không diễn giải số liệu
  fixture/default nhỏ thành benchmark skew triệu dòng.

**Đạt khi:** Cả hai ma trận dữ liệu đối soát khớp 100% từ Bronze → Silver → Gold và vượt qua bài kiểm tra restart/regression.
**Commit gợi ý:** `test(gold): verify five-channel reconciliation replay recovery and persistence`.

### P4C-08 — Make, CI, tài liệu và bàn giao Phase 4D/4E

- [ ] Mở rộng `Makefile` (ví dụ `JOB=dp2_gold` hoặc mở rộng `dp2_silver` cùng các target
  `smoke-gold`, `verify-gold`, `integration-gold`, `integration-gold-reference`,
  `test-gold-dq`, `test-gold-recovery`, `reconcile-gold`), giữ nguyên toàn bộ target cũ.
- [ ] Đảm bảo `make check` chạy nhanh (lint Python/SQL/YAML/shell + unit tests) không cần Docker,
  không skip ngầm các bài kiểm tra bắt buộc.
- [ ] Cập nhật hoặc thêm workflow GitHub Actions manual cho Gold dùng small fixture, timeout rõ ràng,
  diagnostics đã lọc PII/secret và dọn container không xóa volume local.
- [ ] Cập nhật `docs/gold-validation.md`, `README.md`, `docs/runbook.md`, `docs/implementation-status.md`,
  `docs/data-model.md`, `sql/iceberg/gold_contract.sql` và `docs/adr/0009-gold-star-schema.md`.
- [ ] Viết tài liệu bàn giao tiếp theo (`docs/features-warehouse-handoff.md` hoặc cập nhật handoff)
  cho Phase 4D (`dp3_features`, `dp3_return_labels`: point-in-time cutoff `order_ts < as_of_ts`,
  observation horizon, maturity flag, chống data leakage) và Phase 4E (`dp4_warehouse`:
  cửa sổ `[run_date - 29 days, run_date + 1 day)` UTC, atomic publish sang PostgreSQL `warehouse`).
- [ ] Rà soát toàn bộ checkbox trong `TODO.md` theo bằng chứng thực tế; commit theo Conventional Commits
  cho từng bước và chỉ gắn tag `v0.4c-gold` sau khi đạt toàn bộ local DoD. Chưa gắn tag đóng toàn bộ Phase 4.

**Đạt khi:** Có đầy đủ mã nguồn, kiểm thử, tài liệu vận hành và hợp đồng bàn giao sang Phase 4D/4E.
**Commit gợi ý:** `docs(gold): publish validation evidence and Phase 4D/4E handoff`.

## 4. Điều kiện đóng Phase 4C

- [ ] 8 bảng Gold (`dim_date`, `dim_channel`, `dim_customer`, `dim_product`, `dim_seller`,
  `fact_orders`, `fact_order_items`, `fact_returns`) đúng schema, grain, surrogate key uniqueness và FK integrity.
- [ ] Input được pin chặt chẽ từ completed Silver publication ledger theo `snapshot-id`; không dùng `latest`.
- [ ] Không lộ PII rõ trong schema, dữ liệu, exception hay log; `currency` và `DECIMAL(18,2)` được bảo toàn.
- [ ] Replay idempotent, cô lập đúng giữa các `_dataset_run_id`, xử lý đúng Silver correction/retraction và phục hồi sau lỗi.
- [ ] Restart persistence và toàn bộ regression `core`/`sources`/`lakehouse`/`bronze`/`silver` đều xanh.
- [ ] `make check` và bộ integration local đạt; tài liệu, ADR, DDL, runbook và handoff được cập nhật đầy đủ.

## 5. Quy tắc thực hiện

1. Mọi checkbox để trống cho đến khi có implementation và bằng chứng kiểm tra tương ứng.
2. Sau mỗi task nhỏ: chạy kiểm tra liên quan và `make check`, cập nhật `TODO.md`/evidence,
   rồi tạo commit theo Conventional Commits ngay; không chờ dồn cuối phase.
3. Mục không áp dụng phải ghi rõ `N/A` kèm lý do; mục bị chặn giữ nguyên `[ ]` và ghi rõ blocker.
4. Không đổi version/port đã pin, không xóa volume/bucket/DB, không expire các snapshot đang được pin/ledger tham chiếu.
5. Không commit `.env`, secret, HMAC key, raw PII hay file dữ liệu runtime.
6. Mọi quyết định thiết kế về surrogate keys, `dim_seller` và grain phải được chốt trong ADR-0009 trước khi viết code phụ thuộc.

Sau 4C: **Phase 4D** (`dp3_features`, `dp3_return_labels`) → **Phase 4E** (`dp4_warehouse` export sang PostgreSQL)
→ **Phase 4F** (benchmark skew & đóng toàn bộ Phase 4).
