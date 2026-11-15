# TODO — Phase 4D: Xây dựng Feature Store và Return Labels (`dp3_features`, `dp3_return_labels`)

Cập nhật: **2026-11-15**. Trạng thái: **Đã rà soát và chốt kế hoạch chi tiết; chưa bắt đầu triển khai (0/68 checklist)**.

Mục tiêu: triển khai hai job độc lập `dp3_features` và `dp3_return_labels` thuộc phạm vi `dp3`
(theo [ADR-0003](docs/adr/0003-data-contracts.md), [AGENTS.md](AGENTS.md) và [plan](docs/plan.md)),
tiêu thụ bất biến một completed Gold star-schema publication (`/runtime/gold/publication/ledger.json`)
theo `snapshot-id`, xuất bản các bảng `lh.gold.feat_customer_daily`, `lh.gold.feat_product_daily`
và `lh.gold.lbl_order_return` trên Iceberg v2, đảm bảo point-in-time correctness tuyệt đối
(`order_ts < as_of_ts` và `return_ts < as_of_ts`), xử lý đúng cửa sổ quan sát hoàn trả
(maturity censoring: immature labels = `NULL`, không gán âm giả `0`), và kiểm chứng trên cả
hai ma trận dữ liệu (`186ae107d0793d2ee237` và `b1e64afaff0176de180a`).

Tham chiếu: [plan](docs/plan.md), [data model](docs/data-model.md),
[bàn giao Phase 4D/4E](docs/features-warehouse-handoff.md), [Gold contract](sql/iceberg/gold_contract.sql),
[ADR-0003](docs/adr/0003-data-contracts.md), [ADR-0009](docs/adr/0009-gold-star-schema.md).
Lịch sử: [checklist Phase 4C](docs/phase4c-checklist.md),
[bằng chứng Gold](docs/gold-validation.md), tag `v0.4c-gold`.

## 1. Baseline và các điểm rà soát kiến trúc quan trọng

Gold star schema đã hoàn tất kiểm chứng local (`v0.4c-gold`): 8 bảng trong `lh.gold`
(`dim_date`, `dim_channel`, `dim_customer`, `dim_product`, `dim_seller`, `fact_orders`,
`fact_order_items`, `fact_returns`), 110 unit tests pass. Hai dataset độc lập cùng tồn tại:
- Small fixture (`186ae107d0793d2ee237`): `fact_orders=375`, `fact_order_items=375`, `dim_customer=25`, `dim_product=88`, `fact_returns=30`.
- Default reference (`b1e64afaff0176de180a`): `fact_orders=15,000`, `fact_order_items=15,000`, `dim_customer=1,000`, `dim_product=100`, `fact_returns=1,500`.
- Đặc điểm thời gian của dữ liệu nguồn (`generators/common/orders.py`):
  - Đơn hàng (`order_ts`) phân bố trong 3 ngày UTC: `2026-01-01`, `2026-01-02`, `2026-01-03`.
  - Hoàn trả (`return_ts`) sinh theo `order_ts + 1..14 days` (kéo dài từ `2026-01-02` đến `2026-01-17`).

Các điểm chuẩn hóa kiến trúc sau khi review `TODO.md`:
1. **Đúng quy ước namespace và tên bảng (`AGENTS.md` §6, `docs/plan.md` §5.2 & §6.2, `docs/data-model.md`):**
   - Không tạo namespace `lh.features` ngoài chuẩn. Toàn bộ bảng phục vụ аналиtinh/ML ở tầng Gold nằm trong namespace `lh.gold` với tiền tố `feat_*` và `lbl_*`:
     - `lh.gold.feat_customer_daily`
     - `lh.gold.feat_product_daily`
     - `lh.gold.lbl_order_return`
2. **Tách biệt ranh giới sở hữu giữa `dp2` và `dp3` (`ADR-0003`):**
   - `dp2` sở hữu 8 bảng `dim_*`/`fact_*` và `/runtime/gold/publication/ledger.json`.
   - `dp3` chỉ đọc (read-only) 8 bảng `dim_*`/`fact_*` từ snapshot trong `/runtime/gold/publication/ledger.json` và quản lý publication ledger riêng của `dp3` tại `/runtime/dp3/publication/ledger.json` (hoặc tách theo job), tuyệt đối không ghi đè ledger của `dp2`.
3. **Đúng cấu trúc thư mục job (`AGENTS.md` §2 & Airflow DAG `docs/plan.md` §8):**
   - Triển khai đúng 2 package đã scaffold sẵn:
     - `spark/jobs/dp3_features/main.py` (`make spark-submit JOB=dp3_features`)
     - `spark/jobs/dp3_return_labels/main.py` (`make spark-submit JOB=dp3_return_labels`)
   - Dùng chung thư viện tại `spark/lib/dp3_*.py`.
4. **Chống rò rỉ dữ liệu kép (Dual-Timestamp Point-in-Time Leakage Guard):**
   - Không chỉ lọc `order_ts < as_of_ts`, mà mọi feature liên quan đến hoàn trả (`return_count_30d`, `refund_amount_30d`, `return_rate_30d`) bắt buộc phải lọc thêm **`return_ts < as_of_ts`**. Nếu chỉ lọc theo `order_ts`, một đơn hàng ngày `2026-01-01` nhưng đến `2026-01-10` mới hoàn trả sẽ làm lộ thông tin tương lai vào feature snapshot của ngày `2026-01-02`/`2026-01-03`.
5. **Không suy diễn `return_qty` (`ADR-0009`):**
   - Bảng `fact_returns` không có cột số lượng hoàn trả (`return_qty` là N/A từ nguồn). Do đó `feat_product_daily` chỉ tính số dòng/lần hoàn trả (`return_count_7d`, `return_count_30d`), giá trị tiền hoàn trả (`refund_amount_7d`, `refund_amount_30d`) và tỷ lệ hoàn trả theo dòng/tiền, tuyệt đối không bịa cột `return_qty_30d`.
6. **Bảo toàn tiền tệ (`currency`) và `DECIMAL(18,2)` (`ADR-0003`):**
   - Giữ nguyên `currency` gốc theo kênh (`VND` cho Lazada, `USD` cho 4 kênh còn lại); không cộng gộp tiền khác loại tiền tệ.
7. **Quy tắc Maturity & Right-Censoring cho `lbl_order_return` (`ADR-0003`):**
   - Với cửa sổ quan sát `horizon_days` (mặc định 14 ngày): `is_mature = (order_ts + interval horizon_days days <= as_of_ts)`.
   - Khi `is_mature = false`: bắt buộc đặt `is_returned = NULL`, `days_to_return = NULL`, `refund_amount = NULL`, `refund_ratio = NULL` (right-censored theo đúng `ADR-0003`: *"immature labels are NULL, not negative"* và tránh positive-only bias).
   - Khi `is_mature = true`: chỉ xét các hoàn trả hợp lệ thỏa mãn `return_ts <= order_ts + interval horizon_days days` và `return_ts < as_of_ts` để gán `is_returned` (`0` hoặc `1`).

Ngoài phạm vi Phase 4D: `dp4_warehouse` export sang PostgreSQL `warehouse` (Phase 4E),
benchmark skew triệu dòng (Phase 4F), Airflow (Phase 5), Trino (Phase 6), huấn luyện model MLflow.

## 2. Thứ tự và mốc bàn giao

| Task | Phụ thuộc | Đầu ra | Công sức dự kiến |
| --- | --- | --- | --- |
| P4D-01 | — | Baseline Gold, ADR-0010, physical DDL và bộ test fixtures | 2–3 giờ |
| P4D-02 | 01 | Reader Gold publication cố định theo snapshot-id và run/time scoping | 2–3 giờ |
| P4D-03 | 02 | Job `dp3_features`: `feat_customer_daily` & `feat_product_daily` (point-in-time 7d/30d) | 4–6 giờ |
| P4D-04 | 02 | Job `dp3_return_labels`: `lbl_order_return`, horizon & maturity censoring | 3–5 giờ |
| P4D-05 | 03, 04 | Publication ledger `/runtime/dp3/publication`, MERGE idempotent & recovery | 3–4 giờ |
| P4D-06 | 05 | Kiểm thử DQ, chống data leakage, đối soát đa dataset và restart regression | 3–4 giờ |
| P4D-07 | 06 | Makefile, CI allowlist/workflow, runbook, validation docs và bàn giao Phase 4E | 2–3 giờ |

Tổng **19–28 giờ công**, là ước lượng lập kế hoạch, không phải deadline.

## 3. Checklist triển khai chi tiết

### P4D-01 — Baseline Gold, ADR-0010, physical DDL và bộ test fixtures

- [x] Chạy kiểm tra baseline: `make check`, `make verify-gold`, `make reconcile-gold` trên cả 2 run (`186ae107d0793d2ee237` và `b1e64afaff0176de180a`).
- [x] Soạn thảo `docs/adr/0010-features-and-return-labels.md` chốt các quyết định thiết kế:
  - Quy ước quy chiếu thời gian: với một ngày `as_of_date` (`DATE` UTC), mốc cắt `as_of_ts` được định nghĩa thống nhất (ví dụ đầu ngày kế tiếp `as_of_date + 1 day 00:00:00 UTC` cho báo cáo cuối ngày `as_of_date`, hoặc đầu ngày `as_of_date 00:00:00 UTC`), kèm quy tắc join an toàn giữa `lbl_order_return` và `feat_*_daily` sao cho mọi feature gắn với đơn hàng đều có timestamp `< order_ts`.
  - Định nghĩa chính xác cửa sổ trượt 7 ngày (`[as_of_ts - INTERVAL 7 DAYS, as_of_ts)`) và 30 ngày (`[as_of_ts - INTERVAL 30 DAYS, as_of_ts)`).
  - Dual-timestamp cutoff: `order_ts < as_of_ts` cho sự kiện đặt hàng và `return_ts < as_of_ts` cho sự kiện hoàn trả.
  - Định nghĩa grain, surrogate keys (`customer_feature_key`, `product_feature_key`, `order_label_key`), `horizon_days` (mặc định `14`), và quy tắc right-censoring (`is_mature = false` → nhãn `NULL`).
  - Ranh giới publication ledger của `dp3` tách biệt với `/runtime/gold/publication/ledger.json` của `dp2`.
- [x] Cập nhật `sql/iceberg/gold_contract.sql` (và `docs/data-model.md`) bổ sung DDL cho 3 bảng Iceberg v2 (Parquet, ZSTD):
  - `lh.gold.feat_customer_daily` (`PARTITIONED BY (as_of_date)`)
  - `lh.gold.feat_product_daily` (`PARTITIONED BY (as_of_date)`)
  - `lh.gold.lbl_order_return` (`PARTITIONED BY (as_of_date)`)
- [x] Tạo `spark/tests/fixtures/dp3_cases.json` chứa các kịch bản biên: đơn hàng đặt đúng biên `as_of_ts`, hoàn trả xảy ra sau `as_of_ts` (phải bị loại khỏi feature), hoàn trả xảy ra sau `order_ts + horizon_days`, đơn hàng có nhiều dòng hoàn trả, khách hàng/sản phẩm chưa có đơn trong 7d/30d, và đơn hàng `is_deleted = true`.
- [x] Viết unit tests (`spark/tests/test_dp3_cases.py`) kiểm tra fixtures / schemas / logic biên và maturity censoring.

**Đạt khi:** ADR-0010, DDL và bộ fixture kiểm thử chống leakage/censoring được chốt rõ ràng trước khi viết code transform.
**Commit gợi ý:** `docs(adr): define Phase 4D feature store and return label contracts`.

### P4D-02 — Reader Gold publication cố định theo snapshot-id và cô lập dataset run

- [x] Tạo `spark/lib/dp3_input.py` đọc `/runtime/gold/publication/ledger.json`, yêu cầu `status == "completed"`, kiểm tra đủ 8 bảng Gold star schema và xác thực `publication_id` (`SHA-256` canonical JSON) khớp với file bất biến `publication-<id>.json`.
- [x] Từ chối đọc từ `attempt.json` hoặc fallback sang `latest`; báo lỗi rõ nếu snapshot bị thiếu hoặc hết hạn.
- [x] Đọc các bảng `lh.gold.{dim_customer,dim_product,dim_channel,dim_date,fact_orders,fact_order_items,fact_returns}` bằng `option("snapshot-id", ...)`, đối soát summary (`rows`, `h1`, `h2`) khớp với ledger trước khi lọc.
- [x] Lọc chặt chẽ theo `_dataset_run_id == args.run_id` và loại bỏ các đơn hàng `is_deleted == True`; báo lỗi nếu `args.run_id` không có mặt trong `ledger["runs"]`.
- [x] Hỗ trợ tham số `--as-of-date` (một ngày cụ thể `YYYY-MM-DD` hoặc danh sách/phạm vi ngày của run khi chạy batch backfill).
- [x] Viết unit tests (`spark/tests/test_dp3_input.py`) cho các trường hợp: ledger chưa completed, sai digest, thiếu run_id, và cô lập tuyệt đối giữa 2 dataset run.

**Đạt khi:** `dp3` chỉ đọc dữ liệu từ một Gold publication đã hoàn tất theo snapshot ID cố định và cô lập từng `_dataset_run_id`.
**Commit gợi ý:** `feat(dp3): read pinned Gold publication snapshots by dataset run`.

### P4D-03 — Job `dp3_features`: `feat_customer_daily` và `feat_product_daily`

- [x] Tạo `spark/lib/dp3_features.py` và entrypoint `spark/jobs/dp3_features/main.py` hỗ trợ CLI: `--run-id`, `--as-of-date` (hoặc `--all-dates`), `--publication-id`, `--mode full|incremental`, `--verify`.
- [x] Xây dựng `lh.gold.feat_customer_daily` ở grain `(_dataset_run_id, channel, customer_id, as_of_date)`:
  - Keys & context: `customer_feature_key` (`sha256(_dataset_run_id:channel:customer_id:as_of_date)`), `customer_key`, `channel_key`, `as_of_date_key`, `channel`, `customer_id`, `as_of_date`, `as_of_ts`, `currency`.
  - Chỉ xét các đơn hàng của khách hàng có `order_ts < as_of_ts` và `not is_deleted`.
  - Recency: `last_order_ts` (`< as_of_ts`), `days_since_last_order` (`DATEDIFF(as_of_date, DATE(last_order_ts))`, `NULL` nếu chưa có đơn trước `as_of_ts`).
  - Frequency: `lifetime_order_count`, `orders_7d`, `orders_30d`.
  - Monetary (`DECIMAL(18,2)`): `lifetime_net_amount`, `spend_7d`, `spend_30d`, `aov_30d` (`spend_30d / NULLIF(orders_30d, 0)` làm tròn `DECIMAL(18,2)`).
  - Return behavior (chỉ tính các hoàn trả có **`order_ts < as_of_ts` VÀ `return_ts < as_of_ts`**): `returned_orders_30d`, `refund_amount_30d`, `return_rate_30d` (`CAST(returned_orders_30d / NULLIF(orders_30d, 0) AS DECIMAL(18,4))`, `0.0000` hoặc `NULL` khi `orders_30d == 0`).
- [x] Xây dựng `lh.gold.feat_product_daily` ở grain `(_dataset_run_id, channel, product_id, as_of_date)`:
  - Keys & context: `product_feature_key` (`sha256(_dataset_run_id:channel:product_id:as_of_date)`), `product_key`, `seller_key`, `channel_key`, `as_of_date_key`, `channel`, `product_id`, `seller_id`, `as_of_date`, `as_of_ts`, `currency`.
  - Velocity & Revenue (từ `fact_order_items` có `order_ts < as_of_ts`): `orders_7d`, `orders_30d`, `units_sold_7d`, `units_sold_30d`, `gross_revenue_7d`, `gross_revenue_30d`, `net_revenue_7d`, `net_revenue_30d`, `discount_amount_30d` (`DECIMAL(18,2)`).
  - Return metrics (từ `fact_returns` có **`order_ts < as_of_ts` VÀ `return_ts < as_of_ts`**): `return_count_7d`, `return_count_30d`, `refund_amount_7d`, `refund_amount_30d`, `return_rate_30d` (`CAST(return_count_30d / NULLIF(orders_30d, 0) AS DECIMAL(18,4))`).
- [x] Gắn đầy đủ cột audit: `_dataset_run_id`, `_gold_publication_id`, `_record_hash`, `_policy_version`.
- [x] Viết unit tests (`spark/tests/test_dp3_features.py`) kiểm chứng công thức RFM, cửa sổ 7d/30d, chống rò rỉ `order_ts >= as_of_ts` và `return_ts >= as_of_ts`, và bảo toàn `DECIMAL(18,2)`.

**Đạt khi:** Hai bảng `feat_customer_daily` và `feat_product_daily` tính đúng RFM/velocity theo point-in-time mà không lộ dữ liệu tương lai hay trộn tiền tệ.
**Commit gợi ý:** `feat(dp3): build point-in-time customer and product daily features`.

### P4D-04 — Job `dp3_return_labels`: `lbl_order_return`, horizon và maturity guard

- [x] Tạo `spark/lib/dp3_labels.py` và entrypoint `spark/jobs/dp3_return_labels/main.py` hỗ trợ CLI: `--run-id`, `--as-of-date`, `--horizon-days` (mặc định `14`), `--publication-id`, `--mode full|incremental`, `--verify`.
- [x] Chỉ chọn các đơn hàng trong `fact_orders` có `order_ts < as_of_ts` và `not is_deleted`.
- [x] Lọc `fact_returns` theo điều kiện quan sát hợp lệ:
  - Cùng `(_dataset_run_id, channel, order_id)` (hoặc `order_key`).
  - `return_ts >= order_ts`
  - `return_ts < as_of_ts` (đã xảy ra trước thời điểm cắt quan sát).
  - `return_ts <= order_ts + INTERVAL horizon_days DAYS` (nằm trong cửa sổ quan sát hoàn trả).
- [x] Tổng hợp `fact_returns` hợp lệ về grain đơn hàng `(_dataset_run_id, order_key)` trước khi join với `fact_orders` (tránh nhân bản dòng khi 1 đơn có nhiều dòng hoàn trả):
  - `first_return_ts = MIN(return_ts)`
  - `return_line_count = COUNT(*)`
  - ` horizon_refund_amount = CAST(SUM(refund_amount) AS DECIMAL(18,2))`
- [x] Tính toán các cột của `lh.gold.lbl_order_return` ở grain `(_dataset_run_id, order_key, horizon_days, as_of_date)`:
  - Keys: `order_label_key` (`sha256(_dataset_run_id:channel:order_id:horizon_days:as_of_date)`), `order_key`, `customer_key`, `channel_key`, `order_date_key`, `as_of_date_key`, `channel`, `order_id`, `order_ts`, `as_of_date`, `as_of_ts`, `horizon_days`, `currency`, `net_amount`.
  - Maturity flag: `is_mature = (order_ts + INTERVAL horizon_days DAYS <= as_of_ts)`.
  - Censored labels khi `is_mature == False`: `is_returned = NULL` (`INT`), `first_return_ts = NULL`, `days_to_return = NULL` (`INT`), `return_line_count = NULL`, `refund_amount = NULL` (`DECIMAL(18,2)`), `refund_ratio = NULL` (`DECIMAL(18,4)`).
  - Resolved labels khi `is_mature == True`:
    - `is_returned = 1` nếu có hoàn trả trong horizon, ngược lại `0`.
    - `days_to_return = DATEDIFF(DATE(first_return_ts), DATE(order_ts))` nếu `is_returned == 1`, ngược lại `NULL`.
    - `refund_amount = COALESCE(horizon_refund_amount, CAST(0.00 AS DECIMAL(18,2)))`.
    - `refund_ratio = CAST(refund_amount / NULLIF(net_amount, 0) AS DECIMAL(18,4))`.
- [x] Viết unit tests (`spark/tests/test_dp3_labels.py`) kiểm chứng: đơn chưa đủ tuổi (`is_mature = false` → `is_returned IS NULL`), đơn đủ tuổi không hoàn trả (`is_returned = 0`), đơn đủ tuổi có hoàn trả trong horizon (`is_returned = 1`), hoàn trả xảy ra sau horizon (`is_returned = 0`), và đơn có nhiều dòng hoàn trả không bị nhân bản.

**Đạt khi:** `lbl_order_return` tuân thủ tuyệt đối quy tắc maturity censoring của ADR-0003 và không nhân bản đơn hàng.
**Commit gợi ý:** `feat(dp3): build order return labels with horizon and maturity censoring`.

### P4D-05 — Publication ledger `/runtime/dp3/publication`, MERGE idempotent và phục hồi lỗi

- [ ] Tạo `spark/lib/dp3_publish.py` sử dụng POSIX `writer_lock` tại `/runtime/dp3/publication` (tách biệt hoàn toàn với `/runtime/gold/publication` của `dp2`), quản lý `attempt.json`, `ledger.json` và bản lưu bất biến `publication-<sha256>.json`.
- [ ] Ghi idempotent từng bảng (`lh.gold.feat_customer_daily`, `lh.gold.feat_product_daily`, `lh.gold.lbl_order_return`) bằng `MERGE INTO` theo phạm vi `(_dataset_run_id, as_of_date)` (hoặc toàn bộ `_dataset_run_id` khi chạy `mode="full"` / khi `_gold_publication_id` thay đổi), bảo toàn dữ liệu của các `_dataset_run_id` khác cùng tồn tại trong bảng.
- [ ] Nếu gặp đúng tổ hợp `(_dataset_run_id, _gold_publication_id, as_of_dates, horizon_days)` đã completed và read-back khớp summary, cho phép skip an toàn.
- [ ] Khi Gold có publication mới (do Silver/Gold hiệu chỉnh hoặc retract đơn hàng), `dp3` recompute lại `_dataset_run_id` từ snapshot Gold mới và xóa sạch các dòng feature/label mồ côi của run đó.
- [ ] Kiểm tra read-back trước khi ghi `ledger.json`: khớp schema, không trùng khóa chính, không lộ PII rõ, không có `is_returned` khác `NULL` khi `is_mature = false`, và `0 <= refund_ratio <= 1`.
- [ ] Hỗ trợ tiêm lỗi (`fail_after="features" | "labels" | "completion"`) để kiểm thử partial publish: `attempt.json` ghi `failed`, `ledger.json` cũ giữ nguyên từng byte, và chạy lại phục hồi thành công.

**Đạt khi:** Việc xuất bản features và labels có tính nguyên tử (atomic) theo ledger riêng của `dp3`, idempotent và tự phục hồi sau lỗi.
**Commit gợi ý:** `feat(dp3): publish replay-safe features and labels with ledger and recovery`.

### P4D-06 — Verifier read-only, kiểm thử DQ/leakage, đối soát đa dataset và regression

- [ ] Tích hợp chế độ `--verify` read-only cho `dp3_features` và `dp3_return_labels` đọc trực tiếp từ `/runtime/dp3/publication/ledger.json` mà không ghi lại dữ liệu.
- [ ] Viết `spark/jobs/dp3_features/dq_test.py` kiểm chứng trên Spark:
  - **Point-in-time leakage test:** tiêm đơn hàng và hoàn trả ở tương lai (`>= as_of_ts`), chứng minh chúng không làm thay đổi bất kỳ giá trị nào của `feat_customer_daily`, `feat_product_daily` và `lbl_order_return` tại `as_of_date`.
  - **Maturity censoring test:** tại `as_of_date = 2026-01-03` (với `horizon_days = 14`), 100% đơn hàng (`2026-01-01..2026-01-03`) có `is_mature = false` và `is_returned IS NULL`; tại `as_of_date = 2026-01-18`, 100% đơn hàng có `is_mature = true`, `is_returned IN (0, 1)`, và tổng số đơn `is_returned = 1` khớp chính xác số đơn có hoàn trả trong Gold (`30` cho small fixture, `1,500` cho default reference).
  - **Point-in-time training join test:** join `lbl_order_return` với `feat_customer_daily` và `feat_product_daily` theo đúng quy tắc point-in-time trong ADR-0010, chứng minh không có dòng nào lấy feature được tính từ sự kiện tại hoặc sau `order_ts`.
- [ ] Viết `spark/jobs/dp3_features/recovery_test.py` kiểm chứng 3 điểm tiêm lỗi (`fail_after="features" | "labels" | "completion"`), bảo toàn ledger và phục hồi atomic.
- [ ] Chạy và đối soát toàn diện trên cả 2 dataset:
  - Small fixture (`186ae107d0793d2ee237`): `375` orders, `25` customers, `88` products, `30` mature returns.
  - Default reference (`b1e64afaff0176de180a`): `15,000` orders, `1,000` customers, `100` products, `1,500` mature returns.
  - Xác minh cả 2 dataset cùng tồn tại trong `lh.gold.{feat_customer_daily,feat_product_daily,lbl_order_return}` mà không xung đột khóa.
- [ ] Restart `lakekeeper`, `spark-master`, `spark-worker1`, `spark-worker2` rồi chạy verifier read-only (`make verify-features`, `make verify-labels`) cùng toàn bộ chuỗi regression (`make smoke-core`, `make verify-sources`, `make verify-lakehouse`, `make smoke-bronze`, `make verify-silver`, `make verify-gold`).

**Đạt khi:** Cả 2 dataset vượt qua toàn bộ bài kiểm tra chống rò rỉ dữ liệu, maturity censoring, recovery và restart persistence.
**Commit gợi ý:** `test(dp3): verify point-in-time leakage maturity censoring recovery and coexistence`.

### P4D-07 — Makefile, CI, tài liệu và bàn giao Phase 4E

- [ ] Mở rộng `Makefile`:
  - Hỗ trợ `JOB=dp3_features` và `JOB=dp3_return_labels` trong `spark-submit`.
  - Thêm các target: `smoke-features`, `smoke-labels`, `verify-features`, `verify-labels`, `integration-dp3`, `integration-dp3-reference`, `test-dp3-dq`, `test-dp3-recovery` (giữ nguyên toàn bộ target cũ).
- [ ] Cập nhật `docker/batch/ci_events.py` (allowlist các structured event của Gold và `dp3`) và thêm/cập nhật manual workflow `.github/workflows/gold-dp3.yml` (hoặc tương đương) dùng small fixture, timeout rõ ràng và `make down` không xóa volume.
- [ ] Cập nhật `docs/runbook.md`, `docs/data-model.md`, `docs/implementation-status.md`, `README.md`, `docs/features-warehouse-handoff.md` và viết báo cáo bằng chứng `docs/dp3-validation.md`.
- [ ] Rà soát toàn bộ checkbox trong `TODO.md` theo bằng chứng thực tế, lưu trữ vào `docs/phase4d-checklist.md`, commit theo Conventional Commits và gắn tag `v0.4d-features` khi đạt đủ local DoD.

**Đạt khi:** Đầy đủ công cụ `make`, CI allowlist, runbook, báo cáo kiểm chứng và hợp đồng bàn giao sang Phase 4E (`dp4_warehouse`).
**Commit gợi ý:** `docs(dp3): publish Phase 4D validation evidence and Phase 4E warehouse handoff`.

## 4. Điều kiện đóng Phase 4D

- [ ] 3 bảng `lh.gold.feat_customer_daily`, `lh.gold.feat_product_daily`, `lh.gold.lbl_order_return` đúng schema, grain, surrogate key uniqueness và kiểu `DECIMAL(18,2)`.
- [ ] Input được pin chặt chẽ từ completed Gold publication ledger (`/runtime/gold/publication/ledger.json`) theo `snapshot-id`; publication của `dp3` tách riêng tại `/runtime/dp3/publication/ledger.json`.
- [ ] Point-in-time correctness tuyệt đối: không lộ đơn hàng (`order_ts >= as_of_ts`) hay hoàn trả tương lai (`return_ts >= as_of_ts`) vào features.
- [ ] Maturity censoring chuẩn xác: đơn hàng chưa qua hết `horizon_days` có `is_mature = false` và `is_returned IS NULL` (không gán âm giả `0`).
- [ ] Multi-dataset coexistence (`186ae107d0793d2ee237` và `b1e64afaff0176de180a`), replay idempotent, recovery sau lỗi và restart persistence đều xanh.
- [ ] `make check` và toàn bộ regression `core`/`sources`/`lakehouse`/`bronze`/`silver`/`gold` đều xanh; tài liệu, ADR-0010, DDL và runbook cập nhật đầy đủ.

## 5. Quy tắc thực hiện

1. Mọi checkbox để trống cho đến khi có implementation và bằng chứng kiểm tra tương ứng.
2. Sau mỗi task nhỏ: chạy kiểm tra liên quan và `make check`, cập nhật `TODO.md`/evidence, rồi tạo commit theo Conventional Commits ngay; không chờ dồn cuối phase.
3. Mục không áp dụng phải ghi rõ `N/A` kèm lý do; mục bị chặn giữ nguyên `[ ]` và ghi rõ blocker.
4. Không đổi version/port đã pin, không xóa volume/bucket/DB, không expire các snapshot đang được pin/ledger tham chiếu.
5. Không commit `.env`, secret, HMAC key, raw PII hay file dữ liệu runtime.
6. Mọi quyết định thiết kế về `as_of_ts`, cửa sổ trượt 7d/30d và maturity censoring phải được chốt trong ADR-0010 trước khi viết code phụ thuộc.

Sau 4D: **Phase 4E** (`dp4_warehouse` export 30 ngày gần nhất sang PostgreSQL `warehouse.*`)
→ **Phase 4F** (benchmark skew & đóng toàn bộ Phase 4).
