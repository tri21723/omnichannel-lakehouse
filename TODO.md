# TODO — Phase 4D: Xây dựng Feature Store và Return Labels

Cập nhật: **2026-11-15**. Trạng thái: **Đã lập kế hoạch; chưa bắt đầu triển khai (0/48 checklist)**.

Mục tiêu: Triển khai luồng feature engineering và label generation thuộc phạm vi `dp3_features` và `dp3_return_labels` (theo [ADR-0003](docs/adr/0003-data-contracts.md)), tiêu thụ dữ liệu bất biến từ Gold star schema (`lh.gold.*`) thông qua pinned publication ledger `/runtime/gold/publication/ledger.json`. Đảm bảo tính đúng đắn point-in-time (tránh data leakage), xử lý maturity flag cho return labels, và công bố bảng features/labels có kiểm soát trong namespace `lh.features`.

Tham chiếu:
- [plan master](docs/plan.md)
- [data model](docs/data-model.md)
- [Gold validation](docs/gold-validation.md)
- [Handoff 4D/4E](docs/features-warehouse-handoff.md)
- [ADR-0003: Source ownership & keys](docs/adr/0003-data-contracts.md)
- [ADR-0009: Gold star schema](docs/adr/0009-gold-star-schema.md)
Lịch sử: [checklist Phase 4C](docs/phase4c-checklist.md), tag `v0.4c-gold`.

---

## 1. Baseline và phạm vi

Gold đã hoàn tất kiểm chứng local (`v0.4c-gold`):
- 8 bảng Gold trong `lh.gold`: `dim_date`, `dim_channel`, `dim_customer`, `dim_product`, `dim_seller`, `fact_orders`, `fact_order_items`, `fact_returns`.
- Cả 2 ma trận dữ liệu cùng tồn tại: Small fixture (`186ae107d0793d2ee237`, 375 đơn) và Default reference (`b1e64afaff0176de180a`, 15.000 đơn).
- 110 unit tests pass; đối soát tiền tệ `DECIMAL(18,2)` và referential integrity 100%.

Phạm vi Phase 4D:
- Thiết kế ADR-0010 cho Feature Store & Return Label semantics và DDL `sql/iceberg/features_contract.sql`.
- Xây dựng reader pin Gold publication `/runtime/gold/publication/ledger.json` theo snapshot ID, cô lập `_dataset_run_id`.
- Triển khai job `dp3_features`:
  - `lh.features.customer_features_daily`: Tính RFM (Recency, Frequency 7d/30d, Monetary/AOV 7d/30d, Return rate 30d).
  - `lh.features.product_features_daily`: Tính velocity bán hàng 7d/30d, doanh thu 7d/30d, tỷ lệ hoàn trả 30d.
  - Tuân thủ nghiêm ngặt point-in-time cutoff: Chỉ dùng dữ liệu có `order_ts < as_of_ts`.
- Triển khai job `dp3_return_labels`:
  - `lh.features.order_return_labels`: Tính nhãn nhị phân `is_returned`, `days_to_return`, `refund_ratio`.
  - Xử lý maturity flag: Chỉ gán nhãn huấn luyện khi đơn hàng đã qua cửa sổ quan sát hoàn trả (`order_ts + return_window <= observation_end_ts`); các đơn non-mature phải được gắn `is_mature = false` để tránh false negative leakage.
- Quản lý publication ledger `/runtime/features/publication/ledger.json`, atomic merge và kiểm thử recovery.
- Makefile targets, test suites, đối soát dữ liệu và tài liệu bàn giao Phase 4E.

Ngoài phạm vi: `dp4_warehouse` export sang PostgreSQL (Phase 4E), benchmark skew lớn (Phase 4F), Airflow (Phase 5), Trino (Phase 6), Model Training/MLflow.

---

## 2. Thứ tự và mốc bàn giao

| Task | Phụ thuộc | Đầu ra | Ước lượng |
| --- | --- | --- | --- |
| P4D-01 | — | ADR-0010, contract DDL `sql/iceberg/features_contract.sql` và fixtures kiểm thử | 2–3 giờ |
| P4D-02 | 01 | Reader Gold publication snapshot-pinned và pipeline scoping | 2–3 giờ |
| P4D-03 | 02 | Job `dp3_features`: Customer & Product daily features (point-in-time window 7d/30d) | 4–6 giờ |
| P4D-04 | 02 | Job `dp3_return_labels`: Label generation, observation horizons và maturity guard | 3–5 giờ |
| P4D-05 | 03, 04 | Features publication ledger, atomic table publication và failure recovery | 3–4 giờ |
| P4D-06 | 05 | Data Quality tests (leakage prevention, maturity check), multi-dataset verification | 3–4 giờ |
| P4D-07 | 06 | Makefile, CI update, tài liệu `docs/features-validation.md` và bàn giao Phase 4E | 2–3 giờ |

---

## 3. Checklist triển khai chi tiết

### P4D-01 — ADR-0010, contract DDL và fixtures kiểm thử

- [ ] Soạn thảo `docs/adr/0010-features-and-labels.md` quy định rõ ràng:
  - Định nghĩa window 7 ngày và 30 ngày (UTC calendar-based vs rolling hour-based).
  - Quy tắc point-in-time cutoff: $T_{\text{event}} < T_{\text{as\_of}}$.
  - Quy tắc return observation window (mặc định 14 ngày hoặc 30 ngày) và maturity condition: $T_{\text{order}} + W_{\text{return}} \le T_{\text{as\_of}}$.
  - Xử lý đơn hàng non-mature: gắn `is_mature = false` và `is_returned = NULL` để ngăn ngừa label leakage.
  - Xử lý multi-currency trong feature aggregation: giữ currency hoặc tổng hợp theo `(channel, currency)`.
- [ ] Soạn thảo physical Iceberg DDL trong `sql/iceberg/features_contract.sql` cho 3 bảng:
  - `lh.features.customer_features_daily`
  - `lh.features.product_features_daily`
  - `lh.features.order_return_labels`
- [ ] Chuẩn bị bộ test fixtures độc lập và unit tests xác minh công thức RFM, rolling window và non-mature handling.

**Đạt khi:** Contract, DDL và quy tắc chống rò rỉ dữ liệu (data leakage) được chốt trước khi viết transform.
**Commit gợi ý:** `docs(adr): define feature store and return labels contract`.

### P4D-02 — Reader Gold publication snapshot-pinned

- [ ] Tạo module `spark/lib/features_input.py` đọc `/runtime/gold/publication/ledger.json`.
- [ ] Kiểm tra `status == "completed"`, xác thực SHA-256 fingerprint và file lưu trữ bất biến `publication-<id>.json`.
- [ ] Đọc các bảng `lh.gold.{fact_orders, fact_order_items, fact_returns, dim_customer, dim_product}` bằng `option("snapshot-id", ...)`.
- [ ] Lọc nghiêm ngặt theo `_dataset_run_id == args.run_id`; từ chối nếu dataset run không tồn tại trong Gold ledger.
- [ ] Viết unit tests kiểm chứng pinning mechanics và run isolation.

**Đạt khi:** Đầu vào của `dp3` được pin cố định theo snapshot Gold đã publish, không đọc dữ liệu unpinned.
**Commit gợi ý:** `feat(features): implement pinned Gold publication reader`.

### P4D-03 — Job `dp3_features`: Customer & Product daily features

- [ ] Tạo `spark/jobs/dp3_features/` và thư viện tính toán `spark/lib/features_customer.py`, `spark/lib/features_product.py`.
- [ ] Triển khai `customer_features_daily` với các trường:
  - Khóa: `customer_key`, `channel`, `as_of_date`, `_dataset_run_id`.
  - Recency: `days_since_last_order = datediff(as_of_date, max(order_date))`.
  - Frequency: `order_count_7d`, `order_count_30d`.
  - Monetary: `total_spend_7d`, `total_spend_30d`, `avg_order_value_30d` (sử dụng `DECIMAL(18,2)`).
  - Return rate: `return_count_30d`, `return_rate_30d = return_count_30d / nullif(order_count_30d, 0)`.
- [ ] Triển khai `product_features_daily` với các trường:
  - Khóa: `product_key`, `channel`, `as_of_date`, `_dataset_run_id`.
  - Velocity & Doanh thu: `qty_sold_7d`, `qty_sold_30d`, `revenue_7d`, `revenue_30d`.
  - Hoàn trả: `return_qty_30d`, `return_amount_30d`, `return_rate_30d`.
- [ ] Kiểm chứng tính toán độc lập theo từng ngày `as_of_date` trong phạm vi ngày của run, tuân thủ `order_ts < as_of_ts`.

**Đạt khi:** Features được tính toán chính xác, không tính trước tương lai và giữ đúng kiểu `DECIMAL(18,2)`.
**Commit gợi ý:** `feat(features): calculate customer and product daily features`.

### P4D-04 — Job `dp3_return_labels`: Label generation & maturity guard

- [ ] Tạo `spark/jobs/dp3_features/labels_main.py` và thư viện `spark/lib/features_labels.py`.
- [ ] Liên kết `fact_orders` với `fact_returns` theo `(_dataset_run_id, channel, order_id)`.
- [ ] Xác định cửa sổ quan sát hoàn trả `return_window_days` (mặc định 14 ngày hoặc 30 ngày cấu hình được).
- [ ] Tính các trường nhãn:
  - `is_mature`: `order_ts + interval return_window_days days <= as_of_ts`.
  - `is_returned`: Nếu `is_mature == true`, gán `1` nếu có bản ghi trong `fact_returns`, ngược lại `0`. Nếu `is_mature == false`, gán `NULL` (hoặc cờ `is_mature = false`).
  - `days_to_return`: `datediff(return_ts, order_ts)` cho các đơn có hoàn trả.
  - `refund_ratio`: `refund_amount / net_amount`.
- [ ] Viết unit tests kiểm chứng phân tách rõ ràng giữa non-mature vs mature orders.

**Đạt khi:** Nhãn hoàn trả được sinh chuẩn xác, không gán nhãn giả (false negative) cho các đơn hàng chưa hết hạn đổi trả.
**Commit gợi ý:** `feat(features): generate return labels with observation window and maturity guard`.

### P4D-05 — Publication ledger và atomic merge

- [ ] Tạo module `spark/lib/features_publish.py` quản lý `/runtime/features/publication/ledger.json`.
- [ ] Sử dụng `writer_lock`, ghi `attempt.json`, kiểm tra read-back và atomic commit sang ledger.
- [ ] Xuất bản 3 bảng trong `lh.features.*` bằng `MERGE INTO` theo phạm vi `_dataset_run_id` và `as_of_date`.
- [ ] Hỗ trợ tiêm lỗi và kiểm thử phục hồi lỗi atomic recovery.

**Đạt khi:** Features & Labels được công bố an toàn, idempotent và có ledger theo dõi.
**Commit gợi ý:** `feat(features): publish features and labels with ledger and recovery`.

### P4D-06 — Data Quality, chống data leakage và multi-dataset verification

- [ ] Viết test suite `spark/jobs/dp3_features/features_dq_test.py`:
  - Leakage guard: Xác minh không có đơn hàng nào sau `as_of_date` xuất hiện trong tính toán feature của ngày đó.
  - Maturity guard: Xác minh 100% đơn hàng non-mature có `is_mature == false`.
  - Bound checks: Tỷ lệ hoàn trả $0 \le \text{return\_rate} \le 1$, số ngày hoàn trả $\ge 0$.
- [ ] Kiểm chứng trên cả 2 dataset (`186ae107d0793d2ee237` và `b1e64afaff0176de180a`) cùng tồn tại trong `lh.features.*`.

**Đạt khi:** Bộ kiểm thử DQ và chống rò rỉ dữ liệu chạy xanh trên cả 2 dataset.
**Commit gợi ý:** `test(features): verify data quality and leakage prevention`.

### P4D-07 — Makefile, CI và tài liệu bàn giao Phase 4E

- [ ] Mở rộng `Makefile`: bổ sung các target `smoke-features`, `verify-features`, `test-features-dq`, `test-features-recovery`.
- [ ] Đảm bảo `make check` chạy nhanh, pass 100% linter và unit tests mới.
- [ ] Cập nhật `README.md`, `docs/implementation-status.md`, và viết `docs/features-validation.md`.
- [ ] Chuẩn bị hợp đồng bàn giao Phase 4E (`docs/warehouse-handoff.md`).

**Đạt khi:** Đầy đủ công cụ vận hành, CI pass và tài liệu kiểm chứng hoàn tất.
**Commit gợi ý:** `docs(features): complete Phase 4D validation evidence and Phase 4E handoff`.

---

## 4. Điều kiện đóng Phase 4D

- [ ] 3 bảng trong `lh.features` (`customer_features_daily`, `product_features_daily`, `order_return_labels`) chuẩn schema, grain và audit columns.
- [ ] Point-in-time correctness: Không rò rỉ dữ liệu tương lai vào features quá khứ.
- [ ] Maturity guard: Các đơn hàng chưa hết hạn đổi trả được đánh dấu rõ ràng, không bị gán nhãn 0 sai lệch.
- [ ] Multi-dataset coexistence: Cả 2 dataset cùng tồn tại trong `lh.features.*` với khóa phân tách theo `_dataset_run_id`.
- [ ] Publication ledger đầy đủ và phục hồi lỗi atomic.
- [ ] `make check` và kiểm thử integration hoàn toàn xanh; tag `v0.4d-features`.
