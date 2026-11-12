# Data Platform – Kế hoạch triển khai chi tiết

> Personal project Data Engineering theo sơ đồ "Data Platform System Architecture".
> Stack: Postgres · MinIO · Iceberg + Lakekeeper · Spark · Airflow · Trino · Kafka · Flink · Debezium · DataHub · Terraform · Ansible.

---

## Mục lục

1. [Tổng quan & mục tiêu](#1-tổng-quan--mục-tiêu)
2. [Nguyên tắc thực hiện](#2-nguyên-tắc-thực-hiện)
3. [Yêu cầu phần cứng & chi phí](#3-yêu-cầu-phần-cứng--chi-phí)
4. [Ánh xạ sơ đồ → thành phần → luồng](#4-ánh-xạ-sơ-đồ--thành-phần--luồng)
5. [Cấu trúc repo & quy ước](#5-cấu-trúc-repo--quy-ước)
6. [Data model](#6-data-model)
7. [Lộ trình theo phase (0 → 11)](#7-lộ-trình-theo-phase)
8. [Testing & Data Quality](#8-testing--data-quality)
9. [Observability](#9-observability)
10. [Rủi ro & cách giảm thiểu](#10-rủi-ro--cách-giảm-thiểu)
11. [Timeline tổng hợp](#11-timeline-tổng-hợp)
12. [MVP & backlog mở rộng](#12-mvp--backlog-mở-rộng)
13. [Checklist hoàn thành & Portfolio](#13-checklist-hoàn-thành--portfolio)
14. [Phụ lục](#14-phụ-lục)

---

## 1. Tổng quan & mục tiêu

**Bài toán:** Xây data platform cho một doanh nghiệp thương mại điện tử đa kênh (5 channel), xử lý đồng thời:

- **Batch:** file JSONL từ 5 kênh + dữ liệu OLTP Walmart trong Postgres → Medallion (Bronze/Silver/Gold) trên Iceberg.
- **Streaming:** event đơn hàng Amazon, TikTok Shop qua Kafka → Flink (late arrival, dedup, burst).
- **CDC:** thay đổi bảng `walmart.orders` qua Debezium → Kafka → Bronze.
- **Query/Federation:** Trino truy vấn Iceberg + Postgres.
- **Governance:** DataHub (lineage, metadata).
- **IaC:** Terraform + Ansible dựng môi trường AWS.

**Mục tiêu học tập / portfolio:**

| Kỹ năng | Bằng chứng đưa vào CV |
|---|---|
| Lakehouse (Iceberg, REST catalog) | Bảng time travel, MERGE, schema evolution |
| Spark tuning | Benchmark xử lý data skew trước/sau |
| Streaming | Flink watermark, dedup, recovery sau khi kill TM |
| CDC | Debezium xử lý update/delete đúng thứ tự |
| Orchestration | DAG chuỗi, backfill idempotent |
| Governance | Lineage end-to-end trên DataHub |
| IaC | `terraform apply` + `ansible-playbook` dựng stack |

---

## 2. Nguyên tắc thực hiện

1. **Local-first:** làm hết bằng Docker Compose, sau đó mới lên AWS.
2. **Vertical slice:** hoàn thành 1 luồng end-to-end (Walmart batch → Gold → Trino) trước khi mở rộng sang streaming/CDC.
3. **Mỗi phase có Definition of Done (DoD)** kiểm chứng được.
4. **Pin version ngay từ đầu** (Spark, Iceberg runtime, Hadoop-AWS, Flink, Kafka, Trino, Airflow). Ghi vào `docs/adr/0001-versions.md`.
5. **Idempotent mọi thứ:** job chạy lại không nhân đôi dữ liệu.
6. **Dữ liệu bẩn có chủ đích:** generator tạo skew/dup/late/schema drift để các tầng sau có việc thực sự.
7. **Compose profiles** để bật/tắt cụm: `core`, `batch`, `stream`, `cdc`, `query`, `governance`.
8. **Viết ADR** cho mỗi quyết định lớn (vì sao Lakekeeper, vì sao Flink sink CSV, …).
9. **Commit nhỏ, thường xuyên**, mỗi phase một tag git (`v0.1-phase1` …).

---

## 3. Yêu cầu phần cứng & chi phí

### 3.1 Phần cứng

| Cấu hình | Khả năng |
|---|---|
| 8GB RAM | Chỉ chạy được Postgres + MinIO + Spark nhỏ. Cần thuê cloud cho phần còn lại |
| 16GB RAM | Chạy từng cụm một (batch **hoặc** stream **hoặc** DataHub) |
| 32GB RAM | Thoải mái chạy batch + query + stream cùng lúc; DataHub vẫn nên bật riêng |

Dung lượng đĩa: tối thiểu 50GB trống (image Docker + dữ liệu + checkpoint).

### 3.2 Ước lượng RAM từng cụm (tham khảo, điều chỉnh theo thực tế)

| Cụm | RAM gợi ý |
|---|---|
| Postgres + MinIO | 1–1.5GB |
| Lakekeeper | 0.3GB |
| Spark (master + 2 worker, mỗi worker 2GB) | 5–6GB |
| Airflow standalone | 2GB |
| Trino single node | 2–3GB |
| Kafka (KRaft) + kafka-ui | 1.5GB |
| Flink (JM + TM) | 3GB |
| Debezium/Kafka Connect | 1GB |
| DataHub (GMS, frontend, MySQL, OpenSearch, Kafka) | 6–8GB |

### 3.3 Chi phí

- **Local:** ~0đ (toàn bộ open source).
- **AWS:** chỉ phát sinh ở Phase 10. Quy tắc:
  - Đặt **AWS Budget alert** (5–10 USD) trước khi tạo bất kỳ tài nguyên nào.
  - Một EC2 duy nhất, `terraform destroy` ngay sau khi demo.
  - Không dùng NAT Gateway (đặt EC2 ở public subnet + security group chặt).
  - Kiểm tra EBS, Elastic IP, snapshot mồ côi sau khi destroy.
  - Giá thay đổi theo thời điểm/region: kiểm tra bằng AWS Pricing Calculator.

---

## 4. Ánh xạ sơ đồ → thành phần → luồng

### 4.1 Bảng cổng (port)

| Dịch vụ | Port | Ghi chú |
|---|---|---|
| Airflow | 8081 | webserver + scheduler (standalone) |
| Spark master | 7077 / 8080 | cluster / UI |
| Flink JobManager | 8082 | UI |
| Kafka | 9092 | broker |
| kafka-ui | 8085 | |
| Lakekeeper | 8181 | Iceberg REST catalog |
| Trino | 8083 (gợi ý) | tránh đụng 8080/8081 |
| MinIO | 9000 / 9001 | API / console |
| Postgres | 5432 | |
| DataHub frontend / GMS | 9002 / 8080→đổi | đổi port GMS để tránh đụng Spark |

> Lưu ý: DataHub GMS mặc định 8080 trùng Spark UI. Đổi port mapping ngay từ đầu.

### 4.2 Danh sách luồng (theo nhãn trong sơ đồ)

| Nhóm | Mã | Mô tả | Phase |
|---|---|---|---|
| **O** Orchestration | O1 | Data Engineer trigger DAGs | 5 |
| | O2 | `docker compose` chạy generator | 2 |
| | O3 | Airflow `spark-submit` | 5 |
| | O4 | Lineage từ Airflow → DataHub | 9 |
| | O5 | Metadata Airflow lưu trong Postgres | 1, 5 |
| **B** Batch | B1 | Generator → JSONL 5 channels → MinIO | 2 |
| | B2 | Export Walmart JSON từ generator load | 2 |
| | B3 | Copy sources Walmart → Postgres | 2 |
| | B4 | dp1 đọc raw bucket S3 | 4 |
| | B5 | dp1 đọc Postgres qua JDBC | 4 |
| | B6 | Ghi Bronze → Silver → Gold | 4 |
| | B7 | Commit snapshot Iceberg | 3, 4 |
| | B8 | Catalog DB (Lakekeeper ↔ Postgres) | 3 |
| | B9 | dp4 ghi Gold 30 ngày gần nhất → schema `warehouse` | 4 |
| **S** Streaming | S1 | Generator publish event → Kafka | 7 |
| | S2 | Kafka → Flink (Amazon + TikTok) | 7 |
| | S3 | Flink → output CSV (EBS bind mount) | 7 |
| **C** CDC | C1 | Postgres WAL logical replication | 8 |
| | C2 | Debezium → CDC topics | 8 |
| | C3 | dp1_cdc: CDC topics → Bronze | 8 |
| **Q** Query | Q1 | Trino ↔ Iceberg REST catalog + S3 | 6 |
| | Q2 | Trino ↔ Postgres JDBC (federation) | 6 |
| | Q3 | DBeaver ↔ Trino | 6 |
| **G** Governance | G1 | Lineage assertions | 9 |
| **I** IaC | I1 | Terraform provisioning (VPC, S3, EC2, ECR, IAM) | 10 |
| | I2 | Ansible playbook (base, data_disk, app, datahub) | 10 |

---

## 5. Cấu trúc repo & quy ước

### 5.1 Monorepo

```
data-platform/
├── README.md
├── Makefile
├── .env.example
├── .github/workflows/          # CI
├── docs/
│   ├── architecture.png
│   ├── adr/                    # Architecture Decision Records
│   ├── data-model.md
│   ├── runbook.md              # cách vận hành / troubleshoot
│   └── benchmarks/             # kết quả tối ưu skew, throughput
├── docker/
│   ├── compose.core.yml        # postgres, minio
│   ├── compose.batch.yml       # lakekeeper, spark
│   ├── compose.orchestration.yml
│   ├── compose.query.yml       # trino
│   ├── compose.stream.yml      # kafka, flink
│   ├── compose.cdc.yml         # kafka connect + debezium
│   ├── compose.governance.yml  # datahub
│   └── images/                 # Dockerfile: spark, airflow, flink, generator
├── generators/
│   ├── common/                 # config, seed, schema
│   ├── batch/
│   ├── load/
│   └── stream/
├── spark/
│   ├── jobs/{dp1_bronze,dp2_silver,dp3_features,dp3_return_labels,dp4_warehouse,dp1_cdc}/
│   ├── lib/                    # util: io, dq, schema, logging
│   └── tests/
├── flink/
│   └── jobs/{amazon_late,tiktok_dedup}/
├── airflow/
│   ├── dags/
│   ├── plugins/
│   └── tests/
├── sql/
│   ├── postgres/               # DDL, init
│   ├── iceberg/                # DDL bronze/silver/gold
│   └── trino/                  # query mẫu
├── datahub/
│   └── recipes/
├── infra/
│   ├── terraform/{modules,envs}/
│   └── ansible/{roles,inventories,playbooks}/
└── observability/              # prometheus, grafana dashboards
```

### 5.2 Quy ước đặt tên

| Đối tượng | Quy ước | Ví dụ |
|---|---|---|
| MinIO bucket | `raw`, `lakehouse`, `checkpoints` | `s3://raw/amazon/dt=2026-01-01/` |
| Iceberg namespace | `bronze`, `silver`, `gold` | `silver.orders` |
| Bảng Bronze | `<channel>_<entity>_raw` | `bronze.amazon_orders_raw` |
| Bảng Silver | `<entity>` chuẩn hoá | `silver.orders` |
| Bảng Gold | `dim_*`, `fact_*`, `feat_*`, `lbl_*` | `gold.fact_orders` |
| Kafka topic | như sơ đồ | `amazon.order-notifications` |
| Cột metadata | tiền tố `_` | `_ingest_ts`, `_source_file`, `_channel`, `_batch_id` |
| DAG | trùng tên job | `dp1_bronze` |
| Git commit | Conventional Commits | `feat(spark): add dp2 dedup` |

### 5.3 Makefile (mục tiêu tối thiểu)

```
make up-core / up-batch / up-query / up-stream / up-cdc / up-gov
make down
make gen-batch / gen-load / gen-stream
make spark-submit JOB=dp1_bronze
make test / lint
make logs SVC=spark-master
make clean       # xoá volume (cẩn thận)
```

---

## 6. Data model

### 6.1 Entity chuẩn (Silver)

| Bảng | Khoá | Cột chính |
|---|---|---|
| `silver.orders` | `order_id`, `channel` | customer_id, order_ts (event time), status, currency, total_amount, shipping_country, updated_ts |
| `silver.order_items` | `order_id`, `line_no` | product_id, seller_id, qty, unit_price, discount |
| `silver.customers` | `customer_id` | name_hash, email_hash, country, first_order_ts |
| `silver.products` | `product_id` | title, category, brand, seller_id |
| `silver.returns` | `return_id` | order_id, item_id, reason, return_ts, refund_amount |
| `silver.quarantine_*` | – | record lỗi + `_error_reason` |

> Hash PII (email, tên) ngay ở Silver để làm demo governance (tag PII trong DataHub).

### 6.2 Gold

- **Dim:** `dim_date`, `dim_channel`, `dim_customer`, `dim_product`, `dim_seller`.
- **Fact:** `fact_orders`, `fact_order_items`, `fact_returns`.
- **Feature (dp3_features):** `feat_customer_daily` (RFM, số đơn 7/30 ngày, AOV, return_rate), `feat_product_daily`.
- **Label (dp3_return_labels):** `lbl_order_return` (order_id, is_returned, days_to_return).

### 6.3 Khác biệt giữa 5 kênh (để làm schema evolution)

| Kênh | Đặc điểm cố ý |
|---|---|
| Walmart | Nguồn OLTP (Postgres) + JSON; nguồn CDC |
| Amazon | Schema evolution (thêm cột `fulfillment_type`, đổi tên `buyer_id`→`customer_id` giữa các file); streaming late |
| TikTok Shop | Webhook retry → duplicate; burst livestream |
| Kênh 4 (ví dụ Shopee) | Timestamp khác múi giờ / format |
| Kênh 5 (ví dụ Lazada) | Tiền tệ khác, thiếu cột optional |

---

## 7. Lộ trình theo phase

> Ký hiệu: ☐ = việc cần làm. Mỗi phase kết thúc bằng **DoD** và **Deliverables**.

---

### Phase 0 – Chuẩn bị nền tảng (Tuần 1)

**Mục tiêu:** repo, quy ước, data model, version pin.

☐ Tạo repo GitHub, monorepo theo mục 5.1, `.gitignore`, `pre-commit` (ruff/black, sqlfluff, yamllint, terraform fmt).
☐ Chốt domain (e-commerce 5 kênh), viết `docs/data-model.md` v1 (mục 6).
☐ Chọn volume mục tiêu: **1–5 triệu dòng** batch, 100k–1M event stream.
☐ Chọn & pin version (ví dụ: Spark 3.5.x, Iceberg runtime tương ứng, Flink 1.18–1.20, Kafka 3.x KRaft, Trino bản ổn định mới, Airflow 2.x/3.x). Kiểm tra ma trận tương thích trên tài liệu chính thức rồi ghi `ADR-0001`.
☐ Viết `Makefile`, `.env.example`, chuẩn port (mục 4.1).
☐ Viết ADR đầu tiên: *Vì sao Iceberg + Lakekeeper, Trino, Flink, Debezium*.
☐ Vẽ lại sơ đồ kiến trúc vào `docs/architecture.png`.

**DoD:** `docker compose config` hợp lệ cho mọi file; `make help` liệt kê lệnh; data model được review.
**Deliverables:** repo khởi tạo, ADR-0001/0002, data-model.md.

---

### Phase 1 – Core storage: Postgres + MinIO (Tuần 1–2)

**Postgres (một instance, nhiều DB)**

☐ Image `postgres:16` (pin).
☐ Cấu hình: `wal_level=logical`, `max_replication_slots=10`, `max_wal_senders=10`, `max_slot_wal_keep_size=5GB` (chặn WAL phình).
☐ Init script idempotent trong `docker-entrypoint-initdb.d`:
- DB `sources`: schema `walmart` (orders, order_items, customers, products), schema `warehouse`.
- DB `airflow`, `lakekeeper`, `mlflow`.
- Role riêng cho mỗi service (airflow, lakekeeper, spark_reader, spark_writer, trino_ro, debezium_repl).
☐ Tạo publication cho CDC: `CREATE PUBLICATION dbz_pub FOR TABLE walmart.orders, ...` (để Phase 8).
☐ `REPLICA IDENTITY FULL` cho bảng cần before-image trong CDC.

**MinIO**

☐ Buckets: `raw`, `lakehouse`, `checkpoints`, `flink-output` (nếu không dùng bind mount).
☐ Policy + access key riêng (spark, trino, lakekeeper, generator), không dùng root.
☐ Script `mc` init idempotent.
☐ Bật versioning cho `raw` (tuỳ chọn).

**DoD:** DBeaver kết nối được Postgres; `mc ls` và boto3 đọc/ghi được bucket; restart container không mất dữ liệu (volume).
**Deliverables:** `compose.core.yml`, `sql/postgres/*.sql`, script init MinIO.

---

### Phase 2 – Sources Generator (Tuần 2–3)

**Mục tiêu:** sinh dữ liệu có kiểm soát và tái lập được.

**Thiết kế chung**
☐ Python 3.11 + Faker + Pydantic; CLI (`typer`), `--seed`, `--start-date`, `--days`, `--config config.yaml`.
☐ File config YAML điều khiển tỉ lệ: `dup_rate`, `late_rate`, `late_max_minutes`, `skew_top_seller_share`, `schema_drift_at_day`, `burst_multiplier`.
☐ Dockerfile riêng, chạy qua `docker compose run generator ...` (luồng O2).

**3 nhóm job (theo sơ đồ)**

| Job | Hành vi | Đầu ra |
|---|---|---|
| **batch** | (1) Skew 5 channel: 1–2 seller/product chiếm 40–60% đơn; (2) Schema evolution Amazon: từ ngày N đổi/ thêm cột; (3) Duplicate 3–5% mỗi channel | `s3://raw/<channel>/dt=YYYY-MM-DD/part-*.jsonl` (B1) + `stream_manifest.json` |
| **load** | Sinh Walmart JSON "internal DB" → export (B2) → load vào Postgres `walmart.*` (B3) | Bảng OLTP |
| **stream** | (1) Late arrival Amazon: event-time lùi ngẫu nhiên, gửi sai thứ tự; (2) Duplicate TikTok retry: cùng `event_id` gửi lại; (3) Burst livestream: spike 50–100x trong vài phút | Kafka (S1) |

**stream_manifest.json** (để reconcile):
```json
{
  "run_id": "...", "seed": 42, "generated_at": "...",
  "channels": {
    "amazon": {"files": 30, "rows": 120000, "dup_rows": 4800,
               "min_ts": "...", "max_ts": "...", "sha256": {"file": "..."}}
  }
}
```

☐ Job `stream` có chế độ `--rate` (event/giây) và `--burst-at`.
☐ Ghi **ground truth** (số unique, số late thật) ra file để so sánh sau Flink.
☐ Unit test: tỉ lệ dup/late sai số < 1% so với config; cùng seed → output giống nhau.

**DoD:** 1 lệnh sinh đủ batch + load; manifest khớp số file/dòng thực tế trong MinIO; test tỉ lệ pass.
**Deliverables:** `generators/`, `config.yaml`, Dockerfile, README sử dụng.

---

### Phase 3 – Lakehouse: Iceberg + Lakekeeper (Tuần 3–4)

☐ Deploy Lakekeeper (port 8181), backend Postgres DB `lakekeeper`, chạy migrate.
☐ Bootstrap: tạo project, warehouse trỏ `s3://lakehouse/` (endpoint MinIO, **path-style**, region giả lập), cấu hình credential (static key hoặc vended credentials).
☐ Tạo namespace `bronze`, `silver`, `gold`.
☐ Spark Iceberg catalog config (REST):
```
spark.sql.catalog.lh=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lh.type=rest
spark.sql.catalog.lh.uri=http://lakekeeper:8181/catalog
spark.sql.catalog.lh.warehouse=<project>/<warehouse>
spark.sql.catalog.lh.s3.endpoint=http://minio:9000
spark.sql.catalog.lh.s3.path-style-access=true
```
☐ Smoke test bằng Spark SQL: `CREATE TABLE`, `INSERT`, `MERGE INTO`, `SELECT ... VERSION AS OF`, `CALL system.rewrite_data_files`, `DROP TABLE`.
☐ Quy ước bảng: `format-version=2`, `write.parquet.compression-codec=zstd`, partition theo `days(order_ts)` hoặc `ingest_date`, `write.metadata.delete-after-commit.enabled=true`.
☐ Viết DDL Bronze/Silver/Gold vào `sql/iceberg/`.
☐ Lên lịch maintenance (sau này trong Airflow): `expire_snapshots`, `remove_orphan_files`, `rewrite_data_files`.

**DoD:** tạo/đọc bảng Iceberg qua Spark bằng REST catalog; thấy `metadata/` + `data/` trong MinIO; time travel chạy được.
**Deliverables:** `compose.batch.yml` (phần lakekeeper), DDL, ADR về catalog.

---

### Phase 4 – Batch Processing: Spark (Tuần 4–6)

**Hạ tầng**
☐ Image Spark tự build: Spark + `iceberg-spark-runtime` + `hadoop-aws` + `aws-java-sdk-bundle` (hoặc `iceberg-aws-bundle`) + JDBC Postgres.
☐ Spark standalone: master + 2 worker (mỗi worker 2 core / 2GB để vừa RAM).
☐ Thư viện chung `spark/lib`: `get_spark()`, `read_jsonl()`, `write_iceberg()`, `add_audit_cols()`, `dq.py`, `logging`.
☐ Job nhận tham số: `--run-date`, `--channel`, `--mode full|incremental`.

**Job chi tiết**

| Job | Nhiệm vụ |
|---|---|
| **dp1_bronze** | B4: đọc JSONL từ `s3://raw/`; B5: đọc Postgres `walmart.*` qua JDBC (partitioned read: `numPartitions`, `partitionColumn`). Ghi Bronze nguyên bản + `_ingest_ts`, `_source_file` (`input_file_name()`), `_channel`, `_batch_id`. Schema-on-read: lưu cột lạ không mất (`_raw` JSON string hoặc `mergeSchema`). Đối chiếu row count với `stream_manifest.json` |
| **dp2_silver** | Chuẩn hoá 5 schema → 1 schema chung; xử lý schema evolution (map tên cột cũ/mới, `coalesce`); cast kiểu, chuẩn timezone về UTC, chuẩn tiền tệ; **dedup** bằng `row_number() over (partition by order_id, channel order by updated_ts desc)`; hash PII; ghi record lỗi vào `silver.quarantine_*`; ghi bằng `MERGE INTO` |
| **dp3_features** | Tính `feat_customer_daily`, `feat_product_daily` (window 7/30 ngày) |
| **dp3_return_labels** | Join orders–returns, tạo nhãn `is_returned` (tránh leakage: chỉ dùng thông tin tại thời điểm đặt hàng cho feature) |
| **dp4_warehouse** | Dựng dim/fact (star schema) ở Gold; ghi **30 ngày gần nhất** sang Postgres schema `warehouse` bằng JDBC (B9) theo kiểu overwrite theo partition/truncate-insert trong transaction |

**Xử lý data skew (đưa vào benchmark)**
☐ Baseline: chạy join/aggregate với skew, ghi thời gian + Spark UI (task duration max vs median).
☐ Áp dụng lần lượt: AQE (`spark.sql.adaptive.enabled`, `skewJoin.enabled`), broadcast join cho dim nhỏ, salting cho key nóng, repartition theo key hợp lý.
☐ Ghi bảng so sánh vào `docs/benchmarks/skew.md` (thời gian, shuffle read, spill).

**Idempotency & hiệu năng**
☐ `MERGE INTO` / `overwritePartitions` (dynamic) để rerun an toàn.
☐ Kiểm soát small files: `write.target-file-size-bytes`, compaction định kỳ.
☐ `spark.sql.shuffle.partitions` phù hợp dữ liệu nhỏ (ví dụ 16–64 thay vì 200).

**DoD:** chạy `spark-submit` từng job thành công; Bronze row count = manifest; Silver không còn trùng khoá; Gold query được qua Spark; benchmark skew có số liệu.
**Deliverables:** `spark/jobs/*`, `spark/lib`, tests, `docs/benchmarks/skew.md`.

---

### Phase 5 – Orchestration: Airflow (Tuần 6–7)

☐ Image Airflow tự build: cài Java + Spark client + provider spark (client mode, luồng O3).
☐ Airflow standalone, `LocalExecutor`, `parallelism=1` (hoặc 2), metadata DB Postgres `airflow` (O5).
☐ Connections/Variables qua env (không hard-code).
☐ DAGs:
- `dp1_bronze` → `dp2_silver` → (`dp3_features`, `dp3_return_labels`) → `dp4_warehouse`.
- `dp1_cdc` độc lập (lịch riêng, ví dụ mỗi 5–15 phút).
- `maintenance_iceberg` (expire snapshots, compaction) hằng tuần.
☐ Tính năng: `retries`, `retry_delay`, SLA, `on_failure_callback` (log/Slack webhook tuỳ chọn), `FileSensor`/`S3KeySensor` chờ manifest, tham số `ds`, `catchup` + backfill, tags, `max_active_runs=1`.
☐ Dùng Dataset/Asset để DAG hạ nguồn kích hoạt theo dữ liệu (tuỳ chọn).
☐ Test: `DagBag` import không lỗi, kiểm tra cycles, kiểm tra default_args (đưa vào CI).

**DoD:** trigger từ UI chạy xanh toàn chuỗi; backfill 3 ngày chạy lại 2 lần cho cùng kết quả (idempotent); ảnh chụp Graph/Gantt.
**Deliverables:** `airflow/dags`, Dockerfile, test DAG.

---

### Phase 6 – Query layer: Trino + DBeaver (Tuần 7)

☐ Trino single node (coordinator + worker), pin version.
☐ Catalog `iceberg.properties`: `connector.name=iceberg`, `iceberg.catalog.type=rest`, `iceberg.rest-catalog.uri=http://lakekeeper:8181/catalog`, S3 native config cho MinIO (path-style, endpoint) (Q1).
☐ Catalog `postgresql.properties` trỏ DB `sources` bằng role read-only (Q2).
☐ DBeaver: kết nối Trino (Q3), lưu connection template + SQL mẫu `sql/trino/`.
☐ Query mẫu (5–10 cái):
1. Doanh thu theo kênh/ngày.
2. Top 10 seller + tỉ trọng (thấy skew).
3. Return rate theo category.
4. **Federated:** join `iceberg.gold.fact_orders` với `postgresql.walmart.customers`.
5. Time travel: `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF`.
6. Kiểm tra metadata: `"orders$snapshots"`, `"orders$files"`.
☐ (Tuỳ chọn) Superset/Metabase làm dashboard.

**DoD:** câu SQL federated trả kết quả khớp với Spark; DBeaver demo được.
**Deliverables:** `compose.query.yml`, cấu hình catalog, `sql/trino/*.sql`.

---

### Phase 7 – Streaming: Kafka + Flink (Tuần 8–9)

**Kafka (KRaft) + kafka-ui**
☐ Topics: `amazon.order-notifications`, `tiktok_shop.order-webhooks` (+ `platform_cdc.walmart.orders` ở Phase 8).
☐ Partition (ví dụ 3–6), key = `order_id`, retention 3–7 ngày; dead-letter topic `*.dlq`.
☐ (Nâng cấp) Schema Registry + JSON Schema/Avro; ADR về lựa chọn.
☐ S1: generator publish; kiểm tra bằng kafka-ui (offset, lag).

**Flink (JobManager 8082 + TaskManager)**
☐ Pin version; bật checkpoint (interval 30–60s, EXACTLY_ONCE, state backend RocksDB hoặc HashMap cho nhỏ), lưu checkpoint ở `s3://checkpoints/` hoặc volume.
☐ **Job Amazon (late arrival):** event-time + `WatermarkStrategy.forBoundedOutOfOrderness(…)`, `allowedLateness`, **side output** cho event quá trễ; ghi riêng stream "late" để reprocess/đối soát.
☐ **Job TikTok (dedup + burst):** `keyBy(event_id)` + state có TTL (ví dụ 10–30 phút) để loại trùng; tumbling window 1–5 phút aggregate; theo dõi backpressure khi burst.
☐ S3: sink CSV/Parquet vào thư mục bind mount (`/mnt/platform-data/flink/output` → `/opt/flink/output`) theo sơ đồ, file rolling theo thời gian/kích thước.
☐ Thử kill TaskManager → job tự recover từ checkpoint, không mất/nhân dữ liệu.
☐ Đưa output vào Bronze: Airflow DAG nạp CSV/Parquet (hoặc nâng cấp Flink sink thẳng Iceberg).
☐ Chọn API: PyFlink/Flink SQL nếu ưu tiên Python; Java/Scala nếu muốn kiểm soát state/timer sâu. Ghi ADR.

**Kiểm chứng**
☐ Bắn 100k event gồm 10% dup + 5% late; so khớp với ground truth: unique count đúng, late xử lý đúng quy tắc.
☐ Ghi lại: throughput (event/s), latency end-to-end, checkpoint duration, backpressure khi burst.

**DoD:** output sau Flink không còn dup; late event đi đúng nhánh; recovery thành công; báo cáo số liệu.
**Deliverables:** `compose.stream.yml`, `flink/jobs`, `docs/benchmarks/streaming.md`.

---

### Phase 8 – CDC: Debezium (Tuần 9–10)

☐ Kafka Connect + Debezium Postgres connector (pin version).
☐ Connector config chính: `plugin.name=pgoutput`, `slot.name=dbz_walmart`, `publication.name=dbz_pub`, `table.include.list=walmart.orders,...`, `topic.prefix=platform_cdc`, `snapshot.mode=initial`, `heartbeat.interval.ms` (tránh WAL phình khi bảng ít thay đổi), `decimal.handling.mode`, `time.precision.mode`.
☐ Topic output: `platform_cdc.walmart.orders` (C2).
☐ **dp1_cdc** (C3): Spark Structured Streaming `trigger(availableNow=True)` hoặc micro-batch đọc topic → Bronze `bronze.walmart_orders_cdc` với các cột `op`, `ts_ms`, `before`, `after`, `source.lsn`; checkpoint ở `s3://checkpoints/dp1_cdc/`.
☐ Silver từ CDC: xử lý `op` c/u/d bằng `MERGE INTO` theo `lsn`/`ts_ms` để đúng thứ tự; delete → soft delete (`is_deleted`) hoặc SCD2 (`valid_from/valid_to/is_current`).
☐ Kịch bản test: INSERT / UPDATE nhiều lần / DELETE / UPDATE đồng thời nhiều dòng / chạy lại job.
☐ Giám sát replication slot lag: `pg_replication_slots`, `pg_wal_lsn_diff(...)`; cảnh báo khi lag lớn.
☐ Runbook: cách xoá slot, resnapshot, xử lý schema change ở bảng nguồn.

**DoD:** thay đổi ở Postgres xuất hiện ở Bronze/Silver trong vài phút; delete/update thứ tự đúng; có bảng so sánh Postgres vs Silver (checksum/count) khớp.
**Deliverables:** `compose.cdc.yml`, connector JSON, job `dp1_cdc`, runbook.

---

### Phase 9 – Data Governance: DataHub (Tuần 10–11)

☐ Chạy DataHub (quickstart/compose) trong profile `governance`, **chỉ bật khi cần** (rất nặng); đổi port tránh đụng Spark.
☐ Ingestion recipes (`datahub/recipes/`): Postgres, Trino hoặc Iceberg REST, Kafka, Airflow (lineage – luồng O4), (tuỳ chọn) Spark OpenLineage listener.
☐ Làm giàu metadata: owner, domain, tag (`PII`, `gold`, `raw`), glossary term, mô tả cột, tier.
☐ **G1 – lineage assertions:** script Python gọi GraphQL/REST của DataHub, kiểm tra mỗi bảng Gold có upstream Silver, Silver có upstream Bronze; fail CI nếu thiếu.
☐ Lập lịch ingestion bằng Airflow.

**DoD:** UI hiển thị lineage nguồn → Bronze → Silver → Gold → warehouse; assertion script pass.
**Deliverables:** `compose.governance.yml`, recipes, script G1, ảnh chụp lineage.

---

### Phase 10 – Infra as Code: Terraform + Ansible (Tuần 11–12)

**Terraform (I1)**
☐ Module: `network` (VPC, public subnet, IGW, SG), `compute` (EC2 + EBS data disk + key pair), `storage` (S3 buckets), `ecr`, `iam` (role/policy tối thiểu).
☐ Remote state: S3 backend + DynamoDB lock (hoặc lockfile theo phiên bản Terraform).
☐ Biến theo môi trường `envs/dev`; `terraform fmt/validate`, `tflint`, `checkov`/`tfsec`.
☐ Tag mọi tài nguyên (`project`, `owner`, `ttl`).
☐ **Budget alert** bằng Terraform (`aws_budgets_budget`).

**Ansible (I2)**
☐ Role `base` (update, docker, user, hardening SSH, fail2ban/ufw), `data_disk` (format + mount EBS tại `/mnt/platform-data`, fstab), `app` (pull image từ ECR, `docker compose up` theo profile), `datahub` (tách riêng vì nặng).
☐ Inventory động/tĩnh từ output Terraform; `ansible-lint`.
☐ Idempotent: chạy playbook 2 lần, lần 2 `changed=0`.

**Kỷ luật chi phí:** `terraform apply` → demo → `terraform destroy`; kiểm tra EBS/EIP/snapshot mồ côi.

**DoD:** từ repo trắng, `make aws-up` (terraform + ansible) dựng được stack tối thiểu (batch + Trino); `make aws-down` dọn sạch.
**Deliverables:** `infra/terraform`, `infra/ansible`, runbook triển khai.

---

### Phase 11 – Hoàn thiện & Portfolio (Tuần 12)

☐ **CI (GitHub Actions):** lint (ruff, sqlfluff, yamllint), pytest (Spark local), DagBag test, build image, `terraform validate/plan`, `ansible-lint`.
☐ **Contract test** schema Bronze/Silver; **reconciliation test** với manifest.
☐ **Observability** (mục 9).
☐ **Tài liệu:** README (sơ đồ, quickstart một lệnh, yêu cầu máy), ADR, benchmark, runbook, FAQ lỗi hay gặp.
☐ **Demo:** video 3–5 phút (generator → Airflow → Trino → DataHub lineage → Flink recover) + ảnh chụp UI.
☐ Viết blog/LinkedIn post tổng kết bài học (skew, late data, CDC ordering).

**DoD:** người lạ clone repo, làm theo README chạy được luồng batch trong < 30 phút.

---

## 8. Testing & Data Quality

| Tầng | Công cụ | Nội dung kiểm tra |
|---|---|---|
| Unit | pytest + Spark local | Hàm transform, dedup, parse schema cũ/mới |
| Data quality | Great Expectations hoặc Soda | not null, unique key, accepted values, range, freshness |
| Reconciliation | script Python | Row count Bronze = manifest; Postgres ↔ Silver CDC; unique stream = ground truth |
| Contract | JSON Schema / dbt-style tests | Schema Silver không đổi ngoài ý muốn |
| DAG | pytest + DagBag | Import, cycles, tags, owner |
| Integration | docker compose + `make e2e` | Chạy mini dataset end-to-end, assert bảng Gold |
| IaC | tflint, checkov, ansible-lint | Lỗi cú pháp, bảo mật |

Quy ước: dữ liệu fail DQ → **quarantine**, không chặn toàn bộ pipeline (trừ lỗi nghiêm trọng như row count lệch lớn).

---

## 9. Observability

☐ Prometheus + Grafana (profile `obs`): Kafka (JMX/kafka-exporter: consumer lag), Flink (metrics reporter: checkpoint duration, backpressure, records/s), Spark (metrics sink hoặc đọc History Server), Postgres (postgres_exporter: replication slot lag), node/cAdvisor.
☐ Spark History Server (event log lưu `s3://checkpoints/spark-events/`).
☐ Dashboard tối thiểu: "Pipeline health", "Streaming", "CDC lag".
☐ Alert cơ bản: slot lag > ngưỡng, consumer lag tăng liên tục, DAG fail, disk > 80%.
☐ Log: `docker logs` + chuẩn log JSON; (tuỳ chọn) Loki.

---

## 10. Rủi ro & cách giảm thiểu

| Rủi ro | Mức | Giảm thiểu |
|---|---|---|
| Hết RAM khi chạy tất cả | Cao | Compose profiles; DataHub chỉ bật khi demo; hạ memory executor; thuê EC2 spot cho cụm nặng |
| Lệch version Spark/Iceberg/Hadoop-AWS/Flink | Cao | Pin từ Phase 0; ADR; dùng image build cố định |
| Spark/Trino không đọc được MinIO | Trung bình | Path-style, đúng endpoint/region, kiểm tra credential/vended credentials, thử bằng `mc` trước |
| Port trùng (Spark 8080, DataHub GMS 8080, Airflow) | Trung bình | Bảng port ở mục 4.1, đổi ngay từ đầu |
| WAL Postgres phình đầy đĩa | Cao | `max_slot_wal_keep_size`, heartbeat Debezium, alert slot lag |
| Small files trong Iceberg | Trung bình | Compaction, tăng target file size, giảm shuffle partitions |
| Flink state phình | Trung bình | TTL state, watermark hợp lý, RocksDB |
| Scope creep | Cao | Làm đúng thứ tự; ý tưởng ngoài hình → backlog (mục 12) |
| Tốn tiền AWS | Trung bình | Budget alert, destroy sau demo, không NAT Gateway |
| Mất động lực giữa chừng | Trung bình | Vertical slice sớm có kết quả; demo nhỏ sau mỗi phase; tag git |

---

## 11. Timeline tổng hợp

Giả định 8–10 giờ/tuần. Nếu ít thời gian hơn, nhân hệ số tương ứng.

| Tuần | Phase | Kết quả chính |
|---|---|---|
| 1 | 0 + bắt đầu 1 | Repo, ADR, data model |
| 1–2 | 1 | Postgres + MinIO chạy |
| 2–3 | 2 | Generator + manifest |
| 3–4 | 3 | Iceberg + Lakekeeper |
| 4–6 | 4 | Spark dp1→dp4, benchmark skew |
| 6–7 | 5 | Airflow DAG chạy xanh, backfill |
| 7 | 6 | Trino federation + DBeaver |
| 8–9 | 7 | Kafka + Flink, kiểm thử recovery |
| 9–10 | 8 | Debezium CDC → Bronze/Silver |
| 10–11 | 9 | DataHub lineage |
| 11–12 | 10 | Terraform + Ansible lên AWS |
| 12 | 11 | CI, observability, tài liệu, video |

**Milestone (gợi ý tag git):**
- **M1 (hết tuần 7):** Batch lakehouse hoàn chỉnh (Phase 0–6) → *đã đủ để đưa vào CV*.
- **M2 (hết tuần 10):** Streaming + CDC.
- **M3 (hết tuần 12):** Governance + IaC + portfolio.

---

## 12. MVP & backlog mở rộng

**MVP (nếu thiếu thời gian):** Phase 0–6 → có lakehouse batch + Airflow + Trino.
**Giá trị cao tiếp theo:** Phase 7 (Flink), Phase 8 (CDC).
**Làm sau:** Phase 9–11.

**Backlog (ngoài sơ đồ):**
- dbt (Trino/Spark adapter) cho lớp Gold.
- MLflow + model dự đoán return từ `lbl_order_return` / `feat_*`.
- Superset/Metabase dashboard.
- Schema Registry (Avro/Protobuf) + compatibility checks.
- Flink sink trực tiếp vào Iceberg (exactly-once).
- Kubernetes (kind/k3s) + Helm thay Docker Compose.
- Row/column-level security (Trino + OPA/Ranger).
- Cost dashboard, data contract tooling (datacontract-cli).
- Streaming Silver với Spark Structured Streaming thay batch.

---

## 13. Checklist hoàn thành & Portfolio

**Kỹ thuật**
- ☐ 1 lệnh `make up-*` dựng được từng cụm
- ☐ Generator tái lập với `--seed`
- ☐ Bronze/Silver/Gold trên Iceberg, có time travel + MERGE
- ☐ Benchmark skew có số liệu trước/sau
- ☐ DAG chuỗi + backfill idempotent
- ☐ Trino federated query đúng
- ☐ Flink: dedup + late + recovery chứng minh được
- ☐ CDC: update/delete đúng thứ tự
- ☐ DataHub lineage end-to-end + assertion pass
- ☐ Terraform apply/destroy sạch; Ansible idempotent
- ☐ CI xanh

**Portfolio**
- ☐ README có sơ đồ + quickstart + ảnh chụp UI
- ☐ ADR (≥ 8 quyết định)
- ☐ Benchmark docs
- ☐ Video demo
- ☐ Bài viết tổng kết

**Câu chuyện phỏng vấn (chuẩn bị sẵn):**
1. Vì sao chọn Iceberg + REST catalog thay vì Hive Metastore?
2. Xử lý skew thế nào, hiệu quả bao nhiêu?
3. Late data và dedup trong Flink: watermark, allowed lateness, TTL state.
4. CDC đảm bảo thứ tự và xử lý delete ra sao? Rủi ro replication slot?
5. Làm sao job idempotent? Khi backfill thì thế nào?
6. Trade-off của việc dùng 1 Postgres cho nhiều mục đích?
7. Nếu dữ liệu tăng 100x, kiến trúc thay đổi gì?

---

## 14. Phụ lục

### 14.1 Compose profiles gợi ý

| Profile | Dịch vụ |
|---|---|
| `core` | postgres, minio, minio-init |
| `batch` | lakekeeper, spark-master, spark-worker1/2 |
| `orch` | airflow |
| `query` | trino |
| `stream` | kafka, kafka-ui, flink-jobmanager, flink-taskmanager |
| `cdc` | kafka-connect (debezium) |
| `governance` | datahub-* |
| `obs` | prometheus, grafana, exporters |

Ví dụ combo thường dùng:
- Phát triển batch: `core + batch + orch + query`
- Phát triển stream: `core + stream`
- Demo CDC: `core + batch + stream + cdc`
- Demo governance: `core + batch + orch + query + governance` (tắt phần còn lại)

### 14.2 Thứ tự khởi động & healthcheck

1. `postgres` (healthcheck `pg_isready`) → `minio` (healthcheck `/minio/health/live`)
2. `minio-init` (tạo bucket, key)
3. `lakekeeper` (migrate → serve), bootstrap warehouse
4. `spark-*`, `trino`, `airflow`
5. `kafka` → `kafka-ui`, `flink`, `kafka-connect`

Dùng `depends_on: condition: service_healthy`.

### 14.3 Biến môi trường chính (`.env.example`)

```
POSTGRES_USER= / POSTGRES_PASSWORD=
MINIO_ROOT_USER= / MINIO_ROOT_PASSWORD=
S3_ENDPOINT=http://minio:9000
LAKEKEEPER_URI=http://lakekeeper:8181
SPARK_VERSION= / ICEBERG_VERSION= / FLINK_VERSION= / TRINO_VERSION=
AIRFLOW_UID=
```
Không commit file `.env` thật; dùng secret manager/SSM khi lên AWS.

### 14.4 Definition of Done chung cho mọi phase

- Chạy lại từ đầu bằng `make` không cần thao tác tay.
- Có test (tối thiểu smoke test).
- Có tài liệu ngắn trong `docs/` + cập nhật README.
- Có ảnh chụp/log chứng minh.
- Commit + tag git.

### 14.5 Mẫu một ADR

```
# ADR-000X: <Tiêu đề>
- Trạng thái: Accepted
- Bối cảnh:
- Lựa chọn đã cân nhắc:
- Quyết định:
- Hệ quả (ưu/nhược):
```

### 14.6 Tài liệu tham khảo nên đọc

- Tài liệu chính thức: Apache Iceberg, Lakekeeper, Apache Spark, Apache Flink, Apache Kafka, Debezium, Trino, Apache Airflow, DataHub, Terraform, Ansible.
- Chủ đề: Medallion architecture, Iceberg table maintenance, Flink event-time & watermark, Debezium Postgres connector, Spark AQE & skew join.

---

*Kế hoạch này là khung tham chiếu; điều chỉnh thời gian và phạm vi theo phần cứng và quỹ thời gian thực tế của bạn.*
