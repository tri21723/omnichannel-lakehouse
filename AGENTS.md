# AGENTS.md

Hướng dẫn cho AI coding agent (và người đóng góp) khi làm việc trong repo này.
Đọc file này trước, sau đó đọc `docs/data-model.md` và `docs/adr/`. Kế hoạch tổng thể nằm ở `docs/plan.md` (bản `data-platform-plan.md`).

> Tên project: `omnichannel-lakehouse` (đổi nếu bạn chọn tên khác).

---

## 1. Tổng quan project

Data platform cho e-commerce đa kênh (5 channel: Walmart, Amazon, TikTok Shop, + 2 kênh mô phỏng). Đây là personal project học tập/portfolio, **chạy local bằng Docker Compose trước, AWS sau**.

Luồng chính:

- **Batch:** JSONL 5 kênh (MinIO) + Postgres (JDBC) → Spark → Iceberg Bronze/Silver/Gold (catalog: Lakekeeper) → Postgres `warehouse` (30 ngày gần nhất).
- **Streaming:** Generator → Kafka → Flink (late arrival, dedup, burst) → CSV/Parquet → Bronze.
- **CDC:** Postgres WAL → Debezium → Kafka → Spark `dp1_cdc` → Bronze/Silver.
- **Query:** Trino (Iceberg REST + Postgres JDBC), DBeaver.
- **Orchestration:** Airflow (standalone, LocalExecutor).
- **Governance:** DataHub (lineage, metadata).
- **IaC:** Terraform (AWS) + Ansible.

## 2. Cấu trúc repo

```
docker/          compose theo profile + Dockerfile (images/)
generators/      batch/, load/, stream/, common/
spark/           jobs/{dp1_bronze,dp2_silver,dp3_features,dp3_return_labels,dp4_warehouse,dp1_cdc}, lib/, tests/
flink/           jobs/
airflow/         dags/, plugins/, tests/
sql/             postgres/, iceberg/, trino/
datahub/         recipes/
infra/           terraform/, ansible/
observability/   prometheus, grafana
docs/            adr/, data-model.md, runbook.md, benchmarks/
```

Đặt code đúng thư mục; không tạo thư mục cấp cao mới nếu chưa có ADR.

## 3. Lệnh thường dùng

Luôn ưu tiên `make`, không gõ lệnh docker dài.

```
make up-core | up-batch | up-orch | up-query | up-stream | up-cdc | up-gov
make down
make gen-batch | gen-load | gen-stream
make spark-submit JOB=dp1_bronze
make test            # pytest (Spark local) + DagBag
make lint            # ruff, sqlfluff, yamllint
make logs SVC=<service>
make e2e             # mini dataset end-to-end
```

Nếu một lệnh `make` chưa tồn tại và bạn cần nó, **thêm vào Makefile** thay vì chạy lệnh rời.

## 4. Ranh giới tài nguyên (quan trọng)

- Máy dev thường chỉ có 16–32GB RAM. **Không bật tất cả profile cùng lúc.** DataHub chỉ bật khi demo/lineage.
- Spark worker mặc định 2 core / 2GB; `spark.sql.shuffle.partitions` nhỏ (16–64) vì dữ liệu nhỏ.
- Airflow `parallelism=1`.
- Khi thêm service mới: đặt vào đúng profile, khai báo `mem_limit`, thêm healthcheck.

## 5. Bảng port (không đổi tuỳ tiện)

| Dịch vụ | Port |
|---|---|
| Postgres | 5432 |
| MinIO API / console | 9000 / 9001 |
| Lakekeeper | 8181 |
| Spark master / UI | 7077 / 8080 |
| Airflow | 8081 |
| Flink JobManager | 8082 |
| Trino | 8083 |
| Kafka | 9092 |
| kafka-ui | 8085 |
| DataHub frontend | 9002 |
| DataHub GMS | đổi khỏi 8080 (trùng Spark) |

## 6. Quy ước đặt tên

- Bucket: `raw`, `lakehouse`, `checkpoints`.
- Namespace Iceberg: `bronze`, `silver`, `gold`.
- Bronze: `<channel>_<entity>_raw`; Silver: tên entity chuẩn (`orders`); Gold: `dim_*`, `fact_*`, `feat_*`, `lbl_*`.
- Cột metadata bắt đầu bằng `_`: `_ingest_ts`, `_source_file`, `_channel`, `_batch_id`.
- Kafka topic: `amazon.order-notifications`, `tiktok_shop.order-webhooks`, `platform_cdc.walmart.orders`.
- DAG trùng tên job (`dp1_bronze`, …).
- Commit theo Conventional Commits: `feat(spark): ...`, `fix(flink): ...`, `docs: ...`, `chore(infra): ...`.

## 7. Quy tắc code

**Chung**
- Python 3.11, type hints, format bằng `ruff` + `black`. SQL theo `sqlfluff`.
- Không hard-code endpoint, credential, đường dẫn; lấy từ biến môi trường / config (`.env.example` là nguồn sự thật cho tên biến).
- Không commit secret, `.env`, file dữ liệu lớn, state Terraform.
- Log có cấu trúc, không `print` trong job.

**Spark**
- Mọi job nhận `--run-date`, `--channel` (nếu có), `--mode`.
- **Idempotent bắt buộc:** dùng `MERGE INTO` hoặc `overwritePartitions` (dynamic). Chạy lại cùng tham số không được nhân đôi dữ liệu.
- Bronze giữ dữ liệu thô, chỉ thêm cột audit. Mọi làm sạch nằm ở Silver.
- Record lỗi đi vào `silver.quarantine_*` kèm `_error_reason`, không drop im lặng.
- Hash PII (email, tên) từ Silver.
- Tái sử dụng `spark/lib`, không copy-paste logic IO/DQ giữa các job.

**Flink**
- Event-time + watermark rõ ràng; dữ liệu quá trễ đi side output, không bỏ.
- State có TTL; bật checkpoint; ghi lại tham số watermark/lateness trong ADR hoặc docstring.

**Airflow**
- DAG phải import được không lỗi (`DagBag` test), không có logic nặng ở top-level.
- Đặt `retries`, `max_active_runs=1`, tags, owner. Dùng `ds`/`data_interval` thay vì `datetime.now()`.

**Generator**
- Cùng `--seed` phải ra cùng output. Tỉ lệ dup/late/skew đọc từ YAML.
- Cập nhật `stream_manifest.json` và ground truth khi đổi cách sinh dữ liệu.

**Debezium / CDC**
- Giữ `heartbeat.interval.ms`, `REPLICA IDENTITY FULL` cho bảng CDC, `max_slot_wal_keep_size` để WAL không phình.
- Xử lý theo thứ tự (`lsn`/`ts_ms`); delete → soft delete hoặc SCD2, ghi rõ trong data model.

**Terraform / Ansible**
- Không `apply` thật nếu không được yêu cầu rõ. Chỉ chạy `fmt`, `validate`, `plan`.
- Mọi tài nguyên có tag `project`, `owner`, `ttl`. **Không dùng NAT Gateway.**
- Playbook idempotent (chạy lần 2 `changed=0`).

## 8. Testing & Definition of Done

Một thay đổi chỉ xem là xong khi:

1. `make lint` và `make test` xanh.
2. Job/DAG chạy lại được 2 lần cho cùng kết quả.
3. Row count đối chiếu được với `stream_manifest.json` (nếu liên quan Bronze).
4. Có test mới cho logic mới (tối thiểu smoke test); logic transform có unit test.
5. Cập nhật tài liệu liên quan (`docs/`, README, DDL trong `sql/`).
6. Không phá schema Silver/Gold đã công bố; nếu đổi schema, cập nhật `docs/data-model.md` và ghi migration.

## 9. Những việc agent KHÔNG được làm khi chưa hỏi

- Chạy `terraform apply`/`destroy`, hoặc bất kỳ lệnh tạo/xoá tài nguyên cloud.
- Xoá volume Docker, bucket, DB, hoặc chạy `make clean`.
- Đổi version đã pin (Spark, Iceberg, Flink, Kafka, Trino, Airflow) hoặc port ở mục 5.
- Thêm dependency nặng / service mới ngoài sơ đồ kiến trúc.
- Sửa lịch sử git, force push, hoặc commit file chứa secret.
- Thay đổi cấu trúc thư mục cấp cao.

Nếu yêu cầu mâu thuẫn với quy tắc ở đây, nêu rõ mâu thuẫn và hỏi lại.

## 10. Cách làm việc khuyến nghị

- Đi theo **vertical slice** và thứ tự phase trong kế hoạch; không nhảy sang streaming/CDC/governance khi batch chưa chạy end-to-end.
- Sau mỗi task nhỏ hoàn tất và kiểm tra đạt, tự động tạo commit theo Conventional Commits; không chờ người dùng nhắc. Chỉ commit file thuộc phạm vi task, không chứa secret/dữ liệu local.
- Mỗi phase hoàn thành thì tag git (`v0.<phase>`).
- Quyết định kỹ thuật quan trọng (chọn công cụ, đổi thiết kế) → viết ADR ngắn trong `docs/adr/`.
- Gặp lỗi: kiểm tra `docs/runbook.md` trước; sau khi giải quyết lỗi mới, bổ sung vào runbook.
- Báo cáo ngắn gọn khi xong: đã đổi gì, chạy lệnh nào để kiểm chứng, còn rủi ro gì.

## 11. Lỗi hay gặp (nhắc nhanh)

- Spark/Trino không đọc được MinIO → bật `path-style-access`, kiểm tra endpoint/region/credential.
- Lệch version Iceberg runtime ↔ Spark ↔ Hadoop-AWS → kiểm tra ADR-0001.
- Port trùng 8080 (Spark ↔ DataHub GMS) → xem mục 5.
- WAL đầy đĩa → kiểm tra `pg_replication_slots`, heartbeat Debezium.
- Small files trong Iceberg → chạy `rewrite_data_files`, giảm shuffle partitions.
