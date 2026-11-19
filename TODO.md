# TODO — Phase 8: CDC với Debezium

Cập nhật: **2026-11-19**. Trạng thái: **đang triển khai Phase 8 (P8-01…P8-03 xong)**.

Mục tiêu: thay đổi trên PostgreSQL `walmart.orders` (INSERT/UPDATE/DELETE) đi qua Debezium → Kafka
(`platform_cdc.walmart.orders`) → Spark `dp1_cdc` → Bronze `bronze.walmart_orders_cdc` → Silver
trạng thái hiện tại (soft delete), xuất hiện trong vài phút, đúng thứ tự theo LSN, chạy lại không
nhân đôi, và có bảng đối soát PostgreSQL ↔ Silver (count + checksum) khớp (luồng C1–C3).

Tham chiếu: [plan §Phase 8](docs/plan.md), [bàn giao Phase 8](docs/phase8-handoff.md),
[data model](docs/data-model.md), [runbook](docs/runbook.md), ADR-0001/0003/0004/0007/0008/0013/0016.
Lịch sử: [Phase 7](docs/phase7-checklist.md), [kiểm chứng streaming](docs/streaming-validation.md).

## 1. Điểm xuất phát và phạm vi

- Phase 7 đóng ở `v0.7-stream`. Hosted xanh:
  - `Kafka and Flink streaming` trên `ae161cd`
    ([run 44509252927](https://github.com/tri21723/omnichannel-lakehouse/actions/runs/44509252927));
  - `Airflow orchestration e2e` trên `87e5bac`
    ([run 44477792746](https://github.com/tri21723/omnichannel-lakehouse/actions/runs/44477792746));
  - `Trino query layer`, `Skew benchmark smoke`.
- Đã có sẵn:
  - PostgreSQL 16.15 với `wal_level=logical`, `max_replication_slots=10`, `max_wal_senders=10`,
    `max_slot_wal_keep_size=5GB`; role `debezium_repl` (`REPLICATION`, SELECT trên `walmart`);
    `walmart.orders` đã `REPLICA IDENTITY FULL`. Chưa có publication, slot, Kafka Connect.
  - Kafka 4.3.2 KRaft (profile `stream`, `kafka:29092`), tắt auto-create topic; topic tạo bằng
    `docker/stream/kafka-init.sh` (idempotent). `make up-cdc` vẫn `exit 2`.
  - Spark 3.5.5 + Iceberg 1.9.2 **chưa có** connector Kafka (`spark-sql-kafka-0-10`).
  - Khuôn Bronze cho nguồn không phải file (ADR-0007 addendum), khóa driver Spark + pool
    `spark_driver`, CI event allowlist, khuôn tái hiện CI trong project Compose tách biệt.
- Ràng buộc đã biết:
  - `walmart.orders` chứa Walmart của **3 dataset batch** (tiền tố `order_id` = `<run_id>-walmart-`).
    Bronze JDBC của batch lọc theo tiền tố đó, nên kịch bản CDC **chỉ được ghi vào dòng có tiền tố
    riêng** (một `cdc_run_id` 20 hex). Không UPDATE/DELETE dòng của 3 dataset, nếu không replay batch
    sẽ nạp lại JDBC và đổi ledger dây chuyền.
  - Snapshot ban đầu của Debezium sẽ đọc 3.135 order Walmart hiện có (75 + 3.000 + 60): hợp lệ, chỉ
    đọc, nhưng tính vào đối soát và thời gian.
  - Cổng REST mặc định của Kafka Connect là 8083, **trùng Trino**; không thêm port host mới (AGENTS
    §5) — quản lý Connect qua `docker compose exec`.
  - `walmart.customers` có PII thô: **không** đưa vào CDC/Kafka trong phase này.
  - RAM: core + Kafka + Connect + Spark; Flink/kafka-ui/Trino/Airflow dừng khi chạy CDC.
- **Trong phạm vi:**
  - Kafka Connect + Debezium Postgres connector (pgoutput) cho `walmart.orders`, topic + heartbeat;
  - generator kịch bản CDC có ground truth;
  - `dp1_cdc` (Structured Streaming `availableNow`) → Bronze;
  - Silver trạng thái hiện tại (MERGE theo LSN, soft delete);
  - đối soát PostgreSQL ↔ Silver, giám sát slot lag, DAG `dp1_cdc`;
  - runbook (xoá slot, resnapshot, đổi schema).
- **Ngoài phạm vi:**
  - CDC cho `customers` (PII), `order_items`/`returns` (có thể là việc tiếp theo);
  - Schema Registry/Avro, SCD2 đầy đủ (chỉ nếu ADR chọn), Gold/warehouse từ CDC;
  - Debezium Server, HA Connect, DataHub (Phase 9), cloud.

## 2. Quyết định cần chốt trong ADR (P8-02)

| Chủ đề | Đề xuất mặc định | Phương án thay thế / lý do loại |
|---|---|---|
| Version Debezium | Dòng 3.x ổn định mới nhất đã ≥ 2 tuần (ứng viên 3.7.x/3.6.x), hỗ trợ PostgreSQL 16, Java 17, Kafka Connect ≥ 3.1; image chính thức (xác minh registry quay.io/Docker Hub), pin digest | Tự build Connect + plugin: chỉ khi image chính thức không hợp |
| Kafka Connect | 1 worker distributed mode (topic nội bộ configs/offsets/status, replication 1, compact), profile `cdc`, không expose host port | Standalone: offset nằm trong file container, mất khi tạo lại |
| Connector | `pgoutput`, `slot.name=dbz_walmart`, `publication.name=dbz_pub` (đã có sẵn, `publication.autocreate.mode=disabled`), `table.include.list=walmart.orders`, `topic.prefix=platform_cdc`, `snapshot.mode=initial`, `heartbeat.interval.ms=10000`, `decimal.handling.mode=string`, `time.precision.mode=adaptive_time_microseconds`, JSON không schema | `decimal.handling.mode=precise` (bytes base64 khó đọc) hoặc `double` (mất chính xác tiền) |
| Quyền | `debezium_repl` (REPLICATION + SELECT); publication tạo bởi bootstrap Postgres (owner bảng) nếu role không đủ quyền tạo | Superuser: không |
| Topic | `platform_cdc.walmart.orders` (3 partition, key = PK, retention 7 ngày) + heartbeat `__debezium-heartbeat.platform_cdc`, tạo trong `kafka-init` | Auto-create: đã tắt có chủ đích |
| `dp1_cdc` | Spark Structured Streaming `trigger(availableNow=True)` đọc Kafka → Iceberg append `lh.bronze.walmart_orders_cdc` (`op`, `ts_ms`, `lsn`, `before`, `after` dạng JSON, partition/offset, cột audit), checkpoint `s3://checkpoints/dp1_cdc/` | foreachBatch + MERGE theo (partition, offset): dùng nếu Iceberg streaming sink không chứng minh được exactly-once |
| Connector Spark–Kafka | Thêm `spark-sql-kafka-0-10_2.12:3.5.5` + phụ thuộc (kafka-clients, commons-pool2, token-provider) vào image Spark, có checksum + retry Maven; **không đổi version Spark** | Đọc Kafka bằng Python trên driver: không scale, lệch plan |
| Silver | `lh.silver.walmart_orders_cdc_current`: một dòng/`order_id`, `is_deleted`, `_cdc_lsn`, `_cdc_ts_ms`; MERGE chỉ áp khi LSN mới hơn (chịu replay, sai thứ tự giữa batch) | SCD2 (`valid_from/valid_to/is_current`): cân nhắc nếu cần lịch sử, ghi ADR |
| Dataset CDC | Kịch bản ghi vào dòng có tiền tố `<cdc_run_id>-walmart-…`, ground truth trạng thái cuối + số op; không đụng 3 dataset batch | Bảng nguồn riêng: lệch topic/contract đã định |
| Lịch chạy | DAG `dp1_cdc` (pool `spark_driver`, `max_active_runs=1`, pause khi tạo) chạy định kỳ (ví dụ 10 phút) + target Make | Spark streaming chạy liên tục: giữ driver lock lâu, chặn batch |

Nếu quyết định nào mâu thuẫn AGENTS.md (port, version đã pin, dependency nặng, service ngoài sơ đồ,
xoá dữ liệu) thì ghi rõ và hỏi lại trước khi làm.

## 3. Thứ tự thực hiện

| Task | Nội dung | Phụ thuộc | Ước lượng |
|---|---|---|---|
| P8-01 | Preflight: mốc ledger/run ID/JDBC evidence, RAM, quyền Postgres, WAL/slot | Không | 0,5 ngày |
| P8-02 | ADR-0017 CDC + pin Debezium/Connect + jar Spark–Kafka (ADR-0001) | 01 | 0,5–1 ngày |
| P8-03 | Profile `cdc`: Kafka Connect + Debezium, topic, publication/slot, `make up-cdc`, smoke | 02 | 1 ngày |
| P8-04 | Generator kịch bản CDC (tiền tố riêng) + ground truth | 02 | 1 ngày |
| P8-05 | `dp1_cdc` → Bronze `walmart_orders_cdc` (exactly-once, replay) | 03, 04 | 1–1,5 ngày |
| P8-06 | Silver hiện trạng từ CDC (MERGE theo LSN, soft delete) | 05 | 1 ngày |
| P8-07 | Đối soát PostgreSQL ↔ Silver, kịch bản đồng thời, thứ tự, replay | 06 | 1 ngày |
| P8-08 | Vận hành: slot lag, dừng/khởi động connector, resnapshot, đổi schema | 03, 07 | 1 ngày |
| P8-09 | DAG `dp1_cdc`, độ trễ end-to-end ("vài phút") | 07 | 0,5–1 ngày |
| P8-10 | CI, tài liệu, bàn giao Phase 9, tag | 08, 09 | 0,5–1 ngày |

## 4. Checklist triển khai

### P8-01 — Preflight

- [x] Git sạch tại `v0.7-stream` (+ commit sau tag); `make check` xanh.
- [x] Ghi hash ledger Silver/Gold/DP3/warehouse, run ID 3 dataset và **source evidence JDBC Walmart**
  (row count + SHA từng bảng của 3 run) làm mốc; Phase 8 không được đổi.
- [x] Kiểm tra `pg_replication_slots`, `pg_publication`, `wal_level`, `max_slot_wal_keep_size`, quyền
  `debezium_repl` (REPLICATION, SELECT, khả năng tạo publication), `REPLICA IDENTITY` từng bảng.
- [x] Đo RAM core + Kafka (Flink/kafka-ui dừng) + Spark; tính ngân sách Kafka Connect.
- [x] Xác nhận target `up-cdc` còn `exit 2`, thư mục `spark/jobs/dp1_cdc` (nếu có) trống.

**Đạt khi:** mốc và ngân sách ghi vào `docs/cdc-validation.md`.
**Bằng chứng:** [cdc-validation](docs/cdc-validation.md) §P8-01: mốc ledger + evidence JDBC 9 slice +
`make cdc-batch-guard` (mốc sống theo tiền tố); `dbz_pub` đã có sẵn (`debezium_repl` không tạo được
publication); `walmart.orders` = 3.135 dòng; RAM còn ≈ 8 GiB.

### P8-02 — ADR và version

- [x] Xác minh trên tài liệu chính thức:
  - Debezium: dòng stable, Postgres 16, pgoutput, quyền tối thiểu, heartbeat,
    `decimal.handling.mode`, `time.precision.mode`, topic creation khi broker tắt auto-create;
  - image Connect (registry, digest), Kafka Connect ↔ Kafka 4.3.2;
  - `spark-sql-kafka-0-10_2.12:3.5.5` + phụ thuộc (checksum), Iceberg streaming sink (exactly-once).
- [x] Viết `docs/adr/0017-cdc-debezium.md` chốt mọi mục §2: định danh CDC, thứ tự theo LSN, soft
  delete, PII, xử lý tombstone, snapshot ban đầu, đổi schema, slot/WAL.
- [x] Ghi pin vào ADR-0001; hợp đồng event CDC + bảng Bronze/Silver trong `docs/data-model.md`.

**Đạt khi:** ADR có nguồn, version pin, không cần ngoại lệ AGENTS hoặc đã hỏi lại.
**Bằng chứng:** [ADR-0017](docs/adr/0017-cdc-debezium.md), ADR-0001 §Phase 8, data-model §CDC. Chốt:
Debezium 3.6.3.Final (quay.io), Bronze MERGE theo tọa độ Kafka, Silver hiện trạng + xoá mềm (người dùng chọn).

### P8-03 — Hạ tầng `cdc`

- [x] `docker/compose.cdc.yml` profile `cdc`: `kafka-connect` (Debezium image, distributed mode, topic
  nội bộ, `mem_limit`, healthcheck REST nội bộ, không host port); gộp với `compose.stream.yml` để
  dùng lại `kafka` (không bật Flink).
- [x] Bootstrap idempotent:
  - topic CDC + heartbeat + topic nội bộ Connect (compact) trong `kafka-init`;
  - publication `dbz_pub` (nếu cần tạo bằng owner bảng);
  - đăng ký/cập nhật connector qua REST bằng file JSON trong repo
    (`docker/cdc/connectors/walmart-orders.json`); secret qua biến môi trường (Connect
    `FileConfigProvider`/env), không ghi password vào file.
- [x] Make: `env-cdc`, `build-cdc` (nếu cần), `up-cdc` (thay placeholder), `register-cdc`,
  `status-cdc`, `smoke-cdc`, `restart-cdc`; `down`/`logs` gồm `cdc`; yamllint.
- [x] Smoke:
  - connector `RUNNING`, snapshot xong (số message = số order hiện có);
  - một INSERT/UPDATE/DELETE thử trên dòng tiền tố smoke xuất hiện trên topic theo thứ tự;
  - slot `dbz_walmart` active, heartbeat có, secret không có trong log Connect.

**Đạt khi:** `make up-cdc && make smoke-cdc` xanh từ trạng thái dừng và sau restart; batch không đổi.
**Bằng chứng:** [cdc-validation](docs/cdc-validation.md) §P8-03: `smoke-cdc` 12/12 + batch guard sau lần
sửa đầu, sau `restart-cdc`, sau `down` + `up-cdc` (76 s); không snapshot lại. 4 phát hiện đã ghi ADR/runbook.

### P8-04 — Generator kịch bản CDC

- [ ] Lệnh generator `cdc` (model riêng, `cdc_run_id` 20 hex, cùng seed → cùng kịch bản). Tạo
  customer/order có tiền tố riêng, rồi chuỗi thay đổi:
  - INSERT, UPDATE nhiều lần cùng dòng, DELETE;
  - UPDATE nhiều dòng trong một transaction;
  - hai transaction đồng thời trên các dòng khác nhau;
  - xoá rồi chèn lại cùng khóa.
- [ ] Ground truth `s3://raw/manifests/<cdc_run_id>/cdc_ground_truth.json`: trạng thái cuối từng
  order (hoặc đã xoá), số op c/u/d mong đợi, checksum trạng thái cuối. Ghi một lần; chạy lại không
  ghi lại khác nội dung.
- [ ] Ghi bằng role `generator` (đã có INSERT/UPDATE/DELETE trên `walmart`); không chạm dòng 3 dataset.
- [ ] Unit test: tất định, chỉ dùng tiền tố riêng, ground truth = trạng thái mô phỏng.

**Đạt khi:** chạy kịch bản lên Postgres, trạng thái cuối trong Postgres = ground truth; JDBC evidence
batch không đổi.
**Commit gợi ý:** `feat(generator): add deterministic CDC mutation scenarios with ground truth`.

### P8-05 — `dp1_cdc` → Bronze

- [ ] Image Spark thêm connector Kafka (pin + checksum + retry); smoke đọc Kafka từ Spark.
- [ ] `spark/jobs/dp1_cdc/main.py`:
  - Structured Streaming `availableNow`;
  - parse envelope Debezium (`op` c/u/d/r, `ts_ms`, `source.lsn`, `source.txId`, `before`, `after`);
  - tombstone được ghi nhận (không bỏ im lặng); lỗi parse → `quarantine`/DLQ có lý do;
  - ghi Iceberg `lh.bronze.walmart_orders_cdc` + cột audit, checkpoint `s3://checkpoints/dp1_cdc/`.
- [ ] Exactly-once: chạy lại (cùng checkpoint) không nhân đôi; kill driver giữa chừng rồi chạy lại vẫn
  đúng; bằng chứng bằng (partition, offset) duy nhất.
- [ ] Unit test logic parse envelope; Bronze contract (schema, partition) có test.

**Đạt khi:** số dòng Bronze CDC = số message trên topic; replay/kill không nhân đôi.
**Commit gợi ý:** `feat(spark): ingest Debezium CDC events into Bronze`.

### P8-06 — Silver hiện trạng từ CDC

- [ ] `lh.silver.walmart_orders_cdc_current`:
  - một dòng/`order_id`, cột nghiệp vụ đã typed (DECIMAL, TIMESTAMP UTC);
  - cột `is_deleted`, `_cdc_lsn`, `_cdc_ts_ms`, `_cdc_op`;
  - MERGE theo khóa, chỉ áp bản có LSN (rồi `ts_ms`, offset) lớn hơn;
  - delete → `is_deleted = true` (giữ dòng); insert lại sau delete → sống lại.
- [ ] Xử lý trong một micro-batch có nhiều thay đổi cùng khóa: lấy bản mới nhất theo LSN trước khi
  MERGE; idempotent khi replay Bronze.
- [ ] Không đổi bảng Silver batch đã công bố (`silver.orders` …); bảng mới ghi vào `data-model.md`.
- [ ] Unit test chọn bản mới nhất, thứ tự, soft delete, tái tạo; quarantine có `_error_reason`.

**Đạt khi:** Silver hiện trạng khớp ground truth kịch bản; replay không đổi kết quả.
**Commit gợi ý:** `feat(spark): maintain current-state Silver orders from CDC with soft deletes`.

### P8-07 — Đối soát và kịch bản

- [ ] `make verify-cdc` (đọc-only): PostgreSQL (`spark_reader`/`trino_ro`) ↔ Silver hiện trạng:
  - count dòng sống, count đã xoá theo ground truth;
  - checksum (order_id, status, total_amount, updated_ts) trên toàn bảng và trên tiền tố kịch bản.
- [ ] Chạy đủ kịch bản P8-04 (INSERT/UPDATE nhiều lần/DELETE/UPDATE nhiều dòng/đồng thời/xoá-chèn
  lại); chạy lại `dp1_cdc` + Silver 2 lần → không đổi.
- [ ] Kiểm tra thứ tự: dòng cập nhật nhiều lần cuối cùng đúng giá trị cuối; không có bản cũ đè bản mới
  khi replay.

**Đạt khi:** `verify-cdc` xanh hai lần; 3 dataset batch + JDBC evidence không đổi.
**Commit gợi ý:** `test(cdc): reconcile PostgreSQL with CDC Silver across mutation scenarios`.

### P8-08 — Vận hành

- [ ] Giám sát slot: `make cdc-slot-lag` (`pg_replication_slots`, `pg_wal_lsn_diff`), ngưỡng cảnh
  báo; heartbeat giữ slot tiến khi bảng không đổi.
- [ ] Dừng connector khi có thay đổi → slot giữ WAL, lag tăng; bật lại → bắt kịp, không mất event.
- [ ] Restart Kafka Connect/Kafka → connector tiếp tục từ offset; không snapshot lại ngoài ý muốn.
- [ ] Runbook:
  - xoá slot/publication an toàn;
  - resnapshot (incremental snapshot qua signal hoặc slot mới);
  - đổi schema bảng nguồn (thêm cột) và ảnh hưởng tới Bronze/Silver;
  - WAL đầy đĩa.

**Đạt khi:** mỗi tình huống có bằng chứng trong `docs/cdc-validation.md` và mục runbook.
**Commit gợi ý:** `test(cdc): verify connector restart, slot lag and resnapshot procedures`.

### P8-09 — Điều phối và độ trễ

- [ ] DAG `dp1_cdc` (Bronze rồi Silver, pool `spark_driver`, khóa driver, `max_active_runs=1`, pause
  khi tạo, retries/timeout); test DagBag; không chặn `batch_daily` quá lâu.
- [ ] Đo độ trễ commit Postgres → Kafka → Bronze → Silver với lịch đã chọn; đạt "vài phút".

**Đạt khi:** DAG chạy thật thành công; số liệu độ trễ ghi lại.
**Commit gợi ý:** `feat(airflow): schedule CDC ingestion into Bronze and Silver`.

### P8-10 — CI, tài liệu và bàn giao

- [ ] Unit test trong `make test`; workflow thủ công `cdc.yml` (up cdc, smoke, kịch bản, dp1_cdc,
  Silver, verify-cdc, allowlisted events); tái hiện cục bộ trên môi trường trắng trước khi bàn giao.
- [ ] README ("Chạy CDC"), runbook, `implementation-status`, `data-model`, CLAUDE.md, ADR liên quan.
- [ ] `docs/cdc-validation.md`: commit, image, lệnh, kịch bản, đối soát, vận hành, độ trễ.
- [ ] Bàn giao Phase 9 (`docs/phase9-handoff.md`): DataHub lineage cho batch/stream/CDC, ngân sách RAM.
- [ ] Lưu checklist vào `docs/phase8-checklist.md`; tag `v0.8-cdc` khi DoD đạt.

**Commit gợi ý:** `docs(cdc): publish Phase 8 validation and close the phase`.

## 5. Điều kiện đóng Phase 8

- [ ] Debezium/Kafka Connect và connector Spark–Kafka pin version có nguồn; profile `cdc` có
  healthcheck, `mem_limit`, restart ổn; không thêm port host ngoài bảng AGENTS §5.
- [ ] Thay đổi ở PostgreSQL xuất hiện ở Bronze/Silver trong vài phút (có số đo).
- [ ] Update/delete đúng thứ tự (LSN); soft delete; replay/kill không nhân đôi, không đè bản mới.
- [ ] `verify-cdc`: PostgreSQL ↔ Silver khớp count + checksum; kịch bản đủ loại thay đổi.
- [ ] Slot lag được giám sát; runbook xoá slot, resnapshot, đổi schema có kiểm chứng.
- [ ] 3 dataset batch, ledger và JDBC evidence không đổi; PII `customers` không vào Kafka.
- [ ] `make check`, `smoke-cdc`, `verify-cdc` xanh; tài liệu, ADR, bàn giao Phase 9 đầy đủ.

## 6. Rủi ro và cách giảm

| Rủi ro | Dấu hiệu | Cách giảm |
|---|---|---|
| Kịch bản CDC đụng dòng của 3 dataset → JDBC batch nạp lại, ledger đổi | Evidence JDBC khác mốc P8-01 | Tiền tố riêng, test, so mốc sau mỗi bước |
| WAL phình khi connector dừng/slot bỏ quên | `pg_wal_lsn_diff` tăng, đĩa đầy | `max_slot_wal_keep_size` (đã có), heartbeat, `cdc-slot-lag`, runbook xoá slot |
| Mất chính xác tiền/thời gian | Lệch checksum | `decimal.handling.mode=string`, timestamp UTC, test typed |
| Bản cũ đè bản mới khi replay hoặc nhiều thay đổi cùng khóa | Silver ≠ Postgres với dòng cập nhật nhiều lần | MERGE theo LSN, chọn bản mới nhất trong batch, test |
| Spark–Kafka jar lệch version / Maven 429 | Build lỗi, ClassNotFound | Pin + checksum + retry (đã có khuôn), smoke đọc Kafka |
| Trùng port 8083 (Connect ↔ Trino) | Connect không lên khi bật query | Không expose host port; REST qua `docker compose exec` |
| PII lọt vào Kafka | Topic `customers` xuất hiện | `table.include.list` chỉ `walmart.orders`; smoke kiểm tra danh sách topic |
| Thiếu RAM khi bật cùng Spark + Kafka + Connect | OOM, container restart | Dừng Flink/kafka-ui/Trino/Airflow; `mem_limit`; đo ở P8-01 |
| Snapshot ban đầu lớn/chậm | Smoke timeout | Đo thời gian snapshot 3.135 order; timeout có biên |

## 7. Quy tắc cập nhật

1. Chỉ đánh `[x]` khi có implementation và evidence đúng commit; blocker giữ `[ ]` kèm lý do.
2. Sau mỗi task nhỏ: kiểm tra phù hợp, cập nhật evidence/checklist, commit Conventional Commits;
   chỉ commit khi exit code của `make check` là 0 (không đọc qua pipe).
3. Ưu tiên Make targets; chỉ bật service cần thiết; giữ version/port/resource limit đã pin.
4. Không commit secret, `.env`, log thô, dữ liệu; không xoá volume, bucket, DB, slot đang dùng hoặc
   snapshot pin khi chưa hỏi; không đổi run ID hoặc dữ liệu 3 dataset batch.
5. Trước khi bàn giao, tái hiện workflow CI trên môi trường trắng (project Compose tách biệt) để bắt
   các giả định chỉ đúng ở local.
6. Không triển khai Phase 9+ trong kế hoạch này.
