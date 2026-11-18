# TODO — Phase 6: Query layer với Trino + DBeaver

Cập nhật: **2026-11-18**. Trạng thái: **đang triển khai Phase 6 (P6-01…P6-04 xong)**.

Mục tiêu: truy vấn lakehouse (Iceberg qua Lakekeeper REST) và PostgreSQL (`sources.walmart`,
`sources.warehouse`) bằng một Trino single-node chạy local, **chỉ đọc**, không lộ PII thô;
câu SQL federated trả kết quả khớp Spark/ledger; DBeaver kết nối được để demo (luồng Q1–Q3).

Tham chiếu: [plan §Phase 6](docs/plan.md), [bàn giao Phase 6](docs/phase6-handoff.md),
[data model](docs/data-model.md), [runbook](docs/runbook.md), ADR-0001/0003/0004/0006/0009–0014.
Lịch sử: [Phase 5](docs/phase5-checklist.md), [kiểm chứng orchestration](docs/orchestration-validation.md).

## 1. Điểm xuất phát và phạm vi

- Phase 5 đóng ở `v0.5.1-orchestration`; hosted `Scaffold checks` (job `check` + `dags`) xanh:
  [run 44335729476](https://github.com/tri21723/omnichannel-lakehouse/actions/runs/44335729476).
  Việc mang sang: push commit ghi bằng chứng CI (`2d2fb56`) và commit kế hoạch này; (tùy chọn)
  chạy thủ công `Airflow orchestration e2e` và `Skew benchmark smoke` trên GitHub, ghi run URL.
- Dữ liệu sẵn có ([bàn giao Phase 6](docs/phase6-handoff.md)): Bronze/Silver/Gold/DP3 trong
  catalog `lh` (Lakekeeper v0.9.3, warehouse `local_lakehouse`, bucket `lakehouse`),
  `lh.benchmark.*` (không phải contract), PostgreSQL `sources.warehouse` (cửa sổ 30 ngày) và
  `sources.walmart` (OLTP). Ba dataset: `186ae107d0793d2ee237`, `b1e64afaff0176de180a`,
  `5f552019ee41c24af95f`.
- Đã provision (ADR-0004): role PostgreSQL `trino_ro` (`SELECT` trên `walmart.*`,
  `warehouse.*`), user MinIO `trino_local` (chỉ `List/Get` trên `lakehouse`), biến
  `TRINO_RO_DB_PASSWORD`, `TRINO_S3_*`; port 8083 dành cho Trino; profile `query`.
- Ràng buộc đã biết:
  - Lakekeeper chạy local **không xác thực**: Trino gọi REST được mà không cần token, nên phải
    chặn ghi ở lớp Trino (access control) **và** lớp S3 (credential chỉ đọc).
  - `walmart.customers` có `full_name`, `email` thô (`sql/postgres/001_walmart.sql`); Silver/Gold
    chỉ có hash HMAC. Câu federated mẫu của plan (join `gold.fact_orders` với
    `walmart.customers`) **không được** trả cột PII thô.
  - Bảng Iceberg chứa nhiều dataset (`_dataset_run_id`); Spark đọc theo snapshot pin trong
    ledger, còn Trino mặc định đọc `latest`.
  - RAM: `mem_limit` core + batch + orch ≈ 10,5 GiB trên host ~11,7 GiB. Trino không cần Spark
    worker hay Airflow → profile query nên chạy với core + Lakekeeper + Trino.
  - `make up-query` hiện là placeholder `exit 2`; `sql/trino/` đang trống; `.sqlfluff` đang cấu
    hình cho PostgreSQL.
- **Trong phạm vi:** image/compose Trino đã pin, catalog Iceberg + PostgreSQL, quyền chỉ đọc và
  che PII, 6–10 query mẫu, đối soát Trino ↔ Spark/ledger/warehouse, DBeaver template,
  smoke/test/CI, tài liệu.
- **Ngoài phạm vi:** ghi dữ liệu qua Trino, dbt, Superset/Metabase (tùy chọn của plan, để sau),
  Ranger/OPA, TLS/LDAP thật, streaming/CDC (Phase 7–8), DataHub (Phase 9), cloud.

## 2. Quyết định cần chốt trong ADR (P6-02)

| Chủ đề | Đề xuất mặc định | Phương án thay thế / lý do loại |
|---|---|---|
| Version Trino | Bản stable đã phát hành ≥ 2 tuần, image chính thức `trinodb/trino:<ver>`; kiểm tra Iceberg REST + format v2 + native S3; pin vào ADR-0001 | Tự build từ tarball: chỉ khi image không pull được |
| Topology | Single node (coordinator kiêm worker), `127.0.0.1:8083`, `mem_limit` ~2,5 GiB, heap ~1,5–2 GiB | Tách coordinator/worker: thừa với dữ liệu nhỏ |
| Service khi bật `query` | `up-query` = postgres + minio + lakekeeper + trino (không Spark, không Airflow) | Bật cùng orch: chỉ khi đo RAM đủ, ghi vào runbook |
| Catalog Iceberg | `iceberg.catalog.type=rest`, URI Lakekeeper, `warehouse=local_lakehouse`, native S3 (endpoint MinIO, path-style, region), credential `trino_local`, vended credentials tắt | Dùng credential Spark: loại vì có quyền ghi |
| Catalog PostgreSQL | DB `sources`, user `trino_ro`, chỉ schema `walmart`, `warehouse` | Role khác: không cần |
| Chỉ đọc | File-based access control: catalog `iceberg`, `postgresql` read-only; test ghi bị từ chối | Chỉ dựa vào grants S3/PG: không chặn được thay đổi metadata qua REST |
| PII | Luật cột deny/mask `full_name`, `email` của `postgresql.walmart.customers`; federated join theo key + cột không nhạy cảm/hash | View che PII trong PostgreSQL: cần migration DDL, để sau |
| Benchmark | Ẩn `iceberg.benchmark` khỏi người dùng | Để lộ: dễ nhầm với dữ liệu nghiệp vụ |
| Xác thực Trino | Không auth, chỉ bind loopback, read-only (local dev); ghi rõ rủi ro | Password file cần HTTPS hoặc `allow-insecure-over-http`: khi cần chia sẻ |
| Nhất quán đọc | Query nghiệp vụ lọc `_dataset_run_id`; query tái lập dùng `FOR VERSION AS OF <snapshot>` từ ledger; ghi rõ `latest` có thể đọc giữa chuỗi batch | Ép mọi query theo snapshot: khó dùng trong DBeaver |
| Lint SQL | `sql/trino/.sqlfluff` dialect `trino`; giữ PostgreSQL cho phần còn lại | Một dialect chung: sai cú pháp Trino |

Nếu quyết định nào mâu thuẫn AGENTS.md (port, version đã pin, service mới ngoài sơ đồ, xóa dữ
liệu) thì ghi rõ và hỏi lại trước khi làm.

## 3. Thứ tự thực hiện

| Task | Nội dung | Phụ thuộc | Ước lượng |
|---|---|---|---|
| P6-01 | Preflight: dữ liệu, ledger, PII inventory, RAM khi không bật Spark/Airflow | Không | 0,5 ngày |
| P6-02 | ADR-0015 query layer + pin Trino (ADR-0001) | 01 | 0,5 ngày |
| P6-03 | Compose `query`, config Trino, env, Make targets | 02 | 0,5–1 ngày |
| P6-04 | Catalog Iceberg + PostgreSQL, chỉ đọc, che PII, ẩn benchmark | 03 | 0,5–1 ngày |
| P6-05 | Bộ query mẫu `sql/trino/` + runner có tham số an toàn + lint | 04 | 0,5–1 ngày |
| P6-06 | Đối soát Trino ↔ Spark/ledger/warehouse, `verify-query` | 05 | 1 ngày |
| P6-07 | DBeaver: template kết nối, hướng dẫn, kiểm tra JDBC | 04 | 0,5 ngày |
| P6-08 | Vận hành: restart, lỗi phụ thuộc, RAM, chạy cùng orch | 06 | 0,5 ngày |
| P6-09 | (Tùy chọn) DAG `query_checks` read-only sau `batch_daily` | 06 | 0,5 ngày |
| P6-10 | CI, tài liệu, bàn giao Phase 7, tag | 08 (09) | 0,5 ngày |

## 4. Checklist triển khai

### P6-01 — Preflight

- [x] Git sạch tại tag Phase 5 (+ commit đã push); `make check` xanh.
- [x] `verify-gold`, `verify-features`, `verify-labels`, `verify-warehouse` cho ba dataset; ghi hash
  ledger làm mốc (Trino không được làm đổi ledger nào).
- [x] Liệt kê namespace/bảng trong `lh` và schema `sources` sẽ expose; PII inventory từ DDL
  (`walmart.customers.full_name`, `email`; kiểm tra thêm các bảng khác).
- [x] Đo RAM khi chỉ bật postgres + minio + lakekeeper; tính ngân sách cho Trino; ghi phương án
  chạy cùng `orch`.
- [x] Ghi số liệu kỳ vọng để đối soát: row count và `content` theo ledger Gold/DP3, tổng tiền
  warehouse theo run/channel/currency từ `warehouse/export/ledger.json`.

**Đạt khi:** mốc trạng thái, danh sách đối tượng expose và PII, ngân sách RAM ghi vào
`docs/query-validation.md`.
**Bằng chứng:** [query-validation](docs/query-validation.md) §P6-01; phát hiện thêm `iceberg.bronze` chứa PII
thô trong `_raw` → ẩn. **Commit:** `503801a`.

### P6-02 — ADR và version

- [x] Xác minh trên tài liệu chính thức: bản Trino stable, image, JDK đi kèm, Iceberg connector (REST
  catalog, warehouse, native S3 properties), PostgreSQL connector, file-based access control
  (catalog/schema/table/column rules); ghi exact version + nguồn + ngày vào ADR-0001.
- [x] Viết `docs/adr/0015-trino-query-layer.md` (kiểm tra số trước) chốt mọi mục §2, gồm mô hình
  bảo mật local, quy tắc PII và nhất quán đọc.
- [x] Ghi các luồng sơ đồ được hiện thực: Q1 (Iceberg REST + S3), Q2 (PostgreSQL JDBC), Q3 (DBeaver).

**Đạt khi:** ADR có nguồn, version pin, không cần ngoại lệ AGENTS hoặc đã hỏi lại.
**Bằng chứng:** Trino 484 (OpenJDK 25) trong ADR-0001; [ADR-0015](docs/adr/0015-trino-query-layer.md). **Commit:** `eec977a`.

### P6-03 — Compose, config và Make

- [x] `docker/compose.query.yml` profile `query`: service `trino` (image pin), `127.0.0.1:8083`,
  `mem_limit`, healthcheck (`/v1/info` với `starting=false`), `depends_on` postgres/minio/lakekeeper
  healthy; config mount read-only từ `docker/query/trino/` (node, jvm, config, log, catalog,
  access control).
- [x] Secret không nằm trong file catalog: dùng `${ENV:...}` của Trino, giá trị từ `.env`
  (`TRINO_RO_DB_PASSWORD`, `TRINO_S3_*`); biến mới (nếu có) vào `.env.example`; `make env-query`
  không xoay vòng giá trị cũ.
- [x] Make: `config-query`, `up-query` (thay placeholder: core + lakekeeper + trino), `smoke-query`,
  `restart-query`, `trino-cli ARGS=…`; `down`/`logs` bao gồm `query`; yamllint thêm file compose.
- [x] Smoke: `SHOW CATALOGS` có `iceberg`, `postgresql`; `SHOW SCHEMAS`; `SELECT 1`; restart vẫn
  hoạt động; RAM trong ngân sách.

**Đạt khi:** `make up-query && make smoke-query` xanh từ trạng thái dừng và sau restart.
**Bằng chứng:** `up-query` từ trạng thái dừng 36 s, `restart-query`, smoke xanh; Trino ~1,5/2,5 GiB.

### P6-04 — Quyền chỉ đọc, PII và phạm vi expose

- [x] Iceberg catalog đọc được `bronze/silver/gold`; `benchmark` bị ẩn.
- [x] PostgreSQL catalog chỉ thấy `walmart`, `warehouse`; cột PII thô bị deny/mask (chốt hành vi
  `SELECT *` trong ADR) và có test tự động.
- [x] Test âm tự động: `CREATE TABLE`, `INSERT`, `DELETE`, `ALTER`, `DROP`, `CALL system.*`
  (expire/remove orphan) trên `iceberg` và `postgresql` đều bị từ chối; ledger và snapshot hiện
  tại không đổi sau test.
- [x] Kiểm tra credential: `trino_local` không ghi được bucket; `trino_ro` không ghi được DB (chạy
  lại phần tương ứng của `test-warehouse-dq`).

**Đạt khi:** mọi thao tác ghi/PII bị chặn có bằng chứng; hash ledger không đổi.
**Bằng chứng:** `make smoke-query` 20/20; `smoke-core` xanh; ledger không đổi. Sự cố: table procedure
`expire_snapshots` lọt qua file-based rules (ghi 1 phiên bản metadata vô hại cho `gold.dim_channel`,
snapshot/ledger không đổi, `verify-gold` xanh) → thêm `iceberg.security=read_only`. DROP/ALTER trên
bảng thật không thử trực tiếp (Trino kiểm tồn tại trước quyền) — được chặn bởi rule + connector.

### P6-05 — Query mẫu và runner

- [ ] `sql/trino/` (một câu mỗi file, comment mục đích/kỳ vọng):
  1. Doanh thu net theo channel/currency/ngày (Gold, một dataset).
  2. Top 10 seller và tỉ trọng (thấy skew ~50% của default dataset).
  3. Return rate theo category (Gold facts + dim).
  4. **Federated:** Gold `fact_orders` (Iceberg) ⋈ `postgresql.walmart.orders`/`warehouse` theo key;
     chỉ cột không nhạy cảm hoặc hash.
  5. Federated đối soát: tổng tiền 30 ngày Iceberg vs `postgresql.warehouse` theo channel/currency.
  6. Time travel: `FOR VERSION AS OF <snapshot từ ledger>` và `FOR TIMESTAMP AS OF`.
  7. Metadata: `"fact_orders$snapshots"`, `"$files"`, `"$partitions"`.
  8. Labels: phân bố `is_mature`/`is_returned` tại một `as_of_date`.
- [ ] Runner thay tham số an toàn (`dataset_run_id` regex `[0-9a-f]{20}`, snapshot số nguyên), không
  ghép chuỗi tự do; xuất kết quả nhỏ CSV/JSON; không in secret.
- [ ] `sql/trino/.sqlfluff` dialect `trino`; `make lint` chạy cả `sql/trino`.
- [ ] Unit test runner (thay tham số, từ chối giá trị sai) trong `make test`.

**Đạt khi:** mọi query chạy được trên ba dataset; lint xanh.
**Commit gợi ý:** `feat(query): add sample Trino queries with a parameterized runner`.

### P6-06 — Đối soát và độ đúng

- [ ] Row count từng bảng Gold/DP3 qua Trino tại snapshot ledger = `content.rows` trong ledger.
- [ ] Tổng tiền DECIMAL theo run/channel/currency: Trino Iceberg (cửa sổ 30 ngày) = Trino
  PostgreSQL `warehouse` = monetary trong `warehouse/export/ledger.json` (khớp tuyệt đối).
- [ ] Câu federated (4) và câu nghiệp vụ (1–3) so với Spark trên cùng snapshot (job Spark nhỏ đọc
  snapshot pin): diff hai chiều = 0.
- [ ] Time travel: kết quả tại snapshot cũ khác `latest` đúng như ledger mô tả (dataset e2e).
- [ ] `make verify-query` gom các kiểm tra, read-only, exit ≠ 0 khi lệch; chạy hai lần cùng kết quả.

**Đạt khi:** DoD plan — câu federated khớp Spark; mọi đối soát xanh trên ba dataset.
**Commit gợi ý:** `test(query): reconcile Trino results with Spark, ledgers and warehouse`.

### P6-07 — DBeaver

- [ ] Template kết nối không chứa secret: driver Trino, `jdbc:trino://127.0.0.1:8083`, user tùy ý
  (không auth local), catalog mặc định `iceberg`; hướng dẫn trong runbook hoặc `docs/dbeaver.md`.
- [ ] Hướng dẫn mở và chạy `sql/trino/*.sql`; lưu ý `_dataset_run_id` và snapshot.
- [ ] Kiểm tra JDBC từ host (CLI hoặc client tối thiểu) để chắc DBeaver kết nối được.
- [ ] Demo DBeaver là thao tác GUI của người dùng: ghi ảnh khi người dùng cung cấp; không ghi nhận
  "đã demo" khi chưa có bằng chứng.

**Đạt khi:** kết nối JDBC từ host thành công; hướng dẫn đầy đủ.
**Commit gợi ý:** `docs(query): add DBeaver connection template and walkthrough`.

### P6-08 — Vận hành và lỗi

- [ ] `make restart-query` giữ catalog hoạt động; Trino truy vấn lại được sau Lakekeeper/MinIO restart.
- [ ] Lỗi có kiểm soát: Lakekeeper dừng → query Iceberg lỗi rõ, query PostgreSQL vẫn chạy; credential
  S3 sai (tạm, trong container thử nghiệm) → lỗi rõ, không lộ secret trong log.
- [ ] Đo RAM: `query` một mình, `query` + `orch` (+ Spark khi có job); ghi tổ hợp khuyến nghị.
- [ ] Query trong lúc Airflow publish: `latest` có thể đổi giữa các câu, query theo snapshot thì
  không — ghi hành vi.

**Đạt khi:** hành vi lỗi/restart/RAM có bằng chứng trong `docs/query-validation.md`.
**Commit gợi ý:** `test(query): verify Trino restart, dependency failures and memory budget`.

### P6-09 — (Tùy chọn) Kiểm tra sau chuỗi batch

- [ ] DAG `query_checks` (không dùng pool `spark_driver`): sau `batch_daily` của một ngày, gọi Trino
  (HTTP API hoặc client có sẵn trong constraints) chạy đối soát (5); read-only, timeout/retry.
- [ ] Test DagBag + render; không thêm dependency nặng vào image Airflow.

**Đạt khi:** DAG chạy xanh trên dataset e2e, không đổi ledger.
**Commit gợi ý:** `feat(airflow): add read-only Trino reconciliation after the daily batch`.

### P6-10 — CI, tài liệu và bàn giao

- [ ] CI: lint `sql/trino` trong `make lint`; workflow thủ công `query.yml` (core + lakekeeper + trino,
  fixture nhỏ đã có Gold, `smoke-query`, `verify-query`, allowlisted events).
- [ ] Cập nhật README ("Chạy Trino"), runbook (catalog, PII, read-only, lỗi thường gặp),
  `implementation-status`, `data-model` (cách đọc nhất quán), CLAUDE.md nếu đổi lệnh.
- [ ] `docs/query-validation.md`: commit, image, lệnh, đối soát, test âm, RAM.
- [ ] Bàn giao Phase 7 (`docs/phase7-handoff.md`): Kafka/Flink ghi vào Bronze, kiểm tra streaming qua
  Trino, ngân sách RAM khi bật `stream`.
- [ ] Lưu checklist vào `docs/phase6-checklist.md`; tag `v0.6-query` khi DoD đạt, không di chuyển tag cũ.

**Commit gợi ý:** `docs(query): publish Phase 6 validation and close the phase`.

## 5. Điều kiện đóng Phase 6

- [ ] Trino pin version có nguồn; profile `query` có healthcheck, `mem_limit`, restart ổn định.
- [ ] Catalog Iceberg (Q1) và PostgreSQL (Q2) đọc được ba dataset; JDBC/DBeaver (Q3) kết nối được.
- [ ] Mọi thao tác ghi bị chặn ở Trino và ở credential; không lộ PII thô; `benchmark` bị ẩn.
- [ ] Câu federated khớp Spark; tổng tiền Iceberg = warehouse = ledger; time travel đúng.
- [ ] `make check`, `make smoke-query`, `make verify-query` xanh; ledger không đổi do Trino.
- [ ] Tài liệu, ADR, runbook, bàn giao Phase 7 đầy đủ.

## 6. Rủi ro và cách giảm

| Rủi ro | Dấu hiệu | Cách giảm |
|---|---|---|
| Trino không đọc được MinIO | `Access Denied`, `UnknownHost`, sai region | Path-style, endpoint `http://minio:9000`, region từ `.env`; thử `mc` với `trino_local` |
| REST catalog đòi credential/vending | Lỗi khi load table | Tắt vended credentials, S3 tĩnh read-only, kiểm tra warehouse name |
| Ghi qua REST catalog không auth | DDL/commit metadata thành công | Access control read-only + test âm + S3 read-only |
| Lộ PII thô | `SELECT *` trả `email` | Luật cột deny/mask, test tự động |
| Lệch do đọc `latest` | Số khác ledger/Spark | Đối soát theo snapshot pin; tài liệu nhất quán đọc |
| Hết RAM | OOM, Trino restart | Không bật Spark/Airflow nếu không cần; giới hạn heap |
| Sai DECIMAL/timezone | Lệch vài cent, ngày lệch | Không cast sang double; UTC rõ ràng; so sánh DECIMAL tuyệt đối |
| Trino quá mới | Lỗi connector | Bản ≥ 2 tuần, ghi lý do, smoke đầy đủ |

## 7. Quy tắc cập nhật

1. Chỉ đánh `[x]` khi có implementation và evidence đúng commit; blocker giữ `[ ]` kèm lý do.
2. Sau mỗi task nhỏ: kiểm tra phù hợp, cập nhật evidence/checklist, commit Conventional Commits;
   chỉ commit khi exit code của `make check` là 0 (không đọc qua pipe).
3. Ưu tiên Make targets; chỉ bật service cần thiết; giữ version/port/resource limit đã pin.
4. Không commit secret, `.env`, log thô, dữ liệu; không xóa volume, bucket, DB hoặc snapshot pin;
   không ghi dữ liệu qua Trino.
5. Không triển khai Phase 7+ trong kế hoạch này.
