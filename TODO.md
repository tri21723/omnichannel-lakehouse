# TODO — Phase 4A: Batch ingestion vào Bronze

Cập nhật: **2026-11-13**. Trạng thái: **Đang triển khai; vertical slice Walmart JSONL/JDBC đã pass local**.

Mục tiêu: triển khai `dp1_bronze` đọc JSONL theo manifest và Walmart PostgreSQL
qua JDBC, ghi Iceberg có provenance, đối soát được và chạy lại không nhân đôi dữ liệu.
Đây là phần đầu Phase 4 trong plan gốc, **không phải toàn bộ Phase 4**.

Tham chiếu: [plan](docs/plan.md), [kiến trúc](docs/architecture.jpg),
[Bronze contract](docs/bronze-contract.md), [data model](docs/data-model.md),
[ADR-0003](docs/adr/0003-data-contracts.md), [ADR-0006](docs/adr/0006-lakehouse-runtime.md).
Lịch sử: [checklist Phase 3](docs/phase3-checklist.md),
[bằng chứng runtime](docs/lakehouse-validation.md).

## 1. Điểm xuất phát và phạm vi

Nền tảng đã kiểm chứng local: PostgreSQL/MinIO, generator 5 kênh, Lakekeeper,
Spark standalone và Iceberg smoke. Dataset tham chiếu `b1e64afaff0176de180a` có
15.600 dòng JSONL; mỗi kênh 3.120 dòng vật lý / 3.000 order duy nhất.
Walmart PostgreSQL có 3.000 orders, 3.000 items, 200 customers, 20 products,
300 returns cho toàn run. Số liệu này cần kiểm tra lại trước triển khai.

Trong phạm vi: CLI, manifest validation, staging dùng chung cho Spark, Bronze JSONL
5 kênh, JDBC 5 bảng Walmart, snapshot consistency, ghi idempotent, completion record,
kiểm thử, Make/CI và tài liệu. Làm vertical slice nhỏ trước khi mở rộng.

Ngoài phạm vi: Silver/Gold, hash PII, dedup nghiệp vụ, features/labels, warehouse export,
Airflow, Trino, Kafka/Flink/CDC, DataHub, AWS và benchmark triệu dòng.
Giữ nguyên version/port đã pin; không xóa volume, bucket, database hoặc dữ liệu raw.

CI hosted hiện bị chặn bởi billing account theo thông báo người dùng. Lỗi này độc lập
với chất lượng code: chạy kiểm tra local và ghi evidence, không ghi hosted CI đã pass.

## 2. Thứ tự triển khai

| Task | Phụ thuộc | Bàn giao | Ước lượng |
| --- | --- | --- | --- |
| P4A-01 | — | Baseline và fixture nhỏ | 1–2 giờ |
| P4A-02 | 01 | ADR, CLI và DDL chốt | 2–4 giờ |
| P4A-03 | 02 | Manifest validation và staging distributed | 3–5 giờ |
| P4A-04 | 03 | JSONL một kênh/một ngày vào Bronze | 2–4 giờ |
| P4A-05 | 04 | Ghi lại an toàn và completion record | 3–5 giờ |
| P4A-06 | 05 | JSONL đủ 5 kênh, đủ ngày | 2–3 giờ |
| P4A-07 | 02, 05 | JDBC orders nhất quán | 3–5 giờ |
| P4A-08 | 07 | JDBC 5 bảng cùng source snapshot | 4–7 giờ |
| P4A-09 | 06, 08 | Reconciliation, recovery và persistence | 3–5 giờ |
| P4A-10 | 09 | Make/CI/runbook, evidence và bàn giao | 2–4 giờ |

Tổng dự kiến **25–44 giờ công**, không phải cam kết thời gian hoàn thành.
Mốc A: 01–05, một lát cắt JSONL chạy lại được. Mốc B: 06–08, đủ nguồn.
Mốc C: 09–10, có bằng chứng vận hành và bàn giao Silver.

## 3. Checklist chi tiết

### P4A-01 — Kiểm tra baseline và chuẩn bị fixture

- [x] Đọc AGENTS, ADR, Git status; giữ nguyên thay đổi ngoài task.
- [x] Kiểm tra RAM/đĩa và trạng thái core/batch; chỉ khởi động profile cần thiết.
- [x] Chạy `make check`, `make up-batch`, `make verify-runtime`, `make smoke-core`.
- [x] Chạy `make verify-lakehouse` và `make verify-sources`; nếu thiếu fixture, ghi rõ
  rồi dùng target tạo fixture thích hợp, không dùng ghi lại để che lỗi persistence.
- [x] Xác minh manifest/run ID và số liệu nguồn; không in credential/payload PII.
- [x] Chuẩn bị config fixture nhỏ riêng: đủ 5 kênh, 3 ngày, duplicate và Amazon drift;
  không thay config/dataset mặc định. Ghi lại ID để tái lập.

**Đạt khi:** baseline có evidence và fixture không ảnh hưởng dataset hiện hữu.
**File:** `spark/tests/`, `docs/bronze-validation.md` (mới khi triển khai).
**Commit:** `test(bronze): establish ingestion fixtures and baseline`.

### P4A-02 — Chốt contract trước khi viết pipeline

- [x] Viết ADR mới, cập nhật Bronze contract và draft DDL theo quyết định thực tế.
- [x] CLI bắt buộc `--run-id`, `--run-date`, `--channel`, `--mode`; thêm lựa chọn
  `--source jsonl|jdbc`, JDBC chỉ nhận Walmart. Validate allowlist trước mọi IO ghi.
- [x] Định nghĩa `full` là thay toàn bộ slice đã chọn; `incremental` xử lý slice chưa
  hoàn tất, chỉ skip khi completion và snapshot đích vẫn hợp lệ. Cả hai không append mù.
- [x] Tách bảng JSONL Walmart và JDBC: dự kiến `walmart_orders_export_raw` cho JSONL,
  `walmart_orders_raw` cho JDBC; 4 kênh còn lại `<channel>_orders_raw`.
- [x] Chốt audit: `_raw`, `_ingest_ts`, `_source_file`, `_source_table`, `_channel`,
  `_dataset_run_id`, `_batch_id`, `_run_date`, `_source_type`; provenance không dùng là NULL.
- [x] Chốt `_batch_id` ổn định theo run/channel/date/source và contract version;
  attempt ID riêng cho từng lần thực thi, không dùng timestamp làm batch identity.
- [x] Chốt `_parse_status`/lỗi parse và cách giữ nguyên dòng UTF-8 malformed, blank line;
  lỗi encoding/checksum fail rõ trước commit, không drop âm thầm.
- [x] JDBC `_raw` là serialization của row typed, không phải byte payload JSONL;
  bảo toàn DECIMAL, timestamp UTC, NULL và tên cột nguồn.
- [x] Chốt scope theo orders của ngày UTC/run: items/returns theo order ID, customers/products
  theo quan hệ được tham chiếu; returns không lọc nhầm theo ngày hoàn trả.
- [x] Ghi rõ customers/products có thể lặp giữa các slice ngày; Silver sẽ xử lý sau.
- [x] Chốt completion schema, location/quyền ghi và cơ chế chống hai writer cùng batch;
  driver name cố định hiện tại chỉ là giới hạn local, không thay thế thiết kế concurrency.

**Đạt khi:** không còn nhập nhằng nguồn Walmart, mode, ngày nghiệp vụ và đơn vị replace.
**Commit:** `docs(adr): define Bronze ingestion and completion semantics`.

### P4A-03 — Đọc manifest và staging cho Spark distributed

- [x] Tái sử dụng contract manifest hiện có; xác minh version, run ID, channel/date,
  key prefix, duplicate keys, row count, byte size và SHA-256.
- [x] Chỉ đọc object trong manifest hoàn tất; từ chối path traversal, URI/bucket ngoài
  allowlist, file thiếu/sai checksum. Không wildcard toàn bộ bucket hoặc mọi run.
- [x] Đọc bằng credential Spark; timeout/retry hữu hạn, streaming download có giới hạn
  dung lượng từng object và tổng slice, kiểm tra dung lượng đĩa trước download.
- [x] Mount staging volume cùng đường dẫn vào driver và cả hai worker; worker đọc-only.
  Driver-local `/tmp` đơn lẻ không đủ cho `spark.read` trên executor.
- [x] Tách staging theo attempt, publish file bằng rename sau khi verify; không tái sử dụng
  file dở hoặc xóa staging đang được job khác dùng.
- [x] Lưu mapping staging path → S3 URI gốc; `_source_file` phải là nguồn S3,
  không phải đường dẫn container do `input_file_name()` trả về.
- [x] Chứng minh executor ở cả hai worker đọc được staging bằng kiểm thử phân phối có evidence.
- [x] Cleanup đúng thư mục attempt trong finally sau khi Spark actions xong; crash leftovers
  có hướng dẫn dọn giới hạn, không xóa toàn bộ volume.

**Đạt khi:** đọc được exact manifest trên cluster, lỗi input không gây ghi Bronze.
**File:** `spark/lib/`, Compose batch, config/env example, tests.
**Commit:** `feat(bronze): validate manifests and stage distributed inputs`.

### P4A-04 — Vertical slice JSONL một kênh/một ngày

- [x] Tạo entrypoint `spark/jobs/dp1_bronze/`, tái sử dụng session và structured logging.
- [x] Bắt đầu Walmart JSONL export hoặc một kênh đơn giản trên fixture nhỏ.
- [x] Đọc từng dòng thành `_raw` không reserialize; giữ unknown fields và intentional duplicates.
- [x] Thêm audit/parse status theo schema explicit; không tự cast/clean trường nghiệp vụ.
- [x] Tạo bảng Iceberg v2, Parquet/ZSTD; kiểm tra schema bảng hiện hữu, không tự drop/recreate.
- [x] Dùng thư viện writer có filter slice đầy đủ ngay từ đầu; không append tạm rồi
  coi là idempotent. Hoàn thiện kiểm thử failure/concurrency ở P4A-05.
- [x] Assert physical count, raw multiset/fingerprint và provenance khớp nguồn.
- [x] Có unit test CLI/audit/parse và integration một job thực sự trên cluster.

**Đạt khi:** một slice đọc lại đúng payload, duplicate và nguồn gốc.
**Commit:** `feat(bronze): ingest manifest JSONL with raw provenance`.

### P4A-05 — Replay, commit và completion

- [x] Ghi replace theo run/channel/date/source bằng MERGE atomic có kiểm chứng;
  không overwrite cả ngày vì ngày đó có thể chứa nhiều run/source.
- [x] Kiểm tra mọi row đầu ra thuộc filter; xử lý slice 0 row được khai báo hợp lệ,
  phân biệt với source bị thiếu. Không để dữ liệu cũ còn sót vì dynamic overwrite rỗng.
- [x] Chạy cùng batch hai lần: row count và raw multiset không đổi; timestamp/snapshot
  có thể thay đổi. Dữ liệu slice khác phải nguyên vẹn.
- [x] Completion chứa batch/attempt, manifest fingerprint, bảng/snapshot, count, thời gian
  và source snapshot evidence; chỉ đánh dấu completed sau read-back reconciliation.
- [x] Failed/partial attempt không được mang completion của attempt cũ như kết quả mới;
  consumer kiểm tra snapshot references, không chỉ sự tồn tại của marker.
- [x] Kiểm thử lỗi sau commit bảng nhưng trước completion; retry phải khôi phục được.
- [x] Giới hạn writer đồng thời, kiểm thử conflict và retry hữu hạn; không blind retry append.
- [x] Không tuyên bố atomic nhiều bảng: consumer dùng bộ snapshot đã được completion xác nhận.

**Đạt khi:** replay không thêm bản sao, failure không tạo trạng thái thành công giả.
**Commit:** `feat(bronze): make slice writes and completion replay safe`.

### P4A-06 — Mở rộng JSONL đủ 5 kênh

- [x] Chạy toàn bộ ngày/kênh từ manifest theo thứ tự xác định, giữ writer resource budget.
- [ ] Assert Amazon rename/add field được giữ, Shopee timestamp string nguyên trạng,
  Lazada optional field thiếu và currency/amount nguyên trạng.
- [x] Assert mỗi kênh default có 3.120 physical rows, 3.000 unique orders, 120 extra copies;
  toàn JSONL 15.600. Khi dùng fixture khác, kỳ vọng lấy từ manifest, không hard-code default.
- [x] Kiểm thử hai run cùng ngày vào cùng bảng không ghi đè nhau.
- [ ] Dữ liệu malformed dùng fixture riêng có manifest/checksum hợp lệ; raw vẫn được giữ,
  parse status rõ, chưa tạo Silver quarantine trong phase này.

**Đạt khi:** schema drift và duplicate được bảo toàn ở cả 5 kênh.
**Commit:** `test(bronze): cover five-channel drift and source duplicates`.

### P4A-07 — JDBC orders một connection nhất quán

- [x] Truyền credential `spark_reader` vào runtime qua env; không dùng superuser,
  không log JDBC password/URL chứa secret; xác minh read-only permission.
- [x] Lọc đúng run prefix và UTC order date, parameter hóa hoặc validate chặt giá trị SQL.
- [x] Kiểm chứng hành vi JDBC đọc với phiên bản đã pin: transaction, autocommit,
  isolation, session init và fetch size; không giả định option write áp dụng cho read.
- [ ] Bắt đầu một partition/connection REPEATABLE READ; xác minh isolation thực tế và
  test nguồn thay đổi giữa lúc đọc bằng fixture riêng.
- [x] Bảo đảm Spark lazy evaluation/recomputation không âm thầm đọc snapshot mới;
  materialize dữ liệu đọc một lần hoặc giữ lifecycle snapshot đến hết action liên quan.
- [x] Serialize typed rows vào `_raw`, ghi bảng JDBC riêng và đối soát full-row fingerprint.
- [x] Chạy hai lần; default toàn run là 3.000 orders JDBC, không so bằng 3.120 JSONL.

**Đạt khi:** JDBC orders chính xác, nhất quán và replay-safe.
**Commit:** `feat(bronze): ingest Walmart orders through consistent JDBC reads`.

### P4A-08 — JDBC đủ 5 bảng và snapshot coordination

- [x] Thiết kế coordinator giữ transaction export snapshot đến khi tất cả readers kết thúc;
  reader import snapshot trước query, cùng database và isolation phù hợp.
- [x] Kiểm thử tích hợp JDBC import snapshot thực tế trên pinned runtime trước khi mở rộng;
  nếu không khả thi, ghi ADR thay thế rõ và chưa tick hoàn tất consistency.
- [x] Nạp orders, order_items, customers, products, returns theo scope P4A-02.
- [x] Đối soát count/hash của từng bảng từ cùng snapshot; không dùng count nguồn live
  sau transaction để kết luận snapshot ingestion sai.
- [ ] Test update/insert đồng thời trên fixture: không trộn thế hệ dữ liệu giữa bảng.
- [ ] Test reader failure/coordinator mất kết nối, rollback/close tài nguyên đúng;
  attempt incomplete và không reuse snapshot token hết hạn.
- [ ] Chỉ bật partitioned JDBC sau test import cùng snapshot ở mọi connection; nếu giữ
  một partition mỗi bảng thì ghi rõ giới hạn và để tối ưu song song sang đợt sau.
- [x] Publish completion tập bảng sau khi toàn bộ reconcile; partial retry không nhân đôi.

**Đạt khi:** 5 bảng có bằng chứng cùng snapshot, không chỉ row counts tình cờ khớp.
**Commit:** `feat(bronze): coordinate Walmart table snapshots and ingestion`.

### P4A-09 — Bộ kiểm chứng và phục hồi

- [x] Tạo verifier read-only cho count, payload fingerprint, provenance, snapshot/completion.
- [ ] Test thiếu manifest/file, checksum sai, channel/date sai, staging vượt giới hạn:
  nonzero exit, thông báo rõ và không có commit đích mới.
- [ ] Test malformed JSON hợp lệ về inventory; giữ dòng và parse status theo contract.
- [ ] Test catalog lỗi và write failure giữa pipeline; dữ liệu đã commit vẫn đọc được,
  attempt thất bại chưa completed, retry đạt kết quả chuẩn.
- [x] Test rerun, hai run cùng ngày, empty slice và nhiều attempt theo ma trận riêng.
- [ ] Restart catalog/Spark rồi verify read-only trước mọi thao tác ghi lại.
- [ ] Chạy regression `make smoke-core`, `make verify-sources`, `make verify-lakehouse`.
- [ ] Ghi duration, rows/s, staging bytes, data file count/size và tài nguyên fixture;
  không suy diễn thành benchmark scale lớn hoặc tự chạy compaction/cleanup.

**Đạt khi:** có assertion và evidence cho cả happy path lẫn failure/recovery.
**Commit:** `test(bronze): verify reconciliation recovery and persistence`.

### P4A-10 — Make, CI, tài liệu và đóng milestone

- [x] Mở rộng `make spark-submit JOB=dp1_bronze` với tham số job được validate;
  giữ `JOB=smoke`, không làm hỏng các target hiện có.
- [x] Thêm `make smoke-bronze` cho fixture nhỏ, `make verify-bronze` read-only và target
  integration riêng; ghi rõ các target này hiện mới là kế hoạch.
- [x] Unit tests nhẹ nằm trong `make check`; Spark/Docker integration chạy riêng,
  không im lặng skip rồi báo end-to-end thành công.
- [ ] Cập nhật workflow integration dùng fixture nhỏ, timeout, failure logs đã redaction,
  cleanup container không xóa volume dữ liệu local.
- [ ] Chạy lint/test/config và integration local; ghi hosted CI là blocked/not run
  nếu billing chưa được giải quyết, không đánh dấu hosted pass khi chưa có run.
- [ ] Cập nhật README, runbook, implementation-status, data model, DDL và Bronze contract.
- [ ] Hoàn tất `docs/bronze-validation.md`: commit/runtime, dataset, lệnh, expected/actual,
  snapshot, replay, lỗi được thử và giới hạn chưa test.
- [ ] Ghi bàn giao Silver: authoritative Walmart JDBC, schema mappings, parse failures,
  duplicate policy cần xử lý, completion/snapshot đầu vào và PII boundaries.
- [ ] Rà soát checklist; commit từng task, tag `v0.4a-bronze` chỉ sau DoD local đạt.
  Chưa tạo tag tuyên bố toàn Phase 4 hoàn tất.

**Đạt khi:** người khác tái lập Bronze và xác minh kết quả theo Make/runbook.
**Commit:** `docs(bronze): publish validation and Silver handoff`.

## 4. Điều kiện đóng Phase 4A

- [ ] JSONL đủ 5 kênh đối soát với manifest, duplicate/unknown fields còn nguyên.
- [ ] JDBC 5 bảng Walmart có kiểm chứng snapshot consistency và raw serialization.
- [ ] Rerun không nhân đôi; run/source khác cùng ngày không bị ghi đè.
- [ ] Không báo completion khi pipeline mới hoàn tất một phần.
- [ ] Restart vẫn đọc được Bronze; malformed input và failure có hành vi được kiểm thử.
- [ ] Lint/unit/integration local pass, evidence không chứa secret/PII payload.
- [ ] Tài liệu, Make và checklist phản ánh đúng phần đã làm và các giới hạn.

## 5. Các đợt sau — chưa thuộc DoD của Phase 4A

| Đợt | Phạm vi và đầu ra cần lập kế hoạch tiếp |
| --- | --- |
| 4B | `dp2_silver`: schema mapping, UTC/DECIMAL, deterministic dedup, hash PII, quarantine; bắt đầu Walmart rồi 5 kênh |
| 4C | Gold dim/fact do dp2 sở hữu theo ADR-0003; key strategy, DQ và query qua Spark |
| 4D | `dp3_features`, `dp3_return_labels`: point-in-time correctness, maturity và leakage tests |
| 4E | `dp4_warehouse`: publish Gold 30 ngày sang PostgreSQL atomic, retention và replay |
| Đóng Phase 4 | Benchmark skew/AQE/broadcast/salting có số liệu; batch end-to-end và toàn bộ DoD plan gốc |

## 6. Quy tắc đánh dấu và commit

1. Mọi checkbox mới để trống cho đến khi có implementation và kiểm tra tương ứng.
2. Sau mỗi task nhỏ: kiểm tra liên quan, `make check`, ghi evidence, tick đúng mục,
   commit Conventional Commits ngay. Không cần chờ hoàn thành cả nhóm lớn.
3. Nếu chỉ lập quyết định thiết kế thì ghi là quyết định, không coi là runtime đã pass.
4. Mục không áp dụng phải ghi `N/A` và lý do; mục bị chặn giữ unchecked, ghi blocker.
5. Không commit `.env`, payload, file staging, runtime snapshot token hoặc credentials.
6. Không thay version/port hoặc mở rộng phase chỉ để né một lỗi chưa giải quyết.
