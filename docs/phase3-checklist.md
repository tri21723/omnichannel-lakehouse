# TODO — Phase 3: Iceberg + Lakekeeper

Cập nhật: **2026-11-13**. Trạng thái: **Phase 3 hoàn tất kiểm chứng local; sẵn sàng bàn giao Phase 4**.

Mục tiêu: từ core PostgreSQL/MinIO hiện có, dựng Iceberg REST catalog bằng Lakekeeper,
cho Spark tạo/ghi/đọc/MERGE/time travel một bảng kiểm thử và chứng minh dữ liệu tồn tại
sau restart. Kết thúc giai đoạn này phải có nền tảng đủ tin cậy để làm `dp1_bronze`.

Tài liệu gốc: [plan](plan.md), [kiến trúc](architecture.jpg),
[data model](data-model.md), [trạng thái](implementation-status.md).
Checklist này chia nhỏ công việc sắp làm; không thay thế hay đánh dấu hoàn thành plan gốc.

## 1. Điểm xuất phát và ranh giới

Baseline đã được kiểm tra lại và mở rộng trong phiên 2026-11-13:

- [x] Core PostgreSQL + MinIO, named volumes, role và S3 policy riêng.
- [x] Database/role `lakekeeper`; bucket `lakehouse`; credential cho Spark/Lakekeeper.
- [x] Generator batch/load 5 kênh, manifest và load Walmart idempotent.
- [x] Dataset đã kiểm chứng: `b1e64afaff0176de180a`, 15.600 dòng JSONL;
  Walmart JDBC có 3.000 đơn, JSONL Walmart có 3.120 dòng gồm duplicate.
- [x] Báo cáo [core](core-validation.md) và [generator](generator-validation.md);
  lần kiểm tra đã ghi nhận gần nhất có 23 unit test pass.

Trong phạm vi: kiểm tra version, Lakekeeper migration/bootstrap, Spark runtime tối thiểu,
namespace/DDL kiểm thử, kiểm thử quyền truy cập, persistence, lệnh Make, CI và tài liệu.

Ngoài phạm vi: job Bronze nghiệp vụ hoàn chỉnh, Silver/Gold transforms, Airflow,
Trino UI/query, streaming, CDC consumer, DataHub và AWS. Việc thêm runtime Spark là
điều kiện kiểm thử Phase 3, không có nghĩa Phase 4 đã hoàn thành.

## 2. Thứ tự và mốc bàn giao

| Task | Phụ thuộc | Kết quả cần bàn giao | Ước lượng công sức |
| --- | --- | --- | --- |
| P3-01 | — | Baseline core/source được kiểm tra lại | 0,5–1 giờ |
| P3-02 | P3-01 | Ma trận version và quyết định catalog/IO | 2–4 giờ |
| P3-03 | P3-02 | Build Spark runtime và kiểm tra classpath | 2–4 giờ |
| P3-04 | P3-02 | Lakekeeper migrate/serve chạy healthy | 2–3 giờ |
| P3-05 | P3-04 | Project/warehouse bootstrap chạy lại an toàn | 2–3 giờ |
| P3-06 | P3-03, P3-05 | Spark master/worker kết nối catalog/S3 | 2–4 giờ |
| P3-07 | P3-06 | Namespace, bảng kiểm thử và thao tác Iceberg pass | 2–4 giờ |
| P3-08 | P3-07 | Restart, retry, quyền truy cập được kiểm chứng | 1–3 giờ |
| P3-09 | P3-08 | CI, runbook, evidence, README hoàn chỉnh | 1–2 giờ |
| P3-10 | P3-09 | Hợp đồng bàn giao cho Bronze | 1–2 giờ |

Tổng dự kiến **15,5–30 giờ**, phụ thuộc tương thích thư viện, tốc độ tải/build và tài nguyên.
Đây là ước lượng lập kế hoạch, không phải cam kết deadline. Triển khai tuần tự theo bảng.

Mốc A: P3-01…05 — catalog và storage sẵn sàng.
Mốc B: P3-06…08 — chứng minh đọc/ghi và độ bền dữ liệu.
Mốc C: P3-09…10 — có hướng dẫn tái lập và đầu vào rõ cho Phase 4.

## 3. Checklist triển khai chi tiết

### P3-01 — Kiểm tra lại baseline sau thời gian tạm dừng

- [x] Đọc `AGENTS.md`, ADR và trạng thái Git; giữ nguyên thay đổi ngoài task.
- [x] Kiểm tra Docker/Compose, RAM/CPU/đĩa khả dụng và port 8181, 7077, 8080.
- [x] Kiểm tra tên biến trong `.env` so với `.env.example`, không in giá trị credential.
- [x] Khi bắt đầu triển khai, chạy `make up-core`, `make smoke-core`, `make verify-sources`.
- [x] Nếu thiếu dataset: xác định đúng config/run_id trước khi chạy generator; không
  mặc định sinh dataset khác hay xóa volume để sửa lỗi.
- [x] Ghi lại baseline row count/manifest và kết quả `make check`.

**Đạt khi:** nguồn cũ vẫn đối soát được; có đủ tài nguyên và không xung đột port.
**File:** cập nhật `docs/runbook.md` nếu phát sinh lỗi môi trường mới.
**Commit:** chỉ tạo commit nếu có thay đổi tài liệu/code; không tạo commit rỗng cho kiểm tra.

### P3-02 — Xác minh và pin ma trận tương thích

- [x] Tra tài liệu chính thức cho đúng các phiên bản ứng viên; ghi URL, ngày kiểm tra,
  tag/image hoặc tọa độ Maven, Java, Scala và Python yêu cầu vào ADR-0001.
- [x] Chọn bộ Spark ↔ Iceberg Spark runtime ↔ Scala binary version ↔ Java ↔ Python 3.11.
- [x] Xác minh phiên bản Lakekeeper, schema migration, PostgreSQL backend và Iceberg REST API.
- [x] Xác minh S3 endpoint/path-style/region với MinIO đang dùng.
- [x] Chốt FileIO cho **bảng Iceberg** và đường đọc **raw JSONL** riêng biệt. Không suy luận
  rằng S3FileIO của Iceberg tự cung cấp filesystem cho `spark.read.json`.
- [x] Nếu đọc raw qua S3A: pin Hadoop-AWS khớp Hadoop trong Spark và AWS SDK tương ứng;
  tránh đưa các JAR SDK trùng/xung đột vào classpath.
- [x] Pin PostgreSQL JDBC để chuẩn bị Phase 4; chưa mở rộng quyền `spark_reader`.
- [x] Chốt catalog authentication local, warehouse identifier, static/vended credentials,
  đường dẫn API và cơ chế credential encryption theo release Lakekeeper đã chọn.
- [x] Ghi ADR mới về catalog/IO, nêu lựa chọn, giới hạn local và tác động tới Trino sau này.
- [x] Chỉ chuyển trạng thái “validated” sau khi smoke runtime thực sự pass.

**Đạt khi:** đủ version/nguồn tham khảo để build; không dùng `latest`, không copy config
ví dụ trong plan như thể đã kiểm chứng. Chưa chọn version cụ thể trong TODO này.
**File:** `docs/adr/0001-versions.md`, ADR mới, `.env.example` nếu cần.
**Commit gợi ý:** `docs(adr): pin lakehouse runtime compatibility matrix`.

### P3-03 — Build Spark runtime tái lập

- [x] Thêm `docker/images/Dockerfile.spark` theo ma trận đã chốt, pin base image và JAR.
- [x] Tải dependency lúc build; kiểm tra checksum từ nguồn tin cậy khi có; tránh tải Maven
  động mỗi lần chạy job.
- [x] Cài Python/runtime đồng nhất giữa driver và executor; xác minh `spark-submit --version`.
- [x] Kiểm tra sự hiện diện của Iceberg extension, REST catalog, FileIO và JDBC classes.
- [x] Kiểm tra filesystem raw nếu chọn S3A; phân biệt lỗi classpath với lỗi quyền S3.
- [x] Sửa `.dockerignore` đúng phạm vi: hiện allowlist chỉ cho generator; bổ sung source
  Spark cần build, vẫn loại `.env`, `.git`, `.venv` và dữ liệu local.
- [x] Thêm `make build-batch`, build lại lần hai để kiểm tra cache và đường build.

**Đạt khi:** image build được, runtime/JAR version khớp ADR, source cần thiết có trong image.
**File:** Dockerfile Spark, `.dockerignore`, `Makefile`, config trong `docker/`.
**Commit gợi ý:** `feat(spark): add pinned lakehouse runtime image`.

### P3-04 — Dựng Lakekeeper bằng PostgreSQL hiện có

- [x] Thêm `docker/compose.batch.yml`, giữ cùng project/network/volume với core.
- [x] Khai báo Lakekeeper migration one-shot và server; migration thành công trước serve.
- [x] Dùng DB/role `lakekeeper`, không dùng PostgreSQL superuser để vận hành catalog.
- [x] Đọc S3 credential riêng từ env; không thay credential generator hoặc core.
- [x] Nếu cần encryption key: sinh một lần, giữ ổn định qua restart, không ghi Git/log.
- [x] Có `mem_limit`, healthcheck đúng endpoint của release và thứ tự phụ thuộc rõ.
- [x] Map REST port 8181 trên localhost; phân biệt API health, management và catalog.
- [x] Migrate lần hai không lỗi; ghi cách xử lý migration thất bại mà không xóa DB.

**Đạt khi:** server healthy, schema catalog tồn tại trong DB đúng, log không chứa secret.
**File:** `docker/compose.batch.yml`, script/config trong `docker/`, `.env.example`, runbook.
**Commit gợi ý:** `feat(lakekeeper): add catalog migration and service`.

### P3-05 — Bootstrap project và warehouse idempotent

- [x] Tạo script gọi management API để tìm/tạo project, warehouse theo tên cấu hình.
- [x] Xác minh warehouse identifier và catalog URI theo response/API thực tế; không
  hard-code giả định `<project>/<warehouse>` từ ví dụ trong plan.
- [x] Warehouse trỏ đúng bucket/prefix `lakehouse`; kiểm tra endpoint, region, path-style.
- [x] Tra cứu tài nguyên đã tồn tại; xử lý race/conflict có giới hạn, không nuốt lỗi 4xx/5xx.
- [x] Nếu warehouse tồn tại nhưng cấu hình khác: fail rõ, không tự xóa/tạo lại hay đổi location.
- [x] Lưu ID/config không nhạy cảm theo cách ổn định để Spark sử dụng; không commit token/key.
- [x] Bổ sung biến mới vào `.env.example` và cơ chế bổ sung `.env` hiện có an toàn:
  `make env` hiện chỉ tạo file mới, không tự thêm biến vào file cũ.
- [x] Có timeout/retry hữu hạn cho kết nối API và test lỗi quyền, không lặp vô hạn.
- [x] Thêm `make init-lakehouse`; chạy hai lần vẫn cùng project/warehouse ID.

**Đạt khi:** bootstrap tái lập, không tạo bản sao, không thay đổi dữ liệu core.
**File:** script bootstrap trong `docker/`, tests tương ứng, `Makefile`, ADR/runbook.
**Commit gợi ý:** `feat(lakekeeper): bootstrap project and warehouse safely`.

### P3-06 — Kết nối Spark cluster với catalog và MinIO

- [x] Khai báo Spark master + 2 worker trong profile batch, giới hạn từng container.
- [x] Giữ master 7077/UI 8080; chỉ publish UI cần thiết ra localhost.
- [x] Worker mặc định 2 core/2GB theo AGENTS; phân biệt executor memory và overhead
  của container để tránh đặt heap bằng toàn bộ `mem_limit`.
- [x] Chốt vị trí Spark driver ở giai đoạn này và network cho driver/executor hai chiều;
  Airflow client mode là giai đoạn sau.
- [x] Tạo `spark/lib` dùng chung để dựng SparkSession, REST catalog và Iceberg extensions.
- [x] Lấy endpoint/credential từ env/config; log được tên cấu hình nhưng che giá trị bí mật.
- [x] Khai báo catalog `lh`, timezone UTC, shuffle partitions phù hợp dataset nhỏ.
- [x] Kiểm tra Spark thấy cả hai worker; chạy một action thực sự trên executor.
- [x] Kiểm tra credential runtime được phân phối đúng cho cả driver và executor.
- [x] Thêm target `spark-submit` có tham số rõ và job kiểm thử riêng, không ghi vào nguồn raw.

**Đạt khi:** Spark job chạy trên cluster, truy cập được REST và đúng S3 prefix bằng role riêng.
**File:** Compose batch, config Spark, `spark/lib`, job kiểm thử, `Makefile`.
**Commit gợi ý:** `feat(spark): connect cluster to Iceberg REST catalog`.

### P3-07 — Namespace, table contract và smoke Iceberg

- [x] Tạo namespace `bronze`, `silver`, `gold` idempotent; nghiệp vụ Silver/Gold chưa tạo vội.
- [x] Dùng bảng fixture riêng, ví dụ `bronze._smoke_orders`, không dùng bảng đơn hàng thật.
- [x] Chốt DDL fixture: key, timestamp, amount DECIMAL, format v2, Parquet/ZSTD và partition.
- [x] Chạy CREATE → INSERT → SELECT; assert số dòng và giá trị, không chỉ exit code.
- [x] Lưu snapshot ID sau INSERT; MERGE một dòng cũ và thêm một dòng mới; assert kết quả.
- [x] Time travel về snapshot cũ; chứng minh kết quả khác trạng thái hiện tại theo dự kiến.
- [x] Thử thêm cột nullable, ghi/đọc lại và kiểm tra dữ liệu cũ vẫn đọc được.
- [x] Kiểm tra metadata/data objects tồn tại dưới đúng warehouse prefix trên MinIO.
- [x] Chuẩn bị DDL Bronze dự kiến và ghi rõ phần nào mới là contract, chưa chạy ingestion.
- [x] Tách SQLFluff dialect cho SQL Spark/Iceberg: config hiện tại mặc định PostgreSQL.
- [x] Test chạy lại không nhân đôi business rows. Không yêu cầu snapshot ID hoặc thời gian
  commit giống nhau sau mỗi lần chạy; giữ snapshot dùng cho test time travel.

**Đạt khi:** có assertion cho create/read/MERGE/time travel/evolution và rerun.
**File:** `sql/iceberg/`, `spark/tests/`, job smoke, `docs/data-model.md`.
**Commit gợi ý:** `test(lakehouse): verify Iceberg writes snapshots and evolution`.

### P3-08 — Kiểm tra vận hành và bảo toàn dữ liệu

- [x] Chạy `init-lakehouse` và `smoke-lakehouse` hai lần; kiểm tra ID và row count.
- [x] Restart Lakekeeper/Spark; đọc lại bảng và snapshot cũ **trước khi chạy lại job ghi**.
- [x] Kiểm tra credential/encryption key được giữ ổn định, catalog vẫn mở được warehouse.
- [x] Kiểm tra quyền âm: generator không đọc lakehouse; Spark không ghi raw;
  test file riêng, tuyệt đối không sửa dataset generator đã publish.
- [x] Mô phỏng catalog chưa sẵn sàng và cấu hình sai trên job test: lỗi rõ, retry hữu hạn,
  không làm mất bảng đã commit. Không sửa shared credential đang dùng để tiêm lỗi.
- [x] Chạy lại `make smoke-core` và `make verify-sources` để phát hiện regression.
- [x] Kiểm tra `make down` dừng được cả core + batch bằng tập Compose/profile thống nhất,
  không truyền `-v`; tránh để sót container sau khi thêm file Compose mới.
- [x] Ghi kế hoạch maintenance: compaction, retention snapshots, orphan cleanup.
  Chỉ thử compaction trên fixture; chưa chạy expire/orphan cleanup trên dữ liệu dự án.
- [x] Nếu cần thử DROP: chỉ dùng bảng fixture do test vừa tạo; không purge bảng nghiệp vụ.

**Đạt khi:** dữ liệu trước restart còn nguyên, quyền đúng, core/source vẫn pass.
**File:** script/target kiểm thử, runbook, báo cáo kết quả.
**Commit gợi ý:** `test(lakehouse): cover bootstrap replay and restart persistence`.

### P3-09 — Makefile, CI và tài liệu sử dụng

- [x] Lệnh thông thường chỉ cần Make; `make help` phân biệt target có thật và chưa triển khai.
- [x] Compose validation chạy được cho core và core+batch, không in config chứa credential.
- [x] Thêm test config/bootstrap và Spark local phù hợp; giữ bộ test generator hiện có.
- [x] Tách test unit nhẹ khỏi integration Docker; CI không báo pass bằng cách skip im lặng.
- [x] Job integration dùng tài nguyên/test fixture riêng, có timeout, thu log đã che secret.
- [x] README hướng dẫn resume: core → batch/catalog → smoke; có cách dừng giữ dữ liệu.
- [x] Runbook có lỗi JAR/Scala, S3 path-style, credential, warehouse ID, migration,
  worker không gọi được driver và thiếu RAM; ghi triệu chứng + cách kiểm tra.
- [x] Ghi `docs/lakehouse-validation.md`: version thực tế, lệnh đã chạy, assertion,
  row count, snapshot minh họa, persistence và các giới hạn chưa kiểm thử.
- [x] Cập nhật implementation-status/ADR từ bằng chứng; tick TODO đúng task đã đạt.

**Đạt khi:** người đọc repo có thể dựng lại và tự kiểm chứng theo tài liệu.
**Commit gợi ý:** `docs(lakehouse): document setup validation and troubleshooting`.

### P3-10 — Bàn giao hợp đồng cho Phase 4 Bronze

- [x] Chốt cách chọn `run_id`, manifest URI, `run-date`, channel và mode cho `dp1_bronze`.
- [x] File ingestion chỉ đọc danh sách file của manifest đã hoàn tất, không wildcard mọi run.
- [x] Ghi riêng kỳ vọng JSONL vật lý và JDBC duy nhất: 3.120 dòng Walmart JSONL không phải
  3.000 dòng Walmart JDBC. Không dùng chung một assertion row count cho hai đường đọc.
- [x] JDBC lọc đúng tập ID của run; thiết kế cách đọc snapshot nhất quán khi nguồn có update.
- [x] Tách provenance `_source_file` và `_source_table`, `_batch_id`, `_channel`, `_ingest_ts`;
  chốt cách giữ payload thô và schema lạ.
- [x] Chọn ranh giới rerun (run/channel/date/source) và cơ chế ghi idempotent cho Bronze;
  vẫn giữ intentional duplicates của file nguồn, không dedup nghiệp vụ ở Bronze.
- [x] Xác định khi nào xem là ingest xong nếu job có nhiều bảng; tránh tuyên bố atomic
  toàn pipeline chỉ vì từng bảng Iceberg có commit riêng.
- [x] Cập nhật ADR/data model cho quyết định mới; mở checklist Phase 4 riêng khi bắt đầu.

**Đạt khi:** có contract đủ để viết `dp1_bronze`; chưa tuyên bố đã có Bronze production job.
**Commit gợi ý:** `docs(bronze): define manifest ingestion and replay contract`.

## 4. Giao diện Make dự kiến

Các target dưới đây đã có implementation. `spark-submit` hiện hỗ trợ `JOB=smoke`;
job nghiệp vụ sẽ được thêm theo Phase 4. Xem docs/lakehouse-validation.md.

| Target | Hành vi dự kiến |
| --- | --- |
| `make config-batch` | Validate Compose core+batch, không lộ secret |
| `make build-batch` | Build runtime đã pin |
| `make up-batch` | Đưa core/batch về healthy và thực hiện migration/bootstrap theo thứ tự |
| `make init-lakehouse` | Tra cứu/tạo project, warehouse, namespace idempotent |
| `make spark-submit JOB=<job>` | Submit job bằng driver/network đã chốt |
| `make smoke-lakehouse` | Kiểm thử ghi/đọc/MERGE/time travel có assertion |
| `make restart-lakehouse` | Restart dịch vụ liên quan, giữ nguyên dữ liệu |
| `make verify-lakehouse` | Đọc kiểm chứng dữ liệu/snapshot sẵn có, không ghi để che lỗi persistence |
| `make down` | Dừng toàn bộ stack đã triển khai, giữ named volumes |

## 5. Điều kiện đóng Phase 3

- [x] Version được pin và có bằng chứng runtime tương thích.
- [x] Lakekeeper dùng đúng DB, warehouse và S3 credential riêng.
- [x] Bootstrap chạy hai lần không tạo project/warehouse/namespace trùng.
- [x] Spark cluster thực hiện được create/insert/read/MERGE/time travel/schema evolution.
- [x] Sau restart vẫn đọc được dữ liệu và snapshot cũ mà không ghi lại fixture.
- [x] Dữ liệu source trước đó đối soát đúng; không thay đổi raw đã publish.
- [x] Lint, unit test và integration check cần thiết pass; lưu evidence không chứa secret.
- [x] README/runbook/ADR/TODO cập nhật; các giới hạn nêu rõ.
- [x] Mỗi task hoàn tất đã commit; chỉ tag milestone Phase 3 khi các điều kiện trên đạt.

## 6. Quy tắc cập nhật và commit

1. Bắt đầu task bằng đọc dependency và trạng thái Git; triển khai trong phạm vi task.
2. Chạy kiểm tra liên quan; sửa lỗi trước khi đánh dấu hoàn thành.
3. Ghi evidence, tick checkbox và cập nhật tài liệu cùng thay đổi tương ứng.
4. Commit ngay theo Conventional Commits sau mỗi task nhỏ; ghi hash khi báo kết quả.
5. Không tự đánh dấu hoàn thành chỉ vì đã tạo file hoặc service lên healthy.
6. Không nâng version đã pin, xóa volume/bucket/DB hoặc tạo tài nguyên cloud trong checklist này.
7. Nếu chưa đạt, ghi rõ blocker và bước xử lý; giữ checkbox chưa hoàn thành.


## Kết quả đóng giai đoạn — 2026-11-13

Đã kiểm chứng local cả 10 nhóm task. Các mục điều kiện S3A/DROP/compaction được
xử lý theo quyết định phạm vi: không thêm S3A, không DROP/purge dữ liệu, maintenance
được lập chính sách trước khi vận hành. Đây không phải bằng chứng các thao tác phá hủy
đã chạy. CI integration đã thêm workflow thủ công; chưa có kết quả hosted GitHub.

Runtime: Spark 3.5.5 / Scala 2.12.18 / Hadoop 3.3.4 / Java 17 / Python 3.11.11;
Iceberg 1.9.2; Lakekeeper v0.9.3. JDBC/Iceberg/AWS classes load thành công.
Sai catalog path làm job thất bại đúng kỳ vọng, sau đó bảng/snapshot cũ vẫn đọc được.
Xem báo cáo validation, runbook và docs/bronze-contract.md trước khi làm Phase 4.
