# TODO — Phase 9: Data governance với DataHub

Cập nhật: **2026-11-20**. Trạng thái: **đã lập kế hoạch, chưa triển khai Phase 9**.

Mục tiêu: một DataHub chạy local trong profile `governance` (chỉ bật khi cần), có metadata của các
nguồn (PostgreSQL, Iceberg qua Lakekeeper, Kafka, Airflow), được làm giàu (owner, domain, tag `PII`,
glossary, mô tả cột, tier), và hiển thị **lineage nguồn → Bronze → Silver → Gold → warehouse** (cả
nhánh stream và CDC); script **G1** kiểm tra lineage tự động và fail khi thiếu (luồng O4, G1).

Tham chiếu: [plan §Phase 9](docs/plan.md), [bàn giao Phase 9](docs/phase9-handoff.md),
[data model](docs/data-model.md), [runbook](docs/runbook.md), ADR-0001/0004/0006/0007/0013/0015/0016/0017.
Lịch sử: [Phase 8](docs/phase8-checklist.md), [kiểm chứng CDC](docs/cdc-validation.md).

## 1. Điểm xuất phát và phạm vi

- Phase 8 đóng ở `v0.8-cdc`; mọi workflow thủ công (Trino, Skew benchmark, Airflow e2e, Kafka/Flink,
  Debezium CDC) đã xanh trên GitHub.
- Đã có sẵn:
  - `datahub/recipes/` (trống); `make up-gov` vẫn `exit 2`; AGENTS §5 dành port DataHub frontend
    **9002**, GMS phải tránh 8080 (Spark).
  - Lineage "sự thật" đã được ghi trong các ledger: Bronze completion record, Silver/Gold/DP3
    publication ledger (pin snapshot upstream), warehouse export ledger; stream/CDC có manifest và
    ground truth. Đây là nguồn đối chiếu cho G1.
  - Kafka 4.3.2 (tắt auto-create topic), PostgreSQL 16, Lakekeeper REST, MinIO với user chỉ đọc
    `trino_local` (lakehouse), role PostgreSQL chỉ đọc `trino_ro`.
- Ràng buộc đã biết:
  - **Nặng:** DataHub (GMS + frontend + search engine) cần ~3,5–5 GiB; host có ~11,7 GiB. Chỉ bật cùng
    `core` (+ nguồn đang ingest); không bật chung Spark + Flink + Trino; Airflow chỉ bật khi thử plugin.
  - **Phiên bản:** DataHub ra bản dày (1.8.0 ngày 2026-11-18, 1.9.0 ngày 2026-11-19); theo quy tắc ≥ 2 tuần,
    ứng viên là dòng 1.7.x — server và CLI phải cùng bản.
  - **Plugin Airflow** (`acryl-datahub-airflow-plugin`) hỗ trợ Airflow 3 nhưng kéo
    `apache-airflow-providers-openlineage`; cài vào image Airflow có thể lệch constraints 3.3.2 đã pin.
  - **PII:** `_raw` ở Bronze, payload stream và `walmart.customers` có tên/email: không profiling/sampling
    các đối tượng này; gắn tag `PII`. Không nới quyền Trino (`iceberg.bronze` vẫn ẩn).
  - **Chỉ đọc:** recipe dùng credential chỉ đọc; ingestion không được ghi vào nguồn, Iceberg hay Kafka
    nghiệp vụ; topic nội bộ của DataHub tạo tường minh.
- **Trong phạm vi:** compose `governance`, ingestion PostgreSQL / Iceberg (Lakekeeper) / Kafka /
  Airflow, lineage dataset-level đầy đủ các lớp, metadata-as-code (domain, glossary, tag, owner, mô tả),
  script G1, lịch ingestion, tài liệu, ảnh lineage.
- **Ngoài phạm vi:** column-level lineage đầy đủ, DataHub Actions/alerting, SSO/RBAC, Spark
  OpenLineage listener (chỉ nếu ADR chọn), cloud (Phase 10).

## 2. Quyết định cần chốt trong ADR (P9-02)

| Chủ đề | Đề xuất mặc định | Phương án thay thế / lý do loại |
|---|---|---|
| Version | Bản DataHub mới nhất đã ≥ 2 tuần (ứng viên 1.7.x), image `acryldata/datahub-*` pin digest; CLI `acryl-datahub` cùng bản | Bản 1.8/1.9: quá mới theo quy tắc |
| Thành phần | `datahub-gms`, `datahub-frontend-react` (`127.0.0.1:9002`), search engine 1 node (OpenSearch/Elasticsearch theo bản hỗ trợ), job `system-update`; **không** `datahub-actions` | Quickstart đầy đủ: thêm MySQL, Kafka, Schema Registry riêng → vượt RAM |
| Metadata store | Dùng lại PostgreSQL 16: database `datahub`, role `datahub` (bootstrap idempotent ở core) | MySQL riêng: thêm ~0,5–1 GiB |
| Kafka | Dùng lại Kafka 4.3.2; topic nội bộ DataHub tạo tường minh (setup job/kafka-init), schema registry nội bộ của GMS | Schema Registry riêng: thêm service |
| GMS port | Không publish ra host (frontend proxy GraphQL; CLI chạy trong container) | Publish port mới: phải bổ sung bảng AGENTS §5 → hỏi trước |
| Chạy ingestion | Image `datahub-ingest` riêng (CLI + plugin đã pin) chạy qua `make gov-ingest SOURCE=…` | Cài CLI vào image Airflow: xung đột constraints |
| Lịch ingestion | DAG Airflow `datahub_ingest` (pause khi tạo) gọi CLI trong **venv tách riêng** trong image Airflow (`/opt/datahub-venv`), không đụng constraints Airflow | Managed ingestion của DataHub (cần `datahub-actions`); DockerOperator (cần docker socket) |
| Lineage dataset | Phát lineage từ **ledger/contract** của dự án (Bronze → Silver → Gold → DP3 → warehouse, raw/Kafka → stream/CDC Bronze) bằng script emitter, đúng với snapshot pin | Tự suy từ Spark (OpenLineage listener): thêm jar vào Spark, phụ thuộc DataHub lúc chạy job |
| Lineage job | Plugin Airflow (OpenLineage) cho DataJob ↔ dataset, nếu tương thích constraints; nếu không, phát DataFlow/DataJob từ DAG metadata bằng script | — |
| Ingest Iceberg | Source `iceberg` với REST catalog Lakekeeper + MinIO user chỉ đọc; không profiling Bronze | Ingest qua Trino: Bronze bị ẩn có chủ đích |
| Metadata-as-code | `datahub/metadata/*.yaml` (domain, glossary term, tag `PII`/`raw`/`gold`, owner, mô tả, tier) áp dụng idempotent | Chỉnh tay trên UI: không tái lập được |
| G1 | `datahub/assertions/lineage_g1.py` (GraphQL): mỗi Gold có upstream Silver, Silver có Bronze, warehouse có Gold, CDC/stream có upstream đúng; đối chiếu ledger; exit ≠ 0 khi thiếu | Chỉ kiểm bằng mắt trên UI |

Nếu quyết định nào mâu thuẫn AGENTS.md (port, version đã pin, dependency nặng, service ngoài sơ đồ)
thì ghi rõ và hỏi lại trước khi làm.

## 3. Thứ tự thực hiện

| Task | Nội dung | Phụ thuộc | Ước lượng |
|---|---|---|---|
| P9-01 | Preflight: inventory dataset/table/topic/DAG, mốc ledger, RAM, quyền chỉ đọc | Không | 0,5 ngày |
| P9-02 | ADR-0018 governance + pin DataHub/CLI (ADR-0001) | 01 | 0,5–1 ngày |
| P9-03 | Profile `governance`: GMS, frontend, search, setup; `make up-gov`, smoke | 02 | 1–1,5 ngày |
| P9-04 | Image `datahub-ingest` + recipe PostgreSQL, Kafka | 03 | 1 ngày |
| P9-05 | Recipe Iceberg (Lakekeeper REST, chỉ đọc, không profiling Bronze) | 04 | 1 ngày |
| P9-06 | Lineage dataset-level từ ledger/contract (batch, stream, CDC, warehouse) | 05 | 1–1,5 ngày |
| P9-07 | Airflow: DataFlow/DataJob + lineage job; DAG `datahub_ingest` | 06 | 1 ngày |
| P9-08 | Metadata-as-code: domain, glossary, tag, owner, mô tả, tier | 05 | 0,5–1 ngày |
| P9-09 | G1 assertions + idempotency + ảnh lineage | 06–08 | 1 ngày |
| P9-10 | CI, tài liệu, bàn giao Phase 10, tag | 09 | 0,5–1 ngày |

## 4. Checklist triển khai

### P9-01 — Preflight

- [ ] Git sạch tại `v0.8-cdc` (+ commit sau tag); `make check` xanh.
- [ ] Inventory đối tượng cần catalog: schema PostgreSQL (`walmart`, `warehouse`), namespace Iceberg
  (`bronze`, `silver`, `gold`, `benchmark`), topic Kafka (stream, DLQ, CDC), DAG Airflow, bucket/prefix
  MinIO; đánh dấu đối tượng chứa PII.
- [ ] Ghi mốc ledger (Silver/Gold/DP3/warehouse), `cdc-batch-guard`; Phase 9 không được đổi.
- [ ] Đo RAM `core` + Kafka (Spark/Flink/Trino/Airflow dừng); ngân sách DataHub.
- [ ] Xác nhận credential chỉ đọc dùng được cho ingestion (`trino_ro` PostgreSQL, `trino_local`
  MinIO, Lakekeeper REST) hoặc cần role/user riêng `datahub_ro`.

**Đạt khi:** inventory, mốc và ngân sách ghi vào `docs/governance-validation.md`.
**Commit gợi ý:** `docs(gov): record Phase 9 preflight inventory and resource budget`.

### P9-02 — ADR và version

- [ ] Xác minh trên tài liệu chính thức: bản DataHub ≥ 2 tuần, thành phần tối thiểu, search engine được
  hỗ trợ, PostgreSQL làm metadata store, dùng Kafka ngoài + schema registry nội bộ, biến cấu hình port
  GMS, source `iceberg` (REST + S3 endpoint), `postgres`, `kafka`, plugin Airflow 3 và yêu cầu provider.
- [ ] Viết `docs/adr/0018-governance-datahub.md` chốt mọi mục §2 (gồm cách phát lineage, PII, quyền,
  lịch ingestion, giới hạn RAM).
- [ ] Ghi pin vào ADR-0001.

**Đạt khi:** ADR có nguồn, version pin, không cần ngoại lệ AGENTS hoặc đã hỏi lại.
**Commit gợi ý:** `docs(adr): define DataHub governance and pin its version`.

### P9-03 — Hạ tầng `governance`

- [ ] `docker/compose.governance.yml` profile `governance`: GMS, frontend (`127.0.0.1:9002`), search
  engine, job setup/system-update; `mem_limit`, healthcheck, volume riêng; gộp với core + `kafka`.
- [ ] Bootstrap idempotent: database/role `datahub` trên PostgreSQL, topic nội bộ DataHub trên Kafka
  (không bật auto-create); secret qua `.env` (`make env-gov`, không xoay vòng).
- [ ] Make: `env-gov`, `config-gov`, `up-gov` (thay placeholder), `smoke-gov`, `restart-gov`,
  `down`/`logs` gồm `governance`.
- [ ] Smoke: GMS healthy, frontend trả trang đăng nhập, GraphQL trả version, ghi/đọc một entity thử
  (sau đó xoá), không lộ secret trong log; RAM thực tế.

**Đạt khi:** `make up-gov && make smoke-gov` xanh từ trạng thái dừng và sau restart.
**Commit gợi ý:** `feat(gov): add DataHub governance profile`.

### P9-04 — Ingestion PostgreSQL và Kafka

- [ ] Image `datahub-ingest` (CLI + plugin `postgres`, `kafka`, `iceberg` đã pin), chạy qua
  `make gov-ingest SOURCE=<recipe>`; recipe trong `datahub/recipes/`, secret qua biến môi trường.
- [ ] Recipe PostgreSQL: `sources.walmart`, `sources.warehouse` bằng role chỉ đọc; **không profiling**
  `walmart.customers`.
- [ ] Recipe Kafka: topic stream, DLQ, CDC (không đọc message).
- [ ] Kiểm chứng: entity xuất hiện đúng số lượng; chạy lại không tạo entity mới (idempotent); nguồn
  không đổi (`cdc-batch-guard`, ledger).

**Đạt khi:** hai recipe chạy hai lần cho cùng kết quả; nguồn nguyên vẹn.
**Commit gợi ý:** `feat(gov): ingest PostgreSQL and Kafka metadata into DataHub`.

### P9-05 — Ingestion Iceberg

- [ ] Recipe `iceberg` với REST catalog Lakekeeper + MinIO user chỉ đọc; namespace `bronze`, `silver`,
  `gold` (benchmark loại trừ hoặc gắn tier thấp); không profiling/sampling Bronze.
- [ ] Schema cột đúng contract (DECIMAL, timestamp), số bảng khớp catalog.
- [ ] Platform instance/URN ổn định để lineage (P9-06) trỏ đúng.

**Đạt khi:** mọi bảng Iceberg nghiệp vụ có trong DataHub với schema; chạy lại idempotent.
**Commit gợi ý:** `feat(gov): ingest Iceberg tables through the Lakekeeper REST catalog`.

### P9-06 — Lineage dataset-level

- [ ] Script `datahub/lineage/emit_lineage.py`: đọc contract/ledger (Bronze completion, Silver/Gold/
  DP3/warehouse ledger, manifest stream/CDC) và phát upstream lineage:
  - raw JSONL/PostgreSQL `walmart` → Bronze; Bronze → Silver; Silver → Gold; Gold → DP3; Gold →
    PostgreSQL `warehouse`;
  - Kafka topic → Flink output (`raw/stream`) → Bronze stream;
  - PostgreSQL `walmart.orders` → topic CDC → Bronze CDC → Silver CDC.
- [ ] Idempotent (chạy lại không đổi aspect); URN khớp entity ingest ở P9-04/05.
- [ ] Unit test phần dựng lineage từ ledger (không cần DataHub).

**Đạt khi:** UI hiển thị lineage nguồn → Bronze → Silver → Gold → warehouse cho cả batch, stream, CDC.
**Commit gợi ý:** `feat(gov): emit dataset lineage from pipeline ledgers`.

### P9-07 — Airflow

- [ ] Quyết định theo ADR: plugin Airflow (OpenLineage) hoặc phát DataFlow/DataJob từ DAG bằng script;
  kiểm tra không phá constraints Airflow 3.3.2, `make test-dags` vẫn xanh.
- [ ] DataJob ↔ dataset (inlet/outlet) cho `dp1_*`…`dp4_*`, `dp1_stream_bronze`, `dp1_cdc`.
- [ ] DAG `datahub_ingest` (pause khi tạo, không dùng pool `spark_driver`) chạy các recipe + lineage.

**Đạt khi:** DataJob hiển thị lineage; DAG chạy thật thành công.
**Commit gợi ý:** `feat(airflow): schedule DataHub ingestion and job lineage`.

### P9-08 — Metadata-as-code

- [ ] `datahub/metadata/`: domain (Sales, Returns, Streaming, CDC…), glossary term (Order, Return label,
  Net amount…), tag `PII`/`raw`/`gold`/`quarantine`, owner, mô tả bảng/cột lấy từ `docs/data-model.md`,
  tier (Bronze/Silver/Gold).
- [ ] Áp dụng idempotent (`make gov-metadata`); tag `PII` cho mọi đối tượng có PII thô.
- [ ] Test: mọi bảng Gold có owner + domain + mô tả; mọi đối tượng PII có tag.

**Đạt khi:** metadata hiển thị trên UI; chạy lại không đổi.
**Commit gợi ý:** `feat(gov): manage domains, glossary, tags and owners as code`.

### P9-09 — G1 assertions và bằng chứng

- [ ] `datahub/assertions/lineage_g1.py` (GraphQL): mỗi Gold có upstream Silver, mỗi Silver có upstream
  Bronze, warehouse có upstream Gold, stream/CDC có upstream đúng; đối chiếu ledger; exit ≠ 0 khi thiếu.
- [ ] Test âm: xoá một cạnh lineage thử (trên entity thử) → G1 fail; khôi phục → pass.
- [ ] Idempotency toàn bộ ingestion (chạy 2 lần), nguồn không đổi; ảnh chụp lineage UI.

**Đạt khi:** `make gov-assert` xanh hai lần; có ảnh lineage.
**Commit gợi ý:** `test(gov): assert lineage coverage from sources to warehouse`.

### P9-10 — CI, tài liệu và bàn giao

- [ ] Unit test trong `make test`; workflow thủ công `governance.yml` (up gov, ingest fixture, lineage,
  G1, allowlisted events); tái hiện trên môi trường trắng trước khi bàn giao.
- [ ] README ("Chạy DataHub"), runbook, `implementation-status`, `data-model`, CLAUDE.md.
- [ ] `docs/governance-validation.md`: commit, image, lệnh, inventory, lineage, G1, RAM.
- [ ] Bàn giao Phase 10 (`docs/phase10-handoff.md`): Terraform/Ansible, mapping service → AWS, ngân sách.
- [ ] Lưu checklist vào `docs/phase9-checklist.md`; tag `v0.9-governance` khi DoD đạt.

**Commit gợi ý:** `docs(gov): publish Phase 9 validation and close the phase`.

## 5. Điều kiện đóng Phase 9

- [ ] DataHub pin version có nguồn; profile `governance` có healthcheck, `mem_limit`, chỉ bật khi cần;
  frontend `127.0.0.1:9002`, GMS không dùng 8080.
- [ ] Metadata PostgreSQL, Iceberg, Kafka, Airflow có trong DataHub; ingestion chỉ đọc, idempotent.
- [ ] UI hiển thị lineage nguồn → Bronze → Silver → Gold → warehouse (batch, stream, CDC).
- [ ] Metadata làm giàu (owner, domain, tag `PII`, glossary, mô tả, tier) quản lý bằng code.
- [ ] G1 assertion pass (và fail đúng khi thiếu cạnh); ảnh lineage lưu trong `docs/images/phase9/`.
- [ ] Ledger, 3 dataset batch và dữ liệu CDC không đổi; không lộ PII/secret.
- [ ] `make check`, `smoke-gov`, `gov-assert` xanh; tài liệu, ADR, bàn giao Phase 10 đầy đủ.

## 6. Rủi ro và cách giảm

| Rủi ro | Dấu hiệu | Cách giảm |
|---|---|---|
| Hết RAM khi bật DataHub | OOM, GMS restart | Chỉ core + Kafka + DataHub; `mem_limit`, heap nhỏ; đo ở P9-01/P9-03 |
| Bản DataHub quá mới, lỗi tương thích | Setup job/GMS lỗi | Bản ≥ 2 tuần, server = CLI; ghi nguồn |
| Plugin Airflow phá constraints | `make test-dags` đỏ, import lỗi | Venv tách riêng hoặc phát DataJob bằng script |
| Lộ PII qua profiling/sampling | Giá trị email/tên hiện trên UI | Tắt profiling Bronze/`customers`, tag `PII`, kiểm tra trong smoke |
| Ingestion ghi vào nguồn | Ledger/guard đổi | Credential chỉ đọc; so mốc sau mỗi bước |
| Kafka auto-create bị tắt làm DataHub kẹt | Log `UNKNOWN_TOPIC_OR_PARTITION` | Tạo topic DataHub tường minh trong bootstrap (bài học Phase 8) |
| Lineage lệch thực tế | UI khác ledger | Phát lineage từ ledger; G1 đối chiếu ledger |
| Ingestion không idempotent | Entity/aspect tăng mỗi lần chạy | URN ổn định, test chạy 2 lần |
| Image DataHub lớn, pull chậm/treo (quay/Docker Hub qua proxy) | Pull timeout | Pull trước, ghi runbook, retry |

## 7. Quy tắc cập nhật

1. Chỉ đánh `[x]` khi có implementation và evidence đúng commit; blocker giữ `[ ]` kèm lý do.
2. Sau mỗi task nhỏ: kiểm tra phù hợp, cập nhật evidence/checklist, commit Conventional Commits;
   chỉ commit khi exit code của `make check` là 0 (không đọc qua pipe).
3. Ưu tiên Make targets; chỉ bật service cần thiết; giữ version/port/resource limit đã pin.
4. Không commit secret, `.env`, log thô, dữ liệu; không xoá volume, bucket, DB, slot hoặc snapshot pin
   khi chưa hỏi; không đổi run ID hoặc dữ liệu 3 dataset batch.
5. Trước khi bàn giao, tái hiện workflow CI trên môi trường trắng (project Compose tách biệt).
6. Không triển khai Phase 10+ trong kế hoạch này.
