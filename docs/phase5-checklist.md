# Phase 5 checklist — Orchestration với Airflow (hoàn thành)

Cập nhật: **2026-11-18**. Trạng thái: **hoàn thành Phase 5 (2026-11-18), tag `v0.5-orchestration`**.

Mục tiêu: điều phối chuỗi batch đã hoàn thành ở Phase 4 (`dp1_bronze` → `dp2_silver` →
`dp2_gold` → `dp3_features` / `dp3_return_labels` → `dp4_warehouse`) bằng Airflow chạy trên
Docker local, giữ nguyên idempotency, input pin theo ledger và giới hạn một Spark driver;
trigger từ UI chạy xanh toàn chuỗi và backfill 3 ngày chạy lại 2 lần cho cùng kết quả.

Tham chiếu: [plan §Phase 5](plan.md), [bàn giao Phase 5](phase5-handoff.md),
[runbook](runbook.md), [data model](data-model.md),
[trạng thái triển khai](implementation-status.md), ADR-0001/0004/0007–0012.
Lịch sử: [Phase 4F](phase4f-checklist.md), [skew benchmark](benchmarks/skew.md).

## 1. Điểm xuất phát và phạm vi

- Phase 4 đóng ở `v0.4f-benchmark`: mọi job Spark có CLI ổn định, ledger hoàn tất, skip khi
  chạy lại cùng input, verify/reconcile read-only. Hợp đồng vận hành: `docs/phase5-handoff.md`.
- Dataset: small fixture `186ae107d0793d2ee237` (375 orders, dùng cho e2e/CI) và default
  reference `b1e64afaff0176de180a` (15.000 orders, dùng cho backfill local), ngày
  `2026-01-01..2026-01-03`. Manifest nguồn: `s3://raw/manifests/<run_id>/stream_manifest.json`.
- Hạ tầng đã có: DB `airflow` + role `airflow` (ADR-0004), biến `AIRFLOW_UID`,
  `AIRFLOW_PARALLELISM=1`, `AIRFLOW_DB_PASSWORD` trong `.env.example`; port 8081 đã dành.
  `make up-orch` hiện là placeholder `exit 2`; `airflow/{dags,plugins,tests}` đang trống.
- Ràng buộc hiện tại cần xử lý:
  - `spark/lib/session.py` cố định `spark.driver.host=omnichannel-spark-driver`.
  - Job đọc/ghi ledger trong volume `lakehouse-config` (`/runtime`); Bronze cần volume
    `bronze-staging` (`/staging`) dùng chung với worker.
  - Code job được COPY vào image Spark; đổi code phải `make build-batch`.
  - Chỉ một Spark driver tại một thời điểm (tên container/driver host cố định, 2 core).
  - RAM: `mem_limit` core + batch ≈ 7,5 GiB trên host ~11,7 GiB; Airflow + driver phải vừa
    phần còn lại (plan gợi ý Airflow ~2 GiB).
- **Trong phạm vi:** image + compose profile `orch`, DAG theo job, controller chuỗi ngày,
  sensor manifest, retries/timeout/callback, backfill idempotent, test DagBag, e2e, CI, tài liệu.
- **Ngoài phạm vi:** `dp1_cdc` (Phase 8), lineage DataHub (O4, Phase 9), Trino (Phase 6),
  streaming, cloud, đổi version/port đã pin, CeleryExecutor/K8s, tự động expire snapshot
  hoặc xóa dữ liệu (`maintenance_iceberg` chỉ ở chế độ báo cáo, xem P5-09).

## 2. Quyết định cần chốt trong ADR (P5-02)

| Chủ đề | Đề xuất mặc định | Phương án thay thế / lý do loại |
|---|---|---|
| Dòng version Airflow | Xác minh trạng thái hỗ trợ 2.x/3.x trên tài liệu chính thức; ưu tiên dòng còn được hỗ trợ, có image `python3.11` và constraints file; pin exact version vào ADR-0001 | Chọn theo tính năng: SLA kiểu 2.x không còn ở 3.x, nên timeout/cảnh báo phải theo version được chọn |
| Cách submit Spark | Airflow làm driver client mode: image Airflow có Java 17 + PySpark 3.5.5 + đúng các jar của image Spark (copy từ image Spark, kiểm tra checksum), chạy `spark-submit` tới `spark://spark-master:7077` | Gọi `docker compose run` qua `docker.sock`: loại vì trao quyền tương đương root; Spark REST/cluster mode: loại vì standalone Python không hỗ trợ cluster mode |
| Code job trong Airflow | Image Airflow COPY `spark/` + `sql/` giống image Spark; DAG mount read-only để dev nhanh | Mount `spark/` từ host: dễ lệch với image Spark và evidence image ID |
| Topology DAG | Mỗi job một DAG trùng tên (`schedule=None`, nhận `logical_date`/params) + controller `batch_daily` gọi lần lượt bằng `TriggerDagRunOperator(wait_for_completion, deferrable)` | Assets/Datasets: khó truyền đúng ngày logic; ExternalTaskSensor: dễ lệch `logical_date`, chiếm slot |
| Chống deadlock slot | Controller dùng operator/sensor deferrable (cần triggerer) hoặc `parallelism=2` + pool `spark_driver` 1 slot cho mọi task Spark | `parallelism=1` + chờ đồng bộ ⇒ task chờ chiếm slot duy nhất, DAG con không chạy được |
| Ngày logic | `ds` → `--run-date` (Bronze/Silver/warehouse) và `--as-of-date` (DP3); Gold `--mode full` theo run, dựa vào skip; demo `start_date=2026-01-01`, `end_date=2026-01-03` hoặc `catchup=False` + backfill tường minh | Catchup không có `end_date` sẽ tạo hàng trăm run tới hôm nay |
| Dataset run | Param `dataset_run_id` (regex `[0-9a-f]{20}`, mặc định từ env/Variable), không hard-code | — |
| Đồng thời với `make` | Khóa driver dùng chung (`flock /runtime/locks/spark-driver.lock`) cho cả Make target và task Airflow, cộng pool 1 slot | Chỉ dựa vào pool: không chặn được người chạy `make spark-submit` cùng lúc |
| Kết quả task | Exit code ≠ 0 ⇒ fail; `skipped=true` vẫn là success; event allowlist (`docker/batch/ci_events.py`) đẩy vào XCom/log | Parse raw log tự do: có thể lộ dữ liệu |
| Secret | Sinh bởi `make env-orch` (fernet key, secret key, admin password) không xoay vòng; connection qua `AIRFLOW_CONN_*`/env; không commit | Tạo connection bằng tay trong UI: không tái lập được |
| Test DAG | `make test-dags` chạy pytest DagBag trong image Airflow; `make test` bỏ qua test Airflow khi `.venv` không cài Airflow (`importorskip`), CI có job riêng | Cài Airflow vào `.venv` dev: nặng và lệch constraints |

Nếu một quyết định mâu thuẫn AGENTS.md (ví dụ cần `parallelism=2`, thêm service ngoài sơ đồ,
mount `docker.sock`), ghi rõ mâu thuẫn và hỏi lại trước khi triển khai.

## 3. Thứ tự thực hiện

| Task | Nội dung | Phụ thuộc | Ước lượng |
|---|---|---|---|
| P5-01 | Preflight: trạng thái Phase 4, tài nguyên, dataset | Không | 0,5 ngày |
| P5-02 | ADR-0013 orchestration + pin version Airflow (ADR-0001) | 01 | 0,5–1 ngày |
| P5-03 | Chuẩn bị code Spark dùng chung: driver host, khóa driver, tóm tắt event | 02 | 0,5–1 ngày |
| P5-04 | Image Airflow, compose `orch`, env, Make targets | 02, 03 | 1–2 ngày |
| P5-05 | Khung DAG dùng chung + test DagBag | 04 | 0,5–1 ngày |
| P5-06 | DAG theo job: Bronze, Silver, Gold, DP3, warehouse | 05 | 1–2 ngày |
| P5-07 | Controller `batch_daily`, sensor manifest, phụ thuộc theo ngày | 06 | 0,5–1 ngày |
| P5-08 | E2E, backfill 3 ngày ×2, lỗi/khôi phục, bằng chứng | 07 | 1–2 ngày |
| P5-09 | `maintenance_iceberg` chỉ báo cáo (dry-run) | 07 | 0,5 ngày (tùy chọn) |
| P5-10 | CI, tài liệu, bàn giao Phase 6, tag | 08 (09) | 0,5–1 ngày |

Ước lượng là thời gian làm việc. Với default dataset, một ngày logic dự kiến ~30 phút
(warehouse ~15 phút); backfill 3 ngày × 2 lần cần ~3 giờ máy.

## 4. Checklist triển khai

### P5-01 — Preflight và chốt đầu vào

- [x] Git sạch, đúng tag `v0.4f-benchmark`; `make check` xanh; ghi commit và image ID Spark.
- [x] `make up-batch`; chạy `verify-gold`, `verify-features`, `verify-labels`, `verify-warehouse`
  cho hai dataset; ghi hash ledger Gold/DP3/warehouse làm mốc trước Phase 5.
- [x] Xác nhận manifest nguồn của hai dataset có trên MinIO và Bronze/Silver completion của
  từng channel/ngày; ghi danh sách slice (5 kênh JSONL + Walmart JDBC × 3 ngày).
- [x] Đo RAM/CPU khi core + batch idle và khi chạy một job Spark (`docker stats`), disk trống;
  tính ngân sách cho Airflow (UI/scheduler/triggerer) + driver; nếu không vừa, đề xuất phương án
  (giảm `mem_limit` không cần thiết, không bật profile khác) trước khi build.
- [x] Đo thời gian thực tế từng job theo ngày trên default dataset để đặt `execution_timeout`.

**Đạt khi:** có mốc trạng thái, danh sách slice và ngân sách RAM ghi vào `docs/orchestration-validation.md`.
**Bằng chứng:** [orchestration-validation](orchestration-validation.md) §P5-01. **Commit:** `ac3675b`.

### P5-02 — ADR và version

- [x] Xác minh trên tài liệu chính thức: dòng Airflow còn được hỗ trợ, image `python3.11`,
  constraints, provider `apache-spark` (nếu dùng `SparkSubmitOperator`), tương thích Java 17 /
  PySpark 3.5.5 / Python 3.11 của worker; ghi exact version + nguồn + ngày vào ADR-0001.
- [x] Viết `docs/adr/0013-airflow-orchestration.md` (kiểm tra số ADR trước khi tạo) chốt mọi
  mục ở §2: submit, topology, ngày logic, concurrency, lock, retries/timeout, secret, test, RAM.
- [x] Bảng mapping job → DAG → tham số CLI → input ledger → output → điều kiện skip, lấy từ
  `docs/phase5-handoff.md` và kiểm lại với `--help` của từng job.
- [x] Ghi rõ semantics: Gold chạy `--mode full` theo dataset sau Silver của ngày đó; DP3
  `--as-of-date ds`; warehouse `--run-date ds` (cửa sổ 30 ngày); verify sau mỗi publish.
- [x] Ghi tiêu chí thất bại: sensor timeout, job lỗi, verify lỗi, lock bận; hành vi retry.

**Đạt khi:** ADR được duyệt (hỏi lại nếu cần ngoại lệ AGENTS), version pin có nguồn.
**Bằng chứng:** [ADR-0013](adr/0013-airflow-orchestration.md), ADR-0001 (Airflow 3.3.2); chốt theo đề xuất §2,
giữ `parallelism=1` nên không cần ngoại lệ AGENTS. **Commit:** `da21815`, `792d86e`.

### P5-03 — Chuẩn bị code Spark dùng chung

- [x] `spark/lib/session.py`: `spark.driver.host` lấy từ `SPARK_DRIVER_HOST` (mặc định giữ
  `omnichannel-spark-driver`); thêm vào `.env.example`/compose; không đổi hành vi Make hiện tại.
- [x] Khóa driver dùng chung trên `/runtime/locks/spark-driver.lock` (flock không chặn, báo lỗi
  rõ `driver_busy`), bọc mọi Make target Spark và task Airflow; test khóa (giữ/nhả, process chết).
- [x] Helper tóm tắt kết quả job từ event JSON đã allowlist (`skipped`, publication/export ID,
  run_date) để đưa vào XCom; dùng lại `docker/batch/ci_events.py`, không copy logic.
- [x] Chạy lại toàn bộ regression Phase 4 (`make check`, `test-review-regressions`, verify các
  stage) sau khi đổi session/lock; replay một job xác nhận vẫn skip.

**Đạt khi:** job chạy được với driver host khác mặc định; hai driver đồng thời bị chặn có kiểm soát.
**Bằng chứng:** `spark/lib/driver_lock.py` (+`--wait`), test khóa; busy → exit 75 qua `make`.
**Commit:** `4b881f4`, `41e5fb7`.

### P5-04 — Image Airflow, compose và Make

- [x] `docker/images/Dockerfile.airflow`: base Airflow đã pin (python3.11), cài theo constraints;
  Java 17, PySpark 3.5.5, các jar Iceberg/AWS/PostgreSQL lấy từ image Spark (checksum đã kiểm);
  COPY `spark/`, `sql/`, `docker/batch/ci_events.py`. *Non-root: không làm* — container chạy root như
  service `spark-submit` vì ledger/staging trong volume thuộc root (ADR-0013 §2).
- [x] `docker/compose.orchestration.yml` profile `orch`: service Airflow (standalone theo plan
  hoặc scheduler + api/webserver + triggerer nếu ADR chốt), `127.0.0.1:8081`, `mem_limit`,
  healthcheck, `depends_on` Postgres/Spark master healthy; volume `lakehouse-config:/runtime`,
  `bronze-staging:/staging`, volume log riêng; DAG mount read-only.
- [x] Init một lần, idempotent: `airflow db migrate`, tạo admin, pool `spark_driver` (1 slot),
  connection/variable từ env; chạy lại lần 2 không đổi gì.
- [x] `docker/orch/env.py` + `make env-orch`: thêm fernet key, secret key, admin password,
  connection string còn thiếu; không xoay vòng giá trị đã có; cập nhật `.env.example`.
- [x] Make: `env-orch`, `build-orch`, `up-orch` (thay placeholder), `smoke-orch`, `restart-orch`,
  `test-dags`, `airflow-cli ARGS=…`; `down`/`logs` bao gồm profile `orch`; yamllint thêm file compose.
- [x] Smoke: UI đăng nhập được, `airflow dags list` không lỗi import, task thử `spark-submit`
  smoke từ container Airflow chạy được với driver host Airflow; restart giữ metadata.

**Đạt khi:** `make up-orch && make smoke-orch` xanh từ trạng thái sạch và sau restart; RAM trong ngân sách.
**Bằng chứng:** `make up-orch`/`smoke-orch`/`restart-orch` xanh, đăng nhập 201/401, RAM ~2,1/3 GiB khi có driver.
**Commit:** `a4dacc0`.

### P5-05 — Khung DAG dùng chung và test

- [x] Module dùng chung (`airflow/dags/common/` hoặc `airflow/plugins/`): `default_args` (owner,
  `retries`, `retry_delay`, `execution_timeout` theo bảng handoff), `max_active_runs=1`, tags,
  pool `spark_driver`, `on_failure_callback` ghi log có cấu trúc (webhook tùy chọn qua env, mặc định tắt).
- [x] Factory tạo task Spark: lệnh `spark-submit` + khóa driver + tham số từ template
  (`{{ ds }}`, params), parse kết quả đã allowlist; không I/O, không gọi Spark ở top-level DAG.
- [x] Params có schema/regex (`dataset_run_id`, `channels`, `horizon_days`), không dùng
  `datetime.now()`; ngày lấy từ `ds`/`data_interval_start`.
- [x] Test (`airflow/tests/`): DagBag import không lỗi và trong thời gian giới hạn, không cycle,
  mọi DAG có owner/tags/retries/`max_active_runs=1`/pool, task Spark có timeout, lệnh render
  đúng cho một `ds` mẫu, params sai bị từ chối.

**Đạt khi:** `make test-dags` xanh trong image; `make test` vẫn xanh không cần cài Airflow.
**Bằng chứng:** `make test-dags` 7 passed; CI DagBag tái lập trong `python:3.11` sạch. **Commit:** `0bf55be`, `1ed20f4`.

### P5-06 — DAG theo job

- [x] `dp1_bronze`: chờ manifest (sensor có timeout, mode reschedule/deferrable), task cho 5 kênh
  JSONL + Walmart JDBC theo `ds`, `--mode full`, verify sau publish.
- [x] `dp2_silver`: 5 kênh theo `ds`, `--fail-on-reject` theo runbook. Không có ngưỡng: bất kỳ
  record reject nào làm task fail; ngưỡng cho phép có thể thêm bằng ADR sau.
- [x] `dp2_gold`: `--mode full` cho dataset; skip khi Silver publication/policy không đổi; verify.
- [x] `dp3_features` (`--as-of-date ds --mode full`) và `dp3_return_labels`
  (`--as-of-date ds --horizon-days 14`); verify từng cái.
- [x] `dp4_warehouse`: `--run-date ds --mode full`; verify + reconcile.
- [x] Mỗi DAG chạy riêng được từ UI/CLI với params; ghi XCom: skipped, publication/export ID.
- [x] Test render lệnh từng DAG cho `ds=2026-01-02` khớp CLI thực tế (đối chiếu `--help`).

**Đạt khi:** từng DAG chạy xanh độc lập trên small fixture, chạy lại lần 2 đều skip.
**Bằng chứng:** chạy độc lập `dp1_bronze` (01-01), `dp3_return_labels` (01-18), `dp2_gold`; DP3 đổi sang
`incremental`, warehouse `--no-regress` sau khi phát hiện lỗi. **Commit:** `380939c`, `334f6b7`.

### P5-07 — Controller và phụ thuộc theo ngày

- [x] DAG `batch_daily` (theo ADR): `start_date`/`end_date` cho demo hoặc `catchup=False`,
  gọi tuần tự Bronze → Silver → Gold → {Features, Labels} → warehouse cho cùng `logical_date`
  và `dataset_run_id`; dùng chế độ deferrable để không giữ slot.
- [x] Kiểm chứng không deadlock với `parallelism` đã chốt (test thực tế + ghi lại).
- [x] Downstream chỉ chạy khi upstream thành công cho đúng ngày; lỗi giữa chuỗi dừng chuỗi,
  không publish dở dang; rerun từ task lỗi hoạt động.
- [x] (Tùy chọn theo ADR) Asset theo tầng trên task publish/verify cuối, chỉ để quan sát trong UI;
  lineage đầy đủ để Phase 9.

**Đạt khi:** trigger `batch_daily` từ UI cho một ngày chạy xanh toàn chuỗi trên small fixture.
**Bằng chứng:** backfill 3 ngày xanh; deferrable không deadlock với `parallelism=1`; `depends_on_past`.
**Commit:** `16aa6a3`, `766dbd4`.

### P5-08 — E2E, backfill, idempotency và khôi phục

- [x] `make e2e`: thay placeholder, chạy `batch_daily` trên small fixture qua Airflow CLI, assert
  ledger completed + verify/reconcile xanh; có timeout rõ.
- [x] Backfill `2026-01-01..2026-01-03` trên default dataset, lần 1 và lần 2: so hash ledger,
  content summary bảng Gold/DP3, row count + tổng tiền warehouse; lần 2 phải toàn bộ skip,
  không nhân đôi, không mất scope dataset còn lại.
- [x] Đối chiếu kết quả Airflow với mốc P5-01 (cùng publication/snapshot hoặc giải thích khác biệt).
- [x] Lỗi & khôi phục: (a) task Spark fail (dùng `--fail-after` sẵn có) → retry/rerun hoàn tất;
  (b) restart Airflow giữa chừng → scheduler phục hồi, không có 2 driver; (c) chạy
  `make spark-submit` song song → bị khóa driver chặn; (d) manifest thiếu → sensor timeout
  có kiểm soát; ghi lại hành vi.
- [x] Bằng chứng: thời lượng mỗi lượt/ngày, XCom đã lọc, hash ledger; không commit log thô, secret,
  dữ liệu.
- [x] Ảnh Graph/Gantt: `docs/images/phase5/` (Chromium headless đăng nhập qua form, không tắt xác
  thực).

**Đạt khi:** DoD plan: trigger UI xanh toàn chuỗi; backfill 3 ngày chạy lại 2 lần cùng kết quả.
**Bằng chứng:** lượt D: 3/3 ngày, mọi bước skip, hash 4 ledger không đổi; khôi phục lock/restart/manifest;
7 lỗi tìm ra đều đã sửa có test. **Commit:** `14df515`, `433053c`, `2365e2a`, `dccc653`.

### P5-09 — `maintenance_iceberg` (chỉ báo cáo, tùy chọn)

- [x] DAG hằng tuần, mặc định dry-run: liệt kê snapshot/file ứng viên expire/compaction cho
  `bronze/silver/gold/benchmark`, **loại trừ** mọi snapshot được pin trong ledger
  Bronze/Silver/Gold/DP3/warehouse và `/runtime/benchmark/datasets/*.json`.
- [x] Xuất báo cáo (số snapshot, file nhỏ, dung lượng ước tính); không gọi `expire_snapshots`,
  `remove_orphan_files` hay `rewrite_data_files` thật.
- [x] Bật thao tác thật chỉ khi có ADR retention riêng và người dùng đồng ý rõ ràng — ADR-0014 ghi
  rõ điều kiện; thao tác thật vẫn tắt.

**Đạt khi:** báo cáo dry-run chạy được và test chứng minh snapshot pin không nằm trong danh sách.
**Bằng chứng:** [ADR-0014](adr/0014-iceberg-snapshot-retention.md); `make iceberg-report` và run Airflow
`scheduled__2026-11-15` thành công; retention 1 ngày: 289 ứng viên, 0 trùng 1.382 snapshot pin;
unit test chọn ứng viên/pin, test DAG chặn lệnh xóa.

### P5-10 — CI, tài liệu và bàn giao

- [x] `ci.yml`: job DagBag (build image Airflow hoặc cài theo constraints) chạy trên push/PR.
- [x] Workflow thủ công `orchestration.yml`: build, `up-batch up-orch`, fixture nhỏ, `make e2e`,
  event allowlist, `make down`; không đặt ngưỡng wall-clock.
- [x] Cập nhật README, runbook (lỗi thường gặp: import DAG, driver busy, sensor timeout, deadlock
  slot, RAM), `implementation-status`, `phase5-handoff` → handoff Phase 6 (Trino đọc warehouse/Iceberg).
- [x] `docs/orchestration-validation.md`: commit, image ID, lệnh, kết quả backfill, khôi phục.
- [x] Lưu checklist hoàn thành vào `docs/phase5-checklist.md`; tag `v0.5-orchestration` khi DoD đạt.

**Bằng chứng:** workflow `ci.yml` (job `dags`) và `orchestration.yml` (chưa chạy hosted), README, runbook,
implementation-status, [handoff Phase 6](phase6-handoff.md), [checklist](phase5-checklist.md).

## 5. Điều kiện đóng Phase 5

- [x] Airflow pin version có nguồn, chạy profile `orch` với healthcheck, `mem_limit`, restart giữ metadata.
- [x] Mọi DAG import không lỗi; test DagBag + quy tắc (owner, tags, retries, `max_active_runs=1`,
  pool, timeout) trong CI.
- [x] Trigger `batch_daily` từ UI chạy xanh toàn chuỗi; mỗi DAG chạy độc lập được.
- [x] Backfill 3 ngày chạy 2 lần cho cùng kết quả; lần 2 skip, ledger/nội dung không đổi.
- [x] Không bao giờ có hai Spark driver đồng thời (pool + khóa đã kiểm chứng).
- [x] Lỗi/khôi phục (fail task, restart Airflow, lock bận, sensor timeout) có bằng chứng.
- [x] `make check`, `make test-dags`, `make e2e` và regression Phase 4 đều xanh.
- [x] Tài liệu, ADR, runbook và handoff Phase 6 đầy đủ (ảnh Graph/Gantt: người dùng chụp, xem P5-08).

## 6. Rủi ro và cách giảm

| Rủi ro | Dấu hiệu | Cách giảm |
|---|---|---|
| Hết RAM khi thêm Airflow + driver | OOM kill, worker mất kết nối | Đo ở P5-01, `mem_limit` rõ, không bật profile khác, chạy tuần tự |
| Deadlock slot khi chờ DAG con | Run đứng ở `running`, DAG con `queued` | Deferrable + triggerer hoặc `parallelism=2` + pool 1 slot; có test |
| Hai driver cùng lúc | Lỗi bind/port, ledger lock | Pool + `flock` dùng chung cho Make và Airflow |
| Lệch jar/Python giữa Airflow và Spark | `ClassNotFound`, lỗi serialize Python | Copy jar từ image Spark, cùng Python 3.11 / PySpark 3.5.5, smoke test |
| Catchup tạo quá nhiều run | Hàng trăm DAG run từ 2026-01 | `end_date` hoặc `catchup=False` + backfill tường minh |
| Ngày logic sai | Dữ liệu ngày khác bị ghi đè | Test render lệnh theo `ds`, verify sau publish, skip theo ledger |
| Lộ secret trong log/UI | Password trong log task | Env/connection masking, event allowlist, không echo env |
| Warehouse chậm (~15 phút) | Timeout | `execution_timeout` theo số đo P5-01, retry có delay |

## 7. Quy tắc cập nhật

1. Chỉ đánh `[x]` khi có implementation và evidence đúng commit; blocker giữ `[ ]` kèm lý do.
2. Sau mỗi task nhỏ: kiểm tra phù hợp, cập nhật evidence/checklist, commit Conventional Commits.
3. Ưu tiên Make targets; chỉ bật `core + batch + orch`; giữ version/port/resource limit đã pin.
4. Không commit secret, `.env`, log thô, dữ liệu; không xóa volume, bucket, DB hoặc snapshot pin.
5. Không triển khai Phase 6+ (Trino, streaming, CDC, DataHub) trong kế hoạch này.
