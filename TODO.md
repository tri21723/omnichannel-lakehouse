# TODO — Phase 7: Streaming với Kafka + Flink

Cập nhật: **2026-11-19**. Trạng thái: **đang triển khai Phase 7 (P7-01…P7-02 xong)**.

Mục tiêu: luồng sự kiện đơn hàng Amazon và TikTok Shop đi từ generator → Kafka → Flink (event
time, late arrival, dedup, burst) → file output → Bronze, chạy local bằng Docker; output sau
Flink không còn bản trùng, event trễ đi đúng nhánh, job phục hồi sau khi kill TaskManager mà
không mất/nhân dữ liệu, và có số liệu throughput/latency/checkpoint/backpressure (luồng S1–S3).

Tham chiếu: [plan §Phase 7](docs/plan.md), [bàn giao Phase 7](docs/phase7-handoff.md),
[data model](docs/data-model.md), [runbook](docs/runbook.md), ADR-0001/0004/0005/0007/0013/0015.
Lịch sử: [Phase 6](docs/phase6-checklist.md), [kiểm chứng query](docs/query-validation.md).

## 1. Điểm xuất phát và phạm vi

- Phase 6 đóng ở `v0.6-query`; hosted `Scaffold checks` xanh trên `a11d318`
  ([run 44355559275](https://github.com/tri21723/omnichannel-lakehouse/actions/runs/44355559275)).
  Workflow thủ công `Trino query layer` xanh trên `3fe9364`
  ([run 44374160331](https://github.com/tri21723/omnichannel-lakehouse/actions/runs/44374160331)).
  `Skew benchmark smoke` xanh trên `3fe9364` ([run 44449924427](https://github.com/tri21723/omnichannel-lakehouse/actions/runs/44449924427)).
  Việc mang sang (tùy chọn): chạy thủ công `Airflow orchestration e2e` trên GitHub và ghi run URL.
- Đã có sẵn: tên/port dành cho Kafka 9092, kafka-ui 8085, Flink JobManager 8082; topic
  `amazon.order-notifications`, `tiktok_shop.order-webhooks`; bucket `checkpoints`; thư mục rỗng
  `generators/stream/`, `flink/jobs/amazon_late/`, `flink/jobs/tiktok_dedup/`; `make gen-stream`,
  `make up-stream` đang `exit 2`.
- Ràng buộc đã biết:
  - `GeneratorConfig.run_id` băm **toàn bộ** model: thêm trường mới vào model này sẽ đổi run ID
    của 3 dataset batch hiện có. Cấu hình stream phải là model riêng (hoặc loại khỏi phần băm)
    và có test chứng minh run ID batch không đổi.
  - Tên `stream_manifest.json` đã là manifest **batch** (`s3://raw/manifests/<id>/`); ground
    truth của stream phải dùng tên khác (ví dụ `stream_ground_truth.json`) và prefix riêng.
  - Bronze là điểm vào duy nhất (ADR-0007): output Flink phải vào Bronze bằng slice có manifest,
    fingerprint, completion record, replay không nhân đôi; payload thô có PII nên vẫn bị ẩn khỏi
    Trino như `iceberg.bronze`.
  - Spark driver được tuần tự hóa bằng `/runtime/locks/spark-driver.lock` + pool `spark_driver`.
  - RAM host ~11,7 GiB: plan ước Kafka + kafka-ui ≈ 1,5 GiB, Flink ≈ 3 GiB → profile `stream`
    chạy cùng core, không bật Spark/Airflow/Trino khi đo Flink; nạp Bronze làm sau.
  - AGENTS.md: event-time + watermark rõ ràng, dữ liệu quá trễ đi side output (không bỏ), state có
    TTL, bật checkpoint, ghi tham số watermark/lateness trong ADR hoặc docstring.
- **Trong phạm vi:** Kafka KRaft single broker + kafka-ui, generator stream có ground truth, 2 job
  Flink (Amazon late, TikTok dedup + burst), checkpoint/recovery, output file có manifest, nạp vào
  Bronze qua Spark/Airflow, benchmark streaming, test, tài liệu.
- **Ngoài phạm vi:** Schema Registry (nâng cấp tùy chọn của plan, cần ADR riêng), Silver/Gold cho
  dữ liệu stream, Flink ghi thẳng Iceberg, CDC/Debezium (Phase 8), DataHub (Phase 9), cloud,
  multi-broker/HA, TLS/SASL Kafka.

## 2. Quyết định cần chốt trong ADR (P7-02)

| Chủ đề | Đề xuất mặc định | Phương án thay thế / lý do loại |
|---|---|---|
| Version Kafka | Apache Kafka bản ổn định còn hỗ trợ, chế độ KRaft, image chính thức `apache/kafka:<ver>` đã ≥ 2 tuần; pin vào ADR-0001 | Image bên thứ ba: chỉ khi image chính thức không đáp ứng |
| kafka-ui | Image đã pin, `127.0.0.1:8085`, chỉ đọc nếu hỗ trợ | Bỏ kafka-ui: mất công cụ quan sát offset/lag mà plan yêu cầu |
| Version + API Flink | Bản Flink ổn định hỗ trợ Python 3.11; **PyFlink DataStream** (cần side output, keyed state TTL, timer) — kiểm tra kỹ PyFlink hỗ trợ đủ; nếu thiếu thì Java cho job, ghi lý do | Flink SQL: không có side output cho event trễ; Java toàn bộ: lệch stack Python của repo |
| Topic | `amazon.order-notifications`, `tiktok_shop.order-webhooks`, 3 partition, key `order_id`, retention 3–7 ngày, DLQ `<topic>.dlq` cho bản ghi không parse được | Tạo topic tự động: không kiểm soát partition/retention |
| Schema event | JSON có version (`schema_version`), `event_id`, `order_id`, `channel`, `event_ts` (UTC), `emitted_ts`, payload; kiểm tra schema ở Flink, lỗi → DLQ | Avro + Schema Registry: để sau (ngoài phạm vi) |
| Watermark/lateness | Amazon: `forBoundedOutOfOrderness` + `allowedLateness` theo `late_max_minutes`; quá trễ → side output; giá trị cụ thể chốt theo ground truth | Không có allowed lateness: mất dữ liệu trễ hợp lệ |
| Dedup | TikTok: `keyBy(event_id)` + `ValueState` TTL (10–30 phút, ≥ khoảng retry của generator) | Dedup ở Silver: plan yêu cầu dedup trong stream |
| Checkpoint | EXACTLY_ONCE, 30–60 s, state backend HashMap (dữ liệu nhỏ), lưu `s3://checkpoints/flink/` với user MinIO riêng | Volume local: mất khi xóa container, khó kiểm chứng |
| Output | FileSink rolling theo checkpoint (chỉ file committed mới hiển thị), JSONL hoặc Parquet trên MinIO `raw/stream/<run>/<channel>/…` + manifest viết **sau** file; nhánh late/DLQ có prefix riêng | Bind mount host như plan gốc: Spark/Airflow khó đọc, không có quyền tách bạch |
| Quyền | User MinIO `flink` (ghi `raw/stream/*`, `checkpoints/flink/*`), không dùng root; Spark đọc như raw hiện có | Dùng user Spark: trộn quyền ghi |
| Nạp Bronze | Source mới `stream` trong `dp1_bronze` đọc manifest output Flink; DAG `dp1_stream_bronze` (pool `spark_driver`) | Flink sink thẳng Iceberg: plan để là nâng cấp, cần ADR riêng |
| Dataset | Mỗi lần chạy stream có `_dataset_run_id` riêng; không ghi đè 3 dataset batch | — |

Nếu quyết định nào mâu thuẫn AGENTS.md (port, version đã pin, service ngoài sơ đồ, dependency
nặng như thư viện Kafka cho generator, xóa dữ liệu) thì ghi rõ và hỏi lại trước khi làm.

## 3. Thứ tự thực hiện

| Task | Nội dung | Phụ thuộc | Ước lượng |
|---|---|---|---|
| P7-01 | Preflight: trạng thái, RAM với core + stream, run ID batch, MinIO/bucket | Không | 0,5 ngày |
| P7-02 | ADR-0016 streaming + pin Kafka/kafka-ui/Flink (ADR-0001) | 01 | 0,5–1 ngày |
| P7-03 | Compose `stream`: Kafka KRaft, kafka-ui, Flink JM/TM, topic init, MinIO user | 02 | 1 ngày |
| P7-04 | Generator stream tất định + ground truth + `make gen-stream` | 02 | 1–1,5 ngày |
| P7-05 | Job Flink Amazon: event time, watermark, lateness, side output, sink | 03, 04 | 1–2 ngày |
| P7-06 | Job Flink TikTok: dedup TTL, window, burst, sink | 03, 04 | 1–1,5 ngày |
| P7-07 | Checkpoint, kill TaskManager, exactly-once output, DLQ | 05, 06 | 1 ngày |
| P7-08 | Nạp output vào Bronze (`dp1_bronze` source `stream`) + DAG | 07 | 1–1,5 ngày |
| P7-09 | Benchmark 100k event: throughput, latency, checkpoint, backpressure | 07 | 0,5–1 ngày |
| P7-10 | CI, tài liệu, bàn giao Phase 8, tag | 08, 09 | 0,5–1 ngày |

## 4. Checklist triển khai

### P7-01 — Preflight

- [x] Git sạch tại `v0.6-query` (+ commit tài liệu sau tag); `make check` xanh.
- [x] Ghi hash ledger Silver/Gold/DP3/warehouse và run ID 3 dataset batch làm mốc (Phase 7 không
  được đổi).
- [x] Đo RAM core khi tắt Spark/Airflow/Trino; tính ngân sách Kafka + kafka-ui + Flink JM/TM.
- [x] Kiểm tra bucket `raw`, `checkpoints`, policy hiện có; liệt kê quyền cần thêm cho user `flink`.
- [x] Xác nhận các thư mục/placeholder và target `exit 2` cần thay.

**Đạt khi:** mốc và ngân sách RAM ghi vào `docs/streaming-validation.md`.
**Bằng chứng:** [streaming-validation](docs/streaming-validation.md) §P7-01 (mốc run ID/ledger, RAM
core 0,4 GiB / còn 9 GiB, ngân sách stream 4,5 GiB, prefix `raw/stream/*` cho user `flink`).

### P7-02 — ADR và version

- [x] Xác minh trên tài liệu chính thức: Kafka (KRaft, image, cấu hình single node), kafka-ui,
  Flink (bản hỗ trợ Python 3.11, PyFlink DataStream: side output, keyed state TTL, event-time
  timer, FileSink, S3 filesystem plugin), Kafka connector tương thích; ghi exact version + nguồn +
  ngày vào ADR-0001.
- [x] Viết `docs/adr/0016-streaming-kafka-flink.md` (kiểm tra số trước) chốt mọi mục §2, gồm
  watermark/lateness/TTL, exactly-once end-to-end và cách ghi ground truth.
- [x] Hợp đồng event (JSON schema có version) + quy tắc DLQ trong `docs/data-model.md`.

**Đạt khi:** ADR có nguồn, version pin, không cần ngoại lệ AGENTS hoặc đã hỏi lại.
**Bằng chứng:** [ADR-0016](docs/adr/0016-streaming-kafka-flink.md), ADR-0001 §Phase 7, data-model
§Streaming events. Chốt: Kafka 4.3.2, kafka-ui v1.5.0, Flink 2.2.1 + connector 5.0.0-2.2, job **Java**
(người dùng chọn 2026-11-19, PyFlink quá nặng), `kafka-python` cho generator, JSON Lines thay CSV/Parquet.

### P7-03 — Hạ tầng `stream`

- [ ] `docker/compose.stream.yml` profile `stream`: `kafka` (KRaft, listener nội bộ `kafka:9092`,
  host `127.0.0.1:9092`), `kafka-ui` (`127.0.0.1:8085`), `flink-jobmanager` (`127.0.0.1:8082`),
  `flink-taskmanager`; `mem_limit`, healthcheck, volume tên riêng; image Flink tự build nếu cần
  PyFlink/plugin S3/Kafka connector (pin, checksum).
- [ ] Init idempotent: tạo topic (partition/retention/DLQ), user MinIO `flink` + policy (ghi
  `raw/stream/*`, `checkpoints/flink/*`), chạy lần 2 không đổi gì.
- [ ] Make: `env-stream`, `build-stream`, `up-stream` (thay placeholder), `smoke-stream`,
  `restart-stream`, `logs SVC=…`; `down` gồm `stream`; yamllint thêm compose mới.
- [ ] Smoke: produce/consume 1 message thử trên topic smoke riêng, Flink cluster nhận TaskManager,
  quyền MinIO đúng (user `flink` không ghi được ngoài prefix cho phép).

**Đạt khi:** `make up-stream && make smoke-stream` xanh từ trạng thái dừng và sau restart.
**Commit gợi ý:** `feat(stream): add Kafka KRaft, kafka-ui and Flink stream profile`.

### P7-04 — Generator stream và ground truth

- [ ] Model cấu hình stream riêng (không đổi `GeneratorConfig.run_id`): số event, tỉ lệ dup
  (TikTok retry), late (Amazon, ≤ `late_max_minutes`), burst (`burst_multiplier`), seed; test
  chứng minh run ID 3 dataset batch không đổi.
- [ ] Publisher tất định: cùng seed → cùng chuỗi event (thứ tự gửi, `event_id`, `event_ts`), key
  `order_id`; dependency Kafka cho generator được pin (hỏi lại nếu nặng).
- [ ] Ground truth `s3://raw/manifests/<stream_run>/stream_ground_truth.json`: số event gửi,
  unique `event_id`, số dup, số late theo ngưỡng, số event quá trễ dự kiến, phân bố theo window.
- [ ] `make gen-stream` (thay placeholder), tham số rõ ràng; publish an toàn khi chạy lại (không
  ghi đè ground truth khác nội dung).
- [ ] Unit test: tất định, đếm dup/late/burst khớp cấu hình, schema event hợp lệ.

**Đạt khi:** hai lần chạy cùng seed cho cùng ground truth; event đọc lại từ Kafka khớp ground truth.
**Commit gợi ý:** `feat(generator): publish deterministic stream events with ground truth`.

### P7-05 — Job Amazon (late arrival)

- [ ] Event-time từ `event_ts`; `WatermarkStrategy.forBoundedOutOfOrderness` + idle timeout;
  tumbling window (aggregate theo channel/currency/window) với `allowedLateness`.
- [ ] Event quá trễ → side output ghi riêng (`late/`), không bỏ; bản ghi lỗi schema → DLQ.
- [ ] Sink output và late theo checkpoint (chỉ file committed); manifest output.
- [ ] Logic thuần (phân loại on-time/late/too-late, key, aggregate) tách hàm có unit test.
- [ ] Tham số watermark/lateness ghi trong docstring + ADR.

**Đạt khi:** số event on-time + late + too-late = ground truth, không mất bản ghi.
**Commit gợi ý:** `feat(flink): add Amazon late-arrival job with side output`.

### P7-06 — Job TikTok (dedup + burst)

- [ ] `keyBy(event_id)` + `ValueState` có TTL để loại retry trùng; tumbling window 1–5 phút.
- [ ] Theo dõi backpressure khi burst (Flink REST metrics); không tăng tài nguyên ngoài ngân sách.
- [ ] Sink + manifest như P7-05; DLQ cho bản ghi lỗi.
- [ ] Unit test logic dedup (TTL hết hạn, trùng trong/ngoài TTL) và aggregate.

**Đạt khi:** unique count output = ground truth; không còn `event_id` trùng.
**Commit gợi ý:** `feat(flink): add TikTok dedup and burst job with state TTL`.

### P7-07 — Checkpoint, recovery và exactly-once

- [ ] Checkpoint EXACTLY_ONCE lên `s3://checkpoints/flink/`; đo duration/size.
- [ ] Kill TaskManager giữa lúc chạy → job tự restart từ checkpoint; output cuối khớp ground truth
  (không mất, không nhân); file chưa commit không bị đọc.
- [ ] Restart cả profile `stream` → job submit lại/khôi phục theo runbook; offset Kafka không tua lại sai.
- [ ] `make verify-stream`: đối soát output (on-time/late/dedup) với ground truth, đọc-only.

**Đạt khi:** recovery có bằng chứng; `verify-stream` xanh hai lần.
**Commit gợi ý:** `test(flink): verify checkpoint recovery and exactly-once output`.

### P7-08 — Nạp vào Bronze

- [ ] `dp1_bronze --source stream`: đọc manifest output Flink, kiểm SHA/size/count, slice theo
  run/channel/date/source, replay không nhân đôi, completion record (ADR-0007, cập nhật ADR).
- [ ] Bảng Bronze riêng cho stream (`<channel>_<entity>_raw` theo quy ước), cột audit đầy đủ;
  nhánh late được nạp riêng hoặc gắn cờ, không bị bỏ.
- [ ] DAG `dp1_stream_bronze` (pool `spark_driver`, khóa driver, `max_active_runs=1`) chạy sau khi
  manifest output tồn tại; test DagBag.
- [ ] Đối soát: số dòng Bronze stream = output Flink = ground truth; 3 dataset batch không đổi.

**Đạt khi:** Bronze stream khớp ground truth, chạy lại không nhân đôi, ledger batch không đổi.
**Commit gợi ý:** `feat(spark): ingest Flink stream output into Bronze`.

### P7-09 — Benchmark streaming

- [ ] Kịch bản 100k event (10% dup, 5% late, có burst) theo plan; chạy trong ngân sách RAM.
- [ ] Ghi throughput (event/s), latency end-to-end (event_ts/emitted_ts → file committed),
  checkpoint duration/size, backpressure khi burst, số late/too-late/dedup.
- [ ] `docs/benchmarks/streaming.md`: môi trường, cấu hình, số liệu, giới hạn; không đặt ngưỡng
  wall-clock dễ flaky.

**Đạt khi:** báo cáo có số liệu lặp lại được và đối soát đúng.
**Commit gợi ý:** `perf(stream): benchmark Kafka and Flink with dup, late and burst`.

### P7-10 — CI, tài liệu và bàn giao

- [ ] Unit test generator/logic Flink trong `make test`; workflow thủ công `stream.yml` (up stream,
  gen-stream nhỏ, job, verify-stream, nạp Bronze, allowlisted events).
- [ ] README ("Chạy streaming"), runbook (Kafka/Flink, checkpoint, recovery, lỗi thường gặp),
  `implementation-status`, `data-model`, CLAUDE.md nếu đổi lệnh.
- [ ] `docs/streaming-validation.md`: commit, image, lệnh, ground truth, recovery, benchmark.
- [ ] Bàn giao Phase 8 (`docs/phase8-handoff.md`): Debezium dùng Kafka đã có, topic
  `platform_cdc.walmart.orders`, slot/WAL, ngân sách RAM.
- [ ] Lưu checklist vào `docs/phase7-checklist.md`; tag `v0.7-stream` khi DoD đạt.

**Commit gợi ý:** `docs(stream): publish Phase 7 validation and close the phase`.

## 5. Điều kiện đóng Phase 7

- [ ] Kafka, kafka-ui, Flink pin version có nguồn; profile `stream` có healthcheck, `mem_limit`, restart ổn.
- [ ] Generator stream tất định với ground truth; run ID batch không đổi.
- [ ] Output Flink không còn dup; event trễ đi đúng nhánh (không bỏ); DLQ có kiểm soát.
- [ ] Kill TaskManager → phục hồi từ checkpoint, không mất/nhân dữ liệu.
- [ ] Output vào Bronze khớp ground truth, replay không nhân đôi; dữ liệu/ledger batch không đổi.
- [ ] Báo cáo benchmark có số liệu; `make check`, `smoke-stream`, `verify-stream` xanh.
- [ ] Tài liệu, ADR, runbook, bàn giao Phase 8 đầy đủ.

## 6. Rủi ro và cách giảm

| Rủi ro | Dấu hiệu | Cách giảm |
|---|---|---|
| PyFlink thiếu tính năng (side output/TTL/timer) hoặc lệch version Python | Lỗi API, job không submit | Xác minh ở P7-02; fallback Java cho phần thiếu, ghi ADR |
| Hết RAM khi chạy Kafka + Flink | OOM, TaskManager restart | Chỉ chạy core + stream; `mem_limit`, heap/managed memory nhỏ |
| Đổi run ID batch do sửa model generator | Run ID khác mốc P7-01 | Model stream riêng + test run ID |
| Output nhân đôi sau recovery | Count > ground truth | FileSink commit theo checkpoint, chỉ đọc file committed, manifest sau cùng |
| Watermark sai làm dồn late/too-late | Late quá nhiều/quá ít so với ground truth | Tham số từ ground truth, test phân loại, ghi trong ADR |
| Kafka listener sai giữa host và container | Producer/consumer không kết nối | Listener nội bộ + host tách bạch, smoke cả hai phía |
| Small files khi rolling dày | Nhiều file nhỏ trong `raw/stream` | Rolling theo checkpoint + kích thước tối thiểu, ghi số file vào báo cáo |
| Lộ PII từ payload stream | Payload thô đọc được qua Trino | Bronze stream nằm trong `iceberg.bronze` (bị ẩn), không expose qua Trino |

## 7. Quy tắc cập nhật

1. Chỉ đánh `[x]` khi có implementation và evidence đúng commit; blocker giữ `[ ]` kèm lý do.
2. Sau mỗi task nhỏ: kiểm tra phù hợp, cập nhật evidence/checklist, commit Conventional Commits;
   chỉ commit khi exit code của `make check` là 0 (không đọc qua pipe).
3. Ưu tiên Make targets; chỉ bật service cần thiết; giữ version/port/resource limit đã pin.
4. Không commit secret, `.env`, log thô, dữ liệu; không xóa volume, bucket, DB hoặc snapshot pin;
   không đổi run ID hoặc dữ liệu 3 dataset batch.
5. Không triển khai Phase 8+ trong kế hoạch này.
