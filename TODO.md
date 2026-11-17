# TODO — Phase 4F: Performance & Data Skew Benchmark

Cập nhật: **2026-11-17**. Trạng thái: **đã lập kế hoạch, chưa triển khai 4F**.

Mục tiêu: xây dựng benchmark có thể chạy lại trên Docker local, đo hiệu quả xử lý
skew bằng số liệu trước/sau, chứng minh kết quả dữ liệu không đổi, và đóng Phase 4
trước khi triển khai Airflow ở Phase 5.

Tham chiếu: [plan](docs/plan.md), [data model](docs/data-model.md),
[trạng thái triển khai](docs/implementation-status.md),
[bằng chứng review](docs/review-4cde-validation.md).
Lịch sử: [Phase 4E](docs/phase4e-checklist.md),
[checklist 4E và các sửa lỗi review](docs/phase4e-review-checklist.md).

## 1. Điểm xuất phát và phạm vi

- 4C–4E đã có kiểm chứng local; lượt gần nhất: 165 unit tests, 22 ca Spark regression,
  features publish/replay thành công. Đây là bằng chứng trước 4F, không thay thế gate bên dưới.
- Policy hiện tại: Gold v2, features v4, labels v3. Dữ liệu lịch sử chưa được nâng
  đồng loạt; lần chạy tiếp theo sẽ tính lại các scope được yêu cầu.
- Small fixture: `186ae107d0793d2ee237` (375 orders); default reference:
  `b1e64afaff0176de180a` (15.000 orders). Xác nhận lại số liệu từ manifest khi chạy.
- DP2 sở hữu Gold dim/fact; DP3 sở hữu features/labels; DP4 xuất warehouse.
  Giữ ranh giới ownership theo ADR hiện tại dù plan gốc có mô tả cũ.
- Benchmark tập trung Spark join/aggregate; JDBC export là kiểm tra tích hợp và
  giới hạn tài nguyên, không dùng làm đại diện cho khả năng xử lý Spark ở quy mô lớn.
- Ngoài phạm vi: Airflow, Trino, streaming/CDC, DataHub, cloud, nâng version/đổi port,
  dịch vụ History Server mới, tự động expire snapshot hoặc xóa volume/bucket/DB.

## 2. Thứ tự thực hiện

| Task | Nội dung | Phụ thuộc | Ước lượng |
|---|---|---|---|
| P4F-01 | Baseline đồng nhất policy và correctness gate | Không | 0,5–1 ngày |
| P4F-02 | ADR, workload và ngân sách tài nguyên | 01 | 0,5 ngày |
| P4F-03 | Dataset scale/skew có ground truth | 02 | 0,5–1 ngày |
| P4F-04 | Harness, event logs, metrics và artifact | 02 | 1–2 ngày |
| P4F-05 | Baseline và ma trận tối ưu có kiểm soát | 03, 04 | 1–2 ngày |
| P4F-06 | Đối soát, replay và kiểm tra tích hợp | 05 | 0,5–1 ngày |
| P4F-07 | Báo cáo, CI smoke và bàn giao | 06 | 0,5–1 ngày |

Ước lượng là thời gian làm việc, không cam kết tốc độ benchmark hoặc số ngày hoàn tất.

## 3. Checklist triển khai

### P4F-01 — Chuẩn hóa baseline và chốt đầu vào

- [ ] Kiểm tra git sạch, image đúng source, Docker core/batch healthy; ghi commit,
  image ID, CPU/RAM cấp cho Docker, disk trống, số worker/executor/core/memory thực tế.
- [ ] Chạy `make check`, `make test-review-regressions`; lưu bằng chứng theo commit.
- [ ] Xác nhận source manifests, Bronze/Silver completed publications cho small/default;
  nếu mismatch thì xử lý trước khi đo, không ghi đè dữ liệu để che sai lệch.
- [ ] Chạy Gold cho **cả hai dataset trước**, chốt publication cuối, rồi chạy toàn bộ
  dates features/labels cần làm baseline trên cùng Gold publication. Không xen kẽ
  cập nhật Gold giữa các phép so sánh downstream.
- [ ] Xuất warehouse cho hai dataset từ Gold đã chốt; ghi run-date/window cụ thể.
- [ ] Chạy verify/reconcile/DQ tương ứng cho Gold, DP3 và warehouse; kiểm tra currency,
  keys, số dòng, censoring và tổng tiền theo channel/currency.
- [ ] Replay cùng tham số: xác nhận skip/read-back, không nhân đôi hoặc mất scope khác.
- [ ] Ghi manifest baseline: run IDs, publication IDs, table snapshot IDs, policy versions,
  evaluation dates, horizon, warehouse window, counts và checksum bằng chứng.
- [ ] Chặn benchmark nếu snapshot thiếu, policy không đúng, input còn pending/failed
  hoặc kết quả đối soát không đạt.

**Đạt khi:** baseline nhất quán và có thể truy về đúng input. Giữ các snapshot được pin.
**Commit gợi ý:** `test(batch): establish current-policy baseline for skew benchmarks`.

### P4F-02 — Chốt thiết kế và workload

- [ ] Viết ADR tiếp theo (dự kiến `docs/adr/0012-skew-benchmark.md`, kiểm tra số trước khi tạo).
- [ ] Chốt workload từ nghiệp vụ hiện có: join fact items với dimension theo đúng
  grain/key, rồi aggregate seller/channel/currency; kiểm chứng hot key thực sự
  xuất hiện trên join/shuffle key, không chỉ trên một thuộc tính không tham gia shuffle.
- [ ] Tách phép đo join và aggregate nếu cần phân biệt skew join với skew aggregation;
  AQE skew-join không được mặc định coi là giải pháp cho mọi aggregate.
- [ ] Chốt grain đầu ra, điều kiện filter, NULL semantics, công thức sum/count/ratio,
  schema, kiểu DECIMAL và tiêu chí tương đương trước khi tối ưu.
- [ ] Chốt đường dẫn trong cây hiện có: `spark/jobs/benchmark_skew/`, helper dùng chung
  trong `spark/lib`, cấu hình dưới thư mục job, báo cáo trong `docs/benchmarks/`.
- [ ] Chốt chế độ benchmark đọc snapshot đã pin, không cập nhật ledger nghiệp vụ.
  Nếu cần sink để materialize toàn bộ kết quả, dùng output riêng theo experiment/run.
- [ ] Ghi giới hạn thời gian mỗi trial, dung lượng output/event logs, memory và disk
  tối thiểu trước khi tăng scale; ngừng trial khi vượt ngân sách, lưu trạng thái lỗi.

**Đạt khi:** workload, resource budget và phép so sánh được mô tả rõ trong ADR.
**Commit gợi ý:** `docs(benchmark): define skew workload and measurement protocol`.

### P4F-03 — Dataset có scale và skew kiểm chứng được

- [ ] Tạo config riêng, không sửa ngầm `generators/config.yaml` hoặc reference fixtures.
- [ ] Ma trận dự kiến: smoke 15k, medium 100k, large 1M **tổng orders**;
  điều chỉnh theo preflight tài nguyên và ghi rõ tổng/per-channel/per-day thực tế.
- [ ] Với mỗi scale, có control tương đối đều và skew mạnh (dự kiến top seller 80%);
  giữ seed, days, channel mix, dup/return/schema-drift settings nhất quán ngoài biến skew.
- [ ] Đo phân bố thực tế sau Silver/Gold: top-key share, số key, max/median rows per key,
  phân bố theo channel và số partition; so với config/ground truth.
- [ ] Bảo đảm dimension join unique theo key; thêm salting về sau không được làm fan-out.
- [ ] Kiểm tra generator cùng seed/config cho cùng checksum; run ID/prefix độc lập
  cho từng cấu hình để bảo toàn small/default.
- [ ] Ingest và kiểm tra correctness từng cấp trước khi tăng scale. Nếu 1M vượt giới hạn,
  lưu lỗi/ngân sách, dùng scale lớn nhất đạt và ghi giới hạn trong báo cáo.
- [ ] Với workload nhân bản từ dữ liệu nhỏ để nghiên cứu, ghi rõ synthetic amplification,
  khóa/ground truth tương ứng và không gọi đó là ingestion end-to-end 1M orders.

**Đạt khi:** ít nhất control và skew ở scale đủ tạo shuffle, với input được pin và số liệu thực tế.
**Commit gợi ý:** `feat(benchmark): add reproducible scale and skew datasets`.

### P4F-04 — Harness đo và thu thập metrics

- [ ] Thêm CLI/config: experiment ID, dataset run ID, publication/snapshots, variant,
  repetitions, output path, timeout và tham số tuning; validate allowlist/range.
- [ ] Mỗi trial dùng Spark application riêng, cấu hình resources giống nhau; pin input
  một lần cho cả experiment. Không dùng production publication skip làm số đo xử lý.
- [ ] Materialize đầy đủ workload/output; tránh chỉ `count()` khiến optimizer loại bỏ
  các cột/phép tính cần đo. Giữ cách materialize giống nhau giữa variants.
- [ ] Tách thời gian startup/input/setup, query/action, verification và sink I/O;
  công bố rõ metric nào dùng để so sánh. Không collect dữ liệu lớn về driver.
- [ ] Bật Spark event logging tùy chọn qua config; mount/persist đường dẫn đúng quyền,
  không thêm service mới. Lưu initial/final physical plan và effective Spark config.
- [ ] Parse event logs: application/query/stage IDs, task duration median/p95/max,
  max/median ratio, shuffle read/write bytes, memory/disk spill, task failures/retries,
  stage duration và executor metrics có sẵn.
- [ ] Xử lý stage/task retry và speculative attempts: tách metrics successful work
  khỏi overhead; không cộng trùng hoặc chọn attempt tùy tiện. Stage rỗng/median=0
  phải có quy tắc rõ, không tạo ratio vô nghĩa.
- [ ] Artifact JSON/CSV có schema version, source commit/image, input/config hashes,
  trial status, repetition, timestamp UTC, metrics, correctness status và log refs.
- [ ] Test parser bằng event-log fixtures: thiếu/truncated events, retry, stage rỗng,
  field không có. Metric thiếu ghi unavailable, không biến thành zero.
- [ ] Event logs/config có thể chứa credentials/SQL: redaction và allowlist trước khi
  xuất báo cáo/CI artifact; không commit raw logs, raw PII hoặc dataset.
- [ ] Thêm Make targets dự kiến: `benchmark-preflight`, `benchmark-skew`,
  `verify-benchmark`, `report-benchmark`, `test-benchmark`; ghi help và tham số.
  Các target này **chưa tồn tại**, phải triển khai trước khi dùng.

**Đạt khi:** một trial smoke tạo artifact có metrics và có thể đối chiếu với event log.
**Commit gợi ý:** `feat(benchmark): add Spark measurement harness and event metrics`.

### P4F-05 — Chạy baseline và từng biến thể

- [ ] Baseline B0: cố định partition count (khởi điểm 16), AQE off và auto broadcast off;
  xác nhận physical plan thật có shuffle, ghi cả cấu hình production hiện có để tham chiếu.
- [ ] B1: chỉ bật AQE và skew-join handling; ghi final plan có/không split skew partitions.
- [ ] B2: broadcast dimension đủ nhỏ; ghi kích thước dimension, threshold/hint và plan;
  không ép broadcast khi vượt ngân sách memory. Ghi rõ cấu hình nền kế thừa.
- [ ] B3: salting key nóng với salt deterministic từ khóa dòng ổn định; replicate phía
  dimension đúng salt, aggregate hai bước và bỏ salt khỏi output. Count-distinct,
  average và ratio phải giữ semantics (không cộng distinct-count từng salt).
- [ ] B4: repartition/shuffle partitions sweep 16/32/64 ở cùng workload và resources;
  tính cả chi phí repartition, không loại nó khỏi thời gian query để tạo lợi thế giả.
- [ ] So sánh từng biến thể trên nền xác định rõ trước; chỉ phối hợp các thay đổi có ích
  ở vòng cuối. Mỗi trial lưu effective config, không để config của trial trước rò sang.
- [ ] Chạy 1 warm-up và ít nhất 3 trial đo cho mỗi cell bắt buộc; đổi/thay phiên thứ tự
  variants, không chạy job khác đồng thời. Công bố cache/OS-cache caveat; không gọi
  một lần chạy là cold-cache nếu chưa có cơ chế kiểm soát.
- [ ] Cell bắt buộc: baseline + bốn hướng tối ưu trên control/skew ở scale khả thi.
  Mỗi hướng phải được chạy hoặc có bằng chứng không áp dụng; không âm thầm bỏ kết quả xấu.
- [ ] Không so sánh baseline 100k với optimized 1M; mọi cặp phải cùng snapshot/config
  nghiệp vụ/resources và cùng cache protocol.
- [ ] Báo cáo median và min/max qua repetitions, speedup baseline/variant, task skew,
  shuffle/spill, file count/size nếu có sink; giữ failures/OOM/timeouts trong kết quả.
- [ ] Nếu skew quá nhỏ hoặc workload quá nhẹ, tăng scale trong budget hoặc đổi workload
  có lý do; không kết luận “tối ưu hiệu quả” từ nhiễu. Không đặt mục tiêu speedup giả định.

**Đạt khi:** ma trận có số đo lặp lại và lời giải thích từ physical plan/metrics.
**Commit gợi ý:** `perf(benchmark): compare AQE broadcast salting and partition tuning`.

### P4F-06 — Correctness và áp dụng kết quả

- [ ] Mỗi variant phải đạt cùng schema/grain, row counts, unique keys, NULL semantics,
  exact monetary totals theo channel/currency, và equality hai chiều có multiplicity.
- [ ] Dùng distributed `exceptAll`/đối soát đầy đủ khi khả thi; checksum chỉ là bổ trợ,
  không dùng sum/count đơn thuần để kết luận toàn bộ output giống nhau.
- [ ] Test salting với hot/cold keys, NULL keys, nhiều dòng cùng order và dimension keys;
  kiểm tra duplicate/fan-out, count-distinct và tỷ lệ sau ghép partial aggregates.
- [ ] Giữ regression numeric overflow, currency, policy, point-in-time và censoring xanh.
- [ ] Replay benchmark cùng input/config cho cùng kết quả; artifact phân biệt trial mới
  và retry, không ghi đè bằng chứng hoặc ảnh hưởng publication nghiệp vụ.
- [ ] Chỉ áp dụng tuning vào pipeline chính nếu hiệu quả đo được và correctness đạt;
  nếu không có lợi, giữ làm benchmark và nêu kết luận. Tuning mặc định phải qua ADR/config.
- [ ] Nếu đổi pipeline chính: chạy lại job bị ảnh hưởng hai lần, verify/reconcile, kiểm tra
  small/default cùng tồn tại; kiểm tra restart và recovery liên quan trước khi đóng task.
- [ ] Ghi rollback về cấu hình trước tối ưu; không reset dữ liệu để rollback config.

**Đạt khi:** mọi variant được đưa vào so sánh đều có correctness evidence; production
changes (nếu có) có replay/recovery evidence.
**Commit gợi ý:** `test(benchmark): verify equivalent outputs and safe replay`.

### P4F-07 — Báo cáo, CI và bàn giao

- [ ] Viết `docs/benchmarks/skew.md`: môi trường, dataset, seed/skew thật, workload,
  commit/image/publications, commands, bảng median/min/max, shuffle/spill/task skew,
  physical-plan evidence, kết quả không cải thiện, giới hạn và cách tái chạy.
- [ ] Commit summary CSV/JSON đã redacted với kích thước nhỏ; raw event logs/output
  lưu local/artifact ngoài git, có hướng dẫn retention không xóa snapshot đang được pin.
- [ ] Thêm CI smoke/manual workflow và event allowlist phù hợp: build, fixture nhỏ,
  run+verify+report, upload artifact đã lọc. Không đặt ngưỡng wall-clock dễ flaky trên hosted CI.
- [ ] CI failure cho input mismatch, thiếu correctness/metrics bắt buộc hoặc job lỗi;
  timeout rõ. Large benchmark local/manual, không tự chạy 1M trong mỗi PR.
- [ ] Chạy `make check`, CI smoke local và matrix đã chọn; hosted CI chỉ ghi thành công
  khi có run URL/commit, phân biệt bằng chứng local với thông tin user đã báo trước đó.
- [ ] Cập nhật README, runbook, implementation-status và tài liệu bàn giao Phase 5:
  CLI/config ổn định, resources, input dependencies, outputs, retry/idempotency/timeout.
- [ ] Lưu checklist đã hoàn thành vào `docs/phase4f-checklist.md`; chỉ gắn tag dự kiến
  `v0.4f-benchmark` khi DoD đạt, không di chuyển các tag cũ.

**Commit gợi ý:** `docs(benchmark): publish skew results and close Phase 4`.

## 4. Điều kiện đóng Phase 4F / Phase 4

- [ ] Baseline nghiệp vụ đồng nhất policy, verify/reconcile đạt trên small/default.
- [ ] Dataset benchmark có scale/skew thật được đo và input snapshots cố định.
- [ ] Baseline và AQE/broadcast/salting/repartition được đánh giá có bằng chứng;
  hướng không áp dụng phải có giải thích, số liệu không bị chọn lọc.
- [ ] Có repetitions, metrics task/shuffle/spill và bảng so sánh trước/sau tái lập được.
- [ ] Kết quả dữ liệu giữa variants tương đương; không phá các contract numeric/PII/keys.
- [ ] `make check`, benchmark smoke và regression của thay đổi production đều đạt.
- [ ] Báo cáo `docs/benchmarks/skew.md`, checklist, commands và bàn giao Phase 5 đầy đủ.

Không bắt buộc đạt một phần trăm tăng tốc cụ thể: kết luận không cải thiện cũng hợp lệ
nếu phương pháp đo đúng và giới hạn được ghi rõ. Không đóng Phase 4 chỉ vì job chạy xong.

## 5. Quy tắc cập nhật

1. Chỉ đánh `[x]` khi có implementation và evidence đúng commit; blocker giữ `[ ]` kèm lý do.
2. Sau task nhỏ: kiểm tra phù hợp, cập nhật evidence/checklist, commit Conventional Commits.
3. Ưu tiên Make targets; chỉ bật core/batch, giữ versions/ports và resource limits đã pin.
4. Không commit secrets, dữ liệu lớn, raw event logs hoặc state local; không xóa volume,
   bucket, DB hoặc snapshot được pin.
5. Chưa triển khai Phase 5 trong kế hoạch này. Sau khi 4F đạt DoD mới lập TODO Airflow.
