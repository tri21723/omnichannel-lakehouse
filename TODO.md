# TODO — Phase 4B: Chuẩn hóa Bronze thành Silver

Cập nhật: **2026-11-13**. Trạng thái: **Đã lập kế hoạch; chưa triển khai Phase 4B**.

Mục tiêu: triển khai phần Silver của `dp2_silver`, tạo dữ liệu chuẩn cho 5 kênh,
không trùng natural key, bảo vệ PII, có quarantine và đối soát từng bước.

Tham chiếu: [plan](docs/plan.md), [data model](docs/data-model.md),
[bàn giao Silver](docs/silver-handoff.md), [Bronze contract](docs/bronze-contract.md).
Lịch sử: [checklist Phase 4A](docs/phase4a-checklist.md),
[bằng chứng Bronze](docs/bronze-validation.md), tag `v0.4a-bronze`.

## 1. Baseline và phạm vi

Bronze đã kiểm chứng local: JSONL 15.600 dòng, Walmart JDBC 3.000 orders,
completion có snapshot references; 49 unit tests pass tại mốc Phase 4A.
Walmart JDBC là nguồn authoritative; JSONL Walmart chỉ đối soát, không union vào Silver.

Phạm vi: orders, order_items, customers, products, returns cho 5 kênh; seller_id
được giữ trong entity liên quan. Bảng sellers riêng cần quyết định contract vì nguồn
hiện chưa có đủ seller profile. Bắt đầu Walmart trước, rồi mở rộng JSONL.

Ngoài phạm vi: Gold dim/fact, features/labels, warehouse export, Airflow/Trino,
streaming/CDC, DataHub, AWS và benchmark skew lớn. Không đổi runtime/port đã pin.
Hosted CI chưa được kiểm chứng; local DoD và trạng thái hosted phải ghi riêng.

## 2. Thứ tự và mốc bàn giao

| Task | Phụ thuộc | Đầu ra | Công sức dự kiến |
| --- | --- | --- | --- |
| P4B-01 | — | Baseline và fixture | 1–2 giờ |
| P4B-02 | 01 | ADR/DDL/semantics | 3–5 giờ |
| P4B-03 | 02 | Reader snapshot và input manifest | 3–5 giờ |
| P4B-04 | 03 | Walmart orders/customers/items | 4–6 giờ |
| P4B-05 | 04 | Walmart products/returns, DQ quan hệ | 3–5 giờ |
| P4B-06 | 02, 04 | PII và quarantine | 3–5 giờ |
| P4B-07 | 05, 06 | Dedup/MERGE/recovery | 4–7 giờ |
| P4B-08 | 07 | Chuẩn hóa 4 nguồn JSONL còn lại | 4–7 giờ |
| P4B-09 | 08 | Đối soát và regression | 3–5 giờ |
| P4B-10 | 09 | CI/runbook/bàn giao Gold | 2–4 giờ |

Tổng **30–51 giờ công**, là ước lượng lập kế hoạch, không phải deadline.
Mốc A: contract/input; mốc B: Walmart hoàn chỉnh; mốc C: đủ 5 kênh và local DoD.

## 3. Checklist triển khai

### P4B-01 — Baseline và fixture

- [ ] Đọc AGENTS, ADR và Git status; giữ thay đổi ngoài task.
- [ ] Chạy make check, verify-runtime, verify-bronze và test-bronze-contract khi bắt đầu; ghi kết quả mới.
- [ ] Chọn rõ run/date/completion đầu vào; xác minh snapshot còn tồn tại, không tự dùng latest khi thiếu.
- [ ] Chuẩn bị fixture nhỏ riêng cho missing fields, null, JSON scalar, decimal overflow, timestamp lỗi, duplicate và key conflict.
- [ ] Fixture mutation/recovery phải dùng namespace/run riêng; không sửa raw đã publish hoặc reset volume.

**Đạt khi:** Có baseline tái lập, fixture và expected outcomes, không chứa PII thật.
**Commit gợi ý:** `test(silver): establish source snapshots and fixtures`.

### P4B-02 — Chốt contract Silver và DDL

- [ ] Viết ADR mới và cập nhật data-model/DDL cho 5 entity; quyết định sellers là deferred hoặc có nguồn đủ rõ.
- [ ] Chốt natural keys: channel/order_id; channel/order_id/line_no; channel/customer_id; channel/product_id; channel/return_id/return_line_no.
- [ ] Chốt kiểu STRING, UTC TIMESTAMP, DECIMAL(18,2), nullable/required và enum hợp lệ cho từng trường.
- [ ] Chốt scope CLI --run-id, --run-date, --channel, --mode full|incremental và cách chọn completion cụ thể.
- [ ] Định nghĩa full/incremental, source snapshot revision và hành vi khi bản sửa nguồn làm một dòng trước đây valid trở thành invalid hoặc biến mất.
- [ ] Chốt dedup theo updated_ts và tie-break ổn định bằng canonical payload hash; không dùng ingest time hay thứ tự file.
- [ ] Định nghĩa version cho items/returns thiếu updated_ts: kế thừa nguồn nào, xử lý snapshot mới/cũ và conflict ra sao.
- [ ] Chọn partition và audit fields, policy schema evolution; cấm drop/recreate bảng khi mismatch.
- [ ] Chốt ranh giới atomic từng bảng, completion toàn job và cơ chế khóa writer; không tuyên bố transaction nhiều bảng.

**Đạt khi:** Contract giải quyết rõ replay, sửa dữ liệu và phiên bản nguồn trước khi viết MERGE.
**Commit gợi ý:** `docs(adr): define Silver schemas and merge semantics`.

### P4B-03 — Reader Bronze theo snapshot cố định

- [ ] Tạo reader dùng chung trong spark/lib; kiểm tra status completed, contract, selection, batch ID và allowlist bảng.
- [ ] Copy bộ input references thành manifest bất biến cho attempt Silver; completion Bronze bị thay sau đó không làm đổi input đang xử lý.
- [ ] Đọc Iceberg bằng snapshot ID rồi lọc run/channel/date/source/batch; reconcile count/content với completion.
- [ ] Với snapshot NULL chỉ chấp nhận evidence zero rows và tạo input rỗng typed; snapshot bị expire phải fail rõ.
- [ ] Từ chối completion legacy thiếu selection; hướng dẫn replay Bronze full để nâng metadata.
- [ ] JDBC Walmart yêu cầu đủ 5 bảng cùng completion; JSONL Walmart không được chọn cho authoritative Silver.
- [ ] Test Bronze latest thay đổi sau khi chọn input, completion failed/running, thiếu bảng và snapshot không tồn tại.

**Đạt khi:** Input Silver được cố định và đối soát trước mọi ghi đích.
**Commit gợi ý:** `feat(silver): read validated Bronze snapshot inputs`.

### P4B-04 — Vertical slice Walmart

- [ ] Tạo spark/jobs/dp2_silver với thư viện parse/schema/audit dùng chung và structured logs.
- [ ] Parse _raw bằng schema explicit; phân biệt missing, null, cast failure, JSON scalar và invalid syntax.
- [ ] Chuẩn hóa orders/customers/items: timestamp UTC, DECIMAL chính xác, ID dạng string và giữ currency.
- [ ] Không dùng float hoặc làm tròn âm thầm; overflow/sai scale theo policy phải có error code.
- [ ] Kiểm tra qty > 0, line_no hợp lệ, amount không âm và key bắt buộc; định nghĩa kiểm tra tổng tiền với discount.
- [ ] Giữ provenance về batch/snapshot/record fingerprint; chưa publish customer PII rõ ra bảng Silver.
- [ ] Chạy một ngày Walmart trên Spark cluster; assert schema, giá trị và accounting theo entity.

**Đạt khi:** Một slice typed có expected values và provenance; PII chưa được xuất rõ.
**Commit gợi ý:** `feat(silver): normalize Walmart orders customers and items`.

### P4B-05 — Products/returns và quan hệ

- [ ] Mở rộng parser cho products và returns; giữ brand nullable và return line key đầy đủ.
- [ ] Returns chọn theo membership của orders; không loại vì return_ts sau run_date.
- [ ] Dedup customers/products lặp giữa date slices theo version contract, không tăng entity count mỗi ngày.
- [ ] Chốt và test orphan order/item/product/return: quarantine hay deferred lookup phải rõ.
- [ ] Join luôn gồm channel; test cùng source ID xuất hiện ở hai kênh không bị join nhầm.
- [ ] Kiểm tra refund/return quantity theo dữ liệu hiện có; không tự suy ra field nguồn chưa cung cấp.

**Đạt khi:** Đủ 5 entity Walmart với DQ quan hệ và ngày hoàn trả được xử lý đúng.
**Commit gợi ý:** `feat(silver): normalize Walmart products returns and relationships`.

### P4B-06 — PII và quarantine

- [ ] Chốt normalization/hash policy trong ADR: tên/email, giá trị null, key/version, scope liên kết giữa kênh; tránh tự tạo cross-channel identity.
- [ ] Nếu dùng keyed hash/HMAC: secret qua env, không có default key; cùng key cho kết quả ổn định, ghi key version và quy trình rotation.
- [ ] Test không còn full_name/email rõ trong Silver schema, data mẫu, logs và exception; raw/quarantine có policy hạn chế truy cập.
- [ ] Tạo silver.quarantine_<entity> với error code ổn định, provenance và record fingerprint; quyết định rõ có lưu raw hay chỉ tham chiếu Bronze.
- [ ] Quarantine idempotent; retry không tăng bản sao, sửa lỗi nguồn giải quyết hoặc thay trạng thái quarantine cũ theo contract.
- [ ] Định nghĩa thứ tự kiểm tra lỗi và reason list; một record nhiều lỗi không bị đếm thành nhiều record.
- [ ] Ghi accounting: input occurrences = rejected + valid candidates; candidates = winners + duplicate losers. Tách accounting cha/con khi explode.

**Đạt khi:** PII được xử lý nhất quán và mọi dòng bị loại khỏi Silver có lý do đối soát được.
**Commit gợi ý:** `feat(silver): protect PII and reconcile quarantined records`.

### P4B-07 — Dedup, MERGE và phục hồi

- [ ] Dedup trước MERGE để mỗi target key chỉ có một source winner; giữ conflict/duplicate metrics.
- [ ] Test đổi thứ tự input/repartition vẫn chọn cùng winner, kể cả bằng updated_ts nhưng payload khác.
- [ ] MERGE không để batch cũ ghi đè phiên bản mới; kiểm thử late replay của date cũ sau date mới.
- [ ] Full replacement có scope chính xác; không xóa customer/product dùng chung bởi ngày khác. Chốt xử lý record biến mất/invalid trước khi coi full là hoàn chỉnh.
- [ ] Không suy ra CDC delete hoặc is_deleted từ việc thiếu row nếu source contract không đảm bảo full snapshot scope.
- [ ] Publish completion Silver sau read-back/DQ tất cả bảng, gồm input snapshots, output snapshots, counts và policy versions.
- [ ] Tiêm lỗi sau commit một bảng, trước completion và trong quarantine; marker incomplete, retry phục hồi và không tạo bản sao.
- [ ] Test writer conflict, empty output, NULL identity và hai dataset cùng ngày; nguồn Bronze giữ nguyên.

**Đạt khi:** Replay deterministic, không rollback dữ liệu mới và partial publish không thành công giả.
**Commit gợi ý:** `feat(silver): publish deterministic replay-safe entity state`.

### P4B-08 — Mở rộng JSONL đa kênh

- [ ] Amazon: coalesce customer_id/buyer_id; nếu cả hai có giá trị khác nhau thì xử lý conflict rõ; giữ fulfillment_type nullable.
- [ ] TikTok Shop: parse schema hiện có và explode customer/items/products/returns với provenance.
- [ ] Shopee: parse timestamp có +0700 bằng format explicit, chuyển UTC, test boundary qua ngày và malformed date.
- [ ] Lazada: brand nullable, VND và decimal strings giữ nguyên; không dùng hệ số generator như FX rate.
- [ ] Giữ fields lạ trong Bronze; Silver schema evolution theo contract/version, không mergeSchema mù.
- [ ] Phân biệt missing array với empty array; dùng policy để tránh sinh child row NULL hoặc bỏ mất lỗi.
- [ ] Test embedded customer/product lặp ở nhiều orders; natural-key dedup không làm mất order/item/return.
- [ ] Nạp đủ 4 JSONL kênh + Walmart JDBC; chạy lại toàn bộ không tăng business keys.

**Đạt khi:** 5 kênh dùng chung schema Silver, source differences được kiểm thử rõ.
**Commit gợi ý:** `feat(silver): normalize omnichannel JSONL entities`.

### P4B-09 — Đối soát và vận hành

- [ ] Tạo verifier read-only theo completion/snapshot, kiểm tra schema, uniqueness, FK/DQ, PII policy và row accounting.
- [ ] Baseline default kỳ vọng 15.000 orders duy nhất = 3.000/kênh; không cộng Walmart export thành kênh thứ sáu.
- [ ] Expected counts của items/customers/products/returns tính theo source keys và scope, không mặc định bằng số orders.
- [ ] Kiểm tra tổng tiền/refund theo channel/currency bằng DECIMAL; không tổng hợp USD và VND vào cùng một số.
- [ ] Test malformed fixtures có expected reject/dedup/winner counts; mọi đầu vào được giải thích, kể cả parent-child rejection.
- [ ] Test replay hai lần, source correction, late older batch, PII key thiếu, snapshot mất, input completion sai và catalog failure.
- [ ] Restart rồi verify trước mọi ghi lại; regression Bronze/core/source vẫn pass.
- [ ] Ghi runtime, input/output rows, duration, staging/shuffle/file metrics phù hợp; không diễn giải fixture nhỏ thành benchmark lớn.

**Đạt khi:** Có evidence cho correctness, privacy, recovery và persistence.
**Commit gợi ý:** `test(silver): verify reconciliation PII replay and persistence`.

### P4B-10 — Make/CI/tài liệu và bàn giao

- [ ] Mở rộng make spark-submit JOB=dp2_silver; thêm smoke-silver, verify-silver và integration-silver, giữ target Bronze.
- [ ] Tách unit/local Spark tests khỏi Docker integration; không im lặng skip rồi báo pipeline pass.
- [ ] Workflow manual dùng fixture nhỏ, timeout, cleanup không xóa volumes local và failure logs đã lọc PII/secret.
- [ ] Chạy make check và integration local; ghi hosted CI riêng, không tick hosted pass khi chưa có run.
- [ ] Hoàn tất docs/silver-validation.md, README, runbook, implementation-status, DDL và ADR bằng expected/actual evidence.
- [ ] Bàn giao Gold: grain, natural keys, currency, relationships, quarantine accounting, PII fields và snapshot input contract.
- [ ] Rà soát TODO theo bằng chứng; commit mỗi task nhỏ, tag v0.4b-silver chỉ sau local DoD đạt. Chưa tag toàn Phase 4.

**Đạt khi:** Có cách tái lập Silver và contract đủ để lập kế hoạch Gold.
**Commit gợi ý:** `docs(silver): publish validation and Gold handoff`.

## 4. Điều kiện đóng Phase 4B

- [ ] 5 entity/5 kênh đúng schema, key uniqueness và source ownership.
- [ ] Input pinned theo completion/snapshot; replay và correction có semantics được kiểm thử.
- [ ] PII policy đạt; quarantine/accounting không mất dòng âm thầm.
- [ ] Partial failure/retry và restart pass; Bronze/source regression xanh.
- [ ] Lint, unit/integration local đạt; CI hosted và giới hạn được ghi trung thực.
- [ ] DDL/ADR/runbook/evidence/handoff cập nhật, commits và milestone hoàn tất.

## 5. Quy tắc thực hiện

1. Checkbox để trống đến khi có implementation và kiểm chứng tương ứng.
2. Sau mỗi task nhỏ: chạy kiểm tra liên quan và make check, cập nhật evidence/TODO,
   rồi commit Conventional Commits ngay; không cần chờ cả nhóm task lớn.
3. Mục không áp dụng ghi N/A kèm quyết định phạm vi; blocker giữ unchecked.
4. Không đổi version/port, xóa bucket/volume/DB, expire snapshot hoặc tạo cloud resources.
5. Không commit secret, raw payload, hash key, dữ liệu runtime hoặc artifact lớn.
6. Các quyết định chưa chốt ở checklist phải được giải quyết trong ADR trước dependent code.

Sau 4B: 4C Gold dim/fact → 4D features/labels → 4E warehouse export → benchmark và
batch end-to-end để đóng toàn Phase 4. Các phần này không thuộc DoD Silver hiện tại.
