# Architecture brief — LLM observability 1B requests/ngày dưới trần FinOps

**Quyết định đề xuất:** dùng Delta Lake trên object storage, Unity Catalog (hoặc
catalog REST tương thích) làm control plane, và stream Bronze → Silver → Gold.
Prompt/response đã redaction được giữ 7 ngày; bảng aggregate không chứa nội dung
nhạy cảm được giữ 13 tháng. Đây là quyết định để review, không phải claim rằng
mọi con số đã chạy ở quy mô 1B requests/ngày.

## 1. Bài toán (195 từ)

API foundation-model ghi **1 tỷ request/ngày**, trung bình 5 KB/request, tức
**5 TB/ngày raw** trước replication, index và metadata. Product cần dashboard
theo tenant/model (cost, p50/p95 latency, error rate) mới dữ liệu tối đa 5 phút.
SRE cần xem prompt/response đã redaction trong 7 ngày để điều tra incident. Sau
7 ngày chỉ aggregate được giữ 1 năm; tuyệt đối không để PII thô xuất hiện trong
notebook, BI, hay vector index. Trần chi phí **storage** là $5,000/tháng; compute
được theo dõi riêng vì một query scan sai cũng có thể đốt ngân sách.

Điểm khó không phải ghi 58 GB/phút trung bình, mà là ghép ba SLA đối nghịch:
ingest liên tục và truy vấn tenant nhanh, xóa/retention có thể chứng minh được,
và không trả giá S3 bằng hàng triệu small files. Một incident lúc 03:00 không
được biến thành quyền đọc dữ liệu nhạy cảm vô hạn. Mỗi record vì vậy có
`event_id`, `event_ts`, `ingest_ts`, `tenant_id`, `model`, token/cost/latency,
`redacted_prompt`, `redacted_response`, policy version và một `trace_id` bất biến.
Raw payload chỉ tồn tại trong vùng quarantine có khóa KMS riêng để redactor xử lý;
nó không phải một bảng analyst có thể query.

## 2. Luồng dữ liệu và đường query

```text
 API gateway / model workers (1B req/day)
        |  event_id, event_ts, trace_id, encrypted payload
        v
 Kafka (24h replay) --> schema registry + DLQ --------------------------+
        | micro-batch 60 s                                               |
        v                                                               |
 Bronze Delta: append-only, day/hour, encrypted quarantine              |
        | deterministic PII redaction + tokenization; invalid -> DLQ ---+
        v
 Silver Delta: redacted traces, dedup(event_id), CDF, day/hour/tenant-bucket
        |                                         |                       |
        |  7-day DELETE + VACUUM after approved retention               |
        v                                         v                       v
 Gold Delta: 5-min tenant/model aggregates       audit: policy, access, deletion
        |  (13 months; no prompt/response)             ^
        v                                               |
 BI / alerting: tenant + time filter, p95/cost          catalog + row/column policy
```

Bronze, Silver, Gold đều là managed tables; path object storage không phải API
governance. BI chỉ được cấp Gold; incident responders có quyền Silver theo
tenant và ticket được audit. Query hot là `tenant_id + event_ts`; dashboard đọc
Gold trước, chỉ drill-down sang Silver trong 7 ngày.

## 3. Sáu quyết định có chủ đích

### D1 — Table format và transactional boundary

Tôi chọn **Delta Lake** cho Bronze/Silver/Gold: ACID `MERGE` để dedup theo
`event_id`, Change Data Feed cho aggregate/audit downstream, `RESTORE` khi một
release redactor hay aggregation sai. Tôi loại **Parquet thuần** vì retry Kafka
sẽ tạo duplicate mà không có transaction log, và xóa 7 ngày sẽ thành listing
object không chứng minh được. Tôi loại **Iceberg làm format đầu tiên** không vì
Iceberg kém, mà vì workload này phụ thuộc CDF + streaming `MERGE` operationally
ngay ngày đầu; đội đã vận hành Delta và cần rollback đơn giản. Nếu yêu cầu engine
đa vendor (ví dụ Trino/Snowflake ghi trực tiếp) thành ưu tiên số một, quyết định
này phải được review lại chứ không ép Delta làm universal format.

### D2 — Catalog và governance

Tôi chọn **catalog có managed identity, row/column policy, lineage và audit**
(Unity Catalog hiện tại; interface catalog tách khỏi path để có thể chuyển sang
REST catalog sau). Tôi loại **IAM bucket policy duy nhất** vì nó chỉ biết prefix,
không biết analyst đang xem `redacted_response` hay aggregate của tenant nào.
Tôi loại **metastore tự dựng trong sprint đầu** vì hot path compliance không nên
phụ thuộc một service chưa có on-call, backup và policy UI. Catalog ghi ownership,
classification, policy/redactor version, và quan hệ Gold ← Silver ← Bronze; mọi
`SELECT` có PII và mọi deletion request đi vào audit table immutable.

### D3 — Partition, clustering và file size

Tôi chọn partition **`event_date`, `event_hour`, `tenant_bucket=hash(tenant)%64`**
ở Silver, file target 512 MB, rồi `OPTIMIZE ZORDER BY (tenant_id, event_ts)` cho
partition đã đóng sau 90 phút. Bucket tránh một tenant lớn làm hot partition;
Z-order phục vụ filter tenant/time mà không tạo partition nhỏ cho từng tenant.
Tôi loại partition trực tiếp theo **`tenant_id`** vì số tenant và micro-batch tạo
metadata/small-files bùng nổ. Tôi loại partition theo **ngày duy nhất** vì một
drill-down tenant sẽ phải mở quá nhiều file. Tôi cũng loại optimize mỗi phút:
latency ingest sẽ bị tranh chấp và chi phí rewrite tăng; dashboard 5 phút không
cần compaction realtime.

### D4 — Redaction, tokenization và quyền đọc

Tôi chọn redaction **trước Silver**, token HMAC có key-version trong KMS để join
an toàn theo subject khi được ủy quyền; raw encrypted chỉ redactor service đọc.
Tôi loại redaction ở dashboard vì một export/ad-hoc query có thể lộ PII trước
khi UI che nó. Tôi loại hash không-keyed vì số điện thoại/email có entropy thấp,
dễ bị dictionary attack và không cho rotate key rõ ràng. Key rotation ghi
`token_key_version`; Silver không bị rewrite toàn bộ ngay, nhưng join nhạy cảm
phải dùng service map có quyền, không phải analyst tự tính token.

### D5 — Retention và deletion

Tôi chọn workflow hai tầng: policy job xóa dữ liệu Silver quá 7 ngày theo
partition, sau đó VACUUM chỉ sau retention window được phê duyệt; Gold chỉ chứa
aggregate nên giữ 13 tháng. Deletion request được áp vào Silver bằng predicate
token/subject, CDF phát delete event cho cache/search phụ, audit ghi version và
operator. Tôi loại lifecycle object-store xóa trực tiếp vì transaction log còn
tham chiếu file sẽ làm time travel/query hỏng. Tôi loại retention vô hạn “phòng
khi cần điều tra” vì nó vi phạm constraint và biến breach impact thành vô hạn.
Time travel là công cụ rollback có thời hạn, không phải lý do để giữ PII mãi mãi.

### D6 — Dashboard materialization và serving

Tôi chọn Gold 5-minute tumbling aggregates theo `window_start, tenant_id, model`
và table serving/cache chỉ đọc Gold; correction từ late event dùng CDF để cập
nhật đúng window. Tôi loại dashboard quét Silver trực tiếp: ở 5 TB/ngày, dù chỉ
scan 1% cũng là 50 GB/query, không thể có p95 ổn định. Tôi loại stream processor
giữ toàn bộ aggregate là source of truth vì replay/backfill và audit sẽ khó;
Delta Gold là source of truth, cache chỉ là derived state có thể rebuild.

## 4. Failure modes lúc 03:00

| Sự cố | Phát hiện | Cô lập và rollback |
|---|---|---|
| Redactor release bỏ sót mẫu PII | DLP canary trên Silver, tỷ lệ `policy_version`, và access alert tăng bất thường | Freeze Silver grants, chặn Gold consumer bị ảnh hưởng, `RESTORE` Silver về version trước release trong phạm vi 7 ngày, replay Bronze quarantine với rule mới; audit ghi khoảng version đã exposure. |
| Kafka retry/rebalance tạo duplicate hoặc event đến muộn | `event_id` duplicate rate, watermark lag, Gold reconciliation với gateway counter | Silver `MERGE` idempotent; CDF recompute Gold window bị ảnh hưởng. Không overwrite toàn Gold; reprocess partition/hour và so sánh checksum. |
| Compaction job tạo small-file storm hoặc chậm dashboard | file count/partition, median file size, query bytes scanned và queue lag | Disable schedule qua feature flag, giữ file cũ nhờ Delta snapshot, `RESTORE` nếu commit lỗi; chạy compaction backfill ở partition đã đóng. Đây là bài học NB2/NB6: file count và orphan scan phải là metric, không phải “job succeeded”. |
| Xóa subject thành công ở table nhưng cache vẫn trả trace | CDF delete consumer lag, synthetic delete probe trả hit, cache-versus-table reconciliation | Quarantine cache endpoint cho tenant, replay CDF delete từ committed version; nếu offset không tin cậy, rebuild cache từ Silver snapshot hiện tại. |
| Catalog/policy outage làm BI hoặc incident response thất bại | catalog health, policy evaluation latency, denied-query spike | Fail closed cho Silver/PII; Gold dashboard có read-only 15-minute snapshot cache. Không bypass bằng S3 credential; restore catalog DB/policies từ backup đã test. |

## 5. Back-of-envelope chi phí

Giả định 30 ngày/tháng, nén ZSTD ở Silver đạt **3:1** với telemetry/payload đã
redact; Gold chỉ còn 0.5% Silver. Đây là estimate cần thay bằng invoice sau pilot.

| Thành phần | Phép tính | Ước tính/tháng |
|---|---:|---:|
| Silver hot 7 ngày, Standard | 5 TB/ngày × 7 / 3 × $23/TB-tháng | **$268** |
| Bronze quarantine 24 giờ, Standard | 5 TB × 1 / 3 × $23 | **$38** |
| Gold 13 tháng, Standard-IA | (5 × 30 / 3 × 0.005) TB/tháng × 13 × $12.5 | **$406** |
| Delta log, CDF retention, replication/headroom 2× | ($268 + $38 + $406) × 2 | **$1,424** |
| Compaction + streaming compute | 30 days × 24 h × 40 worker-hours/h × $0.08 | **$2,304** |
| BI/query compute guardrail | budget reservation | **$750** |
| **Tổng storage + compute estimate** |  | **$4,478/tháng** |

Storage theo estimate là $1,424/tháng, dưới cap $5,000 với headroom lớn.
Nhưng cap này có thể bị phá bởi request/listing nếu small files; vì vậy alert
`files/TB`, compaction bytes rewritten và query scanned bytes là FinOps SLO.
Compute $3,054 không nằm trong cap storage của đề bài, nhưng được hiển thị để
không “tối ưu storage” bằng một thiết kế đốt compute.

## 6. MVP một tuần: chứng minh rủi ro lớn nhất trước

MVP không ingest 1B request. Nó ingest replay **100,000 events** với 5% duplicate,
1% late events và PII canary, chạy trên một tenant giả. Ngày 1–2: Bronze append,
schema contract/DLQ và redactor-to-Silver với token key-version. Ngày 3: idempotent
`MERGE`, CDF-driven Gold aggregate 5 phút, một dashboard query tenant/time. Ngày
4: row/column grant, audit read/delete, synthetic deletion probe. Ngày 5: chạy
compaction, đo files-pruned, `RESTORE` một redactor release lỗi và replay chính
xác Gold window.

Tiêu chí ship không phải “có dashboard”: ingest-to-Gold lag p95 < 5 phút, cùng
`event_id` replay hai lần không tăng count, canary PII không có ở Silver/Gold,
và deletion probe đạt 0 hit ở table **và** cache trong 15 phút. Nếu một trong
bốn điều đó không đạt, mở rộng throughput sẽ chỉ làm rủi ro compliance nhanh hơn.
