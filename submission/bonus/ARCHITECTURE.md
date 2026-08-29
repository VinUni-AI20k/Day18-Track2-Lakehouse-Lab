# Bonus Architecture — LLM Observability at 1B Requests/Day

**Sinh viên:** Lê Hoàng Nam  
**Mã sinh viên:** 2A202600965  
**Chủ đề:** A — LLM observability ở quy mô 1 tỷ request/ngày

## 1. Problem statement

Nền tảng phục vụ 1 tỷ LLM request/ngày, trung bình 5 KB/request, tương đương 5 TB raw/ngày và khoảng 150 TB/tháng. Dashboard chi phí, latency và error rate theo tenant phải mới trong 5 phút. Prompt/response đầy đủ chỉ được giữ 7 ngày để điều tra sự cố; metrics tổng hợp giữ 1 năm. PII phải được phát hiện và token hóa trước khi dữ liệu cho phép con người truy cập. Ngân sách storage tối đa 5.000 USD/tháng.

Khó khăn không chỉ nằm ở throughput khoảng 11.600 request/giây trung bình và cao hơn nhiều lúc peak. Hệ thống còn phải xử lý retry, event đến muộn, schema thay đổi, quyền xóa dữ liệu, truy vấn theo tenant và rollback khi pipeline che PII bị lỗi. Thiết kế phải giữ raw đủ lâu để replay nhưng không biến Bronze thành kho dữ liệu nhạy cảm, đồng thời tránh small-file explosion từ micro-batch 5 phút.

## 2. Architecture diagram

```text
LLM gateways (multi-region, request_id + tenant_id + event_time)
               │
               ▼
     Kafka / managed event stream ──────► DLQ (schema/size failures)
               │  micro-batch 1 min
               ▼
┌──────────────────────────────────────────────────────────────────────┐
│ BRONZE — Delta, encrypted object storage, 7-day retention           │
│ date/hour partitions; tokenized user_id; encrypted prompt payload;  │
│ immutable ingest metadata; restricted break-glass role              │
└──────────────────┬───────────────────────────────────────────────────┘
                   │ streaming parse + PII policy + dedup(request_id)
                   ▼
┌──────────────────────────────────────────────────────────────────────┐
│ SILVER — Delta, 30-day operational retention                        │
│ typed calls; tenant_id; model; tokens; latency; status; PII removed │
│ liquid/Z-order clustering by tenant_id, event_time                  │
└───────────────┬───────────────────────────────┬──────────────────────┘
                │ 5-min incremental aggregate   │ incident query
                ▼                               ▼
┌───────────────────────────────────┐     SQL warehouse / on-call tools
│ GOLD — Delta, 365-day retention   │
│ tenant × model × 5-min/day:       │
│ p50/p95, tokens, cost, error_rate │
└───────────────┬───────────────────┘
                ▼
      BI dashboards / budget alerts

Catalog + RBAC + audit log + lineage span all three layers.
Orchestrator runs quality gates, OPTIMIZE, retention and recovery drills.
```

## 3. Key decisions and rejected alternatives

### 3.1 Table format: Delta Lake

Tôi chọn **Delta Lake** vì pipeline cần atomic micro-batch writes, schema enforcement, MERGE để dedup retry, time travel để rollback và hệ sinh thái SQL/Spark mạnh. Tôi loại **plain Parquet** vì không có transaction log: reader có thể thấy trạng thái ghi dở và việc MERGE/rollback phải tự xây dựng. Tôi loại **Apache Iceberg** cho bản đầu vì yêu cầu chính không cần partition evolution phức tạp, trong khi nhóm đã có năng lực vận hành Delta; đổi format lúc này tăng rủi ro mà không cải thiện SLA 5 phút. Một PoC Iceberg vẫn được giữ như phương án thoát lock-in.

### 3.2 Ingestion: event stream + one-minute micro-batch

Tôi chọn **Kafka-compatible stream và checkpointed micro-batch một phút**. Cách này chịu burst, replay được và còn bốn phút cho Silver/Gold trước SLA dashboard. Tôi loại **ghi trực tiếp từ gateway vào object storage** vì retry và backpressure sẽ xâm nhập request path. Tôi loại **batch theo giờ** vì vi phạm SLA 5 phút. Exactly-once ở storage được thực hiện bằng `(request_id, event_version)` và MERGE; không giả định transport tự tạo exactly-once end-to-end.

### 3.3 Partitioning và clustering

Bronze partition theo **UTC date/hour**, Silver theo **date** và cluster theo `(tenant_id, event_time)`; Gold partition theo date và cluster theo tenant. Tôi loại partition trực tiếp theo **tenant_id** vì hàng triệu tenant sẽ tạo partition nhỏ và metadata explosion. Tôi loại partition theo **model** vì model có cardinality thấp, skew lớn và không phục vụ truy vấn tenant — hot path chính. Compaction chạy khi median file size dưới 128 MB hoặc active files/partition vượt 500, thay vì lịch cố định mù quáng.

### 3.4 PII governance

Tôi chọn **tokenization ngay tại ingress**, tách khóa token vào KMS/HSM, mã hóa riêng prompt/response và chỉ cấp quyền break-glass có audit. Silver không chứa prompt raw hoặc định danh trực tiếp. Tôi loại **redact ở Silver** vì khi đó con người và công cụ Bronze vẫn có thể đọc PII trước khi policy chạy. Tôi loại **hash không khóa** vì email/số điện thoại có không gian tìm kiếm nhỏ, dễ bị dictionary attack. Quyền đọc dùng RBAC kết hợp tenant scope và mọi break-glass access được ghi vào immutable audit table.

### 3.5 Retention và lifecycle

Tôi chọn **Bronze full payload 7 ngày**, Silver operational fields 30 ngày và Gold aggregates 365 ngày. Delta log/data tombstones được giữ đủ cho cửa sổ rollback vận hành; VACUUM chỉ chạy sau khi quality gate và legal-hold check thành công. Tôi loại **giữ raw một năm** vì 1,825 PB logical/năm vượt xa nhu cầu incident và budget. Tôi loại **xóa raw ngay sau aggregate** vì mất khả năng replay khi pricing, parser hoặc PII rule sai. Payload hết 7 ngày được lifecycle-delete; không chuyển Glacier vì yêu cầu nói chỉ giữ aggregate sau bảy ngày.

### 3.6 Serving layer

Tôi chọn **Gold 5-minute aggregates + SQL warehouse autoscaling**, dashboard chỉ đọc Gold; truy vấn incident mới đọc Silver/Bronze. Tôi loại để BI **scan Bronze** vì 35 TB cửa sổ hot khiến latency và chi phí khó dự đoán. Tôi loại **một database OLTP phụ làm source of truth cho metrics** vì tạo dual-write consistency và phá khả năng replay. Cache dashboard 60 giây được phép vì vẫn nằm trong SLA freshness 5 phút.

### 3.7 Catalog và lineage

Tôi chọn **catalog hỗ trợ open APIs, column-level tags, RBAC và OpenLineage events** từ ingest đến dashboard. Tôi loại chỉ dùng **filesystem paths** vì không thể kiểm soát ownership, schema contract và truy vết cột `cost_usd`. Tôi loại catalog đóng không có export metadata vì làm kế hoạch thoát vendor trở nên đắt. Mỗi Gold commit lưu input table versions, code version và pricing-table version để tái lập kết quả.

## 4. Data contracts and correctness

Bronze bắt buộc có `request_id`, `tenant_id`, `event_time`, `ingest_time`, `model`, `status`, usage và payload classification. Unknown fields được giữ trong raw envelope, nhưng thiếu khóa hoặc payload vượt giới hạn sẽ vào DLQ. Silver áp dụng:

- Dedup theo `request_id`, ưu tiên `event_version` mới nhất rồi `ingest_time`.
- Chấp nhận late data trong 24 giờ; aggregate liên quan được cập nhật bằng MERGE.
- Kiểm tra `prompt_tokens >= 0`, `completion_tokens >= 0`, latency hợp lý và model thuộc dimension có version.
- Chặn publish nếu tỷ lệ parse failure > 0,1%, PII scan dương tính hoặc row-count lệch ngoài biên lịch sử.

Gold lưu p50/p95 latency, tổng token, `error_rate`, `cost_usd`, record count và watermark. Cost join với pricing dimension có `effective_from/effective_to`; vì vậy backfill lịch sử không vô tình dùng giá hiện tại.

## 5. Failure modes, detection and rollback

| Failure lúc 3 giờ sáng | Phát hiện | Containment và rollback |
|---|---|---|
| PII rule deployment bỏ sót số điện thoại trong prompt | Canary corpus + DLP scan trên mỗi Bronze commit; alert nếu any high-confidence finding | Dừng Silver publisher, thu hồi quyền Bronze, sửa rule rồi replay từ version Bronze an toàn. Xóa/rewrite các file vi phạm và ghi audit incident. |
| Schema mới đổi `usage.output` từ integer sang string | Contract check và DLQ rate vượt 0,1% | Không promote commit sang Silver. Parser hỗ trợ song song hai schema, replay DLQ; schema evolution chỉ được merge sau approval. |
| Job Gold dùng sai bảng giá làm cost tăng 20× | Reconciliation theo token × price và anomaly alert theo tenant/model | Time travel Gold về version trước deploy trong vài phút, pin pricing version đúng rồi backfill chỉ các partition ảnh hưởng. |
| Retry storm tạo file nhỏ và trùng request | Active-files, duplicate-rate và Kafka lag alerts | Tăng batch size tạm thời, dedup bằng MERGE, compact partition bị ảnh hưởng; không VACUUM cho đến khi xác nhận replay hoàn tất. |
| Region ingest mất kết nối 45 phút | Consumer lag, watermark và regional heartbeat | Buffer trên stream, đánh dấu dashboard stale; khi phục hồi, process theo event time trong late-data window và MERGE lại Gold. |
| Xóa nhầm partition Bronze trước hết retention | Lifecycle audit và expected-partition manifest | Khôi phục bằng object-versioning nếu còn; nếu không, replay stream retention. Drill hàng tháng xác nhận RTO < 30 phút. |

Mục tiêu vận hành: Bronze/Silver pipeline RPO ≤ 1 phút, RTO ≤ 30 phút; Gold dashboard RPO ≤ 5 phút. Mỗi quý chạy restore drill trên một partition thật đã sao chép sang môi trường cô lập.

## 6. Cost back-of-envelope

Các con số sau là **planning assumptions**, cần thay bằng giá hợp đồng và benchmark trước production:

### Storage

- Raw ingress: `5 TB/day × 7 days = 35 TB logical`.
- Giả định JSON nén ZSTD còn 50%: `35 × 0.5 = 17.5 TB Bronze hot`.
- Silver chỉ giữ operational columns, 15% raw và 30 ngày: `5 × 0.15 × 30 = 22.5 TB`.
- Gold bằng 0,05% raw và giữ 365 ngày: `5 × 0.0005 × 365 = 0.91 TB`.
- Delta logs, checkpoints, temporary rewrite và safety headroom 25%:  
  `(17.5 + 22.5 + 0.91) × 1.25 = 51.14 TB-month`.
- Với planning rate object storage `$23/TB-month`:  
  `51.14 × $23 = $1,176/month`.
- Cross-region backup chỉ gồm Gold + catalog/audit 2 TB: `2 × $23 = $46/month`.

**Storage estimate: khoảng $1,222/month**, thấp hơn cap $5,000/tháng và còn khoảng $3,778 buffer cho tăng trưởng, request versioning và chênh lệch giá vùng. Egress, compute và observability platform được quản lý ở budget riêng; nếu cap $5,000 phải bao gồm tất cả, kiến trúc cần benchmark compute trước khi cam kết.

### Compute planning envelope

Ở 11.600 req/s trung bình, giả định một worker xử lý parse + DLP 1.000 req/s tại utilization 60%: cần khoảng 20 worker-equivalents gồm headroom. Với planning rate `$0.20/worker-hour`, ingest/Silver là `20 × 730 × $0.20 = $2,920/month`. Gold và compaction dùng autoscaling envelope `$800/month`. Tổng storage + compute ước tính là `$1,222 + $2,920 + $800 = $4,942/month`.

Con số này chỉ vừa cap, nên guardrail là budget alert ở 70/85/95%, autoscaling ceiling và cost-per-million-requests dashboard. Nếu benchmark thực tế vượt `$0.20/worker-hour`, lựa chọn đầu tiên là dùng DLP hai tầng (deterministic scan trên mọi row, model scan trên sample/risk traffic), không giảm retention hoặc bỏ mã hóa.

## 7. One-week MVP

MVP không cố xử lý 1 tỷ request ngay. Mục tiêu là chứng minh đường đi khó nhất — PII-safe, replayable và có chi phí đo được — ở 10 triệu synthetic requests/ngày:

1. Ngày 1: định nghĩa event contract, sinh retry/late events/PII canary và tạo stream local/managed test.
2. Ngày 2: ghi Bronze Delta theo minute batch, token hóa user ID, mã hóa payload và tạo DLQ.
3. Ngày 3: Silver parser + MERGE dedup + quality gate; test schema evolution có chủ đích.
4. Ngày 4: Gold 5-minute metrics, versioned pricing join và dashboard tenant/model.
5. Ngày 5: inject bad pricing deployment; chứng minh time-travel restore và replay; đo p95 freshness, throughput, file size và cost/million requests.

MVP đạt khi không có PII canary lọt vào Silver, duplicate còn 0 theo contract, dashboard freshness p95 < 5 phút, restore Gold < 10 phút và projection cho 1 tỷ request/ngày nằm trong budget envelope. Kết quả benchmark quyết định có cần scale test 100 triệu/ngày ở tuần tiếp theo hay thay đổi engine/layout.

## 8. Acceptance checklist

- [x] Problem statement có scale, SLA, retention và budget.
- [x] Một sơ đồ Bronze → Silver → Gold thể hiện ingestion và query paths.
- [x] Bảy quyết định chính; mỗi quyết định có ít nhất hai phương án bị loại.
- [x] Sáu failure modes với detection và rollback.
- [x] Áp dụng medallion, ACID, MERGE, time travel, catalog, lineage, security và FinOps.
- [x] Storage và compute math kiểm tra được.
- [x] MVP một tuần có tiêu chí pass/fail định lượng.
