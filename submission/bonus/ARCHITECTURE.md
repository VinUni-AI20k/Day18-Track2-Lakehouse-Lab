# Architecture Brief — LLM Observability Lakehouse tại 1B requests/ngày

**Topic:** A (LLM observability). **Tác giả:** Nguyễn Phú Cường

---

## 1. Problem Statement

Một team foundation-model API log mọi request/response: **1B req/ngày, ~5KB/req → 5TB/ngày raw**. Bốn ràng buộc cứng:

1. Dashboard cost & latency theo tenant, refresh mỗi **5 phút**
2. Prompt/response đầy đủ giữ **7 ngày** cho incident review, sau đó chỉ giữ aggregate **1 năm**
3. **PII phải được redact trước khi bất kỳ ai đọc** — không có ngoại lệ "đọc raw để debug"
4. Tổng chi phí storage **≤ $5,000/tháng**

Cái khó không phải là lưu 5TB/ngày — S3 làm việc đó dễ dàng. Cái khó là **ba ràng buộc đối kháng nhau**: retention ngắn (7 ngày) để tiết kiệm tiền, nhưng phải đủ dữ liệu để incident review; PII phải ẩn *trước* mọi lần đọc, nhưng dashboard vẫn cần đọc nhanh theo tenant; và ingestion liên tục ở throughput ~11,574 req/giây phải không tự huỷ hoại hệ thống bằng file nhỏ (bài học trực tiếp từ NB6).

---

## 2. Architecture Diagram

```
                    INGESTION PATH
┌──────────┐   ┌─────────────────┐   ┌──────────────────────────────┐
│  API     │──▶│ Kafka           │──▶│ Spark Structured Streaming    │
│  gateway │   │ (durable queue, │   │ trigger(processingTime=30s)   │
│ (1B/day) │   │  7-day buffer)  │   │  1. schema-enforce (NB1)      │
└──────────┘   └─────────────────┘   │  2. tokenize PII → vault join │
                                      │     key (redact BEFORE write)│
                                      └───────────────┬────────────────┘
                                                       │
                                                       ▼
        ┌──────────────────────────────────────────────────────────────┐
        │  BRONZE  s3://lake/bronze/llm_calls/date=YYYY-MM-DD/          │
        │  Delta, partition=date, Z-ORDER BY (tenant_id)                │
        │  retention: DELETE ts<now()-7d  (scheduled, Job 3, NB6)       │
        │  ~2,880 files/day @ 30s trigger → OPTIMIZE nightly (Job 1)    │
        └───────────────────────────┬────────────────────────────────────┘
                                     │  DuckDB/Spark: parse+dedup(request_id)
                                     ▼
        ┌──────────────────────────────────────────────────────────────┐
        │  SILVER  llm_calls_parsed — typed, deduped, PII-tokenized     │
        │  Delta, partition=date, Z-ORDER BY (tenant_id)                │
        │  Change Data Feed ON  ← propagates deletes (NB7 lesson)       │
        └───────────────────────────┬────────────────────────────────────┘
                                     │  streaming micro-batch agg,
                                     │  5-min tumbling window
                                     ▼
        ┌──────────────────────────────────────────────────────────────┐
        │  GOLD  tenant_5min_metrics (p50/p95 latency, cost, err_rate)  │
        │  Delta, NOT partitioned by tenant (low cardinality time dim)  │
        │  retained 1 year, S3 Standard (tiny: ~200GB/year)             │
        └───────────────────────────┬────────────────────────────────────┘
                                     │
                    QUERY PATH       ▼
        ┌──────────────────────────────────────────────────────────────┐
        │  Trino  (dashboard: SELECT ... WHERE tenant_id=? AND ts>?)    │
        │  Grafana polls Gold every 5 min — never touches Bronze/Silver │
        └──────────────────────────────────────────────────────────────┘

        PII VAULT (separate, ACL-restricted, catalog-enforced — §3.5)
        s3://lake/vault/pii_map/  token → {phone, email, raw_ip}
        Read path: incident-response role ONLY, via catalog row policy
```

---

## 3. Quyết định chính (6 quyết định, mỗi cái loại ≥2 phương án)

### 3.1 Table format: **Delta Lake**
- Loại **Iceberg**: giá trị chính của Iceberg là catalog liên engine (nhiều engine cùng đọc/ghi qua REST catalog, NB5). Ở đây chỉ có 1 stack ghi (Spark) + 1 engine đọc query (Trino) — không đủ áp lực đa engine để bù chi phí vận hành thêm 1 catalog server.
- Loại **Hudi**: Hudi MOR tối ưu cho **update tần suất cao trên từng row** (CDC từ OLTP). Workload của chúng ta là **append-heavy** (log request, gần như không update), nên lợi thế MOR của Hudi không áp dụng; hệ sinh thái Hudi quanh Trino cũng kém trưởng thành hơn Delta ở thời điểm này.
- **Lý do chọn Delta:** `enableChangeDataFeed` (NB7) là cơ chế đúng để lan truyền xoá PII xuống mọi bảng phái sinh mà không phải full-rescan — đây là yêu cầu cứng (#3), không phải "nice to have".

### 3.2 Ingestion trigger: **micro-batch 30 giây**, không phải 5 giây hay continuous
- Loại **trigger 5 giây**: đây **chính xác** là kịch bản NB6 mô tả ("Kafka→lakehouse job với 5-second trigger") và đã đo hậu quả thật: ở quy mô 1B req/ngày (~11,574 req/s), 5s trigger → **17,280 batch/ngày** → file trung bình siêu nhỏ, GET request bùng nổ (NB6 đo: 200 file nhỏ → $4/ngày GET cost cho tập dữ liệu bé hơn nhiều bậc).
- Loại **continuous/per-event streaming**: Delta không tối ưu cho write-per-event ở throughput này; latency SLA của chúng ta là 5 phút (dashboard refresh), không cần sub-giây.
- **Lý do chọn 30s:** → 2,880 batch/ngày, mỗi batch ~347K request ~1.7GB → nén Parquet đạt target 128-512MB/file production **mà không cần OPTIMIZE liên tục**, chỉ cần lịch nightly.

### 3.3 Partitioning Bronze: **`date` only + Z-ORDER(tenant_id)**, không partition theo tenant
- Loại **partition theo `tenant_id`**: hàng nghìn tenant × volume/tenant/ngày không đều → nhiều partition cực nhỏ (over-partitioning kinh điển) — đúng bẫy NB6 cảnh báo: "file nhỏ bị phạt kép".
- Loại **partition theo `date + hour`**: thêm 1 chiều phân mảnh cho giờ thấp điểm, không tương xứng lợi ích vì query "lọc theo tenant, 5 phút gần nhất" được phục vụ bởi Gold rollup, không phải Bronze thô.
- **Lý do chọn Z-ORDER(tenant_id) trong partition ngày:** đúng cơ chế NB2 đo được — min/max tenant_id per-file cho phép data-skipping khi dashboard lọc theo tenant, mà không tạo bùng nổ partition.

### 3.4 Retention: **DELETE tường minh theo lịch + VACUUM RETAIN 168h**, không dựa vào VACUUM mặc định
- Loại **"cứ để VACUUM tự dọn"**: NB6 đo trực tiếp — VACUUM chỉ dọn file đã bị **tombstone trong log**; nó không tự động hết hạn dữ liệu quá 7 ngày kinh doanh. Phải có job `DELETE WHERE ts < now()-7d` chủ động, VACUUM chỉ dọn *sau* đó.
- Loại **giữ raw vĩnh viễn "phòng khi cần"**: 5TB/ngày × 30 ngày = 150TB **chỉ riêng 1 tháng raw** ở giá Standard (~$0.023/GB) ≈ $3,450/tháng — gần hết ngân sách $5K chỉ để giữ 1 tháng, chưa nói 1 năm.
- **Lý do chọn lịch tường minh:** đây chính là Job 3 (expiry) NB6 dạy — expiry và VACUUM là một **cặp** phải chạy cùng nhau, không phải một job ngầm định.

### 3.5 Điểm redact PII: **tokenize tại Bronze (trước commit đầu tiên)**, không mask lúc đọc
- Loại **dynamic masking view lúc đọc** (mask ở tầng Trino/Spark view): Delta là **open format** — bất kỳ ai đọc thẳng file Parquet dưới `_delta_log/` (không qua view) sẽ thấy PII thô. NB1 dạy schema enforcement chặn ở **write time**, không phải access time — không thể trông cậy layer đọc để chặn PII.
- Loại **redact ở Silver** (giữ Bronze raw): vi phạm thẳng ràng buộc #3 ("PII redact **trước khi bất kỳ ai đọc**") — Bronze vẫn là điểm đọc hợp lệ cho debug/backfill.
- **Lý do chọn tokenize tại Bronze:** PII thật chỉ tồn tại trong 1 bảng `pii_vault` riêng, ACL hẹp; mọi bảng downstream chỉ có token — không có "cửa sau" nào đọc được PII thô.

### 3.6 Catalog: **1 catalog quản lý tập trung** (vd. Unity Catalog/Polaris) với ACL cấp hàng/cột cho `pii_vault`
- Loại **Hive Metastore thuần**: HMS chỉ tra path→schema, không enforce policy — mỗi engine (Spark, Trino) phải tự cài masking view riêng, quay lại rủi ro mục 3.5.
- Loại **không dùng catalog, truy cập path trực tiếp**: mất hoàn toàn khả năng audit "ai đọc `pii_vault` khi nào" — bắt buộc phải trả lời được câu này khi có incident.
- **Lý do chọn catalog tập trung:** đúng luận điểm NB5 — catalog 2026 là **control plane**, không phải name→path lookup; đây là nơi duy nhất enforce được ACL nhất quán qua nhiều engine.

---

## 4. Failure modes (3 kịch bản 3 giờ sáng)

**FM1 — SDK client đẩy schema hỏng (field đổi kiểu đột ngột).**
*Detection:* Bronze write dùng schema enforcement mặc định (NB1) — batch bị chặn ngay, alert on-call qua write-failure metric. Nếu vô tình bật `mergeSchema=true` tràn lan, lỗi này sẽ **lọt qua âm thầm** — vì vậy chính sách: chỉ cho phép `mergeSchema` qua allowlist cột được duyệt trước, không bao giờ mặc định bật.
*Rollback:* Kafka giữ 7 ngày buffer → replay batch lỗi sau khi vá producer. Nếu dữ liệu hỏng đã lọt vào trước khi phát hiện, `RESTORE` (NB3) partition-ngày bị ảnh hưởng về version tốt gần nhất, rồi replay lại từ Kafka.
*Liên hệ Day18:* schema enforcement (NB1) + RESTORE/time travel (NB3).

**FM2 — Job compaction (Job 1) trễ lịch 3 ngày do cluster outage.**
*Detection:* metric `numFiles` per-partition (chính là con số NB6 đo) vượt ngưỡng cảnh báo (vd. >500 file/partition-ngày) — với trigger 30s × 3 ngày lỡ lịch ≈ 8,640 file tích luỹ.
*Rollback:* chạy `OPTIMIZE` khẩn cấp **chỉ trên các partition bị ảnh hưởng** (không full-table) với resource cấp phát cao hơn tạm thời; NB6 chứng minh compaction là thao tác an toàn, idempotent (file cũ chỉ tombstone, không mất dữ liệu) — rủi ro duy nhất là chi phí/độ trễ tạm thời, không phải mất dữ liệu.
*Liên hệ Day18:* Job 1 compaction, đúng nguyên văn bài học NB6 "nguyên nhân phổ biến nhất không phải code sai, mà là thiếu cron job".

**FM3 — Yêu cầu xoá PII (right-to-erasure) trong cửa sổ 7 ngày time-travel còn hiệu lực.**
*Detection:* job kiểm tra định kỳ "N ngày sau yêu cầu xoá hợp pháp, `DESCRIBE HISTORY` còn version nào chứa `subject_id` đó không" — chính là hiện tượng NB8 đo được: DELETE xoá ở version hiện tại, nhưng version cũ qua time travel **vẫn còn** dữ liệu cho tới khi VACUUM hết hạn nó.
*Rollback/mitigation:* cửa sổ retention 7 ngày (đã quyết định ở §3.4) **chính là** cửa sổ phơi nhiễm time-travel được duyệt trước về mặt pháp lý — không phải mặc định tình cờ. VACUUM tự động chạy ở ngày thứ 8 đóng cửa sổ này lại.
*Liên hệ Day18:* mâu thuẫn time-travel vs quyền xoá — nguyên văn phát hiện NB8.

---

## 5. Ước lượng chi phí (back-of-envelope)

**Storage:**
- Bronze+Silver hot (7 ngày, PII đã token hoá): 7 × 5TB = 35TB raw JSON. Nén Parquet+Zstd điển hình ~3× → **≈11.7TB thực lưu**. S3 Standard $0.023/GB-tháng → 11,700GB × $0.023 ≈ **$269/tháng**.
- Gold (1 năm, rollup 5 phút/tenant): giả định 10,000 tenant × 288 bucket/ngày × 365 ngày × ~200B/row ≈ 210GB. S3 Standard → **≈$5/tháng**.
- `pii_vault` (7 ngày, nhỏ — chỉ token→PII map, không phải payload đầy đủ): ước lượng ~50GB → **≈$1/tháng**.
- **Tổng storage ≈ $275/tháng.**

**Compute (phần chiếm ngân sách chính, ~$4,700/tháng còn lại):**
- Spark Structured Streaming cluster chạy 24/7 (ingestion, 30s trigger): cụm vừa (vd. 6× worker cỡ trung) — ước lượng theo giá on-demand phổ biến ≈ **$2,800/tháng**.
- Maintenance jobs (OPTIMIZE nightly + DELETE/VACUUM lịch, chạy ngắn hạn theo giờ thấp điểm): **≈$400/tháng**.
- Trino cluster phục vụ dashboard (coordinator + worker, scale theo giờ cao điểm): **≈$1,200/tháng**.
- Buffer/monitoring/misc: **≈$300/tháng**.
- **Tổng compute ≈ $4,700/tháng.**

**Tổng: ≈$4,975/tháng — sát trần $5K, còn ~$25 dư.** Đây là lý do quyết định §3.2 (30s trigger, không phải 5s) và §3.4 (retention tường minh 7 ngày, không giữ raw vĩnh viễn) là **bắt buộc**, không phải tối ưu tuỳ chọn — nếu chọn sai 1 trong 2 quyết định đó, ngân sách vỡ ngay.

---

## 6. MVP tuần đầu tiên (slice nhỏ nhất chứng minh kiến trúc chạy được)

**Không build:** multi-tenant PII vault ACL đầy đủ, dashboard Trino production-grade, toàn bộ 1B req/ngày.

**Build:**
1. Bronze ingestion 1 tenant giả lập, quy mô 1/1000 (≈1M req/ngày thay vì 1B), trigger 30s, schema-enforced (NB1 pattern)
2. Silver: parse + dedup + PII tokenize đơn giản (1 cột `phone` → hash, không cần vault đầy đủ)
3. Gold: 1 bảng rollup 5-phút (p50/p95 latency, cost) — dùng chính công thức cost NB4 đã áp dụng
4. 1 job `OPTIMIZE` chạy theo lịch (cron/Airflow đơn giản), đo `numFiles` trước/sau (đúng phép đo NB6)
5. 1 panel Grafana/Metabase đọc Gold, refresh 5 phút

**Tiêu chí "xong":** chạy liên tục 48 giờ không cần can thiệp tay, `numFiles` per-partition không vượt ngưỡng cảnh báo, dashboard không bao giờ đọc trực tiếp Bronze/Silver. Slice này chứng minh đúng 3 rủi ro lớn nhất (small-file, PII leak qua đọc trực tiếp, retention không tường minh) đã được kiến trúc giải quyết **trước khi** đầu tư 20 team và 1B req/ngày thật.
