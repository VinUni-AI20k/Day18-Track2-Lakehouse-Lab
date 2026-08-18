# Architecture Brief — LLM Observability ở Quy Mô 1B Requests/Ngày

**Nguyễn Hùng Mạnh — AICB-P2T2 · Day 18 Bonus Challenge (Topic A)**

---

## 1. Bài Toán & Constraints Cứng

Một foundation-model API team cần log toàn bộ request/response:

| Metric | Giá trị |
|--------|---------|
| Throughput | 1B req/ngày ≈ 11.600 req/s peak |
| Raw volume | ~5 KB/req → **~5 TB/ngày** |
| Dashboard refresh | mỗi 5 phút, latency p95 < 2s |
| Audit retention | Prompt/response đầy đủ: **7 ngày** → aggregate only: **1 năm** |
| PII policy | Phải redact *trước* khi bất kỳ analyst nào đọc |
| Budget cap | **≤ $5.000/tháng** tổng storage |

---

## 2. Phương Án Đã Loại & Lý Do

### ❌ Loại: Event log thẳng vào Data Warehouse (BigQuery/Snowflake)

*Lý do loại:* 5 TB/ngày × $5/TB = $25.000/tháng chỉ riêng storage query scan — vượt budget 5× trước khi tính compute. Không có file-level pruning theo tenant → every query là full scan.

### ❌ Loại: Lưu raw blob trong S3 không có cấu trúc

*Lý do loại:* Không có time-travel (không rollback khi có incident), không schema enforcement (trường mới từ model v2 phá pipeline), không partition pruning → analytics không thể scale.

### ❌ Loại: Dùng một bảng đơn "unified" (không Medallion)

*Lý do loại:* PII trong raw log; nếu analyst đọc Bronze trực tiếp là vi phạm policy ngay cả khi chưa có breach. Không thể expire raw sau 7 ngày mà không làm hỏng aggregate của analyst.

---

## 3. Kiến Trúc Đề Xuất: Delta Lake Medallion + Lifecycle Tiering

```
[Kafka / Kinesis]
        │ (11.600 msg/s)
        ▼
┌─────────────────────────────────────────────────┐
│  BRONZE — Raw Landing (Delta + Structured Stream) │
│  • PII redact tại đây (tokenize phone/email/IP)  │
│  • Partition: date=YYYY-MM-DD / tenant_id        │
│  • Retention: 7 ngày → VACUUM + S3 Lifecycle     │
│  • Z-ORDER by tenant_id (hot path: filter tenant) │
└─────────────────────────────────────────────────┘
        │ (MERGE / micro-batch, lag < 5 phút)
        ▼
┌─────────────────────────────────────────────────┐
│  SILVER — Parsed & Validated                     │
│  • Dedup by request_id (CDF-driven MERGE)        │
│  • Schema enforcement: model, tokens, latency_ms │
│  • Partition: date + model                       │
│  • Retention: 90 ngày (analyst hot path)         │
└─────────────────────────────────────────────────┘
        │ (batch aggregate, 5-phút trigger)
        ▼
┌─────────────────────────────────────────────────┐
│  GOLD — Metrics Dashboard (DuckDB / Trino query) │
│  • p50/p95 latency, error_rate, cost_usd         │
│  • Partition: date + tenant_id + model           │
│  • Retention: 1 năm                              │
│  • Z-ORDER by tenant_id: dashboard query < 2s    │
└─────────────────────────────────────────────────┘
```

---

## 4. FinOps: Giữ Ngân Sách ≤ $5.000/Tháng

| Layer | Size/tháng | Tier S3 | Cost ước tính |
|-------|-----------|---------|---------------|
| Bronze raw (7 ngày) | ~35 TB active + 115 TB IA | S3 Standard 7d → IA → Glacier 30d | ~$1.200/tháng |
| Silver (90 ngày) | ~20 TB (sau dedup + compression) | S3 Standard-IA | ~$920/tháng |
| Gold aggregate (1 năm) | ~2 TB (agg rất nhỏ) | S3 Standard | ~$46/tháng |
| Compute (streaming + batch) | — | Spark Serverless / Flink | ~$1.500/tháng |
| **Total** | | | **~$3.666/tháng ✅** |

**Lever quan trọng:** Bronze raw chỉ 7 ngày thật sự → automate với `VACUUM RETAIN 168` + S3 Lifecycle Rule `ExpirationInDays=8`. Không tắt Delta time-travel (giữ 7 ngày), chỉ expire file thật.

---

## 5. PII Tokenization Tại Bronze (Trước Mọi Read)

**Vấn đề:** Analyst ở Silver/Gold không được thấy IP, số điện thoại, email.

**Giải pháp:** Streaming job viết vào Bronze chạy qua tokenizer trước khi commit:
- `ip → sha256(ip + daily_salt)[:16]` (deterministic trong ngày, reversible chỉ bởi security team)
- `phone/email → format-preserving encryption (FPE)` với AWS KMS
- Raw PII **không bao giờ landing** vào S3; chỉ token vào Delta table

Audit trail: mọi lần mở token (giải mã) đều log vào bảng audit riêng (append-only, không ai xóa được).

---

## 6. Partition & Z-ORDER Strategy

**Hot query pattern của team dashboard:** `WHERE date = today AND tenant_id = 'acme'`

- **Partition by `date`:** loại ngay ~95% data (chỉ đọc 1 ngày)
- **Z-ORDER by `tenant_id`:** trong 1 ngày, mỗi tenant co cụm vào ~1-3 file
- **Kết quả đo được (NB2 pattern):** từ scan toàn bộ 200 file → chỉ đọc 1-2 file = speedup > 10× hoặc pruning ratio > 50×

---

## 7. Phương Án Đã Xem Xét Nhưng Không Chọn (Trade-offs Rõ Ràng)

**Apache Iceberg thay Delta:** Iceberg có hidden-partition tốt hơn và field-ID-safe rename. Tuy nhiên delta-rs (Rust) có throughput write cao hơn đáng kể với small-batch streaming (đo trong NB2); delta-spark đã proven ở 1M rows trong lab này. Với 1B req/ngày, write throughput là bottleneck số 1.

**Real-time serving từ Bronze (không Medallion):** Muốn dashboard 5 phút → cần pre-aggregate. Đọc 5TB raw mỗi 5 phút để tính p95 là không khả thi về latency lẫn cost.

**Vector database cho prompt search:** Có giá trị nhưng là opt-in feature phase 2. Phase 1 cần ổn định pipeline cơ bản trước. Lifecycle synchronization giữa Delta và vector index là rủi ro (NB7 lifecycle bug đã chứng minh).

---

## 8. Kết Luận

Kiến trúc này bảo vệ được trong design review vì:
1. **Budget dưới $5.000/tháng** với margin 25% nhờ aggressive tiering.
2. **PII không bao giờ ở trong S3 raw** — tokenize tại wire, không tại read time.
3. **Dashboard < 2s** nhờ Z-ORDER + Gold pre-aggregate, không cần cache layer riêng.
4. **7-ngày rollback** bất kỳ lúc nào qua Delta time-travel — critical cho incident response.
5. **Thêm model mới:** chỉ cần thêm row vào cost table → không cần schema migration.

*Thiết kế dựa trực tiếp trên kết quả đo từ Lab 18 (NB2, NB4, NB6, NB7, NB8).*
