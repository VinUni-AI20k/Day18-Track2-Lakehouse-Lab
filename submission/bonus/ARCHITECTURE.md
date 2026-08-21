# Bonus Architecture Brief — Topic C: CDC từ ride-hailing Việt Nam → Lakehouse (Nghị định 13/2023/NĐ-CP)

## 1. Problem statement

Một app gọi xe Việt Nam ghi 100 triệu chuyến/năm (~274K chuyến/ngày, đỉnh
30K writes/giây) từ Oracle production DB. Mỗi chuyến sinh ~4 bản ghi CDC
(tạo → gán tài xế → hoàn thành → đánh giá). Dữ liệu chứa PII thuộc phạm vi
Nghị định 13/2023/NĐ-CP: SĐT, số CMND/CCCD, tọa độ GPS của cả tài xế lẫn
hành khách. Yêu cầu: dashboard analyst refresh ≤ 60s kể từ commit nguồn,
ad-hoc query p95 < 1s, và mạng ở tỉnh xa khiến sự kiện đến trễ (đôi khi
hàng giờ) là chuyện thường xuyên chứ không phải ngoại lệ. Cái khó không nằm
ở khối lượng (100M chuyến/năm là vừa phải) mà ở ba ràng buộc va chạm nhau:
độ tươi 60 giây, tính đúng đắn khi dữ liệu đến trễ, và nghĩa vụ pháp lý phải
hạn chế + kiểm soát truy cập PII — trong khi vẫn phải cho phép audit truy vết
"ai đã đọc SĐT của khách hàng X vào lúc nào."

## 2. Architecture diagram

```
 Oracle (production)                                    Analyst / Product
 ┌──────────────┐                                       ┌────────────────┐
 │ trips, riders │                                       │ BI dashboards  │
 │ drivers, fares│                                       │ (≤60s fresh)   │
 └──────┬───────┘                                        └────────▲───────┘
        │ LogMiner/redo                                            │ SQL (Trino/DuckDB)
        ▼                                                          │
 ┌──────────────┐      ┌───────────────────────────────┐   ┌───────┴────────┐
 │  Debezium    │─────▶│   Kafka (per-table topics)      │  │  GOLD (Delta)  │
 │  connector   │      │   compacted, 7-day retention     │  │  z-order(city, │
 └──────────────┘      └───────────────┬───────────────┘  │  driver_id)    │
                                        │ Spark Structured  └───────▲────────┘
                                        │ Streaming (foreachBatch)   │ nightly agg
                                        ▼                            │ + OPTIMIZE
                          ┌─────────────────────────┐        ┌──────┴────────┐
                          │  BRONZE (Delta)          │        │ SILVER (Delta)│
                          │  tokenize PII @ landing  │───────▶│ SCD2, deduped │
                          │  raw before/after CDC    │  MERGE │ dedup by      │
                          │  partition: ingest_date  │  WHEN  │ trip_id,      │
                          │  append-only, CDF=on     │  src.ts│ partition:    │
                          └─────────────────────────┘  >tgt.ts│ event_date    │
                                        │                     └───────────────┘
                                        │ late (>2h) events
                                        ▼
                          ┌─────────────────────────┐
                          │ DEAD-LETTER (Delta)      │──nightly reconcile──▶ Silver
                          │ side table, watermark-   │  MERGE (ignores watermark)
                          │ missed corrections       │
                          └─────────────────────────┘

 ┌───────────────────────────────────────────────────────────────────────┐
 │  Vault table (KMS-backed): token ↔ raw PII, access-controlled          │
 │  Audit log table: (query_id, user, columns_touched, ts) — column-level│
 │  Catalog: AWS Glue (Bronze/Silver/Gold registration + Trino/Athena)    │
 └───────────────────────────────────────────────────────────────────────┘
```

## 3. Quyết định chính (6 quyết định, kèm alternatives đã loại)

1. **Table format: Delta Lake.** Loại **Apache Hudi** vì MOR tables của Hudi
   rất hợp cho upsert-nặng như CDC nhưng đòi một compaction/timeline service
   riêng — team không có kinh nghiệm vận hành đó, và ở quy mô 30K/s ta không
   cần MOR mới đạt SLA. Loại **Iceberg** vì tại thời điểm thiết kế, cơ chế
   tương đương Change Data Feed (đọc incremental qua metadata/position
   deletes) chưa được test rộng cho pattern *streaming consumer đọc mỗi
   batch trong 60s*; Delta's CDF là con đường đã production-hardened cho
   đúng use-case "downstream Silver MERGE mỗi micro-batch."
2. **Ingestion path: Spark Structured Streaming `foreachBatch` + `MERGE`**,
   không dùng **Kafka Connect Delta Sink connector** hay **JDBC polling**.
   Loại Kafka Connect sink vì nó không cho chèn logic tokenization + SCD2 +
   late-arrival condition vào giữa đường — nó ghi thẳng, còn ta cần biến đổi
   trước khi commit. Loại JDBC polling vì polling định kỳ (dù 30s/lần) vẫn
   thêm tải đọc trực tiếp lên Oracle production và không đảm bảo thứ tự
   redo-log chuẩn Debezium cung cấp.
3. **Partitioning: `event_date` (thời điểm chuyến diễn ra) làm partition
   chính, `city` làm Z-order phụ.** Loại "partition theo `ingest_date`"
   (thời điểm nhận CDC) vì late-arriving events sẽ luôn văng đúng vào
   partition hôm nay dù chuyến xảy ra hôm qua — phá vỡ khả năng time-travel
   theo ngày thực tế và làm compaction/lifecycle theo tuổi dữ liệu sai lệch.
   Loại "partition theo `city` là chính" vì với >30 thành phố × 365 ngày sẽ
   tạo hàng chục nghìn partition nhỏ — small-file problem y hệt NB2, chỉ là
   do partition key sai thay vì do streaming ingestion.
4. **PII: tokenize/pseudonymize ngay tại Bronze landing** (deterministic
   token cho SĐT qua HMAC + rotating key trong KMS, số CMND lưu tách trong
   vault table quyền hạn chế). Loại "chỉ mask qua view ở tầng phục vụ" vì
   Silver/Gold khi đó vẫn chứa PII thô — một token truy cập bị lộ ở Silver là
   lộ toàn bộ, vi phạm nguyên tắc *minimization* của Nghị định 13. Loại "chỉ
   mã hóa at-rest (SSE-KMS)" vì mã hóa at-rest không ngăn một analyst có
   quyền đọc bảng thấy SĐT thô qua SQL — Nghị định 13 đòi kiểm soát *mục
   đích sử dụng*, không chỉ chống rò rỉ đĩa cứng.
5. **Catalog: AWS Glue Data Catalog.** Loại **Unity Catalog** vì nó khóa
   platform vào Databricks trong khi 20 team hiện đang dùng cả Trino lẫn
   DuckDB nội bộ — chi phí migrate + license không tương xứng lợi ích ở quy
   mô một domain (ride-hailing), chưa cần catalog đa nền tảng cấp doanh
   nghiệp. Loại **tự host Hive Metastore/Nessie** vì thêm một service phải
   vá bảo mật, backup, HA — trong khi Glue là managed, rẻ, và đã có sẵn
   IAM-based access control tích hợp cho các bảng chứa PII.
6. **Late-data & audit: watermark 2h cho streaming path + dead-letter table
   reconciled bằng nightly `MERGE ... WHEN MATCHED AND src.updated_at >
   tgt.updated_at`, cộng một audit-log table riêng ghi lại cột nào bị đọc bởi
   ai.** Loại "drop sự kiện trễ quá watermark" vì tài xế vùng sâu có mạng
   kém sẽ hệ thống hóa mất chính xác dữ liệu của họ — vừa sai vừa bất công.
   Loại "chỉ dựa vào IAM/CloudTrail log" cho audit vì log đó chỉ thấy cấp
   bucket/object, không thấy "ai đã SELECT cột `phone_number` của dòng nào" —
   không đáp ứng nghĩa vụ accountability theo Nghị định 13.

## 4. Failure modes (4, ≥1 gắn với concept Day 18)

1. **3h sáng — Debezium tụt hậu do Oracle xoay redo log nhanh hơn tốc độ
   đọc (LogMiner overflow).** Phát hiện: alert khi Kafka consumer lag > 5
   phút. Rollback: restart connector từ archived redo log; nhờ key MERGE là
   `(trip_id, updated_at)` idempotent nên replay không tạo trùng lặp.
2. **Schema drift không báo trước** — production thêm cột `promo_code` trên
   Oracle mà platform team không biết → job Spark crash (schema strict) hoặc
   âm thầm mất cột (nếu tắt `mergeSchema`). *Gắn trực tiếp với NB1 (Schema
   Enforcement/Evolution)*: batch đó được cách ly vào bảng
   `schema_pending`, sau khi bật `mergeSchema=true` và review cột mới, chạy
   lại batch đó với `schema_mode="merge"` — đúng cơ chế NB1 minh họa.
3. **Rotate khóa tokenization mà không re-encrypt dữ liệu Bronze cũ** → join
   giữa hai partition được ghi ở hai "epoch" khóa khác nhau trả về 0 kết quả
   — lỗi âm thầm, không crash. Phát hiện: job hàng ngày so match-rate giữa
   hai partition liền kề, alert nếu tụt đột ngột. *Gắn với NB3 (Time
   Travel)*: dùng `dt.history()`/time-travel đọc lại version Bronze trước
   rotation, re-tokenize dưới khóa cũ + mới vào một bảng reconciliation, rồi
   forward-fix về một token version thống nhất.
4. **Sự kiện trễ vượt watermark 2h** (tài xế mất sóng 6 tiếng ở vùng sâu) →
   bản cập nhật bị Structured Streaming âm thầm bỏ qua, Silver giữ dữ liệu
   sai vĩnh viễn nếu không có dead-letter. Phát hiện: job reconcile đêm so
   count nguồn Oracle vs Silver theo `trip_id`. Rollback: sự kiện trễ được
   route vào dead-letter table, MERGE đêm áp dụng lại bỏ qua watermark.

## 5. Ước lượng chi phí (back-of-envelope)

- CDC events/năm: 100M chuyến × 4 bản ghi = **400M events/năm**.
- Kích thước 1 event Debezium (JSON, before+after) ≈ 1 KB →
  **400 GB/năm raw** trước nén.
- Nén Parquet + ZSTD (lặp schema cao, tỷ lệ nén thực tế ~6×) →
  **≈ 67 GB/năm** cho Bronze compacted; Silver (SCD2, current+history) cỡ
  tương đương ≈ 80 GB/năm; Gold (aggregate) không đáng kể (< 5 GB tổng).
- Giữ 3 năm theo luật lưu trữ chứng từ kinh doanh + nguyên tắc minimization
  của Nghị định 13 (xóa/aggregat hóa sau đó) →
  **Bronze+Silver cộng dồn 3 năm ≈ (67+80) × 3 ≈ 440 GB**.
- **Storage cost**: 90 ngày gần nhất "hot" (S3 Standard, ~110 GB)
  × \$0.023/GB-tháng ≈ **\$2.5/tháng**; phần còn lại (~330 GB) chuyển IA/Glacier
  sau 90 ngày, ~\$0.01/GB-tháng trung bình ≈ **\$3.3/tháng**.
  → Storage: **~\$6/tháng** — không đáng kể.
- **Compute cost (chiếm phần lớn)**: 1 cluster Spark Structured Streaming
  chạy 24/7 để bắt kịp đỉnh 30K/s (3× r5.xlarge, EMR) ≈
  \$0.252/giờ × 3 × 730 giờ ≈ \$552, cộng ~20% phụ phí EMR ≈ **\$660/tháng**.
  Job compact/OPTIMIZE + reconcile đêm (1× r5.2xlarge, 2 giờ/đêm) ≈
  \$0.504 × 2 × 30 ≈ **\$30/tháng**.
- **Tổng ước tính: ≈ \$700/tháng**, trong đó **>95% là compute** (cluster
  streaming chạy liên tục), không phải storage — ngược với trực giác thông
  thường "lakehouse thì tốn tiền lưu trữ." Đây là điểm cần bảo vệ rõ trong
  design review: cắt chi phí ở đây nghĩa là scale-down cluster ngoài giờ cao
  điểm, không phải nén dữ liệu thêm.

## 6. MVP một tuần

Chỉ ingest **một bảng `trips`** (bỏ qua `riders`/`drivers`/`fares`): Debezium
→ Kafka → một job Spark Structured Streaming duy nhất thực hiện tokenization
SĐT + `MERGE` có điều kiện late-arrival vào Bronze/Silver Delta. Bỏ qua Gold
aggregates và bỏ qua audit-log table đầy đủ (thay bằng log thủ công qua view
wrapper + grep query log) — mục tiêu duy nhất của tuần đầu là **chứng minh
cơ chế tokenize + MERGE late-arrival chạy đúng đầu-cuối** trước khi mở rộng
sang các bảng còn lại và xây audit/lineage đầy đủ.

## PoC

`submission/bonus/poc/tokenize_and_merge.py` — demo cơ chế khó nhất của
thiết kế: tokenization xác định (deterministic) cho SĐT tại Bronze landing,
và `MERGE ... WHEN MATCHED AND src.updated_at > tgt.updated_at` xử lý đúng
một sự kiện CDC đến trễ mà không ghi đè bản cập nhật mới hơn đã có.
