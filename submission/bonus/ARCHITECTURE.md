# Bonus Challenge — Truy vết Feature Store (Topic G)

## 1) Tóm tắt vấn đề (<=200 từ)

Một ngân hàng vận hành 200 feature cho 10.000 model production. Pipeline feature do ~50 job Spark quản lý, backfill hằng ngày cho 90 ngày gần nhất. Risk team phải trả lời trong < 5 phút: "Nếu drop cột customer.address_country thì model nào hỏng và bao nhiêu tiền bị rủi ro?" Analyst cần feature mới trong vòng 60 giây sau source commit. Dữ liệu đến muộn thường xuyên, data contract yếu. Hệ thống cần lineage tới mức cột, phân tích tác động, rollback nhanh và audit truy cập PII. Ràng buộc: p95 offline query < 2 giây cho 7 ngày gần nhất, p95 online feature fetch < 20 ms, và tái dựng lineage đầy đủ trong 10 phút sau sự cố pipeline. Yêu cầu tái lập lịch sử 90 ngày và log truy cập PII.

## 2) Sơ đồ kiến trúc (một diagram)

```
Sources (OLTP, CDC, Files)
        |
        v
+---------------------+
| Bronze (raw, append)|  <-- PII tokenized at landing
+---------------------+
        |
        v
+---------------------+       +------------------+
| Silver (clean, SCD) |<----->| Data Contracts   |
+---------------------+       +------------------+
        |
        v
+---------------------+       +------------------+
| Gold (features)     |<----->| Feature Registry |
| Delta tables        |       | + SLA metadata   |
+---------------------+       +------------------+
        |                               |
        v                               v
+---------------------+       +------------------+
| Online Store (KV)   |       | Lineage (OpenLineage
| Redis/Scylla)       |       | + Marquez)        |
+---------------------+       +------------------+
        |
        v
Models / Serving
```

## 3) Bố cục medallion và luồng dữ liệu

- Bronze: raw CDC + events, append-only. PII được token hóa ngay khi landing, truy cập raw được log.
- Silver: validate schema, de-dup, xử lý late data bằng MERGE (src.ts > tgt.ts). SCD Type 2 cho thuộc tính user.
- Gold: bảng feature theo entity_id và feature_date, backfill theo incremental. Delta time travel giữ khả năng tái lập 90 ngày.
- Online store: KV latency thấp, key theo entity_id để phục vụ feature.
- Lineage: OpenLineage từ Spark, lưu ở Marquez; nối cột -> feature -> model.

## 4) Quyết định chính (kèm lựa chọn loại)

1) Table format
   - Chọn: Delta Lake cho ACID, backfill bằng MERGE, và time travel.
   - Loại: Iceberg (row-level ops tốt, nhưng team đã dùng Delta và cần CDC tooling gắn chặt với Spark/Delta).
   - Loại: Hudi (upsert tốt nhưng ecosystem nhỏ hơn với query stack hiện tại).

2) Catalog và governance
   - Chọn: Hive Metastore + Delta + OpenLineage/Marquez để lineage trung lập vendor.
   - Loại: Unity Catalog (rủi ro lock-in cho ngân hàng dùng nhiều engine).
   - Loại: Tự viết metadata DB (chi phí bảo trì cao, khó tích hợp chuẩn).

3) Partitioning + clustering
   - Chọn: partition theo feature_date, Z-ORDER theo entity_id cho point lookup.
   - Loại: partition theo entity_id (quá nhiều partition nhỏ, hot spot).
   - Loại: không partition (chi phí scan bùng nổ với backfill 90 ngày).

4) Online store
   - Chọn: Redis/Scylla làm KV store cho SLA < 20 ms.
   - Loại: truy vấn Delta trực tiếp cho online serving (latency cao, không đạt SLA).
   - Loại: cache trong app (stale, khó scale).

5) Thu thập lineage
   - Chọn: OpenLineage emitter trong Spark + Marquez cho lineage theo cột.
   - Loại: tài liệu thủ công (lỗi thời, không truy vấn được).
   - Loại: chỉ lineage mức job (không đủ cho tác động khi drop cột).

6) Chính sách schema evolution
   - Chọn: data contract chặt, thay đổi breaking phải có deprecation window.
   - Loại: evolve quá dễ dãi (gây break ngầm ở model).
   - Loại: rewrite toàn bộ mỗi lần đổi schema (quá tốn kém).

## 5) Failure modes (>=3) với phát hiện + rollback

1) Thay đổi schema làm gãy Silver (drop cột)
   - Phát hiện: contract validation fail + lineage chỉ ra feature phụ thuộc.
   - Rollback: Delta time travel về version tốt gần nhất; pin Gold theo version đó.

2) Backfill lỗi làm sai feature trong 2 ngày
   - Phát hiện: kiểm tra chất lượng (range + drift) và alert hiệu năng model.
   - Rollback: restore Gold về trước backfill; chạy lại backfill với logic đã sửa.

3) Lineage emitter bị gián đoạn
   - Phát hiện: thiếu OpenLineage events trong một cửa sổ job.
   - Rollback: replay lineage từ Spark event logs; tạm khóa thay đổi schema.

4) Late data tăng đột biến
   - Phát hiện: SLA lag metrics và watermark delay.
   - Rollback: MERGE với src.ts > tgt.ts để sửa; re-materialize feature bị ảnh hưởng.

## 6) Ước lượng chi phí (rough math)

Giả định:
- 100M entity, 200 feature, trung bình 16 bytes/feature -> 3.2 KB mỗi entity/ngày.
- Kích thước Gold/ngày: 100M * 3.2 KB = 320 GB/ngày.
- Lịch sử 90 ngày: 28.8 TB.

Storage (S3 Standard $23/TB-tháng):
- 28.8 TB * $23 = $662/tháng.

Compute (Spark + streaming):
- 3 cluster cỡ vừa, $1.20/giờ, chạy 24x7:
  3 * 1.20 * 24 * 30 = $2,592/tháng.

Tổng chi phí ước tính: ~$3,300/tháng (storage + compute lõi), chưa tính online store.

## 7) MVP trong 1 tuần

Ngày 1-2: dựng pipeline Bronze->Silver với contract + tokenization.
Ngày 3: tạo Gold cho subset nhỏ (10 feature, 1 job).
Ngày 4: emit OpenLineage và lưu vào Marquez.
Ngày 5: triển khai truy vấn tác động: cột -> feature -> model.
Ngày 6-7: thêm rollback bằng time travel + cảnh báo cơ bản.

## 8) Concept Day 18 đã áp dụng

- Medallion (Bronze/Silver/Gold), ACID Delta tables, MERGE cho late data.
- Time travel cho rollback và tái lập.
- Lineage + governance cho tác động thay đổi và audit PII.
- FinOps: compaction cadence + partitioning để kiểm soát chi phí scan.
