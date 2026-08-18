# Lakehouse Anti-Patterns — Real-World Reflections

## 1. Small-Files Problem (Streaming Ingestion)
**Vấn đề:** Stream data vào Delta table mà không gộp file → hàng nghìn small files. Query trở nên cực chậm vì Spark phải scan quá nhiều metadata.

**Giải pháp:** Chạy `OPTIMIZE` + `Z-ORDER BY` định kỳ. Dùng `autoOptimize` trong Spark write.

## 2. Bỏ qua Data Retention / Vacuum
**Vấn đề:** Không xóa old data hoặc không chạy `VACUUM` → disk bị phình, chi phí tăng.

**Giải pháp:** Set `delta.logRetentionDuration` và `delta.deletedFileRetentionDuration` phù hợp. Tự động hóa `VACUUM` với retention policy.

## 3. Không Dùng Schema Evolution
**Vấn đề:** Schema thay đổi (thêm column) nhưng table không support → ingestion fail hoặc data bị mất.

**Giải pháp:** Bật `mergeSchema: true` hoặc `schemaEvolutionMode: true` khi write.

## 4. Thiếu Partitioning / Z-Order
**Vấn đề:** Table không partition → full scan khi query theo ngày/tháng → query chậm, cost cao.

**Giải pháp:** Partition theo `date`/`month`. Dùng `Z-ORDER BY` cho các column thường xuyên filter.

## 5. Không Có Data Quality Check
**Vấn đề:** Data lỗi (null, duplicate, out-of-range) không được phát hiện → báo cáo sai, decision bias.

**Giải pháp:** Dùng `delta.constraints`, Great Expectations, hoặc DQ framework để validate trước khi write.

---

**Kết luận:** Team mình dễ mắc nhất là **Small-Files Problem** vì thường dùng Spark Structured Streaming mà quên chạy `OPTIMIZE`. Giải pháp: integrate `OPTIMIZE` vào pipeline CI/CD và monitor file count.
