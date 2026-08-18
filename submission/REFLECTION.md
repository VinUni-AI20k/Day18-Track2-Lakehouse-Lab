# Phân Tích Lakehouse Anti-Pattern & Giải Pháp Khắc Phục

**Học viên:** Phạm Quốc Thanh (2A202601407)  
**Chủ đề:** Anti-Pattern nguy cơ cao nhất & Chiến lược phòng ngừa  

---

### 1. Anti-Pattern nguy cơ cao nhất: Bỏ qua OPTIMIZE dẫn đến Small-Files Problem từ Streaming Ingestion

Trong hệ thống thu thập telemetry và trace của LLM/Agent, dữ liệu liên tục được ghi dạng micro-batch theo thời gian thực trực tiếp vào tầng Bronze (`llm_calls_raw`). Do mỗi batch ghi chỉ chứa vài trăm KB đến vài MB, bảng nhanh chóng tích tụ hàng chục nghìn file Parquet kích thước nhỏ (small-files problem). 

Hậu quả: Chi phí metadata scan tăng vọt, các câu truy vấn phân tích (ad-hoc & BI dashboard) bị nghẽn I/O nghiêm trọng và tỷ lệ file skipping suy giảm tới 10–50×.

### 2. Giải pháp khắc phục

1. **Auto-Compaction định kỳ:** Lập lịch job chạy `OPTIMIZE` (bin-packing) mỗi giờ gom các file nhỏ thành các khối chuẩn 128MB–512MB.
2. **Đa chiều Z-ORDER Clustering:** Chạy `Z-ORDER BY (tenant_id, timestamp)` hàng ngày trên tầng Silver/Gold nhằm kích hoạt cơ chế stats-based data skipping tối ưu cho các bộ lọc tenant.
3. **Dọn dẹp Orphan & Vacuum:** Thiết lập pipeline `VACUUM` hàng tuần kèm script quét orphan ngoài băng nhằm xóa triệt để file rác uncommitted sinh ra từ streaming task bị fail.
