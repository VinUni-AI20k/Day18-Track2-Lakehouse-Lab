# Reflection: Lakehouse Anti-Patterns in Production

**Học viên:** Bùi Duy Hải  
**MSSV:** 2A202601878  

---

Trong 5 Lakehouse Anti-Patterns, team chúng tôi dễ vướng phải **Small File Problem (Micro-batching / Streaming không có Compaction định kỳ)** nhất.

### Lý do và Rủi ro thực tế:
1. **Tần suất ghi cao:** Các pipeline thu thập log/event và LLM traces thường ghi theo micro-batch thời gian thực. Nếu không cấu hình auto-compaction, số lượng file Parquet nhỏ tăng nhanh chóng lên hàng chục nghìn file.
2. **Suy giảm hiệu năng nghiêm trọng:** Như đã đo lường trong NB2 và NB6, việc đọc hàng trăm file nhỏ làm tăng overhead của metadata và driver, khiến thời gian truy vấn chậm hơn từ 3x đến 10x và tăng vọt chi phí API calls trên Cloud Storage (S3/GCS).

### Giải pháp áp dụng sau bài Lab:
* Thiết lập **Job 1 (Compaction)** và **Job 2 (Z-ORDER clustering)** định kỳ (hoặc kích hoạt `auto-compact`/`optimizeWrite`).
* Kết hợp lịch trình chạy **Snapshot Expiry** và **Orphan Cleanup (Vacuum)** để dọn sạch dữ liệu rác, đảm bảo bảng luôn đạt hiệu năng tối ưu.
