# Reflection - Lab 18 Lakehouse

**Question:** Anti-pattern nào trong slide §5 team bạn dễ vướng nhất, vì sao? (Max 200 words)

---

Team mình dễ vướng vào anti-pattern **"The Small File Problem"** (Vấn đề nhiều file nhỏ) nhất. 

**Lý do:**
Trong các hệ thống Lakehouse hiện đại, dữ liệu thường được đổ về liên tục (streaming hoặc micro-batch). Nếu không có cơ chế tự động chạy `OPTIMIZE` và `VACUUM` thường xuyên, hệ thống sẽ tích tụ hàng nghìn file Parquet nhỏ. Điều này làm tăng đáng kể metadata overhead và khiến tốc độ truy vấn (query performance) giảm sụt nghiêm trọng do Spark/DuckDB phải mở và đóng quá nhiều file thay vì đọc các block lớn. Qua bài Lab, mình nhận thấy việc duy trì Medallion Architecture cùng với việc bảo trì định kỳ (compaction) là yếu tố sống còn để giữ cho Data Lake không trở thành "Junk Drawer".
