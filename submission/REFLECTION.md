# Reflection - Day 18 Lakehouse Lab

Trong các anti-pattern được thảo luận, team tôi dễ vướng nhất vào **"Small File Fragmentation" (Sự phân mảnh file nhỏ)**.

**Lý do:**
Khi xây dựng các pipeline ingest dữ liệu liên tục hoặc streaming (như demo LLM-observability), dữ liệu thường được ghi xuống storage theo từng đợt nhỏ. Nếu không có cơ chế `OPTIMIZE` và `Z-ORDER` định kỳ như đã thực hành trong NB2, hệ thống sẽ tích tụ hàng nghìn file Parquet kích thước cực nhỏ. Điều này gây áp lực lớn lên metadata layer và khiến tốc độ truy vấn giảm đáng kể do overhead của việc I/O quá nhiều file.

Việc áp dụng kiến trúc Lakehouse với Delta Lake giúp chúng tôi giải quyết vấn đề này một cách tự động thông qua cơ chế compaction mà vẫn đảm bảo tính nhất quán (ACID), giúp hệ thống tránh được tình trạng "Data Swamp" và duy trì hiệu suất cao khi quy mô dữ liệu tăng trưởng.
