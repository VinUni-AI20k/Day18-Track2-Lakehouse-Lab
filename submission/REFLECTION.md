# Reflection: Lakehouse Anti-patterns

Dựa trên kiến thức bài học, dữ liệu của nhóm chúng tôi dễ gặp rủi ro nhất với anti-pattern: **The Small File Problem (Vấn đề tệp tin rác/nhỏ)**.

Trong thực tế, hệ thống LLM Observability của nhóm liên tục ghi nhận các request API thời gian thực (như bài `04_medallion`). Nếu liên tục ghi (append) luồng dữ liệu này vào lớp Bronze mà không có cơ chế gom cụm, hệ thống sẽ bị phân mảnh nghiêm trọng. Minh chứng là ở file `tree_lakehouse.txt`, chỉ một thao tác giả lập `02_optimize` đã sinh ra hơn 160 file `.parquet` siêu nhỏ trong `scratch/events_smallfiles` cùng hàng tá file metadata `.json`.

Khi áp dụng vào quy mô thực, engine truy vấn (DuckDB/Spark) sẽ bị "nghẽn" vì tốn quá nhiều I/O để mở hàng ngàn file vật lý thay vì thực sự đọc dữ liệu. Hậu quả là các dashboard tính toán chi phí và độ trễ p50/p95 ở lớp Gold sẽ tải vô cùng chậm.

Để giải quyết, nhóm phải thiết lập các pipeline bảo trì tự động: định kỳ chạy lệnh `dt.optimize.compact()` để gom file và `dt.optimize.z_order(["model"])` (như đã làm ở bài 4) nhằm tối ưu hiệu năng đọc, đảm bảo hệ thống Lakehouse hoạt động bền vững.
