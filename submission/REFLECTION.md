### Reflection: Anti-pattern phổ biến trong Data Lakehouse

Theo slide §5, anti-pattern mà nhóm mình dễ vướng phải nhất là **"The Small-File Problem" (Vấn đề quá nhiều file nhỏ)**.

**Vì sao?**
Trong các dự án thực tế, đặc biệt là khi làm việc với Streaming Data hoặc dữ liệu được ingest liên tục thành từng batch nhỏ (ví dụ: log từ web app, CDC từ database), việc ghi đè hoặc append trực tiếp vào Delta Lake thường xuyên sẽ tạo ra hàng nghìn file JSON và Parquet có dung lượng siêu nhỏ (vài KB).

**Hậu quả:**
Khi thực hiện các tác vụ query, engine (như DuckDB hay Spark) sẽ mất rất nhiều thời gian (overhead) chỉ để đọc siêu dữ liệu (metadata) thay vì đọc dữ liệu thực tế, dẫn đến hiệu năng truy vấn giảm sút nghiêm trọng.

**Giải pháp đã học:**
Để khắc phục, nhóm mình nhận thức được sự cần thiết của việc chạy lệnh `OPTIMIZE` định kỳ để gom các file nhỏ thành các file lớn (compaction), kết hợp với `Z-ORDER` trên các trường thường xuyên được filter (như `user_id` hoặc `date`) để tối ưu hóa việc Data Skipping (pruning files), từ đó cải thiện tốc độ đọc lên gấp nhiều lần.
