# Reflection

**Anti-pattern dễ vướng nhất:** Small File Problem (hoặc Over-partitioning).

**Vì sao:** Trong quá trình streaming data hoặc ingest data liên tục từ nhiều nguồn với batch size nhỏ (như log events, LLM calls), nếu không chủ động thực hiện `OPTIMIZE` và `Z-ORDER` thường xuyên, Delta Lake sẽ sinh ra hàng ngàn file Parquet nhỏ. Điều này làm tăng chi phí I/O overhead khi đọc metadata và quét file, khiến performance của các query phân tích giảm sút nghiêm trọng. Với team đang xây dựng pipeline dữ liệu thời gian thực hoặc micro-batch, đây là rủi ro rất phổ biến nếu quên đặt lịch auto-optimize hoặc maintenance routine định kỳ. Việc áp dụng đúng Medallion Architecture kết hợp với compaction sẽ giúp tránh được cái bẫy này, đảm bảo Lakehouse luôn giữ được tốc độ truy vấn tối ưu.
