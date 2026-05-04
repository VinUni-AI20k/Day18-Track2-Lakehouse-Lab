# Reflection

**Anti-pattern dễ vướng nhất:** The "Small File Problem" (Vấn đề file nhỏ).

**Vì sao?**
Trong thực tế, khi xây dựng pipeline dữ liệu (như CDC hay streaming) với tần suất cập nhật cao (ví dụ: liên tục ghi log, LLM calls), team rất dễ tạo ra hàng ngàn file Parquet nhỏ lẻ trong Delta Lake nếu chỉ áp dụng thao tác append mà quên bảo trì định kỳ. Đây là một anti-pattern cực kỳ phổ biến.

Hậu quả là Engine sẽ tốn quá nhiều thời gian cho việc I/O, quét metadata và mở file thay vì thực thi tính toán query thực sự (được minh chứng rõ trong bài lab `02_optimize_zorder`), làm giảm hiệu suất đáng kể. Dù thiết kế kiến trúc Medallion (Bronze/Silver/Gold) có chuẩn xác, nhưng nếu các layer bị phân mảnh với hàng triệu file nhỏ, toàn bộ pipeline phân tích và AI vẫn sẽ bị nghẽn (bottleneck).

**Giải pháp đã học được:**
Để phòng tránh, chúng ta cần thiết lập các job bảo trì chạy `OPTIMIZE` kết hợp `Z-ORDER` định kỳ (ví dụ: hàng đêm), cũng như dọn dẹp bằng `VACUUM` để gộp các file nhỏ lại, giúp tăng tốc độ truy vấn lên nhiều lần.
