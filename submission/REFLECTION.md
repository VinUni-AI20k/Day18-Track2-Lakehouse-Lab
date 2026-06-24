# Reflection — Lab 18 Lakehouse

**Họ tên:** Đặng Thanh Tùng  
**Mã SV:** 2A202600023

---

## Anti-pattern dễ vướng nhất: Small-File Problem

Trong số các anti-pattern ở slide §5, **small-file explosion** là cái mà nhóm
tôi dễ mắc phải nhất khi xây pipeline LLM observability.

Lý do cụ thể: LLM API calls đến liên tục theo kiểu micro-batch — mỗi request
từ chatbot, mỗi inference job độc lập đều được ghi xuống Bronze ngay lập tức
để đảm bảo không mất sự kiện. Với throughput vài nghìn calls/phút, chỉ sau
một ngày Bronze layer đã tích lũy hàng trăm file nhỏ (< 1 MB mỗi file).
Hậu quả: mỗi query scan Gold/Silver phải mở hàng nghìn file, footer Parquet
bị đọc lặp, latency tăng gấp 5–10×, và Spark driver bị áp lực scheduling.

NB2 của lab đã chứng minh điều này một cách rõ ràng: 200 tiny appends → 200
files → query chậm hơn rõ rệt so với sau khi chạy `OPTIMIZE + Z-ORDER`.
File-skipping nhờ min/max stats chỉ phát huy tác dụng khi số file đủ lớn và
dữ liệu được sắp xếp theo cột truy vấn (`user_id`, `model`).

**Bài học:** lên lịch `OPTIMIZE` sau mỗi batch ingestion (hoặc định kỳ hàng
giờ) là bước **không thể bỏ qua** trong production Lakehouse.
