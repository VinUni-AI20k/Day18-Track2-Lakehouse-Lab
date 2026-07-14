# Reflection - Day 18 Lakehouse Lab

## Anti-pattern Reflection (Slide §5)

Trong các nội dung đã thực hành, tôi nhận thấy team tôi dễ vướng phải anti-pattern **"The Undo-less Data Lake" (Data Lake không có nút hoàn tác)** nhất.

### Tại sao?
Trước khi áp dụng Lakehouse (qua bài NB3), việc xử lý các mẻ dữ liệu lỗi (như version v3 với `score < 0`) thường cực kỳ tốn kém:
1. **Khó phát hiện:** Dữ liệu lỗi trộn lẫn với dữ liệu cũ, không có Audit Trail (History) để biết lỗi từ đâu.
2. **Khó khắc phục:** Phải viết script xóa thủ công hoặc khôi phục từ bản backup vật lý (rất chậm).

### Giải pháp từ NB3:
Bài Lab NB3 đã chỉ ra cách Lakehouse giải quyết triệt để vấn đề này:
- **Audit Trail:** Lệnh `DESCRIBE HISTORY` cho phép tôi nhìn rõ ai đã làm gì, vào lúc nào.
- **Nút hoàn tác:** Lệnh `RESTORE` cho phép quay ngược thời gian về phiên bản v2 sạch sẽ chỉ trong chưa đầy 30 giây, loại bỏ hoàn toàn các dòng dữ liệu "độc hại" mà không cần can thiệp thủ công phức tạp.
- **Efficient Updates:** Thay vì ghi đè toàn bộ bảng (Overwrite), lệnh `MERGE` giúp thực hiện Upsert 100K dòng một cách thông minh và có tính ACID cao.

---
*Môi trường thực hiện: Spark/Docker Path*
