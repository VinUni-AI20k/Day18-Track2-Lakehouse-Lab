# Lab 18 Reflection

**Họ và tên:** Nguyễn Anh Đức  
**Mã sinh viên:** 2A202600387  

---

**Câu hỏi:** Anti-pattern nào trong slide §5 dễ vướng nhất, vì sao?

**Trả lời:**
Theo em, anti-pattern dễ mắc phải nhất trong thực tế là **"Small-File Problem" (Vấn đề tạo ra quá nhiều file nhỏ)**. 

Lý do là vì trong các dự án thực tế, dữ liệu thường được đẩy vào hệ thống liên tục theo dạng streaming (ví dụ: log hệ thống, event từ ứng dụng) với các batch rất nhỏ. Nếu đội ngũ kỹ sư chỉ tập trung vào việc ghi dữ liệu (write) mà quên không thiết lập các job định kỳ để gộp file (sử dụng lệnh `OPTIMIZE` / `COMPACT`), Lakehouse sẽ nhanh chóng bị phình to với hàng vạn file có kích thước chỉ vài KB. 

Hậu quả là khi engine truy vấn dữ liệu, nó sẽ phải tốn quá nhiều thời gian để duyệt qua metadata của từng file thay vì đọc dữ liệu thực sự, làm hiệu năng truy vấn giảm sút nghiêm trọng. Bài học rút ra là luôn phải có chiến lược dọn dẹp và gom nhóm file (kết hợp Z-order) đi kèm với luồng đẩy dữ liệu.
