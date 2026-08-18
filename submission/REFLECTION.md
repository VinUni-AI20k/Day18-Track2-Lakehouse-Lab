# Reflection

**Câu hỏi:** Trong "Top 5 Lakehouse Anti-Patterns" được đề cập ở bài học (ví dụ: Small files, Không xóa orphans, Giữ snapshot quá lâu, Không quản lý metadata, v.v.), team bạn dễ vướng phải cái nào nhất, và vì sao?

**Trả lời:**

Em thấy dễ mắc lỗi **"Small files"** nhất vì dữ liệu đang được thêm vào hệ thống theo những lô nhỏ liên tục nhưng không có quy trình **compaction/Tối ưu hóa** nhất quán nào diễn ra. Ban đầu mọi thứ dường như đều bình thường về quy trình này và rất khó để nhận ra vấn đề ở giai đoạn đầu nhưng cuối cùng khi số lượng các tập tin tăng lên thì quá trình truy vấn sẽ yêu cầu đọc nhiều tập tin và metadata hơn do đó làm tăng độ trễ và chi phí liên quan đến lưu trữ và yêu cầu. Hơn nữa nếu không kết hợp **hết hạn chụp ảnh và xóa gác** thì tất cả những tập tin cũ và các tập tin gác từ công việc thất bại sẽ chiếm dụng dung lượng lưu trữ.
