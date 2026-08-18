# Reflection — Lakehouse Anti-Pattern

Anti-pattern có nguy cơ gặp nhất là **quá nhiều file nhỏ (small-files problem)**. Dữ liệu quan sát LLM thường được đẩy liên tục theo micro-batch; nếu mỗi batch tạo một file, số object và metadata sẽ tăng nhanh dù tổng dung lượng chưa lớn. Điều này làm tăng thời gian lập kế hoạch truy vấn, chi phí liệt kê object và lượng file phải mở.

Lab tái hiện vấn đề bằng 200 micro-batch. Sau compaction và Z-ORDER theo `user_id`, số file giảm mạnh và point-query bỏ qua phần lớn file không liên quan. Kết quả cho thấy partition dữ liệu là chưa đủ; bảng còn cần lịch bảo trì chủ động.

Với dữ liệu của nhóm, tôi sẽ theo dõi số file, kích thước trung vị và tỷ lệ file được quét; cảnh báo khi file nhỏ tăng bất thường; đồng thời chạy compaction định kỳ và clustering theo cột truy vấn phổ biến. Đây phải là job bảo trì bắt buộc, không phải biện pháp thủ công sau khi hiệu năng suy giảm.
