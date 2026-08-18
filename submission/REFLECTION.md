# Reflection — Top 5 Lakehouse Anti-Patterns

Trong năm anti-pattern, em thấy dữ liệu của mình dễ gặp nhất vấn đề **small files do streaming micro-batch quá ngắn**. Dữ liệu observability và agent trajectory được ghi liên tục. Nếu ưu tiên độ trễ thấp và append từng batch nhỏ, số file sẽ tăng nhanh dù dung lượng chưa lớn.

NB2 minh họa rõ vấn đề này: 200 lần append tạo ra 200 file. Sau `OPTIMIZE` và Z-ORDER, bảng còn 55 file; truy vấn theo `user_id` chỉ đọc 1/55 file và nhanh hơn 11,6 lần. Ở NB6, compaction giảm số file từ 200 xuống 11, còn clustering giúp bỏ qua 90% số file. Qua đó, em nhận ra tăng tài nguyên tính toán không giải quyết được nguyên nhân gốc nếu cách ghi vẫn tạo quá nhiều file nhỏ.

Nếu triển khai thực tế, em sẽ điều chỉnh thời gian micro-batch theo SLA, theo dõi số lượng và kích thước file, rồi chạy compaction và clustering khi vượt ngưỡng. Em cũng sẽ đối chiếu file trên đĩa với transaction log để tìm orphan files, vì `VACUUM` không phải lúc nào cũng xử lý được file chưa từng commit.
