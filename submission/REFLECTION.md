Anti-pattern nhóm mình dễ vướng nhất là "đổ hết vào một bảng lớn rồi query trực tiếp". Lúc đầu rất nhanh: ingest xong là có dashboard. Nhưng khi dữ liệu tăng (retry, schema đổi, truy vấn theo tenant/model/date), hệ thống xuống cấp: chi phí đọc tăng, query dao động mạnh, và khi số liệu lệch thì khó truy nguyên.

Lab này cho mình thấy trọng tâm không phải SQL nhanh, mà là luồng dữ liệu có kiểm soát. Medallion tách trách nhiệm rõ: Bronze giữ raw để audit/replay, Silver chuẩn hóa + dedup để đảm bảo tính đúng, Gold phục vụ KPI với độ trễ thấp. Delta log, time travel/restore và optimize/z-order biến vận hành từ "chữa cháy" thành quy trình lặp lại được.

Với dữ liệu thật, rủi ro lớn nhất không phải thiếu report, mà là thiếu khả năng khôi phục và giải thích dữ liệu khi có incident.
