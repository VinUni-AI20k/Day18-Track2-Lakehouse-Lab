# Reflection — Day 18 Lakehouse Lab

**Anti-pattern nguy hiểm nhất với nhóm tôi: coi index dẫn xuất (vector DB) như system-of-record.**

NB7 dựng lại đúng tình huống đó. Tôi gửi yêu cầu xoá cho `user_042` — 8 document. Lakehouse giảm từ 2.000 xuống 1.992 dòng, truy vấn trong bảng trả về **0**. Nhưng external index vẫn giữ nguyên 2.000 vector và vẫn trả về đủ **8** document ấy, sẵn sàng vào prompt RAG — và nếu sync là upsert một chiều thì là mãi mãi, vì delete là thao tác pipeline hay quên nhất.

Nhóm đồ án của tôi dễ vướng vì đang xây RAG trên tài liệu có thông tin cá nhân, với kiến trúc mặc định "warehouse giữ dữ liệu, vector DB giữ embedding, đồng bộ hằng đêm" — tức coi index là cache. Nhưng cache không khiến ta vi phạm PDPL; một bản sao dữ liệu cá nhân thì có.

Tôi sẽ làm khác. Một: bỏ sync toàn bảng — NB7 cho thấy Change Data Feed phát đúng 8 sự kiện `delete` kèm `doc_id`, index phải đăng ký nhận thay vì đoán. Hai: giữ embedding trong cùng dòng dữ liệu, để vòng đời do chính bảng cưỡng chế.
