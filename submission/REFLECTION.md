# Reflection

Anti-pattern em dễ gặp nhất là **small files**. Dữ liệu observability được đẩy
liên tục từ nhiều worker; nếu mỗi micro-batch hoặc mỗi retry tạo một file, số
object tăng nhanh hơn nhiều so với dung lượng thực. NB2 tái hiện đúng tình huống
này: 100.000 dòng tạo 200 file nhỏ. Sau compaction và Z-ORDER, bảng còn 55 file,
truy vấn nhanh hơn 10,6× và chỉ cần đọc 1/55 file cho một `user_id`.

Rủi ro không chỉ nằm ở latency. NB5 cho thấy metadata của một bảng demo nhiều
file nhỏ bằng 283,7% kích thước data; NB6 còn ước tính request GET có thể giảm
từ 10 triệu xuống 200 nghìn mỗi ngày sau compaction. Vì vậy em sẽ đặt target
128–512 MB/file, điều chỉnh trigger interval của writer, theo dõi file
count/average file size, và chạy compaction cùng clustering theo lịch. Expiry
phải đi kèm orphan sweep; chỉ chạy `VACUUM` không xử lý được file của writer
chết trước commit.
