# REFLECTION — Day 18

**Anti-pattern dễ mắc nhất: chạy expiry mà không quét orphan.**

Pipeline RAG/agent của team mình (ingest tài liệu → embed → vector store, quan
sát qua Langfuse) là chỗ rủi ro nhất. Job ingest chạy theo lịch và có thể chết
giữa chừng; mỗi lần như vậy để lại file đã ghi xuống đĩa nhưng chưa từng vào
log. Từ trước tới nay team chỉ đặt retention rồi mặc định coi như đã dọn sạch —
chưa bao giờ kiểm chứng con số thật.

NB6 cho thấy giả định đó sai ở hai tầng. `expire_snapshots` của Iceberg giảm
xuống còn 3 snapshot nhưng xoá **0 file avro**, metadata thậm chí phình thêm.
`VACUUM` của Delta cũng không nhìn thấy orphan ở bất kỳ mức retention nào, vì
nó chỉ thu hồi file đã bị tombstone trong transaction log — file chưa vào log
thì vô hình. Hệ quả thực tế: dashboard báo đã expire, hoá đơn lưu trữ không giảm.

Khắc phục: coi Job 3 và Job 4 là một **cặp**, không chạy riêng lẻ. Sau mỗi lần
expiry, quét hiệu tập hợp `Disk \ Log` để liệt kê file mồ côi và cảnh báo khi số
lượng > 0. Song song, đối chiếu định kỳ dung lượng thật trên storage với tổng
size ghi trong metadata — chênh lệch kéo dài chính là orphan đang tính tiền.
