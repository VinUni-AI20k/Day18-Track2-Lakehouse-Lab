# Reflection — Top 5 Lakehouse Anti-Patterns

**Họ tên:** Nguyen Thanh Dat

## Anti-pattern dễ mắc nhất: Small-File Problem

Anti-pattern tôi cho rằng team dễ vướng nhất là **small-file problem** — tình trạng
tích lũy hàng trăm file nhỏ do mỗi lần streaming ingest chỉ ghi một batch nhỏ.

Trong NB2, tôi tái hiện đúng kịch bản này: 200 lần append tạo ra 200 file riêng
lẻ. Trước khi OPTIMIZE, mỗi query buộc phải mở và scan toàn bộ 200 file đó, dù
chỉ cần tìm một user_id duy nhất. Sau OPTIMIZE + Z-ORDER, Delta Lake gộp và sắp
xếp lại data, query chỉ cần đọc 1–2 file nhờ file-skipping dựa trên min/max stats.

Trong thực tế, các team thường ưu tiên ingest nhanh và bỏ qua bước OPTIMIZE định
kỳ vì tưởng không cần thiết. Hậu quả là query ngày càng chậm theo thời gian mà
không rõ nguyên nhân, đến khi bảng có hàng nghìn file nhỏ thì rất tốn công sửa.

Bài học: nên lên lịch `OPTIMIZE + Z-ORDER` chạy tự động hàng đêm, đặc biệt với
các bảng nhận streaming data liên tục.
