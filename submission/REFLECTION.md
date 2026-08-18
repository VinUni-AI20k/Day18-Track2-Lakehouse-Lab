# Reflection

**Trong "Top 5 Lakehouse Anti-Patterns", dữ liệu của tôi dễ vướng cái nào nhất, vì sao?**

Tôi chọn anti-pattern số 4: đặt `VACUUM 0 HOURS` để tiết kiệm storage. Dữ liệu tôi làm
trong lab này là log quan sát hệ thống LLM, 200.000 bản ghi trải 7 ngày, ghi liên tục
theo từng request nên phình nhanh. Khi hoá đơn lưu trữ tăng, phản xạ đầu tiên là hạ
retention xuống thấp nhất, đúng cái bẫy slide cảnh báo.

Cái giá phải trả: sau khi vacuum, bảng thu hồi 16,1 MB nhưng time travel về v0 biến
mất, tức là mất khả năng replay đúng phiên bản dữ liệu đã dùng huấn luyện, thứ NB8
chứng minh là bắt buộc cho provenance.

Nhưng hạ retention cũng không chạm tới vấn đề gốc. NB6 đo được bảng báo 100.000 dòng,
trên đĩa có 15 file parquet mà log chỉ ghi nhận 10. Năm file kia do job crash để lại,
chưa từng commit nên chưa bị tombstone, và VACUUM ở mọi retention đều không thấy. Tôi
vẫn trả tiền cho 5 file vô hình.

Vì vậy tôi giữ retention 168 giờ mặc định, và tách riêng job hàng tuần lấy hiệu tập hợp
giữa file trên đĩa và file trong log để dọn orphan.
