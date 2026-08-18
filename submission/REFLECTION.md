# Reflection — Top 5 Lakehouse Anti-Patterns

Anti-pattern nhóm mình dễ mắc nhất: **bỏ qua `OPTIMIZE` dẫn đến Small-Files Problem do streaming ingestion**.

Log LLM observability (NB4) ghi theo lô nhỏ mỗi phút — giống streaming thật. NB2 mô phỏng đúng kịch bản: 200 lần append nhỏ tạo ra **200 file**. Truy vấn theo `user_id` cụ thể mất **126.4 ms** vì phải quét gần hết 200 file, không có min/max stats nào đủ hẹp để loại trừ.

Sau `OPTIMIZE (compact)` + `Z-ORDER BY user_id`: file giảm còn **55**, cùng truy vấn chỉ còn **13.9 ms** — nhanh **9.1×**. Đo theo tỉ lệ file bị loại còn ấn tượng hơn: chỉ **1/55 file** chứa `user_id` cần tìm — pruning **55×**, vì sau Z-order khoảng giá trị mỗi file gần như không chồng lấn.

**Giải pháp:** không tắt streaming ingestion, mà lên lịch `OPTIMIZE` định kỳ (mỗi giờ, hoặc khi số file vượt ngưỡng) như job bảo trì nền — đúng tinh thần Job 1+2 ở NB6 — thay vì coi ghi và tối ưu đọc là cùng một bước.
