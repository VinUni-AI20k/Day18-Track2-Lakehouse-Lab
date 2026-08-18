# Báo cáo: Lakehouse Anti-Patterns

- **Họ và tên:** Đỗ Thành Đạt
- **Mã học viên:** 2A202601278

Anti-pattern mà nhóm tôi có nguy cơ gặp nhiều nhất là **#4: không quản lý snapshot hết hạn và file mồ côi**. Pipeline LLM observability ghi và cập nhật dữ liệu thường xuyên. Nếu job bị lỗi trước khi commit, file có thể vẫn nằm trên storage mà không xuất hiện trong transaction log.

Notebook 06 làm rủi ro này rõ hơn. Với Iceberg, `expire_snapshots` xóa các tham chiếu metadata cũ nhưng không tự động xóa mọi file vật lý còn sót lại. Với Delta Lake, `VACUUM` xóa các file đã được ghi nhận là tombstone nhưng không tìm thấy ba file orphan do writer bị crash. Những file này không được truy vấn sử dụng nhưng vẫn chiếm dung lượng.

Vì vậy, chạy một lệnh bảo trì là chưa đủ. Nhóm cần lập lịch cho snapshot expiry, quét file orphan và compaction; đồng thời theo dõi số file và dung lượng trước–sau. Bài học chính của tôi là metadata sạch không đồng nghĩa với storage sạch: phải kiểm tra cả hai lớp.
