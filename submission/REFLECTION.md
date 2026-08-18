# Reflection — Day 18 Lakehouse

Trong số "Top 5 Lakehouse Anti-Patterns", dự án của nhóm em có nguy cơ cao nhất mắc phải lỗi **"Small-Files Problem"** và **"Bỏ quên Maintenance Jobs"**.

**Lý do:**
Hệ thống nhận luồng micro-batches liên tục từ client. Việc liên tục ghi các file Parquet vài KB mà không nén sẽ làm bùng nổ số lượng file vật lý, gây phình to metadata, làm chậm quá trình scan planning và tăng vọt chi phí API storage (S3 PUT/GET).

**Giải pháp:**
Nhóm thiết lập cronjob chạy định kỳ 4 tác vụ bảo trì cốt lõi:
- **Compaction (`OPTIMIZE`):** Gom các file nhỏ thành file lớn chuẩn 256MB.
- **Clustering (`Z-ORDER`):** Gom cụm dữ liệu theo cột hay lọc để tối ưu file skipping.
- **Snapshot Expiry:** Thu hồi snapshot quá hạn để kiểm soát kích thước metadata.
- **Orphan Sweep (`VACUUM`):** Xóa triệt để các file Parquet mồ côi để tối ưu chi phí lưu trữ.
