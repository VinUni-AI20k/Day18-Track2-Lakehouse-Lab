
# Reflection: Các Anti-Pattern của Lakehouse trong Production

**Anti-Pattern:** *Vấn đề Small-File & Bảo trì không được quản lý (Anti-Pattern #1)*

Trong các kiến trúc streaming và LLM observability có throughput cao, workload của chúng ta liên tục append các micro-batch gồm trace, tool call và tương tác của người dùng. Nếu không có các job compaction được lập lịch, điều này nhanh chóng tạo ra hàng nghìn file Parquet nhỏ.

Như được minh họa trong NB2 và NB6, điều này gây ra hai bottleneck nghiêm trọng trong production:

1. **Suy giảm hiệu năng truy vấn & gia tăng chi phí:** Các query engine lãng phí CPU theo cấp số nhân cho việc liệt kê metadata và mở file thay vì thực hiện tính toán thực tế, làm suy giảm tốc độ point-lookup và aggregation từ $3\times$ đến $10\times$.
2. **Phình to storage một cách vô hình:** Các lỗi streaming và các lần ghi chưa được commit để lại các file orphan không được tham chiếu mà `VACUUM` tiêu chuẩn bỏ qua (vì chúng chưa bao giờ được commit vào `_delta_log`), dẫn đến chi phí cloud storage ngày càng tăng.

**Mitigation:** Chúng ta phải triển khai một lịch cron tự động để thực thi 4 job bảo trì bắt buộc: (1) compaction bằng `OPTIMIZE`, (2) clustering bằng `Z-ORDER` trên các cột predicate có tần suất truy vấn cao (`user_id`, `request_id`), (3) hết hạn snapshot, và (4) quét orphan bằng phép set-difference tùy chỉnh để loại bỏ các file chưa được commit.
