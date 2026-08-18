# Reflection

**Sinh viên:** Nguyễn Hùng Phát  
**MSSV:** 2A202601094

Em dễ gặp **Small-Files Problem** nhất. Khi thu thập log LLM theo luồng
streaming, mỗi micro-batch nhỏ có thể tạo thêm nhiều Parquet file. Điều này
làm chi phí mở file, đọc metadata và lập kế hoạch truy vấn tăng mạnh, dù tổng
dung lượng dữ liệu không lớn. Về lâu dài, truy vấn dashboard về latency và
cost sẽ chậm, đồng thời chi phí object storage tăng.

Giải pháp là kiểm soát kích thước micro-batch khi ghi, theo dõi số file và
thực hiện compaction/OPTIMIZE theo lịch. Sau compaction, clustering hoặc
Z-ORDER theo các cột lọc phổ biến như `user_id` và thời gian sẽ giúp engine
skip nhiều file hơn. Em cũng cần đặt retention, checkpoint và orphan-file
cleanup thành các maintenance job có đo lường trước/sau, thay vì chỉ chạy khi
hệ thống đã chậm.
