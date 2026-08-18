# Reflection — Lakehouse Anti-Pattern

Trong 5 Lakehouse Anti-Patterns, hệ thống dễ gặp nhất là **quản lý Data Lifecycle và Retention không phù hợp**.

Lakehouse lưu nhiều phiên bản dữ liệu để hỗ trợ Time Travel và khôi phục khi xảy ra lỗi. Tuy nhiên, nếu chỉ liên tục ghi dữ liệu mà không có chính sách retention, các snapshot và data file cũ sẽ tích tụ theo thời gian. Điều này làm tăng dung lượng lưu trữ và chi phí, đồng thời có thể gây vấn đề về governance khi dữ liệu đã được yêu cầu xóa vẫn còn tồn tại trong các version cũ.

Giải pháp là xây dựng **retention policy rõ ràng** cho từng loại dữ liệu. Chạy các maintenance job định kỳ như **snapshot expiration, VACUUM và orphan-file removal** để loại bỏ dữ liệu không còn cần thiết. Retention window cần đủ dài để đảm bảo khả năng rollback nhưng không giữ dữ liệu vô thời hạn.

Ngoài ra, với dữ liệu được đồng bộ sang hệ thống khác như vector database, thao tác DELETE cần được propagate thông qua **Change Data Feed (CDF)** để tránh dữ liệu cũ vẫn được truy xuất.