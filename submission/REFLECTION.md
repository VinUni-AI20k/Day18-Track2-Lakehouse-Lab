# Top 5 Lakehouse Anti-Patterns Reflection

Trong 5 anti-patterns của Lakehouse, rủi ro cao nhất đối với hệ thống dữ liệu thực tế là **Small-File Problem (Vấn đề tệp nhỏ)**. 

Do đặc thù streaming và real-time ingestion, hệ thống liên tục tạo ra các tệp kích thước nhỏ. Tình trạng này làm tăng hàm mũ chi phí truy xuất đối tượng (S3/MinIO GET requests) và làm tăng độ trễ khi lập kế hoạch truy vấn (query planning) do lượng metadata phình to. Dù các định dạng như Delta hay Iceberg quản lý siêu dữ liệu hiệu quả, nhưng nếu thiếu quy trình bảo trì định kỳ (Compaction/OPTIMIZE), lợi thế lưu trữ sẽ biến thành rào cản hiệu năng. Do đó, việc thiết lập chiến lược bảo trì tự động và giám sát số lượng tệp là yêu cầu bắt buộc để kiểm soát chi phí vận hành của Lakehouse.
