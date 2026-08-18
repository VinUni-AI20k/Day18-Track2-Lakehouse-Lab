Trong hệ thống AI Observability và Agentic Workflow, rủi ro lớn nhất của team là Small File Fragmentation & Uncoordinated Maintenance do dữ liệu streaming liên tục.

Log từ hàng trăm agent và LLM calls được ghi theo các micro-batch nhỏ, tạo ra rất nhiều file Parquet chỉ vài KB. Nếu chỉ append mà không có compaction định kỳ, số lượng file và metadata sẽ tăng nhanh, khiến truy vấn phân tích chậm hơn, latency p95 tăng và chi phí thao tác metadata trên S3 cũng cao hơn.

Ngoài ra, khi job bị gián đoạn, một số file uncommitted hoặc orphan có thể còn sót lại ngoài transaction log. Những file này không phải lúc nào cũng được xử lý bằng VACUUM thông thường, nên storage có thể tăng dần theo thời gian.

Vì vậy, team cần tự động hóa quy trình bảo trì gồm auto-compaction, Z-ORDER theo model_id và timestamp, cùng với việc quét và xóa định kỳ các orphan file. Cách này giúp duy trì hiệu năng truy vấn ổn định và kiểm soát chi phí lưu trữ tốt hơn.