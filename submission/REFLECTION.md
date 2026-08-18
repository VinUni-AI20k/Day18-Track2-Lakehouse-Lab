# Reflection: Lakehouse Anti-Patterns

Trong số "Top 5 Lakehouse Anti-Patterns", Anti-Pattern mà team chúng tôi dễ vướng phải nhất là **"Vấn đề File Nhỏ" (The Small-File Problem)**.

**Lý do:**
Thứ nhất, bản chất hệ thống của team yêu cầu đồng bộ dữ liệu liên tục (streaming ingestion) từ các nguồn về Lakehouse để phục vụ phân tích gần với thời gian thực (near real-time). Quá trình này sẽ sinh ra hàng nghìn file Parquet với dung lượng rất nhỏ mỗi giờ. 

Thứ hai, team thường có thói quen lập trình theo tư duy của Database truyền thống: "Ghi dữ liệu thành công là xong". Việc thiết lập một Cron Job độc lập để chạy bảo trì định kỳ (Compaction/OPTIMIZE) rất dễ bị bỏ quên hoặc bị gạt xuống cuối backlog do ưu tiên các tính năng kinh doanh. 

**Hậu quả & Cách khắc phục:**
Chính lượng rác khổng lồ này sẽ làm suy giảm nghiêm trọng tốc độ truy vấn và đẩy hoá đơn "S3 GET requests" tăng phi mã. Rút kinh nghiệm từ Notebook 6, chúng tôi rút ra bài học cốt lõi: **Không bao giờ đưa một Pipeline vào Production nếu chưa cấu hình kèm một Job chạy OPTIMIZE định kỳ.**
