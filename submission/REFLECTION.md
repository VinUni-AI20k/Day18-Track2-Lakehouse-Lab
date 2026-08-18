# REFLECTION

**Họ tên:** Lê Kim Tính
**MSSV:** 2A202601560

## Anti-pattern dễ vướng nhất: Small-Files Problem do bỏ qua OPTIMIZE/Compaction trong streaming ingestion

NB2 và NB6 tái hiện đúng tình huống này: ghi 200 micro-batch nhỏ (mô
phỏng trigger Kafka 5 giây) tạo ra 200 file Parquet riêng lẻ — mỗi
commit streaming đều *đúng*, nhưng sự tích tụ là lỗi. Đo được trên máy
tôi: sau `OPTIMIZE`, số file giảm 200 → 11 (18×); sau `Z-ORDER`, một
truy vấn điểm chỉ mở 1/10 file thay vì toàn bộ (skip rate 90%). Trước
tối ưu, min/max stats các file chồng lấn nên engine buộc phải quét hết
— stats chỉ hữu ích khi dữ liệu đã được cluster.

Đây là rủi ro lớn nhất cho một hệ thống có nguồn ghi liên tục (agent
traces, LLM observability logs) vì nó không đến từ code sai — mỗi
commit riêng lẻ vẫn ACID và hợp lệ — mà từ việc thiếu một cron job bảo
trì định kỳ. Chi phí tăng phi tuyến theo số file chứ không theo dung
lượng vì object storage tính phí theo số request GET; NB6 cho thấy
phần chi phí theo số object có thể chiếm tới 24% hoá đơn compaction.

**Giải pháp:** lên lịch `OPTIMIZE` + `Z-ORDER` (Delta) hoặc
`rewrite_data_files`/`sort` (Iceberg) chạy định kỳ theo ngưỡng số
file hoặc thời gian, nhắm target 128–512MB/file như khuyến nghị
production thay vì để tích tụ rồi mới xử lý thủ công; đi kèm Job 3
(snapshot expiry) và Job 4 (orphan removal) để metadata và storage
không phình theo thời gian.
