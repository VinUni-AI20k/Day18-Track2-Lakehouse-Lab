# REFLECTION

**Họ tên:** Lê Kim Tính
**MSSV:** 2A202601560

## Anti-pattern dễ vướng nhất: Small-Files Problem do bỏ qua OPTIMIZE/Compaction trong streaming ingestion

NB2 và NB6 tái hiện đúng tình huống này: ghi 200 micro-batch nhỏ (mô
phỏng trigger Kafka 5 giây) tạo ra 200 file Parquet riêng lẻ — mỗi
commit streaming đều *đúng*, nhưng sự tích tụ là lỗi. Đo được trên máy
tôi: sau `OPTIMIZE`, số file giảm 200 → 11 (18×); sau `Z-ORDER`, một
truy vấn điểm chỉ mở 1/10 file thay vì toàn bộ (skip rate 90%). Trước
tối ưu, min/max stats các file chồng lấn nên engine buộc phải quét hết.

Đây là rủi ro lớn nhất cho hệ thống có nguồn ghi liên tục (agent
traces, LLM logs) vì nó không đến từ code sai, mà từ thiếu job bảo trì
định kỳ — chi phí tăng phi tuyến vì object storage tính phí theo số
request GET, không theo dung lượng.

**Giải pháp:** lên lịch `OPTIMIZE` + `Z-ORDER` (Delta) hoặc
`rewrite_data_files` (Iceberg) định kỳ theo ngưỡng file/thời gian, nhắm
128–512MB/file như production, kèm Job 3 (expiry) và Job 4 (orphan
removal) để metadata không phình.
