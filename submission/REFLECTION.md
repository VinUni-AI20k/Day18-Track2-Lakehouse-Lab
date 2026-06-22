# Reflection — Day 18 Lab (Track 2)

## Anti-pattern dễ vướng nhất: Small-File Problem

Trong bối cảnh xây dựng pipeline LLM observability, anti-pattern nguy hiểm nhất là **small-file problem** — tích lũy hàng trăm nghìn file nhỏ do streaming ingestion liên tục từ các LLM API call.

Mỗi inference request được log theo thời gian thực: latency, token count, cost, error code. Ở quy mô production (hàng triệu call/ngày), mỗi micro-batch ghi ra một Parquet file nhỏ vào Bronze layer. Sau vài ngày, Delta table có thể chứa hàng chục nghìn file — mỗi file chỉ vài KB. Hệ quả: query p95 latency theo model/ngày phải scan toàn bộ file list từ `_delta_log`, overhead metadata còn lớn hơn overhead đọc data thực.

NB2 minh họa điều này: 200 appends nhỏ → query median tăng ~3× so với sau `OPTIMIZE + Z-ORDER`. Ở production với 200.000 appends, tác động nghiêm trọng hơn nhiều.

Giải pháp: lập lịch `OPTIMIZE` định kỳ (hàng đêm) trên Silver/Gold layer, kết hợp `Z-ORDER BY (model_id, date)` để file-skipping hoạt động hiệu quả khi dashboard filter theo model và khoảng thời gian.
