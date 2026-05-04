# Reflection — Lab 18 Lakehouse

## Anti-pattern dễ vướng nhất: Small-File Problem

Anti-pattern nguy hiểm nhất mà team dễ gặp là **small-file problem** trong pipeline streaming.

Khi ingest dữ liệu LLM observability theo từng request (hoặc micro-batch ngắn), mỗi lần write tạo ra một file Parquet nhỏ. Sau vài ngày, table có thể có hàng nghìn file nhỏ — mỗi query phải mở và scan tất cả, làm latency tăng đột biến dù data volume không lớn.

Lý do team dễ vướng: lúc dev/test với vài trăm rows thì query vẫn nhanh, không ai để ý. Chỉ đến khi lên production với traffic thật (hàng triệu LLM calls/ngày) thì dashboard mới bắt đầu chậm — và lúc đó table đã có hàng chục nghìn file, việc OPTIMIZE tốn nhiều thời gian hơn.

Bài học: cần schedule `OPTIMIZE + Z-ORDER` định kỳ ngay từ đầu, không đợi đến khi có vấn đề.
