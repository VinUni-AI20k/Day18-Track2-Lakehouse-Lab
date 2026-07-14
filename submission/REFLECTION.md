# Reflection — Lab 18: Data Lakehouse Architecture

## Anti-pattern dễ vướng nhất: Small-File Problem

Trong các anti-pattern được đề cập ở slide §5, **small-file problem** là anti-pattern mà team tôi dễ vướng nhất.

Khi xây dựng pipeline thu thập dữ liệu real-time (streaming ingestion), mỗi micro-batch tạo ra một file Parquet nhỏ. Sau vài ngày, một table có thể chứa hàng nghìn file chỉ vài KB mỗi file. Điều này gây ra:

1. **Query chậm** — engine phải mở và đọc metadata từ hàng nghìn file thay vì vài file lớn.
2. **Chi phí storage tăng** — mỗi file nhỏ có overhead metadata riêng, làm tăng tổng dung lượng thực tế.
3. **Z-order/file-skipping vô hiệu** — khi mỗi file chứa quá ít rows, min/max stats per-file không đủ selective để skip file hiệu quả.

Qua NB2, tôi thấy rõ: 200 files nhỏ query mất ~207ms, sau `OPTIMIZE + Z-ORDER` compact xuống 55 files thì query chỉ còn ~31ms — speedup 6.7×, và files-pruned ratio đạt 55× (chỉ 1/55 file chứa target user_id). Giải pháp là schedule `OPTIMIZE` định kỳ (hàng giờ hoặc hàng ngày) và set `target_size` phù hợp để tránh compact quá mức thành 1 file duy nhất, giữ lại khả năng file-skipping.
