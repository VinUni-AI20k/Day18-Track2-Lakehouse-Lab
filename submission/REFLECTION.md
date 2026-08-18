
# Reflection — Bỏ qua OPTIMIZE

Anti-pattern mà nhóm tôi dễ gặp nhất là bỏ qua OPTIMIZE sau khi dữ liệu được nạp liên tục theo các micro-batch. Trong NB2, 200 lần append tạo 200 file Parquet nhỏ. Trước tối ưu, point query `user_id=4242` mất trung vị 91,0 ms. Sau compact và Z-order, số file giảm còn 55, thời gian còn 8,9 ms, nhanh hơn 10,2 lần. Nhờ min/max statistics, truy vấn chỉ phải đọc 1 trong 55 file, đạt pruning ratio 55 lần.

Trong production, tình trạng này làm tăng chi phí metadata, số request tới object storage, độ trễ và thời gian compute dù dữ liệu không lớn. Giải pháp của nhóm là theo dõi số lượng và kích thước file, đặt ngưỡng compaction, chạy OPTIMIZE định kỳ sau ingestion, và Z-order theo các cột thường được lọc như `user_id`. Job phải ghi lại số file trước/sau và chỉ chạy khi lợi ích lớn hơn chi phí rewrite.
