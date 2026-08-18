# Reflection

Anti-pattern mà nhóm tôi dễ gặp nhất là **Small-Files Problem**. Với
streaming ingestion, mỗi micro-batch có thể được ghi đúng schema và commit
thành công, nhưng lại tạo thêm một file Parquet nhỏ. Vì không gây lỗi ngay
lập tức, vấn đề chỉ bộc lộ khi số file đã rất lớn: query planning chậm,
nhiều request tới object storage và chi phí vận hành tăng dần.

Kết quả NB2 và NB6 cho thấy compaction làm giảm mạnh số file, còn
Z-order thu hẹp khoảng min/max theo `user_id`, giúp engine bỏ qua phần lớn file
không liên quan. Qua đó, tôi nhận ra tối ưu câu SQL là chưa đủ; layout vật
lý và maintenance schedule cũng là thành phần của kiến trúc.

Giải pháp là theo dõi file count, kích thước file trung bình và metadata growth;
điều chỉnh chu kỳ streaming; đồng thời lên lịch compaction, clustering,
snapshot expiry và orphan cleanup. Các job này phải có SLA, monitoring và cảnh
báo như các pipeline production khác.
