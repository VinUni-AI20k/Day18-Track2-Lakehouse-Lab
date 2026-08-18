# Reflection — Top 5 Lakehouse Anti-Patterns

Anti-pattern hệ thống dữ liệu của team dễ mắc nhất: **bỏ qua OPTIMIZE, dẫn
đến Small-Files Problem do streaming/micro-batch ingestion.**

NB2 tái hiện đúng lỗi này: 200 micro-batch ghi liên tục tạo 200 file Parquet
nhỏ, khiến point-query `user_id=4242` chậm (median 215.5 ms). Sau
`dt.optimize.compact()` + `dt.optimize.z_order(["user_id"])`, file giảm còn
55, query nhanh hơn ~10×, pruning ratio đạt 55×. Đây là pattern dễ gặp ở
team ghi log LLM call theo thời gian thực (giống Bronze `llm_calls_raw` ở
NB4): mỗi request/micro-batch tạo một file riêng, không ai chủ động compact
định kỳ.

Rủi ro: metadata phình (mỗi file là một entry trong transaction log), read
amplification vì engine phải mở hàng trăm file nhỏ, chi phí list/scan trên
object storage tăng tuyến tính theo số file — khớp NB6 Job 1 (compaction
giảm ≥10× số file).

**Giải pháp:** lên lịch job compaction định kỳ thay vì thủ công, đặt
`target_size` hợp lý cho production (128 MB–1 GB, khác 256 KB dùng demo ở
NB2), kết hợp Z-ORDER trên cột lọc phổ biến (`user_id`, `model`) để tối đa
hoá data skipping cho truy vấn điểm.
