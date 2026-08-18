# Reflection - Lakehouse Anti-Pattern

Anti-pattern mà em dễ gặp nhất là **small-file problem**. Dữ liệu observability thường được ghi liên tục theo từng micro-batch nhỏ, trong khi nhiều job hoặc agent có thể ghi song song. Cách này giúp dữ liệu xuất hiện nhanh nhưng lâu dần tạo ra hàng nghìn file nhỏ. Chi phí không chỉ nằm ở dung lượng lưu trữ: query engine phải liệt kê, mở và đọc metadata của rất nhiều file, khiến thời gian lập kế hoạch và số request tới object storage tăng mạnh.

Kết quả ở NB2 và NB6 cho thấy compaction làm giảm rõ rệt số file, còn clustering/Z-order giúp engine bỏ qua phần lớn file không liên quan. Vì vậy team nên theo dõi file count, kích thước file trung vị và pruning ratio theo từng partition; chạy compaction định kỳ dựa trên ngưỡng thay vì theo lịch cố định; đồng thời giới hạn partition cardinality. Việc dọn snapshot và orphan file cũng phải là job riêng, vì `VACUUM` hoặc `expire_snapshots` không bảo đảm xóa mọi dữ liệu vật lý bị bỏ lại.
