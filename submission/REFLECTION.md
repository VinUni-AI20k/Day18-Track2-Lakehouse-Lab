# Reflection: Các Anti-Pattern Trong Lakehouse

Trong số các anti-pattern phổ biến của Lakehouse, dữ liệu của team chúng tôi có nguy cơ cao nhất với **vấn đề Small Files (tệp nhỏ)**.

Trong suốt lab này, chúng tôi đã gặp vấn đề này trực tiếp khi tạo dữ liệu synthetic mà không có compaction đúng cách. Notebook `02_optimize_zorder` của chúng tôi đã minh chứng rõ ràng cách các lượt ghi nhanh mà không tối ưu tích lũy hàng trăm tệp nhỏ, làm giảm hiệu suất đọc đáng kể. Trước khi chạy OPTIMIZE, các truy vấn của chúng tôi phải quét hơn 100 tệp; sau khi compaction, con số này giảm xuống còn một vài tệp.

Anti-pattern này đặc biệt nguy hiểm cho trường hợp sử dụng của chúng tôi vì:
- Dữ liệu của chúng tôi được nhập liên tục (mô hình giống streaming)
- Nhiều team ghi độc lập mà không có sự phối hợp
- Chi phí tăng theo cấp số nhân khi quy mô lớn (nhiều tệp hơn = nhiều thao tác liệt kê tệp, nhiều thao tác metadata, nhiều Spark tasks hơn)

Lab này dạy chúng tôi rằng compaction phải được lên lịch thường xuyên, và chiến lược partitioning cần tính đến tốc độ ghi, không chỉ mẫu truy vấn. Nếu không có bảo trì chủ động, Lakehouse của chúng tôi sẽ dần thoái hóa thành "data swamp" nơi chi phí truy vấn tăng vọt và chất lượng dữ liệu suy giảm.

Kinh nghiệm này thúc đẩy chúng tôi triển khai các job compaction tự động trong pipeline sản xuất.
