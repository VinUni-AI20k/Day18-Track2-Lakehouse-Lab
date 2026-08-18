# Lab 18 Reflection

Trong quá trình làm việc với dữ liệu từ AI-generated content và user interaction
tracking, anti-pattern mà team tôi dễ vướng nhất là **bỏ qua medallion layers**
— tức là dump thẳng raw output của LLM và user events vào query layer mà không
đi qua pipeline Bronze → Silver → Gold.

Khi build nhanh, rất dễ viết một script gọi model, parse response rồi insert
thẳng vào reporting table. Cách này chạy được — cho đến khi model trả về format
lạ, một retry tạo ra request_id trùng lặp, hoặc một thay đổi schema từ API
phá vỡ toàn bộ downstream query cùng một lúc.

NB4 minh họa rõ điều này. Layer Bronze nhận raw JSON nguyên trạng. Silver dedup
theo request_id và loại bỏ các row lỗi — số dòng giảm rõ rệt. Gold mới tổng hợp
ra p50/p95 latency và cost_usd đáng tin cậy. Nếu không có Silver, các con số ở
Gold sẽ âm thầm bao gồm cả duplicate và null mà không ai biết.

Giải pháp không phức tạp — đó là kỷ luật. Coi raw AI output là untrusted input,
enforce schema boundary tại ingestion, và không để downstream dashboard đọc
thẳng từ Bronze. Schema enforcement và ACID guarantees của Delta Lake giúp
maintain boundary này với chi phí rất thấp.
