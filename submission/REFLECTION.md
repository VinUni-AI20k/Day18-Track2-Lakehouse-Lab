Anti-pattern: quản lý schema yếu và drift schema một cách âm thầm.

Tại sao là vấn đề: khi producer ghi JSON bán cấu trúc vào Bronze mà không có
validation hoặc ràng buộc schema rõ ràng, các bước downstream (Silver/Gold)
có thể gặp kiểu dữ liệu sai, thiếu trường hoặc cấu trúc lồng bất ngờ. Điều
đó dẫn tới kết quả tổng hợp sai (p50/p95 không đáng tin) hoặc lỗi runtime
đến muộn, làm việc gỡ lỗi tốn thời gian.

Giải pháp thay thế: kiểm soát schema tại thời điểm ghi (từ chối các ghi sai)
cho các cột quan trọng, cho phép evolve có kiểm soát với `schema_mode='merge'`,
và thêm validation nhẹ phía producer cùng test end-to-end kiểm tra invariants
(ví dụ: tổng số hàng, phân phối khóa). Bổ sung cảnh báo tự động khi `history()`
thể hiện thay đổi schema bất ngờ hoặc khi tỉ lệ dedupe tăng đột biến.

Kết quả: giảm rủi ro dữ liệu bị hỏng âm thầm, làm cho restore đáng tin cậy
và giữ được SLA cho các chỉ số phân tích.
