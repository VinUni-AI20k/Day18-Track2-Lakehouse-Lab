# Reflection

Anti-pattern nhóm mình dễ vướng nhất là **#3 — bỏ qua OPTIMIZE, dẫn tới small-file problem**.

Dữ liệu trong lab mô phỏng log gọi LLM — ghi liên tục, mỗi lần một ít, giống hệt pattern streaming/logging thật. NB2 cho thấy chỉ vài chục lần ghi nhỏ đã đủ tạo ≥100 file rời rạc; mỗi file nhỏ vẫn tốn một lần mở/đọc metadata riêng, nên query phải trả thêm chi phí I/O cho từng file thay vì gộp lại. Sau khi chạy `OPTIMIZE` + `Z-ORDER`, tốc độ tăng 8.4×, tỷ lệ file được prune tăng 55× — tức là *trước* optimize, engine đang quét dư gấp hàng chục lần so với cần thiết cho cùng một câu query.

Điều khiến nhóm mình dễ vướng là OPTIMIZE không tự chạy, phải chủ động lên lịch (daily cron). Nếu pipeline ghi log 24/7 mà quên setup job này, small-file tích luỹ âm thầm — hệ thống vẫn chạy đúng, chỉ chậm và tốn compute dần, đến khi dashboard trễ hoặc bill tăng mới lộ ra, lúc đó đã khó truy ngược nguyên nhân. Bài học: coi OPTIMIZE là job vận hành bắt buộc từ đầu, không phải "tối ưu sau".
