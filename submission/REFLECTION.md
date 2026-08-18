# Reflection — Day 18 Lakehouse Lab

Trong nhóm “Top 5 Lakehouse Anti-Patterns”, team em dễ mắc nhất là xem `VACUUM` như cơ chế dọn dẹp toàn diện. Khi triển khai pipeline, nhóm thường tập trung vào dữ liệu đã commit và tin rằng retention là đủ, nhưng ít nghĩ đến file do writer bị lỗi trước khi commit. Những file orphan này không xuất hiện trong transaction log, `history()` hay dashboard, nên vẫn phát sinh chi phí lưu trữ mà không ai thấy.

NB6 cho thấy với `deltalake` (Rust/Python), `VACUUM` chỉ thu hồi file đã tombstone; ba orphan được tạo ra vẫn cần được tìm bằng phép hiệu giữa file trên đĩa và file được metadata tham chiếu. Bài học của nhóm là maintenance phải có job quét orphan riêng, kèm age guard để không đụng vào writer đang chạy, thay vì chỉ dựa vào một lệnh dọn dẹp.
