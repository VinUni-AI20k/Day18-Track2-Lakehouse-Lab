# Reflection — Day 18 Lakehouse Lab

**Anti-pattern nhóm tôi dễ vướng nhất: bỏ qua OPTIMIZE, để ingestion sinh small files.**

Nhóm tôi xây RAG: mỗi tài liệu nạp vào là một append nhỏ, không job nén nào chạy sau — đúng công thức sinh small-files.

NB6 cho thấy giá của nó. 100.000 dòng rải trong 200 file, trung bình 51.5 KB, mức hợp lý là 128–512 MB. Full-scan tốn 10 triệu GET, **$4.00/ngày** chỉ tiền request; nén còn 4 file thì **$0.08/ngày** — chênh 50 lần cho cùng dữ liệu. Compaction: 200 → 11 file (18×). NB2 thêm Z-ORDER: speedup 8.4×, pruning 55×.

Hai điều tôi không đoán trước. Một: ngay sau compaction dung lượng **tăng** 10.1 → 16.1 MB, vì file mới ghi xong trước khi file cũ bị thu hồi — cần ngân sách cho quãng trả tiền hai lần. Hai: VACUUM thu 16.1 MB tombstone nhưng bỏ sót 5 file ngoài log, `rows` vẫn báo đủ 100.000 — dung lượng trả tiền mà không thấy. Tự diff đĩa với log mới lộ 3 file `crashed-writer`.

Vậy nên: compaction + Z-ORDER định kỳ, và diff orphan thay vì tin VACUUM.
