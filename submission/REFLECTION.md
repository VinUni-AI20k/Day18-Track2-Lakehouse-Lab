**Nhóm:**

- Nguyễn Trần Khương An : mail-github (khuongan66@gmail.com), mail-vinuni(26ai.anntk@vinuni.edu.vn) , github username: kuongan
- Phạm Minh Việt: mail-github (viettpm26@gmail.com), mail-vinuni (26ai.vietpm@vinuni.edu.vn),  github username: tuturuuu


Anti-pattern dễ vướng nhất: "transform trực tiếp trên Bronze và không enforce schema/ownership".

Giải thích: để nhanh, team thường sửa, làm sạch hoặc enrich dữ liệu ngay trên layer Bronze (ghi đè file, chạy notebook trực tiếp lên path của Bronze) thay vì coi Bronze như nguồn nguyên thủy bất biến. Hậu quả thực tế gồm schema drift (cột xuất hiện/biến mất bất ngờ), duplicate hoặc partial rows, và downstream Silver/Gold bị ôm technical debt — rollback bằng time-travel trở nên rối rắm, test không đáng tin cậy, và báo cáo sai lệch.

Nguyên nhân: áp lực deadline, thiếu quy tắc ownership (ai chịu trách nhiệm ingest vs transform), thiếu invariant checks ở write-time, và tiện lợi của notebook khiến thao tác trực tiếp trở thành thói quen.

Biện pháp tránh: enforce schema khi write (reject bad-writes), giữ Bronze immutable (append-only), tách pipeline rõ ràng với ownership và PR review cho transform, áp dụng kiểm tra tự động (row counts, null checks, min/max stats), và dùng transactional Delta features + lineage để audit. Những bước này giữ được ACID, time-travel hữu dụng và giảm rủi ro khi promote dữ liệu lên Silver/Gold.
