# REFLECTION — Nguyễn Khắc Huy (2A202602036)

**Anti-pattern nhóm mình dễ vướng nhất: coi vector index ngoài là
system-of-record, để nó lệch vòng đời với bảng.**

Dữ liệu nhóm mình là corpus tài liệu + embedding cho RAG, pipeline là upsert
một chiều sang vector store rời. Upsert xử lý được thêm và sửa, nhưng **xoá thì
không có đường đi** — xoá không phải một dòng mới để ghi đè, nó là sự vắng mặt.

NB7 tái hiện đúng kịch bản đó: xoá 8 tài liệu, bảng còn 1.992 dòng và trả về
**0** kết quả cho doc đã xoá, còn index ngoài vẫn giữ 2.000 vector và trả về
**8**. Nếu người dùng yêu cầu xoá dữ liệu theo Nghị định 13, nhóm mình báo cáo
đã xoá mà nội dung của họ vẫn đi vào prompt — vĩnh viễn, vì không ai sync một
phép xoá.

Hướng sửa: giữ vector ngay trong bảng, truy vấn bằng SQL khi quy mô còn cho
phép; khi buộc phải tách index, bắt nó **subscribe Change Data Feed** — CDF
phát đúng 8 sự kiện delete kèm `doc_id` cần evict. Index là thứ dựng lại được;
bảng mới là sự thật.
