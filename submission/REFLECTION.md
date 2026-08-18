# REFLECTION

Tôi làm lab một mình, chưa vận hành pipeline production, nên tôi đọc câu hỏi thành: **thói quen nào của chính tôi sẽ đẻ ra anti-pattern trước tiên?** Câu trả lời: small files — và niềm tin rằng `VACUUM` là đủ để dọn.

Phản xạ mặc định của tôi là ghi lô nhỏ cho "gần real-time". NB6 cho thấy cái giá: 200 commit → 200 file, 51.5 KB/file, trong khi production nhắm 128–512 MB. Tiền mất ở số request chứ không phải dung lượng: 10.000.000 GET/ngày = $4.00/ngày, so với $0.08 khi compaction còn 11 file.

Hai số phản trực giác tôi tự đo mới thực sự đổi cách tôi nghĩ:

- `VACUUM` báo thu hồi 211 file, mà **5 orphan vẫn nằm trên đĩa**. File chưa từng vào transaction log thì chưa từng bị tombstone — vacuum không thấy nó.
- Iceberg `expire_snapshots`: 20 → 3 snapshot nhưng **0 file avro bị xoá**, metadata còn tăng 313.5 KB → 320.1 KB. Phải nối orphan-sweep mới thu hồi được 36.2 KB.

Job 3 và Job 4 là một **cặp**. Chạy expiry rồi tưởng đã xong chính là lý do hoá đơn storage không giảm.
