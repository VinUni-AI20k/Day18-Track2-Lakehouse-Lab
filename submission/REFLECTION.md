# REFLECTION

**Anti-pattern dễ vướng nhất: chạy expiry mà không quét orphan — coi
maintenance là một job trong khi nó là bốn.**

Tôi chọn nó vì đây là thứ duy nhất trong lab *không báo lỗi khi sai*.
Hai số đo từ NB6:

- `VACUUM` không thấy 3 file orphan tôi cố ý tạo: chúng chưa từng vào
  transaction log nên không có tombstone để thu hồi, vô hình ở mọi retention.
  Bảng vẫn báo đúng 100.000 dòng trong khi 5 file rác nằm trên đĩa.
- `expire_snapshots` đưa 20 → 3 snapshot nhưng xoá **0 file avro**; metadata
  còn phình 321,6 → 328,6 KB. Chỉ orphan sweep chạy sau mới thu hồi 36,4 KB.

Điều làm tôi đổi cách nghĩ: ở cả hai, chỉ số ai cũng theo dõi — số snapshot —
vẫn giảm đúng kỳ vọng. Một dashboard "maintenance xanh" vẫn tương thích
với hoá đơn lưu trữ không giảm đồng nào. Rủi ro lớn nhất không nằm ở loại dữ
liệu, mà ở chỗ không ai có tín hiệu để biết mình sai.

Việc đầu tiên: đo **bytes thu hồi được** thay vì số snapshot, và chạy
hiệu tập hợp `disk \ log` sau mỗi expiry.
