# Reflection

**Họ tên:** Nguyễn Hoàng Duy
**MSSV:** 2A202600158

---

Anti-pattern mà em dễ mắc phải nhất là **vấn đề small-file** (quá nhiều file nhỏ).

Trong pipeline LLM observability, mỗi lần gọi API tạo ra một bản ghi log theo thời gian thực. Nếu dùng cách đơn giản nhất là `mode="append"` cho từng batch nhỏ, chỉ sau vài giờ hoạt động, layer Bronze đã có thể tích lũy hàng nghìn file Parquet cực nhỏ. NB2 đã minh họa rõ điều này: 200 lần append nhỏ tạo ra đúng 200 file, khiến thời gian truy vấn tăng gấp ~7.8 lần so với sau khi chạy OPTIMIZE+ZORDER.

Rủi ro này cao vì hai lý do. Thứ nhất, khi prototyping, lập trình viên tự nhiên dùng `mode="append"` mà không nghĩ đến lịch compaction. Thứ hai, vấn đề không lộ ra ngay — pipeline vẫn chạy đúng và dữ liệu vẫn đầy đủ, nhưng hiệu năng truy vấn âm thầm giảm theo thời gian. Đến khi phát hiện thì số file đã lên tới hàng chục nghìn.

Giải pháp là lập lịch chạy `dt.optimize.compact()` và `dt.optimize.z_order(["model"])` định kỳ (ví dụ: mỗi giờ cho Bronze, mỗi ngày cho Silver/Gold), đồng thời theo dõi `numFiles` như một metric vận hành song song với row count.
