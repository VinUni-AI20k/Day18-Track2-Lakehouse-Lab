# REFLECTION — Day 18 Lakehouse Lab

**Tên:** Nguyễn Văn Hoàng
**MSSV / ID:** 2A202600742

---

> **Đề bài:** Trong slide "Top 5 Lakehouse Anti-Patterns", anti-pattern nào
> team/dữ liệu của bạn dễ vướng nhất, và vì sao? (≤ 200 từ)

## Anti-pattern em chọn

**→ Small files problem** (ghi quá nhiều file nhỏ, query chậm, không OPTIMIZE/compact định kỳ)

## Vì sao

Em thấy cái mà team em dễ vướng nhất là **Small files problem**. Lý do: dữ liệu bọn em chủ yếu đến từ log sự kiện ghii liên tục (gần như streaming), mỗi micro-batch lại sinh ra một vài file nho nhỏ. Lúc đầu lượng dữ liệu ít nên không ai để ý, nhưng càng append thì số file càng phình ra, đúng như NB2 trong lab: chỉ 200K dòng đã tạo hơn 100 file, và query phải mở từng file nên chậm hẳn. Vấn đề là team dễ ghi xong là quên luôn, không lên lịch nén file, nên rủi ro càng ngày càng lớn.

Cách phòng tránh em rút ra từ hôm nay: chạy `OPTIMIZE` (compact) định kỳ cộng
`Z-ORDER` theo cột hay lọc, để gộp file nhỏ và tận dụng min/max stats bỏ qua file
không liên quan. Kết hợp partition theo ngày như tầng Silver/Gold giúp giữ số
file mỗi partition ở mức hợp lý.

---

## (Tuỳ chọn) Liên hệ với bài lab

NB2 cho thấy rõ nhất rủi ro này: trước khi OPTIMIZE, bảng có hơn 100 file nhỏ và
query chậm; sau khi chạy `optimize.compact()` + `optimize.z_order(...)` thì số
file giảm mạnh và tốc độ/độ pruning cải thiện rõ rệt.
