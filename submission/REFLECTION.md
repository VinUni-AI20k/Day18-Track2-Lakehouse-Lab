# Lab 18 Reflection

**Họ tên:** Bùi Quang Hải
**Mã số sinh viên:** 2A202600006

---

## Anti-pattern dễ vướng nhất: Schema Chaos

Anti-pattern mà nhóm dễ mắc phải nhất là **schema hỗn loạn** — nhiều người
cùng ghi vào một Delta table với schema khác nhau mà không có cơ chế kiểm soát.

Trong thực tế, data đến từ nhiều nguồn: các microservice khác nhau, ETL job
do nhiều thành viên viết, hoặc API upstream thay đổi âm thầm thêm/đổi tên
trường. Nếu không có schema enforcement ở tầng Bronze, mỗi job tự giả định
schema của mình là đúng. Kết quả là một table mà một số file có `latency_ms`
kiểu INTEGER, số khác kiểu FLOAT, thậm chí có file thiếu cột hoàn toàn.
Pipeline Silver và Gold downstream sẽ vỡ theo cách khó đoán — thường không
xảy ra ngay lập tức mà âm ỉ nhiều ngày sau khi có model mới hoặc trường mới
xuất hiện.

Delta Lake giải quyết vấn đề này bằng cách mặc định chặn các write không khớp
schema, và chỉ cho phép thêm cột mới khi bật tường minh `schema_mode="merge"`.
Điều này buộc cả nhóm phải coi thay đổi schema là một quyết định có chủ đích,
không phải tai nạn.

Không có kỷ luật này, việc debug trở thành công việc khảo cổ: job nào đã ghi
file nào, và schema bắt đầu lệch từ lúc nào?

---

*AICB-P2T2 · Day 18 · Data Lakehouse Architecture*
