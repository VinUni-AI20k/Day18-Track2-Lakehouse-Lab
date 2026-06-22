# Reflection — Top 5 Lakehouse Anti-Patterns

**Anti-pattern dễ vướng nhất: Small File Problem**

Trong pipeline LLM observability mà team đang xây (NB4), mỗi lần
ingest là một batch nhỏ: một service gọi API, ghi vài trăm row, rồi
append tiếp vài phút sau. Nếu không có bước OPTIMIZE định kỳ, sau một
tuần chạy thực tế `_delta_log/` sẽ chứa hàng nghìn file Parquet nhỏ
dưới 1 MB — đúng cái "small file hell" mà NB2 minh họa.

**Vì sao team dễ bỏ qua?**

Lúc dev, bảng chỉ có vài file nên query vẫn nhanh. Vấn đề chỉ lộ ra
ở production sau vài ngày append liên tục — lúc đó DuckDB/Spark phải
mở hàng nghìn file thay vì vài chục, latency tăng âm thầm và không ai
nhìn vào `DESCRIBE DETAIL` để phát hiện. Deadline dự án không để lại
thời gian cho ops/maintenance task như OPTIMIZE.

**Lesson learned từ lab:**

`dt.optimize.compact()` + `dt.optimize.z_order(["model"])` ở NB2 cho
thấy speedup ≥ 3× chỉ với một lệnh. Team sẽ đưa bước này vào
scheduled job (ví dụ: chạy sau mỗi 6 giờ ingest) thay vì để accumulate.

---
*Vu Dinh Phuong · Day 18 · Track 2 — Lakehouse Lab*
