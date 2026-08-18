# Reflection

**Anti-pattern nhóm dễ vướng nhất: #3 — "Bỏ qua OPTIMIZE → small-file problem".**

Bronze `llm_calls_raw` (200K dòng LLM-call log trải 7 ngày) mô phỏng đúng
pattern nguy hiểm nhất với team chúng tôi: ghi log theo request, tần suất cao,
mỗi lần một batch nhỏ — công thức đẻ ra hàng nghìn file Parquet vài KB, không
phải lỗi thiết kế một lần mà là hệ quả tự nhiên của kiến trúc streaming.

NB2 đo trực tiếp cái giá phải trả: trước OPTIMIZE bảng có >100 file nhỏ; sau
OPTIMIZE + Z-ORDER, chúng tôi đo được speedup **10.0×** và files-pruned
**55.0×** khi lọc theo cột đã Z-ORDER. Khoảng cách hai con số là bằng chứng:
wall-clock nhiễu bởi tải máy, còn tỉ lệ pruning dựa trên min/max statistics
thì tất định — bỏ OPTIMIZE, một query quên predicate sẽ quét cả 100+ file
thay vì được prune gần hết.

Ở quy mô LLM observability thật (hàng tỷ dòng/ngày thay vì 200K), small-file
problem nhân lên gấp hàng trăm lần nếu thiếu `daily OPTIMIZE cron` — đúng fix
slide đề xuất, và cũng là Job 1 bắt buộc trong NB6.
