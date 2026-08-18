<!--
Lưu ý: bài lab yêu cầu chọn 1 mục trong "Top 5 Lakehouse Anti-Patterns" trên
slide của khóa học. Slide đó không có trong repo nên bản dưới đây chọn theo
bằng chứng đo được trực tiếp từ NB2/NB6 khi chạy lab. Hãy đối chiếu lại tên
gọi anti-pattern với đúng thuật ngữ trên slide trước khi nộp, và đổi nếu slide
dùng tên khác cho cùng hiện tượng.
-->

# Reflection

**Anti-pattern:** Small-Files Explosion do micro-batch ingest thiếu lịch bảo trì (neglected compaction/maintenance debt).

Dữ liệu LLM-observability xuyên suốt lab này ghi log theo từng request, gần như append-only với tần suất cao — đúng hình dạng workload dễ vướng anti-pattern này nhất: mỗi batch nhỏ tạo một file Parquet vài chục KB, xa mốc production 128–512MB/file.

Số đo thực trên máy tôi: NB2 cho thấy point-query chậm 9.4× khi 200 file nhỏ chưa OPTIMIZE (225ms → 24ms sau compact + Z-ORDER, files-pruned 55×). NB6 cho thấy cùng kiểu dữ liệu tốn ~10 triệu GET request/ngày (~$4/ngày) khi phân mảnh, giảm còn $0.08/ngày sau compaction — chênh lệch 50×.

Nguy hiểm hơn một lỗi một lần: pipeline ghi liên tục 24/7, không có batch lớn nào "tự nhiên" tạo file to. Thiếu lịch Job 1 (compaction) + Job 2 (clustering) định kỳ, chi phí GET và độ trễ query tái phát mỗi ngày, âm thầm cộng dồn tới khi dashboard chậm mới bị phát hiện.

**Rủi ro cao nhất với team:** ghi liên tục, không batch lớn tự nhiên → small-files là anti-pattern đầu tiên và tốn kém nhất sẽ gặp nếu thiếu job bảo trì tự động.
