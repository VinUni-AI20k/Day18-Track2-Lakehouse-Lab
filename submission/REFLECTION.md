# Reflection: Lakehouse Anti-Patterns

Trong "Top 5 Lakehouse Anti-Patterns", cái team tôi dễ vướng nhất là
**Small-File Problem**.

**Lý do.** Hệ thống của team cần streaming ingestion liên tục cho phân tích near
real-time, nên mỗi giờ sinh ra hàng nghìn file Parquet rất nhỏ. Tệ hơn, team quen
tư duy database — "ghi xong là xong" — nên cron job compaction luôn bị đẩy xuống
cuối backlog, nhường chỗ cho tính năng kinh doanh.

**Hậu quả.** NB6 cho con số thay vì cảm tính: 200 file nhỏ khiến full-scan tốn
**$4.00/ngày** chỉ riêng GET request; sau compaction còn 11 file, **$0.08/ngày**
— chênh **50×** cho một bảng vỏn vẹn 10 MB. Khoản đắt hơn là thứ NB2 đo: file nhỏ
rải đều làm `min/max` stats chồng lấn nhau, nên planner **không loại được file
nào**; sau Z-ORDER chỉ 1 trong 55 file phải đọc (**55×**). Small file không chỉ
tốn tiền request — nó phá luôn khả năng data skipping.

**Cách khắc phục.** Không đưa pipeline lên production nếu chưa có job OPTIMIZE
định kỳ, và theo dõi `avg_file_size` như một SLO chứ không phải việc dọn dẹp.
