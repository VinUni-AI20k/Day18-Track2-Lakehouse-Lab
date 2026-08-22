# REFLECTION

## Hoàng Ngọc Thạch - 2A202600068

Từ các kiến thức thực hành trong Notebooks (đặc biệt là NB2 và NB4), anti-pattern từ slide §5 mà team dễ vướng phải nhất là **"The Small File Problem" (Vấn đề file nhỏ - Thiếu Compaction)**.

**Vì sao?**
Hệ thống hiện tại của team ghi nhận log LLM calls liên tục (streaming) với tần suất cao, nhưng mỗi payload lại rất nhỏ. Nếu chỉ ghi append liên tục vào lớp Bronze mà không xử lý, Delta Lake sẽ liên tục sinh ra hàng nghìn file Parquet với dung lượng rất bé (chỉ vài KB). 

Hậu quả là khi query tổng hợp dữ liệu cho lớp Silver hoặc Gold, hệ thống sẽ mất quá nhiều thời gian cho I/O và đọc metadata (list files) thay vì thực sự tính toán dữ liệu. Điều này làm giảm hiệu suất nghiêm trọng (full table scan rất chậm).

**Cách khắc phục rút ra từ Lab:**
- Cần lên lịch chạy lệnh `OPTIMIZE` (compaction) định kỳ để gom các file nhỏ thành các file lớn tối ưu hơn.
- Kết hợp `Z-ORDER BY` trên các trường thường xuyên filter (như `model_id` hoặc `timestamp/date`) để tận dụng sức mạnh của Data Skipping, giúp giảm lượng file phải đọc (files-pruned) và tăng tốc độ xử lý lên nhiều lần.
