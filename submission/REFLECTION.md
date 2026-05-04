# Reflection — Day 18 Lakehouse Lab

## Name: Nguyen Van Thuc
## StudentID: 2A202600238

## Anti-patterns vướng phải

**Vấn đề 1: Query chậm trên big data (Missing Partitioning)**

Team gặp phải tình trạng query duyệt toàn bộ dataset thay vì dùng partition pruning. Nguyên nhân: chưa partitionBy theo `date` hoặc thời gian khi viết Silver/Gold, khiến DuckDB/DuckDB phải scan hết 200K+ rows mỗi lần query. Tác động: query aggregate lên 100K rows chậm 5-10×, phải đợi 30+ giây thay vì 3-5 giây. Giải pháp: thêm `partition_by=["date"]` vào `write_deltalake()` ở Silver layer. Z-order trên `model` cũng giúp filter-by-model dashboard nhanh hơn.

**Vấn đề 2: Dedup chậm (Inefficient Row Numbering)**

Ban đầu dùng naive `GROUP BY request_id` rồi aggregate, nhưng khi có retries (cùng request_id nhưng timestamps khác nhau), phải windowing với `ROW_NUMBER()` để chọn đúng 1 row. Nguyên nhân: lúc đầu không dùng `PARTITION BY request_id ORDER BY ts` nên logic dedup sai. Tác động: Silver nhận số rows bằng Bronze (dedup không hoạt động), deliverable không pass.

## Bài học

Partitioning và windowing phải là reflex — chúng chi phí O(n) nhưng tiết kiệm 10-100× effort ở query time. Không thể optimize later, phải design vào schema từ đầu.
