# REFLECTION — Lab 18 Lakehouse Anti-Pattern

**Anti-pattern nguy hiểm nhất mà team chúng em dễ vướng:**

## Tiny-Write Accumulation (Small-File Problem)

Team chúng em chạy nhiều streaming job (Kafka consumer, Lambda triggered per event) ghi trực tiếp vào Delta/Iceberg mỗi 5–30 giây. Đây chính là **tiny-write accumulation** — mỗi lần ghi đúng 1 batch, tất cả đúng schema, không lỗi. Nhưng sau 1 đêm, 200 commits = 200 file nhỏ xíu. Sau vài tuần: hàng triệu file.

Tại sao nguy hiểm hơn các anti-pattern khác:

- **Không có error, không có alert.** Job chạy "đúng", mọi test pass. Vấn đề nằm ở accumulated state, không phải single-run behavior.
- **Ảnh hưởng bất ngờ khi scale.** 10K file → query chậm x2. 1M file → platform collapse. Không ai báo trước.
- **Fix tốn chi phí.** Compaction cần rewrite toàn bộ data — job nặng, tốn time và money. Mà chúng em càng đợi lâu, data càng nhiều, fix càng đắt.
- **Dễ bỏ qua trong thiết kế ban đầu.** Ai cũng nghĩ "batch thì có vấn đề gì" nhưng tích lũy qua thời gian mới lộ.

**Giải pháp cần áp dụng sớm:** Compaction cron job (ví dụ: mỗi 2h cho hot data, mỗi đêm cho cold data) không phải là "nice-to-have" mà là **bắt buộc từ ngày đầu**. Tương tự, monitoring file count, không chỉ data size, vì count drive cost.
