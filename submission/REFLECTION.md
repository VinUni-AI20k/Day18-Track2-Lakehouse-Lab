# Báo cáo Reflection - Lab 18 Lakehouse

**Họ và tên:** Phạm Lê Hoàng Nam  
**MSHV:** 2A202600416

---

## Phản tư về Anti-pattern

Dựa trên các nội dung thực hành và lý thuyết, anti-pattern mà hệ thống dữ liệu của nhóm tôi dễ gặp rủi ro nhất là **"Small File Problem" (Vấn đề tệp tin quá nhỏ)**.

**Lý do:**
Trong bối cảnh hệ thống thực tế thường xuyên tiếp nhận dữ liệu streaming hoặc các lô (batch) nhỏ liên tục (ví dụ: log API từ người dùng, telemtry data), nếu ghi thẳng vào Data Lakehouse mà không có cơ chế quản lý, hệ thống sẽ nhanh chóng sinh ra hàng ngàn tệp dữ liệu kích thước cực nhỏ. Hậu quả là thời gian truy vấn bị kéo dài đáng kể do chi phí I/O (overhead) của việc đọc metadata và mở tệp, tương tự như vấn đề đã gặp phải trong Notebook `02_optimize_zorder`.

**Hướng khắc phục:**
Để giảm thiểu rủi ro này, chúng tôi sẽ cần thiết lập lịch trình bảo trì dữ liệu định kỳ bằng tính năng `OPTIMIZE` (nhằm gom gộp / compact các tệp nhỏ) kết hợp với `Z-ORDER` theo các trường thường xuyên được truy vấn. Việc tận dụng tính năng tự động tối ưu hóa (auto-compaction) của Delta Lake cũng sẽ giúp duy trì hiệu năng ổn định về dài hạn mà không đòi hỏi thao tác thủ công.

---

## Các minh chứng (Evidence)

Dưới đây là các ảnh màn hình minh chứng cho các bài thực hành đã thực hiện thành công:

- **NB1 - Delta Basics:** [01_delta_basics_evd.png](./01_delta_basics_evd.png)
- **NB2 - Optimize & Z-Order:** [02_optimize_zorder_evd.png](./02_optimize_zorder_evd.png)
- **NB3 - Time Travel:** [03_time_travel_evd.png](./03_time_travel_evd.png)
- **NB4 - Medallion Pipeline:** [04_medallion_evd.png](./04_medallion_evd.png)
- **Minh chứng Delta Log on disk:** [delta_log_evd.png](./delta_log_evd.png)

---

## Bonus: Minh hoạ Luồng CDC & Late-arriving Data

Nhằm chứng minh khả năng xử lý Late-Arriving Data trong Delta Lake thông qua cơ chế `MERGE`, một kịch bản Proof-of-Concept (PoC) phân tích sự kiện đã được lập trình bằng Python.

**1. Trạng thái bảng Silver ban đầu:**
Dữ liệu của xe `TRIP_123` đang ở trạng thái `IN_PROGRESS`.

```text
Silver initial state:
shape: (1, 4)
┌──────────┬──────────────────┬─────────────┬─────────────────────┐
│ trip_id  ┆ driver_mapped_id ┆ status      ┆ updated_at          │
│ ---      ┆ ---              ┆ ---         ┆ ---                 │
│ str      ┆ str              ┆ str         ┆ str                 │
╞══════════╪══════════════════╪═════════════╪═════════════════════╡
│ TRIP_123 ┆ HASH_ABC999      ┆ IN_PROGRESS ┆ 2026-05-04 10:00:00 │
└──────────┴──────────────────┴─────────────┴─────────────────────┘
```

**2. Quá trình Merge CDC Batch mới:**
Hệ thống nhận một lô dữ liệu Bronze mới, trong đó có sự kiện trễ mạng:

- `TRIP_123`: Cập nhật `COMPLETED` lúc `10:15:00`.
- `TRIP_123`: Cập nhật cũ (Late Data) `PICKED_UP` lúc `10:05:00` đến trễ mạng.
- `TRIP_456`: Sự kiện mới `IN_PROGRESS` lúc `10:20:00`.

**3. Kết quả Terminal sau khi MERGE lưu xử lý bản ghi trễ:**
Bằng cách sử dụng logic `when_matched_update_all("s.updated_at > t.updated_at")`, Delta tiến hành bỏ qua hoàn toàn bản ghi bị trễ (`PICKED_UP` 10:05) và chỉ nhận bản ghi mới nhất.

```text
Silver table after MERGE (SCD Type 1 with late-data handling):
shape: (2, 4)
┌──────────┬──────────────────┬─────────────┬─────────────────────┐
│ trip_id  ┆ driver_mapped_id ┆ status      ┆ updated_at          │
│ ---      ┆ ---              ┆ ---         ┆ ---                 │
│ str      ┆ str              ┆ str         ┆ str                 │
╞══════════╪══════════════════╪═════════════╪═════════════════════╡
│ TRIP_123 ┆ HASH_ABC999      ┆ COMPLETED   ┆ 2026-05-04 10:15:00 │
│ TRIP_456 ┆ HASH_XYZ111      ┆ IN_PROGRESS ┆ 2026-05-04 10:20:00 │
└──────────┴──────────────────┴─────────────┴─────────────────────┘
```

**Lưu ý:** Diagram minh họa trực quan tương tác bằng HTML & Mermaid cho kịch bản sự kiện này có thể được xem và tương tác trực tiếp ở [submission/bonus/poc/demo_diagram.html](./bonus/poc/demo_diagram.html).
