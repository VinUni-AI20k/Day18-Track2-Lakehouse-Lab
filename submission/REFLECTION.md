# Reflection — Day 18 Lakehouse Lab

**Anti-pattern dễ vướng nhất:** "Dump tất cả vào S3 — raw JSON, không có schema enforcement."

Trong dự án xây dựng chatbot bán xe VinFast, hệ thống thu hàng triệu conversation logs, intent classification results và vehicle recommendation payloads mỗi ngày. Nếu đổ thẳng toàn bộ JSON thô vào S3 mà không enforce schema từ Bronze layer, hậu quả là **data swamp**: không biết cột nào tồn tại, kiểu dữ liệu thay đổi tuỳ lúc, không thể query đáng tin cậy.

Đặc biệt nguy hiểm khi chatbot tích hợp nhiều nguồn (API spec thay đổi, model version mới trả field khác) — không có schema contract thì mỗi lần downstream consumer đọc data đều gặp breaking change.

**Bài học từ lab:** Giải pháp đúng là dùng Delta Lake ngay từ Bronze layer để enforce schema, từ chối ghi sai kiểu (như NB1 đã demo). Schema evolution phải là opt-in có chủ đích (`schema_mode="merge"`), không phải mặc định. Medallion architecture (Bronze → Silver → Gold) kết hợp dedup và validation ở Silver đảm bảo data consumer luôn đọc được dữ liệu sạch, có cấu trúc — tránh tình trạng "data lake" biến thành "data swamp" khi dự án scale.
