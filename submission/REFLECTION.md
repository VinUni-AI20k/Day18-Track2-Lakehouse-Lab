# Reflection — Day 18 Lakehouse Lab

**Họ tên:** Dương Quang Đông  
**MSSV:** 2A202600445  
**Path:** Lightweight (deltalake + DuckDB + Polars)

## Anti-pattern dễ vướng nhất: Small-File Problem

Trong các anti-pattern ở slide §5, **Small-File Problem** là rủi ro lớn nhất đối với team chúng tôi.

Lý do: hầu hết pipeline thực tế đều dùng streaming/micro-batch ingestion — mỗi lần ghi tạo một file mới. Sau vài ngày, Bronze layer có thể chứa hàng nghìn file nhỏ (< 1 MB). Hậu quả là query chậm do overhead mở file, metadata phình to, và cost cloud storage tăng vì mỗi LIST/GET request đều tính phí.

Qua NB2, tôi thấy rõ tác động: 200 small files khiến point-query mất ~588 ms; sau OPTIMIZE + Z-ORDER chỉ còn ~68 ms (speedup 8.7×). Quan trọng hơn, Z-order giúp Delta chỉ cần đọc 1/55 file thay vì scan toàn bộ (files-pruned ratio 55×).

**Giải pháp thực tế:** schedule `OPTIMIZE` job chạy định kỳ (hourly/daily) trên Bronze/Silver layer, kết hợp Z-ORDER trên cột filter phổ biến. Đây là thao tác đơn giản nhưng team mới thường bỏ qua vì pipeline "vẫn chạy được" — cho đến khi query chậm không chấp nhận được.
