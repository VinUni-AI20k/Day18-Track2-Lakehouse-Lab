# Lab18 Reflection — Anti-pattern trong Slide §5

## Anti-pattern dễ vướng nhất của team tôi

**Partition hot-spot** — Đây là anti-pattern mà team tôi dễ gặp nhất trong thực tế.

### Vì sao dễ vướng?

1. **Áp lực time-to-market**: Khi cần deliver nhanh, developer thường dùng `partitionBy("date")` hoặc `bucketBy` mà không phân tích workload thực tế. Một bảng LLM calls với lượng request không đều giữa các ngày (giờ cao điểm, ngày nghỉ) sẽ tạo partition có kích thước chênh lệch lớn.

2. **Không visualize data distribution**: Không ai kiểm tra row count per partition trước khi production. Khi thấy query chậm, mới phát hiện partition "hot" có vài triệu rows trong khi partition khác chỉ vài trăm.

3. **Fallback quen thuộc**: Developer thường xử lý bằng cách thêm filter rồi tự hỏi "sao không ai complain" — nhưng thực ra là user không biết nó chậm vì đang filter đúng partition.

### Hậu quả cụ thể

- Query trên partition hotSpot có thể chậm hơn 10-50× so với partition nhỏ
- Khi chạy `OPTIMIZE` hoặc `VACUUM`, hiệu quả không đều vì file sizes không đồng đều
- Time travel / restore bị ảnh hưởng khi history không evenly distributed

### Cách tránh

- Trước khi production: chạy `df.repartition(n).write` để distribute đều, hoặc dùng `coalesce` sau khi partition
- Monitor partition sizes bằng metrics trước khi user report
- Dùng `z-order` trên column có cardinality cao thay vì chỉ partitionBy date

---

Đây là anti-pattern cụ thể trong medallion pipeline: dùng date partitioning đơn giản mà không kiểm tra skewed distribution, dẫn đến hot-spot partition gây bottleneck cho toàn bộ Silver→Gold query.