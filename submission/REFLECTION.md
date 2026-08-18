# REFLECTION — Anti-pattern từ slide §5

## Anti-pattern dễ vướng nhất: **Bỏ qua Silver layer, nhảy thẳng Bronze → Gold**

### Lý do dễ vướng:

**1. Áp lực thời gian.**
Khi deadline đuổi, đội ngũ thường muốn đưa dữ liệu lên Gold càng nhanh càng tốt để "có kết quả". Silver bị xem là bước trung gian không cần thiết, ai đó sẽ "refactor sau". Nhưng refactor sau hiếm khi xảy ra.

**2. Thiếu định nghĩa rõ ràng về "Gold".**
Không có contract cứng giữa 3 layers, Gold dễ bị biến thành "dump everything here" — vừa chứa dữ liệu thô, vừa chứa business metrics. Query performance tệ, data quality không kiểm soát được.

**3. Tư duy "raw is better".**
Một số engineer nghĩ rằng giữ nguyên Bronze là an toàn nhất. Nhưng Bronze không clean, không validated — downstream dashboards lấy sai dữ liệu, không ai biết vì sao.

### Hậu quả:
- Data quality không ai chịu trách nhiệm
- Gold table bị duplicate logic ở nhiều nơi
- Khi schema thay đổi ở nguồn, không có chỗ nào "intercept" để fix — toàn bộ pipeline phải sửa lại

### Giải pháp đã học:
Enforce schema ở Bronze, validate ở Silver, aggregate ở Gold — mỗi layer có ownership rõ ràng.