# Phản Ánh: Anti-patterns trong Data Lakehouse

## Rủi Ro Cao Nhất: **Biến Đổi Schema Không Kiểm Soát**

Team của chúng tôi có nguy cơ cao nhất với **biến đổi schema mà không có kiểm soát** vì:

### 1. Nguyên Nhân Gốc
Nhiều upstream data producers ghi vào Bronze layer không đồng bộ với tên cột, kiểu dữ liệu và ràng buộc nullable không nhất quán. Nếu không có xác thực schema tại lúc ingestion, những xung đột này sẽ lặng lẽ lan truyền vào Silver và Gold layers.

### 2. Tác Động Trên Production
- **Query bị hỏng**: Các aggregation ở Silver/Gold bị break khi có cột hoặc kiểu dữ liệu không mong đợi.
- **Chất lượng dữ liệu giảm**: Các metrics trở nên không tin cậy (ví dụ: NULLs bị hiểu nhầm thành giá trị mặc định, dẫn tới tính toán chi phí sai).
- **Rủi ro tuân thủ**: Những thay đổi schema không được ghi chép sẽ fail audit và kiểm tra quy định.

### 3. Bằng Chứng từ Lab Này
- **NB1**: Chứng minh schema enforcement (block write `age=str`) và controlled evolution qua `schema_mode="merge"`.
- **NB2**: Small-file problem và Z-ordering đảm bảo query reliability—rất quan trọng khi schema churn làm tăng I/O.
- **NB3**: Time-travel và RESTORE cung cấp khả năng rollback khi phát hiện schema issues trên production.

### 4. Chiến Lược Giảm Thiểu
1. **Schema contracts**: Enforce Delta table contracts tại Bronze ingestion để reject breaking changes.
2. **Controlled merge**: Dùng `schema_mode="merge"` chỉ cho backward-compatible additions; audit tất cả schema changes.
3. **Data quality gates**: Chạy validation checks trước khi promote lên Silver (ví dụ: null counts, type distributions).
4. **Time-travel governance**: Lưu giữ transaction history cho compliance và incident investigation.
