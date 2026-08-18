# Reflection: Lakehouse Anti-Patterns trong Quản lý Data Lineage bằng AI Agent

Dự án của nhóm tích hợp các AI agent để tự động hóa việc theo dõi nguồn gốc dữ liệu (data lineage) qua các pipeline đa tầng. Anti-pattern mà dữ liệu của nhóm có nguy cơ gặp phải cao nhất là **"Decoupled Index Drift" (Lỗi vòng đời giữa bảng Lakehouse và Index bên ngoài)**.

### Lý do gặp rủi ro
Các Agent liên tục ghi nhận trajectory, provenance và ánh xạ cột vào bảng Lakehouse (Delta/Iceberg), đồng thời đồng bộ sang cơ sở dữ liệu Vector/Graph bên ngoài để phục vụ truy vấn ngữ nghĩa nhanh qua giao thức MCP.

Khi các snapshot trong Lakehouse bị rollback, dọn dẹp hoặc xóa định kỳ thông qua các tác vụ bảo trì (`RESTORE`, `VACUUM`, `expire_snapshots`), index bên ngoài rất dễ giữ lại các vector và con trỏ lineage lỗi thời. Điều này dẫn đến hiện tượng **"Silent Provenance Hallucination"**, khi Agent tra cứu ra các nút lineage ảo trỏ tới các file Parquet không còn tồn tại trên disk.

### Giải pháp
Cần đồng bộ hóa giao dịch (transactional synchronization): lưu trực tiếp vector embedding và metadata vào chính bảng Lakehouse, hoặc xây dựng cơ chế kích hoạt tự động vô hiệu hóa index bên ngoài ngay khi commit log có thay đổi.