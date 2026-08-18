# Reflection

Anti-pattern nhóm dễ vướng nhất: **dữ liệu phái sinh/lịch sử âm thầm giữ lại thứ đáng lẽ phải xoá** - time travel và derived index xung đột trực tiếp với quyền xoá dữ liệu

NB8 tự nêu thẳng mâu thuẫn: xoá `user_007` khỏi bảng governed (8→0 dòng ở version hiện tại), nhưng version cũ (v0) - vẫn truy cập được qua time travel - còn nguyên 8 dòng đó. "Hỗ trợ time travel" và "tôn trọng quyền xoá" xung đột trừ khi retention window là quyết định có chủ đích, không phải mặc định. NB7 đo cùng họ bệnh ở lớp khác: xoá `user_042` khỏi lakehouse (system-of-record) nhưng external vector index - bản sao sync một chiều - vẫn trả về 8/8 tài liệu đã xoá cho RAG, mãi mãi nếu sync không đọc change feed

Rủi ro này không lý thuyết: EU AI Act Art.10 (hiệu lực từ 2/8/2026) và PDPL Việt Nam (Luật 91/2025) đều yêu cầu chứng minh được nguồn gốc *và* xoá được khi có yêu cầu - hai nghĩa vụ mà một pipeline chỉ ghi log/sync một chiều không đáp ứng nổi cùng lúc

Khắc phục: dùng Change Data Feed cho mọi derived index (NB7), và biến retention window thành quyết định viết ra rõ ràng thay vì để mặc định (NB6 Job 3, NB8)
