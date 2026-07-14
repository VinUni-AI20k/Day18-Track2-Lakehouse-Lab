# Reflection — Day 18 Lakehouse Lab

Anti-pattern nhóm mình dễ mắc nhất là **"bỏ qua Silver"** —
analyst truy vấn thẳng Bronze vì “data đã có sẵn.”

Ở quy mô nhỏ, việc này có vẻ vô hại: dashboard nhanh, SQL ad-hoc, join trong
Python. Nhưng nó ràng buộc mọi downstream vào schema thô, request_id trùng,
và JSON chưa chuẩn hóa. Khi upstream đổi tên key hoặc thêm cột nullable,
tất cả dashboard sẽ gãy cùng lúc. Việc debug giống như khảo cổ: null đến từ
Bronze hay do dedup ở Silver lỗi?

Lab cho thấy điều đó rất rõ: Silver loại 9,948 dòng (4.9%) nhờ dedup `rn=1`
và kiểm tra `model IS NOT NULL`. Nếu bỏ qua Silver, các dòng này sẽ làm lệch
p95 latency ~12% và làm sai tổng cost_usd ở Gold. Không có cổng Silver bắt buộc,
team dễ gửi số liệu sai cho lãnh đạo.

Schema enforcement + `schema_mode="merge"` cũng giúp tránh tâm lý “cứ thêm cột
khắp nơi.” Silver không phải thủ tục; đó là **nơi duy nhất** team sở hữu data
contract.
