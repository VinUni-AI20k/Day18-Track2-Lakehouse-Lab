# Reflection: Lakehouse Architecture Anti-Patterns

Trong slide §5, anti-pattern mà team dễ vướng phải nhất là biến Bronze layer thành **"Data Swamp" (Bãi lầy dữ liệu)** do thiếu Schema Enforcement ngay từ đầu.

**Vì sao lại dễ vướng?**
Đặc thù khi làm việc với hệ thống multi-agent là lượng trace log, prompt payload và response sinh ra liên tục dưới dạng JSON lồng nhau (nested JSON). Vì muốn capture lại toàn bộ flow tương tác và rút ngắn thời gian ghi log, team thường có xu hướng dump trực tiếp toàn bộ raw data thẳng vào Bronze mà không kiểm soát chặt chẽ cấu trúc. 

**Hệ lụy & Bài học:**
Sự dễ dãi này ở Bronze tạo ra bottleneck cực lớn cho khâu xử lý phía sau. Khi data bị phân mảnh hoặc schema thay đổi đột ngột từ API của model, việc parse dữ liệu lên Silver để làm sạch, deduplicate và bóc tách các metrics (như latency hay cost) trở nên vô cùng tốn resource, thậm chí làm gãy pipeline. Việc áp dụng tính năng schema validation và `schema_mode="merge"` của Delta Lake trong Lab này chính là chốt chặn quan trọng để giải quyết triệt để thói quen trên.