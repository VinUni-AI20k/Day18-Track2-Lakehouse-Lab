# Reflection
- Họ và tên: Lê Minh Hoàng
- Mã HV: 2A202600101

Trong các anti-pattern ở slide §5, dự án cá nhân của em có nguy cơ mắc phải **"Data Swamp" (Hồ dữ liệu biến thành đầm lầy)** cao nhất. 

**Nguyên nhân:**
Vì đang làm việc solo và muốn hoàn thiện mô hình nhanh nhất có thể, em thường ưu tiên việc đẩy toàn bộ dữ liệu từ các nguồn (API, file raw) vào Data Lake cho xong mà bỏ qua bước thiết kế Schema cẩn thận. Việc chạy đua tiến độ một mình khiến khâu quản lý chất lượng ở lớp Bronze dễ bị lỏng lẻo, dẫn đến tình trạng:
1. Dữ liệu rác, thiếu cột hoặc sai kiểu dữ liệu bị lọt vào hệ thống mà không được kiểm soát.
2. Một thời gian sau nhìn lại, chính em cũng quên mất ý nghĩa các cột và không rõ phiên bản dữ liệu nào là đáng tin cậy.
3. Sinh ra quá nhiều file nhỏ (small files) sau các lần test liên tục làm chậm tốc độ query.

**Giải pháp hướng tới:** 
Mặc dù làm solo, em sẽ cố gắng rèn kỷ luật áp dụng cơ chế **Schema Enforcement** của Delta Lake và kiến trúc **Medallion** để chặn dữ liệu bẩn ngay từ vòng ngoài (trước khi vào lớp Silver), đồng thời gom các lệnh dọn dẹp (`OPTIMIZE`, `ZORDER`) vào một script tự động để hệ thống luôn gọn gàng.
