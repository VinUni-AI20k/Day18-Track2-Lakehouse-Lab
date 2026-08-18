# Reflection

Anti-pattern có rủi ro cao nhất với dữ liệu của nhóm là **small files**. Trong
NB2 và NB6, việc ghi liên tục các micro-batch tạo hàng trăm file Parquet nhỏ.
Mỗi file vẫn cần thao tác mở file, đọc metadata và lập kế hoạch truy vấn, nên
chi phí điều phối có thể lớn hơn thời gian xử lý dữ liệu thực tế. Kết quả đo
trước và sau compaction cho thấy số file giảm mạnh; Z-order còn cho phép bỏ qua
phần lớn file khi truy vấn theo `user_id`.

Trong hạ tầng thực tế, anti-pattern này dễ xuất hiện từ streaming jobs có trigger
quá ngắn hoặc partition quá chi tiết. Hậu quả là độ trễ truy vấn tăng, metadata
phình to và chi phí object-storage API cao. Nhóm nên đặt kích thước file mục
tiêu, chạy compaction theo lịch, theo dõi số file trên mỗi partition và chỉ
clustering theo các cột lọc có giá trị vận hành rõ ràng.
