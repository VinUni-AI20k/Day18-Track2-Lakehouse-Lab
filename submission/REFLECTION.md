# Reflection

Anti-pattern nhóm tôi dễ gặp nhất là **không có kế hoạch bảo trì bảng**. Dữ liệu LLM observability được ghi liên tục theo từng request; nếu chỉ tập trung đưa dữ liệu vào Bronze mà không quy định compaction, clustering, snapshot expiry và orphan removal, số lượng object sẽ tăng nhanh hơn dung lượng thực tế.

Kết quả NB6 làm rủi ro này rất cụ thể: compaction giảm 200 file xuống 11 file, clustering giúp bỏ qua 90% file cho point query, còn VACUUM của `deltalake` không nhìn thấy file do writer crash trước khi commit. Iceberg cũng cho thấy expire snapshot từ 20 xuống 3 nhưng chưa tự xoá manifest bị bỏ lại. Vì vậy, “đã bật VACUUM/expiry” chưa đồng nghĩa với dữ liệu được dọn sạch.

Nhóm tôi nên coi maintenance là một phần của data contract: đặt SLO cho kích thước file và số snapshot, chạy bốn job theo lịch, theo dõi chi phí object request, và cảnh báo khi chênh lệch giữa file trên storage với file được transaction log/catalog tham chiếu tăng bất thường.
