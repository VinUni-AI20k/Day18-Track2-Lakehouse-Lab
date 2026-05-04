# Reflection - Lab 18 Data Lakehouse

Trong các anti-pattern được nhắc tới ở slide §5, team mình dễ vướng phải lỗi **Small-File Problem** (Vấn đề nhiều file nhỏ) nhất.

**Lý do:**
Do đặc thù của bài toán quan sát LLM (LLM Observability), dữ liệu thường được ghi nhận liên tục theo thời gian thực (streaming-like ingestion) mỗi khi có một request được thực hiện. Nếu chúng ta ghi trực tiếp mỗi batch nhỏ này vào hệ thống mà không có cơ chế gom nhóm (buffering) hay nén định kỳ, Lakehouse sẽ nhanh chóng bị tràn ngập bởi hàng ngàn file Parquet có kích thước chỉ vài KB. 

Việc có quá nhiều file nhỏ sẽ làm tăng chi phí quản lý Metadata tại tầng Transaction Log và khiến các engine truy vấn phải thực hiện quá nhiều thao tác I/O mở/đóng file. Qua bài Lab 18 (NB2), mình nhận thấy hiệu năng truy vấn có thể bị kéo thấp đáng kể nếu không thực hiện `OPTIMIZE` và `Z-ORDER` định kỳ để gộp các file nhỏ và tối ưu hóa khả năng lọc dữ liệu (file skipping).
