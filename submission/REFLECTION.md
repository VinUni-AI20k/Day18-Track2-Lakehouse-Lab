# Reflection

Theo em, dữ liệu của nhóm có nguy cơ gặp **“Small Files Problem”** nhiều nhất.

Pipeline của nhóm xử lý dữ liệu theo các tầng Bronze → Silver → Gold và có nhiều dữ liệu được cập nhật liên tục. Nếu mỗi lần pipeline chạy lại tạo thêm nhiều file nhỏ, số lượng file sẽ tăng dần theo thời gian. Ban đầu vấn đề này có thể không đáng chú ý, nhưng khi dữ liệu lớn hơn, việc đọc và quản lý quá nhiều file nhỏ sẽ làm truy vấn chậm hơn và tăng overhead.

Điều em thấy đáng chú ý là pipeline vẫn có thể chạy đúng và cho ra kết quả đúng dù vấn đề này đang tồn tại. Vì vậy, nó khá dễ bị bỏ qua nếu nhóm chỉ kiểm tra số lượng row hoặc tính đúng của dữ liệu.

Từ bài lab này, em nghĩ ngoài việc đảm bảo pipeline xử lý dữ liệu chính xác, nhóm cũng cần quan tâm đến cách dữ liệu được lưu trữ, đặc biệt là kích thước và số lượng file khi dữ liệu tăng lên.
