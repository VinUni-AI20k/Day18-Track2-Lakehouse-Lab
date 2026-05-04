# Lab 18 Reflection

<!-- Viết ≤ 200 chữ: anti-pattern nào trong slide §5 team bạn dễ vướng nhất, vì sao? -->

Anti-pattern mà nhóm tôi dễ vướng nhất là **"schema drift"** — tức là để schema thay đổi tự do qua thời gian mà không có kiểm soát rõ ràng.

Trong thực tế, khi team phát triển nhanh, các engineer thường thêm cột mới vào pipeline mà không thông báo cho downstream consumers. Với Delta Lake, nếu bật `schema_mode="merge"` một cách mặc định thay vì opt-in, các bảng Silver/Gold sẽ dần tích lũy các cột NULL không rõ nguồn gốc, khiến aggregation cho ra kết quả sai hoặc query chậm hơn do file stats bị loãng.

Bài lab NB1 đã cho tôi thấy rõ điều này: bad-schema write bị block theo mặc định là một thiết kế đúng đắn — schema evolution phải là quyết định chủ động, không phải side effect. Về sau khi làm pipeline LLM observability (NB4), tôi sẽ giữ nguyên schema enforcement ở tầng Bronze và chỉ cho phép merge có kiểm soát ở tầng Silver trở lên.
