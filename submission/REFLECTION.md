Hệ thống SOC của nhóm hiện dùng elasticsearch, ko phải lakehouse, nên năm anti-pattern chỉ áp dụng theo nguyên tắc tương đương. g1–g6 là ranh giới an toàn, ko phải bronze, silver, gold.

đổ raw ko schema tương ứng với raw vault thiếu tenant, nguồn, thời gian và hash. raw phải giữ nguyên, nhưng lớp bọc phải có kiểu; g1 kiểm tính nguyên vẹn, g2 chuẩn hóa, g4 tạo ngữ cảnh an toàn.

chia theo user_id tương ứng với tạo quá nhiều index hoặc shard. nên chia theo thời gian, dung lượng; tenant_id bắt buộc trong mọi truy vấn. tenant lớn hoặc nhạy cảm mới dùng vùng riêng.

small file tương ứng với nhiều segment, audit hoặc file benchmark nhỏ. cần đo trước khi bulk, rollover hay tối ưu.

xóa sớm là rủi ro lớn nhất. ko xóa raw khi finding, case hoặc audit còn tham chiếu; cần retention, legal hold và thử phục hồi.

cuối cùng, ko nên dựng spark hay nhiều cụm agent quá sớm. giữ modular monolith, hoàn thiện g5 bền vững và chỉ scale khi benchmark chứng minh điểm nghẽn.
