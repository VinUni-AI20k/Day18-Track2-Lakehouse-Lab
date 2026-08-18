# Reflection

Trần Hoàng Long - 2A202601646 - Track 2

Trong top 5 reflection được học thì tôi thấy việc bỏ qua OPTIMIZE là một điều tại hại vì nó dẫn tới small-file problem. Trong pipeline thực tế thì sẽ thường ghi nhiều lần, ingest theo kiểu streaming/micro-batch (e.g. event log, log llm call), mỗi dòng một ít. Điển hình như kịch bản NB2 khi có 200 lần append, 5000 dòng mỗi lần. Nếu không có job compaction định kì thì số file nhỏ (small-file) sẽ được tích lũy dân dần và rồi chỉ được phát hiện khi query chậm hẳn. 

NB2 cho ta thấy cái giá của việc chủ quan khi bỏ qua OPTIMIZE. Một query mất 190ms trên 200 file nhỏ. Sau khi compact + z_order(["user_id"]), chỉ còn lại 55 file và query chỉ mất 16ms. Nhanh hơn gần 12 lần và giảm 1/55 file. Khi soi correctness, vấn đề sẽ không lộ vì dữ liệu vẫn đúng chỉ chậm dần theo thời gian. Nên việc OPTIMIZE và Z-ORDER định kỳ là một điều bắt buộc.  