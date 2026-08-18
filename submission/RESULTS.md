# Lab 18 — Measured Results

Tài liệu này tóm tắt các số đo trong tám notebook đã thực thi. Các notebook `.ipynb` là nguồn bằng chứng đầy đủ.

## Part A — Foundations (44/44 tiêu chí máy chấm)

| Notebook | Kết quả đo | Cách đọc kết quả |
|---|---|---|
| NB1 | `_delta_log` có commit JSON; bad-schema write bị chặn; `tier` được thêm bằng `schema_mode="merge"` | Transaction log tạo ACID và audit trail; schema evolution phải được bật chủ động. |
| NB2 | 200 → 55 file; speedup 9.8×; pruning 55×, chỉ 1/55 file có thể chứa `user_id=4242` | Compaction giảm chi phí mở file; Z-order làm min/max chặt để engine file-skip. Pruning là phép đo ổn định hơn wall-clock. |
| NB3 | MERGE 100K trong 0.15 s; lịch sử 5 version gồm RESTORE; `score < 0` còn 0 dòng | RESTORE là transaction mới, không xóa audit trail; rollback giữ khả năng giải thích dữ liệu đã thay đổi thế nào. |
| NB4 | Bronze 200,000 → Silver 190,052, loại 9,948 duplicate; Gold có 8 ngày × 3 model = 24 dòng | Bronze giữ raw evidence, Silver áp dụng data-quality contract, Gold trả lời trực tiếp câu hỏi latency/cost/error. |

## Part B — Lakehouse 2026 (50/50 tiêu chí máy chấm)

| Notebook | Kết quả đo | Cách đọc kết quả |
|---|---|---|
| NB5 | Hidden partition pruning 10× (10 → 1 file); 10 snapshot; rename giữ `field_id=4`; `spec_id` gồm 1 và 2; 5,500 dòng vẫn đọc được | Iceberg suy ra `day(ts)` từ filter trên `ts`; field-ID và partition evolution cho phép đổi schema/layout mà không rewrite dữ liệu cũ. |
| NB6 | Compaction 200 → 11 file, 18×; clustering skip 90%; vacuum thu hồi 16.1 MB; 3 Delta orphan được xóa; Iceberg 20 → 3 snapshot và quét 17 manifest list; checkpoint tồn tại | Expiry và orphan removal là một cặp. VACUUM dựa vào tombstone có thể bỏ sót file chưa từng commit; age guard bảo vệ writer đang chạy. |
| NB7 | Random-read amplification 200×; INT8 nhỏ hơn 5.8×; recall@10 = 0.904; topic fidelity = 1.000; lakehouse còn 0 hit nhưng stale index còn 8; CDF phát 8 delete | Column pruning bảo vệ analytical scan nhưng row-group granularity gây random-read amplification. Vector DB là derived index và phải nhận delete event. |
| NB8 | Silver có 2 `agent_version`; replay version 0 trả đúng 1,578 step; 5 lượt agent chỉ gây 1 catalog read; destructive call trả `input_required`; đủ 4 provenance bucket; xóa subject 8 → 0 dòng | Pin version tạo reproducibility contract. Human approval phải nằm ở protocol boundary; provenance phải là cột/partition có thể query và audit. |

## Part C — Reproducibility (6/6 tiêu chí máy chấm)

- Smoke test: 9/9.
- Pytest thực tế: 24/24 (README cũ ghi 22).
- `scripts/run_all.py`: 8/8 notebook pass trong 61.9 giây ở lượt xác minh cuối.
- Tám `.ipynb` đều có execution count, output cells và không có error output.

## Hai kết luận production cần nêu khi được hỏi

1. Small-file problem là lỗi vận hành do streaming bình thường cộng với thiếu maintenance schedule; không phải dữ liệu sai. Theo dõi file count và kích thước trung bình thường hữu ích hơn chỉ nhìn tổng byte.
2. Time travel hỗ trợ audit/rollback nhưng xung đột với right-to-erasure. Current version sạch chưa có nghĩa dữ liệu đã biến mất khỏi snapshot cũ; retention phải là quyết định governance có chủ đích.
