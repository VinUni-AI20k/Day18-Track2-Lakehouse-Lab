# Báo cáo bài lab Day 18 — Lakehouse

**Sinh viên:** Truong Minh Hoang (`2A202602004`)  
**Môi trường thực thi:** JupyterLab kernel `127.0.0.1:8889`, ngày 18/08/2026  
**Công nghệ:** Delta Lake lightweight, PyIceberg và DuckDB; chạy offline, không dùng API hoặc model bên ngoài.

## 1. Kết quả theo rubric

| Notebook | Kết quả đã kiểm chứng | Tiêu chí rubric |
|---|---|---|
| NB1 | Tạo được Delta table và `_delta_log/`; ghi `age=str` bị chặn; `schema_mode="merge"` thêm cột `tier`; DuckDB nhận diện 2 nhóm tier. | Delta creation, schema enforcement/evolution — 8 điểm |
| NB2 | OPTIMIZE/Z-ORDER hoàn tất; speedup đo được **10,1×** và pruning **55,0×**. | Small files, speed/pruning, compaction — 12 điểm |
| NB3 | MERGE thành công 100K dòng; RESTORE hoàn tất; history có **5 version** gồm RESTORE; số dòng `score < 0` sau restore bằng 0. | Time travel, MERGE, RESTORE — 12 điểm |
| NB4 | Bronze, Silver và Gold đều được ghi xuống storage; Gold có **8 ngày × 3 model = 24 dòng**; có p50/p95, cost và error rate. | Medallion architecture — 12 điểm |
| NB5 | Bảng được tạo qua catalog; hidden partition pruning, metadata walk, rename giữ field ID và 2 partition spec đều PASS. | Iceberg catalog/control plane — 13 điểm |
| NB6 | Đủ 5 job maintenance: compaction ≥10×, clustering skip ≥50%, vacuum thu hồi bytes, tìm/xóa 3 Delta orphan, checkpoint, Iceberg expiry còn 3 snapshot và orphan sweep. | Maintenance — 13 điểm |
| NB7 | Đo amplification random-read; int8 đạt ngưỡng kích thước/recall/topic fidelity; semantic search bằng SQL; tái hiện lifecycle bug và CDF delete events. | Vector/multimodal lifecycle — 13 điểm |
| NB8 | Replay đúng version đã pin; `tools/list` được cache (5 lượt gọi/1 catalog read); destructive call yêu cầu xác nhận; task poll hoàn tất; đủ 4 nhóm Art. 10 và xử lý erasure. | Agent provenance — 11 điểm |

## 2. Phân tích các kết quả chính

- **NB2:** pruning ổn định hơn thời gian chạy. Truy vấn user mục tiêu chỉ cần đọc 1/55 file, tương đương 55× file-pruning.
- **NB6:** `VACUUM` không phát hiện file orphan chưa từng commit. Với Iceberg, `expire_snapshots` chỉ loại metadata khỏi snapshot hiện hành; cần chạy orphan sweep tiếp theo để thu hồi file thực tế.
- **NB7:** vector index chỉ là derived state. Sau khi xóa dữ liệu, lakehouse trả về 0 hit nhưng external index cũ vẫn trả về hit; CDF cung cấp document ID để đồng bộ xóa.

## 3. Artefact nộp bài

- Tám notebook đã chạy, giữ output cells: `notebooks/01_delta_basics.ipynb` đến `notebooks/08_agents_provenance.ipynb`.
- Ảnh kết quả NB1–NB8 trong `submission/screenshots/`.
- `STORAGE_LAYOUT_DELTA_LOG.png`: layout `_lakehouse/` và nội dung một Delta transaction-log JSON.
- `REFLECTION.md`: 185 từ, không vượt giới hạn 200 từ.

## 4. Kiểm thử tái lập

- `scripts/run_all.py`: **8/8 notebook PASS**.
- Pytest: **24 tests PASS**, không có error output trong 8 notebook.
- Đã xử lý lỗi Windows giữ lock SQLite của PyIceberg bằng cách đóng các catalog handle trước khi xóa catalog directory trong `reset_catalog()`.

## 5. Kết luận

Bài lab đáp ứng các cổng assert của cả 8 notebook và có đầy đủ output định lượng để đối chiếu rubric. Các minh chứng ảnh tập trung vào PASS block, storage layout và Delta transaction log; phần giải thích trong báo cáo làm rõ ý nghĩa vận hành của pruning, maintenance và lifecycle consistency.
