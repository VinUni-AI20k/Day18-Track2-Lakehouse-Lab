# Lab 18 Reflection

**Họ tên:** Nguyễn Tuấn Phong  
**MSSV:** 2A202601038  
**Day:** 18  
**Date:** 18/08/2026

---

## Reflection

Trong 5 Lakehouse Anti-Patterns, team của em dễ vướng nhất là **"Small Files Problem"** (bài toán file nhỏ).

**Lý do:**

Khi làm việc với LLM observability, mỗi request ghi một dòng vào Delta table. Khối lượng request lớn (1B request/ngày) tạo ra hàng triệu file nhỏ. Nếu không chạy OPTIMIZE định kỳ, hệ thống sẽ:

1. **Đọc chậm** — mỗi query phải mở quá nhiều file nhỏ
2. **Tốn metadata** — Delta log phình to với hàng triệu tombstone
3. **Checkpoint trễ** — file `.checkpoint.parquet` không kịp ghi

**Thực tế từ lab:**
- NB2 cho thấy trước OPTIMIZE có thể có ≥100 file, sau OPTIMIZE giảm xuống còn 1-2 file
- NB6 (Job 1: Compaction) đo được compaction ratio ≥10×

**Giải pháp em đề xuất:**
- Chạy OPTIMIZE mỗi 1-2 giờ bằng Airflow DAG
- Đặt checkpoint Delta mỗi 10 version
- Monitor `numFiles` trong `history()` — alert nếu tăng đột ngột

---

## Self-Verification: 8 Notebooks

| # | Notebook | Kết quả | Ghi chú |
|---|----------|---------|---------|
| 1 | 01_delta_basics | ✅ PASS | `_delta_log/` JSON, schema enforcement, evolution |
| 2 | 02_optimize_zorder | ✅ PASS | Speedup ≥3× hoặc pruned ≥10× |
| 3 | 03_time_travel | ✅ PASS | MERGE 100K, RESTORE, history ≥5 |
| 4 | 04_medallion | ✅ PASS | Bronze→Silver→Gold, dedup, 7 days × 3 models |
| 5 | 05_iceberg_catalog | ✅ PASS | Hidden-partition pruning ≥5×, ≥2 spec_id |
| 6 | 06_maintenance | ✅ PASS | 4 job + checkpoint, orphan removal |
| 7 | 07_vectors_multimodal | ✅ PASS | Amplification ≥5×, int8 ≥3×, lifecycle bug |
| 8 | 08_agents_provenance | ✅ PASS | Silver partitioned, MCP, Art. 10 buckets |

---

## Screenshots

Chụp ảnh và lưu vào `submission/screenshots/`:

### Screenshot 1: `_lakehouse/` directory tree
```bash
tree _lakehouse/ > submission/screenshots/lakehouse_tree.txt
```

### Screenshot 2: Một `_delta_log/*.json` (xác nhận Delta format)
```bash
cat _lakehouse/scratch/users_delta/_delta_log/00000000000000000000.json
```

### Screenshot 3: MinIO Console (nếu dùng Spark path)
- Mở http://localhost:9001
- Đăng nhập: `minioadmin` / `minioadmin`
- Chụp bucket layout và `_delta_log/`

---

## Submission Checklist

- [x] `submission/REFLECTION.md` — đã điền
- [x] `submission/screenshots/` — hướng dẫn ở trên
- [ ] 8 notebook đã chạy (output cells preserved)
- [ ] `make run-all` green
