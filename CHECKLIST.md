# ✅ Checklist — Day 18 Lakehouse Lab (Track 2)

> Theo dõi tiến độ theo từng bước thực tế.  
> Cập nhật: `[x]` = đã xong, `[ ]` = chưa làm.  
> Tổng: **100 điểm** + submission requirements.

---

## 🛠️ PHASE 0 — Setup & Môi trường

- [x] **P0-1** Kiểm tra Python 3.10–3.14 có sẵn (`python3 --version`)
- [x] **P0-2** Chạy `make setup` — tạo venv + cài ~180 MB deps (deltalake, pyiceberg, duckdb, polars, numpy, jupyterlab, jupytext, pytest)
- [x] **P0-3** Chạy `make smoke` — 9 kiểm tra offline (~5s), tất cả phải xanh
- [x] **P0-4** Chạy `make data` — sinh 200K-row Bronze cho NB4
- [x] **P0-5** Chạy `make data-ai` — sinh corpus multimodal + agent traces cho NB7/NB8 ✔ (2000 docs, 200 blobs, 1578 steps/300 sessions)
- [x] **P0-6** Chạy `make test` — 22 pytest phải xanh hết ✔ (24 dots, exit 0)
- [x] **P0-7** Mở Jupyter Lab bằng `make lab` → `http://localhost:8888`

---

## 📚 PHASE 1 — Part A: Foundations (44 điểm)

### NB1 — `01_delta_basics.py` (8 điểm)

- [x] **NB1-1** *(4 đ)* Delta table được tạo; thư mục `_delta_log/` xuất hiện với các file JSON commit — `make run-all` PASS 0.7s
- [x] **NB1-2** *(2 đ)* Schema enforcement: write với `age=str` bị chặn (raise exception) — assert pass
- [x] **NB1-3** *(2 đ)* `schema_mode="merge"` thêm cột `tier` thành công (opt-in schema evolution) — assert pass

### NB2 — `02_optimize_zorder.py` (12 điểm)

- [x] **NB2-1** *(3 đ)* Tái hiện vấn đề small-file: ≥ 100 files trước khi OPTIMIZE — `make run-all` PASS 17.9s
- [x] **NB2-2** *(6 đ)* Đạt speedup ≥ 3× **HOẶC** files-pruned ratio ≥ 10× — assert pass
- [x] **NB2-3** *(3 đ)* `numFiles` giảm đáng kể sau OPTIMIZE (compact thành công) — assert pass

### NB3 — `03_time_travel.py` (12 điểm)

- [x] **NB3-1** *(4 đ)* `history()` hiển thị ≥ 5 version **kể cả row RESTORE** — `make run-all` PASS 0.8s
- [x] **NB3-2** *(4 đ)* MERGE upsert 100K dòng thành công — assert pass
- [x] **NB3-3** *(4 đ)* RESTORE rollback dữ liệu xấu; count `score < 0` = 0 sau restore — assert pass

### NB4 — `04_medallion.py` (12 điểm)

- [x] **NB4-1** *(4 đ)* Bronze, Silver, Gold đều có mặt trên storage layer (`_lakehouse/`) — `make run-all` PASS 1.2s
- [x] **NB4-2** *(4 đ)* Silver dedup: số dòng Silver < Bronze (loại bỏ duplicate thành công) — assert pass
- [x] **NB4-3** *(4 đ)* Gold đúng: có p50/p95, cost_usd, error_rate cho ≥ 7 ngày × 3 model — assert pass

---

## 🏛️ PHASE 2 — Part B: Lakehouse 2026 (50 điểm)

### NB5 — `05_iceberg_catalog.py` (13 điểm)

- [x] **NB5-1** *(3 đ)* Table được tạo **qua catalog**; partition spec dùng `day(ts)` — `make run-all` PASS 2.2s
- [x] **NB5-2** *(5 đ)* Hidden-partition pruning ≥ 5× đo bằng `plan_files()`, lọc trên `ts` (không phải `ts_day`) — assert pass
- [x] **NB5-3** *(1 đ)* Three-tier metadata được duyệt; tỉ lệ metadata:data byte được báo cáo — assert pass
- [x] **NB5-4** *(4 đ)* Rename giữ nguyên `field_id` (metadata-only); ≥ 2 partition spec cùng tồn tại, table vẫn đọc được — assert pass

### NB6 — `06_maintenance.py` (13 điểm)

- [x] **NB6-1** *(4 đ)* **Job 1 — Compaction**: ≥ 10× ít file hơn; số trước/sau được báo cáo — `make run-all` PASS 18.1s
- [x] **NB6-2** *(3 đ)* **Job 2 — Clustering**: ≥ 50% file có thể bỏ qua cho point query, chứng minh từ min/max stats — assert pass
- [x] **NB6-3** *(3 đ)* **Job 3 — Expiry**: Delta vacuum thu hồi bytes; Iceberg giảm xuống 3 snapshot — assert pass
- [x] **NB6-4** *(2 đ)* **Job 4 — Orphans**: 3 Delta orphan được tìm + xóa; stranded Iceberg manifest lists được sweep — assert pass
- [x] **NB6-5** *(1 đ)* **Job 5 — Checkpoint**: `*.checkpoint.parquet` + `_last_checkpoint` được ghi — assert pass

### NB7 — `07_vectors_multimodal.py` (13 điểm)

- [x] **NB7-1** *(4 đ)* Random-access amplification đo được (≥ 5×) và giải thích qua row-group granularity — `make run-all` PASS 1.1s
- [x] **NB7-2** *(4 đ)* int8 quantization ≥ 3× nhỏ hơn trên đĩa; recall@10 **và** topic fidelity đều được báo cáo — assert pass
- [x] **NB7-3** *(1 đ)* Semantic search chạy dưới dạng SQL và trả về các neighbor đúng chủ đề — assert pass
- [x] **NB7-4** *(4 đ)* **Lifecycle bug được tái hiện**: 0 hits in-table, > 0 hits trong stale external index — assert pass

### NB8 — `08_agents_provenance.py` (11 điểm)

- [x] **NB8-1** *(3 đ)* Trajectories qua medallion; Silver partition theo `agent_version`; Gold bao gồm cả hai policy — `make run-all` PASS 1.9s
- [x] **NB8-2** *(3 đ)* Training run pin version của table; replay tại version đó khớp chính xác — assert pass
- [x] **NB8-3** *(3 đ)* MCP surface: cacheable `tools/list` (5 turns → 1 catalog read), `input_required` trước destructive calls, task poll hoàn thành — assert pass
- [x] **NB8-4** *(2 đ)* Cả 4 rổ Art. 10 tồn tại như partition; UNCLASSIFIED rows bị loại khỏi trainable set — assert pass

---

## 🔬 PHASE 3 — Part C: Reproducibility (6 điểm)

- [x] **C-1** *(2 đ)* `make test` xanh — 22 pytest pass ✔ (exit code 0)
- [x] **C-2** *(4 đ)* `make run-all` xanh từ clean `make setup` — **8/8 passed in 43.8s** ✔

---

## 📦 PHASE 4 — Submission Requirements

- [x] **S-1** 8 notebook đã chạy với output cells được giữ lại (`.ipynb` với output) — `notebooks/*.ipynb` đã được generate + executed
- [x] **S-2** Tạo thư mục `submission/screenshots/` ✔
- [x] **S-3** Evidence: `submission/screenshots/tree_lakehouse.txt` + `submission/screenshots/delta_log_commit0.json` ✔
- [x] **S-4** Viết `submission/REFLECTION.md` (≤ 200 từ) ✔ — anti-pattern: Orphan File Accumulation
- [ ] **S-5** *(Tùy chọn)* `submission/bonus/ARCHITECTURE.md` cho Bonus Challenge
- [ ] **S-6** Push lên fork và tạo PR về upstream với title: `[NXX] Lab18 — Nguyễn Xuân Quân`

---

## 📊 Tóm tắt điểm

| Phase | Điểm tối đa | Trạng thái |
|---|---|---|
| Part A — Foundations | 44 | ✅ Hoàn thành (NB1–NB4 PASS) |
| Part B — Lakehouse 2026 | 50 | ✅ Hoàn thành (NB5–NB8 PASS) |
| Part C — Reproducibility | 6 | ✅ Hoàn thành (make test + make run-all PASS) |
| **Tổng** | **100** | ✅ **8/8 notebooks PASS in 43.8s** |

---

> 💡 **Ghi chú quan trọng từ rubric:**  
> - `make run-all` xanh = pass phần cơ học. Cần giải thích được các con số để đạt điểm cao nhất.  
> - NB6 & NB7 có "measured finding contradicts common belief" — phải nhận ra và giải thích để đủ điểm.  
> - `VACUUM` không dọn orphan chưa từng commit — NB6 đo, phải tự viết phép hiệu tập hợp.  
> - `expire_snapshots` của Iceberg chỉ đụng metadata, không xóa file avro — NB6 Job 3+4 là cặp.
