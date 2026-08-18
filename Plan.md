# 🗂️ Plan.md — Day 18 Lakehouse Lab (Track 2)
**Sinh viên:** Hồ Quang Minh — 2A202601906  
**Deadline:** nộp PR trước khi hết hạn Track-2 Daily Lab  
**Tổng điểm:** 100 pts = 30% track grade

---

## 📌 Tổng quan chiến lược

Tất cả 8 notebook **đã có code đầy đủ sẵn** trong `notebooks/` — không cần viết thêm logic.
Nhiệm vụ là: **setup môi trường → chạy từng notebook → lưu output → nộp bài**.

> **Windows note:** Repo dùng `make` (Linux/macOS). Trên Windows cần chạy lệnh thủ công thay cho `make`.

---

## ✅ Checklist thực thi (theo thứ tự)

### Phase 0 — Setup môi trường
- [ ] **0.1** Kiểm tra Python version: `python --version` (cần 3.10–3.14)
- [ ] **0.2** Tạo virtual environment:
  ```powershell
  cd "d:\Desktop\TRACK2_Day18_2A202601906_HoQuangMinh"
  python -m venv .venv
  .venv\Scripts\Activate.ps1
  ```
- [ ] **0.3** Cài dependencies:
  ```powershell
  pip install -r requirements.txt
  ```
- [ ] **0.4** Smoke test (xác nhận cài đặt thành công):
  ```powershell
  python scripts\verify_lite.py
  ```
  > Kỳ vọng: 9 checks xanh, không lỗi

- [ ] **0.5** Sinh dữ liệu Bronze cho NB4:
  ```powershell
  python scripts\generate_data_lite.py
  ```
- [ ] **0.6** Sinh dữ liệu AI cho NB7/NB8:
  ```powershell
  python scripts\generate_ai_data.py
  ```

---

### Phase 1 — Chạy từng Notebook (trong Jupyter)

#### Bước quan trọng trước khi mở Jupyter: Convert `.py` → `.ipynb`
```powershell
# Chạy từ thư mục gốc repo
jupytext --to notebook notebooks\01_delta_basics.py
jupytext --to notebook notebooks\02_optimize_zorder.py
jupytext --to notebook notebooks\03_time_travel.py
jupytext --to notebook notebooks\04_medallion.py
jupytext --to notebook notebooks\05_iceberg_catalog.py
jupytext --to notebook notebooks\06_maintenance.py
jupytext --to notebook notebooks\07_vectors_multimodal.py
jupytext --to notebook notebooks\08_agents_provenance.py
```
> ⚠️ Bước này bắt buộc trên Windows — nếu bỏ qua, Jupyter sẽ không thấy file `.ipynb`

#### Khởi động Jupyter Lab
```powershell
jupyter lab
```
> Mở trình duyệt → `http://localhost:8888`  
> Vào thư mục `notebooks/` và thấy 8 file `.ipynb`

---

#### 📓 NB1 — Delta Lake Basics (8 pts)
**File:** [`notebooks/01_delta_basics.py`](notebooks/01_delta_basics.py)

- [ ] **1.1** Mở `01_delta_basics.ipynb` trong Jupyter
- [ ] **1.2** Chạy **tất cả cells** (Kernel → Restart & Run All)
- [ ] **1.3** Kiểm tra output cuối cùng thấy:
  ```
  [PASS] _delta_log/ has JSON commits
  [PASS] schema enforcement blocked bad write
  [PASS] tier column added via schema_mode=merge
  [PASS] duckdb sees 2 tier groups
  NB1 complete.
  ```
- [ ] **1.4** Save notebook (giữ output)
- [ ] **1.5** Screenshot: chạy lệnh `tree _lakehouse\scratch\users_delta\_delta_log` và chụp màn hình

**Đầu ra cần đạt:**
| Tiêu chí | Mục tiêu |
|---|---|
| `_delta_log/` có file JSON | ≥ 2 file `.json` |
| Schema enforcement | Blocked `age=str` write |
| Schema evolution | `tier` column xuất hiện |

---

#### 📓 NB2 — Optimize & Z-Order (12 pts)
**File:** [`notebooks/02_optimize_zorder.py`](notebooks/02_optimize_zorder.py)

- [ ] **2.1** Mở `02_optimize_zorder.ipynb`
- [ ] **2.2** Chạy tất cả cells (sẽ mất ~2–5 phút vì ghi 200 batch × 5K rows)
- [ ] **2.3** Kiểm tra output:
  ```
  [PASS] compaction reduced file count
  [PASS] speedup ≥ 3x OR pruning ≥ 10x
  [PASS] stats isolate the target user
  NB2 complete.
  ```
- [ ] **2.4** Save notebook

> ⚠️ Nếu speedup < 3× (chậm do RAM): không sao — `pruning_ratio ≥ 10×` là tiêu chí dự phòng và notebook đã tính sẵn cả hai.

**Đầu ra cần đạt:**
| Tiêu chí | Mục tiêu |
|---|---|
| Files trước OPTIMIZE | ≥ 100 files |
| Speedup **hoặc** pruning | ≥ 3× hoặc ≥ 10× |
| File count giảm | Đáng kể sau compact() |

---

#### 📓 NB3 — Time Travel & MERGE (12 pts)
**File:** [`notebooks/03_time_travel.py`](notebooks/03_time_travel.py)

- [ ] **3.1** Mở `03_time_travel.ipynb`
- [ ] **3.2** Chạy tất cả cells (MERGE 100K rows ~1s trên lightweight)
- [ ] **3.3** Kiểm tra output:
  ```
  [PASS] history ≥ 5 versions
  [PASS] history includes the RESTORE
  [PASS] MERGE recorded in history
  [PASS] bad rows gone after restore
  NB3 complete.
  ```
- [ ] **3.4** Save notebook

**Đầu ra cần đạt:**
| Tiêu chí | Mục tiêu |
|---|---|
| `history()` versions | ≥ 5 (gồm RESTORE row) |
| MERGE upsert | 100K rows thành công |
| RESTORE | `score < 0` count = 0 |

---

#### 📓 NB4 — Medallion Pipeline (12 pts)
**File:** [`notebooks/04_medallion.py`](notebooks/04_medallion.py)

- [ ] **4.1** Mở `04_medallion.ipynb`
- [ ] **4.2** Chạy tất cả cells (tự sinh Bronze nếu chưa có)
- [ ] **4.3** Kiểm tra output cuối:
  - Bronze rows > Silver rows (dedup hoạt động)
  - Gold có ≥ 7 distinct dates × 3 models
  - Columns: `p50_latency_ms`, `p95_latency_ms`, `cost_usd`, `error_rate`
- [ ] **4.4** Save notebook

**Đầu ra cần đạt:**
| Tiêu chí | Mục tiêu |
|---|---|
| Bronze/Silver/Gold trên disk | Đều tồn tại trong `_lakehouse/` |
| Silver dedup | Silver rows < Bronze rows |
| Gold metrics | ≥ 7 ngày × 3 models |

---

#### 📓 NB5 — Iceberg Catalog (13 pts)
**File:** [`notebooks/05_iceberg_catalog.py`](notebooks/05_iceberg_catalog.py)

- [ ] **5.1** Mở `05_iceberg_catalog.ipynb`
- [ ] **5.2** Chạy tất cả cells
- [ ] **5.3** Kiểm tra output:
  ```
  [PASS] pruning ratio ≥ 5x
  [PASS] ≥ 10 snapshots
  [PASS] field_id stable on rename
  [PASS] ≥ 2 partition specs
  [PASS] all rows readable
  NB5 complete.
  ```
- [ ] **5.4** Save notebook

> 💡 NB5 dùng catalog riêng (`CAT = "nb5"`) — không xung đột với NB6/NB8.

**Đầu ra cần đạt:**
| Tiêu chí | Mục tiêu |
|---|---|
| Hidden-partition pruning | ≥ 5× qua `plan_files()` |
| Field ID bền vững | `latency_millis` giữ `field_id=4` sau rename |
| Partition specs | ≥ 2 `spec_id` cùng tồn tại |

---

#### 📓 NB6 — Table Maintenance (13 pts)
**File:** [`notebooks/06_maintenance.py`](notebooks/06_maintenance.py)

- [ ] **6.1** Mở `06_maintenance.ipynb`
- [ ] **6.2** Chạy tất cả cells (sẽ mất ~3–5 phút vì 200 micro-batches)
- [ ] **6.3** Kiểm tra 4 jobs chạy đủ:
  - **Job 1 Compaction:** ≥ 10× ít file hơn
  - **Job 2 Clustering:** ≥ 50% files skippable
  - **Job 3 Expiry:** Delta VACUUM reclaim bytes; Iceberg → 3 snapshots
  - **Job 4 Orphans:** 3 orphan Delta tìm & xoá; Iceberg stranded manifests swept
  - **Job 5:** `*.checkpoint.parquet` + `_last_checkpoint` tồn tại
- [ ] **6.4** Save notebook

> 🔑 **Điểm quan trọng về Job 4:** `VACUUM` Delta **không** xoá orphan chưa từng commit — notebook đo điều này bằng set-difference thủ công.

**Đầu ra cần đạt:**
| Job | Tiêu chí |
|---|---|
| Job 1 Compaction | Trước/sau file count, ≥ 10× |
| Job 2 Clustering | Skip ratio ≥ 50% từ min/max stats |
| Job 3 Expiry | Iceberg: 20 → 3 snapshots |
| Job 4 Orphans | 3 orphan Delta tìm được + xoá |
| Job 5 Checkpoint | File `.checkpoint.parquet` + `_last_checkpoint` |

---

#### 📓 NB7 — Vectors & Multimodal (13 pts)
**File:** [`notebooks/07_vectors_multimodal.py`](notebooks/07_vectors_multimodal.py)

- [ ] **7.1** Mở `07_vectors_multimodal.ipynb`
- [ ] **7.2** Chạy tất cả cells (tự sinh data nếu chưa có)
- [ ] **7.3** Kiểm tra:
  - Random-access amplification ≥ 5×
  - int8 nhỏ hơn float32 ≥ 3×
  - Semantic SQL search trả kết quả đúng topic
  - **Lifecycle bug:** 0 hits in-table, > 0 hits in stale index
- [ ] **7.4** Save notebook

> ⚠️ **Lưu ý cast:** Khi query DuckDB trên Delta vector column, phải cast: `emb::FLOAT[256]`

**Đầu ra cần đạt:**
| Tiêu chí | Mục tiêu |
|---|---|
| Random-read amplification | ≥ 5× (và giải thích row-group granularity) |
| int8 quantization | ≥ 3× nhỏ hơn; recall@10 + topic fidelity báo cáo |
| Semantic SQL search | Trả on-topic neighbours |
| **Lifecycle bug tái hiện** | 0 hits in-table, > 0 hits in stale external index |

---

#### 📓 NB8 — Agents & Provenance (11 pts)
**File:** [`notebooks/08_agents_provenance.py`](notebooks/08_agents_provenance.py)

- [ ] **8.1** Mở `08_agents_provenance.ipynb`
- [ ] **8.2** Chạy tất cả cells
- [ ] **8.3** Kiểm tra:
  - Silver partitioned by `agent_version`
  - Training run pins table version; replay khớp chính xác
  - MCP: 5 turns → 1 catalog read (cacheable `tools/list`)
  - 4 rổ Art. 10 thành partitions; UNCLASSIFIED không vào training set
- [ ] **8.4** Save notebook

**Đầu ra cần đạt:**
| Tiêu chí | Mục tiêu |
|---|---|
| Trajectories medallion | Silver partition theo `agent_version` |
| Version pinning | Replay tại pinned version khớp chính xác |
| MCP surface | 5 turns → 1 catalog read; `input_required` trước destructive calls |
| EU AI Act Art. 10 | 4 rổ: APPROVED, RESTRICTED, UNCLASSIFIED, PROHIBITED |

---

### Phase 2 — Kiểm tra tổng thể

- [ ] **9.1** Chạy toàn bộ tests:
  ```powershell
  python -m pytest tests/ -v
  ```
  > Kỳ vọng: **22 tests xanh**

- [ ] **9.2** Chạy headless (cổng chấm điểm của giảng viên):
  ```powershell
  python scripts\run_all.py
  ```
  > Kỳ vọng: **8/8 notebooks PASS** — đây là điều kiện đủ cho Part C (6 pts)

---

### Phase 3 — Chuẩn bị submission

- [ ] **10.1** Tạo thư mục submission:
  ```powershell
  mkdir submission\screenshots
  ```

- [ ] **10.2** Chụp màn hình `_lakehouse/` structure:
  ```powershell
  # Chụp output của lệnh này
  tree "_lakehouse" /F
  ```

- [ ] **10.3** Chụp màn hình nội dung 1 file `_delta_log/*.json`:
  ```powershell
  # Lấy file đầu tiên trong delta log của NB1
  Get-Content "_lakehouse\scratch\users_delta\_delta_log\00000000000000000000.json" | ConvertFrom-Json | ConvertTo-Json -Depth 5
  ```

- [ ] **10.4** Lưu screenshots vào `submission/screenshots/`

- [ ] **10.5** Tạo thư mục và viết **REFLECTION.md** (≤ 200 từ):
  ```powershell
  mkdir submission -ErrorAction SilentlyContinue
  New-Item -Path submission\REFLECTION.md -ItemType File -Force
  ```
  **Template mẫu để điền vào** (chỉnh sửa theo trải nghiệm thực của bạn):
  ```markdown
  # Reflection — Top 5 Lakehouse Anti-Patterns

  Anti-pattern nguy hiểm nhất với team tôi là **Small Files** (NB2/NB6).

  Hệ thống của team thường ingest data qua streaming với trigger ngắn (mỗi vài giây),
  dẫn đến hàng nghìn file nhỏ tích tụ mỗi ngày. Lab NB6 đã đo: 200 micro-batch
  tạo ra 200 file, query chậm phi tuyến tính. Nếu không có compaction job chạy
  định kỳ, metadata overhead tăng và file-skipping (Z-order) mất tác dụng vì
  quá nhiều file cần scan.

  NB6 cũng làm lộ ra điều không ai nói: VACUUM Delta không xoá orphan chưa từng
  commit — file từ job crash biến mất khỏi log nhưng vẫn chiếm disk. Job 3 và
  Job 4 phải chạy thành cặp; expire_snapshots một mình không giảm được hoá đơn S3.
  ```

- [ ] **10.6** Commit & push toàn bộ (bao gồm `.ipynb` có output):
  ```powershell
  git add notebooks\*.ipynb submission\
  git status   # kiểm tra trước khi commit
  git commit -m "[2A202601906] Lab18 — Ho Quang Minh"
  git push origin main
  ```
  > PR sẽ tự làm thủ công sau khi push.

---

## 🗺️ Sơ đồ luồng thực thi

```
Phase 0 (Setup)
    │
    ├─ pip install -r requirements.txt
    ├─ python scripts/verify_lite.py      ← 9 checks xanh
    ├─ python scripts/generate_data_lite.py
    └─ python scripts/generate_ai_data.py
    │
Phase 1 (Notebooks — chạy tuần tự)
    │
    ├─ NB1 → NB2 → NB3 → NB4   (Part A — Foundations)
    └─ NB5 → NB6 → NB7 → NB8   (Part B — Lakehouse 2026)
    │
Phase 2 (Kiểm tra)
    │
    ├─ pytest tests/ -v          ← 22 tests
    └─ python scripts/run_all.py ← 8/8 PASS
    │
Phase 3 (Submission)
    │
    ├─ Screenshots _lakehouse/ tree
    ├─ submission/REFLECTION.md
    └─ git commit + PR
```

---

## ⏱️ Ước tính thời gian

| Phase | Thời gian ước tính |
|---|---|
| Phase 0 — Setup | ~10–20 phút (tải packages) |
| NB1 — Delta basics | ~2 phút |
| NB2 — Optimize/Z-Order | ~5–8 phút (200 batch writes) |
| NB3 — Time Travel | ~2 phút |
| NB4 — Medallion | ~3 phút |
| NB5 — Iceberg | ~3 phút |
| NB6 — Maintenance | ~5–8 phút (200 micro-batches) |
| NB7 — Vectors | ~3 phút |
| NB8 — Agents | ~3 phút |
| Phase 2 — Tests | ~2 phút |
| Phase 3 — Submission | ~10 phút |
| **TỔNG** | **~50–65 phút** |

---

## 🐛 Troubleshooting nhanh

| Triệu chứng | Fix |
|---|---|
| `python3: command not found` | Dùng `python` thay vì `python3` trên Windows |
| `'DeltaTable' has no attribute 'files'` | Đang dùng `deltalake` 0.x — chạy lại `pip install -r requirements.txt` |
| `No function matches array_cosine_similarity` | Thiếu cast: `emb::FLOAT[256]` (NB7) |
| NB2 speedup < 3× | OK — dùng `files-pruned ≥ 10×` thay thế (notebook đã in cả hai) |
| `make` không chạy trên Windows | Thay bằng lệnh Python/PowerShell tương đương trong Plan này |
| `AttributeError: 'Relation' object has no attribute 'arrow'` | Upgrade DuckDB: `pip install duckdb>=1.1` |
| Jupyter không thấy `.ipynb` | Chạy `jupytext --to notebook notebooks/*.py` |

---

## 📊 Bảng điểm theo dõi tiến độ

| NB | Điểm | Trạng thái | Ghi chú |
|---|---|---|---|
| NB1 Delta basics | 8 | ⬜ Chưa làm | |
| NB2 Optimize/Z-Order | 12 | ⬜ Chưa làm | |
| NB3 Time Travel | 12 | ⬜ Chưa làm | |
| NB4 Medallion | 12 | ⬜ Chưa làm | |
| NB5 Iceberg | 13 | ⬜ Chưa làm | |
| NB6 Maintenance | 13 | ⬜ Chưa làm | |
| NB7 Vectors | 13 | ⬜ Chưa làm | |
| NB8 Agents | 11 | ⬜ Chưa làm | |
| Part C Reproducibility | 6 | ⬜ Chưa làm | `make test` + `make run-all` |
| **TỔNG** | **100** | | |

> Cập nhật trạng thái: ⬜ Chưa làm → 🔄 Đang làm → ✅ Hoàn thành → ❌ Lỗi cần fix

---

## 🎯 Bonus Challenge (không bắt buộc)

Nếu còn thời gian, viết `submission/bonus/ARCHITECTURE.md` — chọn một bài toán lakehouse thực tế (ví dụ: LLM observability 1B req/ngày) và thiết kế chiến lược lưu trữ. Xem [`BONUS-CHALLENGE.md`](BONUS-CHALLENGE.md) để biết chi tiết.

---

*Plan được tạo bởi Antigravity AI — 2026-08-18*
