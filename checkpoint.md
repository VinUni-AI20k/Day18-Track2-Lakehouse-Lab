# 🎯 SỔ TAY CHECKPOINT & HƯỚNG DẪN ĐẠT 100% LAB 18
## Data Lakehouse Architecture (AICB-P2T2 · Day 18)

> **Mục đích tài liệu:** Hướng dẫn từng bước từ cài đặt, thực thi 8 Notebook, giải thích cặn kẽ bản chất từng task (Tại sao làm? Đo cái gì? Bẫy sản xuất ở đâu?), cung cấp câu lệnh chi tiết và nhắc nhở lưu lại toàn bộ chứng cứ để đạt **100/100 điểm** theo đúng [`rubric.md`](rubric.md).

---

## 🗺️ 1. BẢN ĐỒ TỔNG QUAN & CHIẾN LƯỢC ĐẠT ĐIỂM TỐI ĐA

Bài lab được thiết kế gồm **8 Notebook** chia làm 2 phần lớn và hệ thống kiểm thử tự động:

```
                          ┌────────────────────────────────────────────────────────┐
                          │   DAY 18 LAKEHOUSE LAB (100 ĐIỂM)                      │
                          └────────────────────────────────────────────────────────┘
                                    │
        ┌───────────────────────────┴───────────────────────────┐
        ▼                                                       ▼
┌─────────────────────────────────┐   ┌───────────────────────────────────────────────┐
│ PART A: FOUNDATIONS (44đ)       │   │ PART B: LAKEHOUSE 2026 (50đ)                  │
│ • NB1: Delta ACID & Schema (8đ) │   │ • NB5: Iceberg Catalog as Control Plane (13đ) │
│ • NB2: Small Files & Z-Order(12đ│   │ • NB6: 4+1 Mandatory Maintenance Jobs (13đ)   │
│ • NB3: Time Travel & MERGE (12đ)│   │ • NB7: Multimodal, Vectors & Lifecycle (13đ)  │
│ • NB4: Medallion Pipeline (12đ) │   │ • NB8: Agent Trajectory & Provenance (11đ)    │
└─────────────────────────────────┘   └───────────────────────────────────────────────┘
                                    │
                                    ▼
                      ┌───────────────────────────┐
                      │ PART C: REPRODUCIBILITY   │
                      │ • make test (22 pass) (2đ)│
                      │ • make run-all (4đ)       │
                      └───────────────────────────┘
```

### 💎 Điểm khác biệt giữa "Đủ điểm" (Adequate) và "Điểm tối đa" (Top Band)
* **Adequate:** Chỉ chạy code ra số (ví dụ: in ra `speedup = 3.2x` hoặc `pruning ratio = 10x`).
* **Top Band:** Hiểu và giải thích được **cơ chế bên dưới**:
  1. Tại sao Z-ORDER và Hidden Partitioning lại giảm thiểu lượng data quét?
  2. Tại sao `VACUUM` lại không thể dọn orphan file của job bị crash?
  3. Tại sao `expire_snapshots` của Iceberg không làm giảm dung lượng đĩa nếu không quét orphan?
  4. Tại sao embedding vector lưu ngoài Vector DB lại gây lỗi vi phạm quyền riêng tư (Right-to-Erasure)?

---

## ⚙️ 2. CHUẨN BỊ MÔI TRƯỜNG & LỆNH ĐIỀU KHIỂN

Lab hỗ trợ đường chạy **Lightweight Path** (khuyến nghị cho mọi máy, không cần Docker, không cần Java/JVM, chạy thuần Python với `deltalake`, `pyiceberg`, `duckdb`, `polars`).

### 🛠️ Lệnh khởi tạo môi trường (Chạy 1 lần đầu)

#### Trên Linux / macOS / WSL:
```bash
# 1. Tạo venv và cài thư viện
make setup

# 2. Kiểm tra nhanh tính sẵn sàng của môi trường (Smoke test ~5s)
make smoke

# 3. Sinh dữ liệu mẫu cho NB4, NB7, NB8
make data
make data-ai
```

#### Trên Windows (PowerShell nếu không dùng Make):
```powershell
# 1. Tạo môi trường ảo
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2. Cài đặt dependencies
pip install -r requirements.txt

# 3. Chuyển đổi mã nguồn notebook sang ipynb
python -m jupytext --to notebook --update notebooks/*.py

# 4. Smoke test & sinh dữ liệu
python scripts/verify_lite.py
python scripts/generate_data_lite.py
python scripts/generate_ai_data.py
```

### 🚀 Mở giao diện Jupyter Lab để làm bài:
```bash
make lab
# Hoặc trên Windows: .\.venv\Scripts\jupyter.exe lab --notebook-dir=notebooks
```

---

## 📋 3. CHI TIẾT TỪNG TASK: MỤC ĐÍCH, THỰC THI & CHỨNG CỨ LƯU LẠI

---

### 🔹 TASK 1: NOTEBOOK `01_delta_basics` (8 Điểm)
* **File:** [`notebooks/01_delta_basics.py`](file:///c:/Users/ngant/ai_act_lab/lab_18/TRACK2_Day18_2A202601258_TaKimNgan/notebooks/01_delta_basics.py) / `.ipynb`
* **Vấn đề giải quyết:** Trong Data Lake truyền thống (Parquet thuần), ghi đè đồng thời hoặc dữ liệu lỗi kiểu (data corruption) sẽ phá hỏng toàn bộ bảng. Delta Lake bổ sung ACID transaction log (`_delta_log/`) để giải quyết triệt để vấn đề này.

#### 🎯 Mục đích & Điều cần chứng minh:
1. Tạo bảng Delta và thấy được các file commit dạng JSON trong thư mục `_delta_log/`.
2. **Schema Enforcement:** Cố tình ghi 1 dòng có `age="thirty"` (chuỗi thay vì số) → Bị thư viện chặn lại, phát sinh Exception.
3. **Schema Evolution:** Chủ động bổ sung cột mới `tier` bằng cách chỉ định tham số `schema_mode="merge"` (opt-in evolution).
4. **Zero-copy DuckDB:** Đọc dữ liệu Delta qua bộ nhớ Arrow mà không cần tải plugin mạng.

#### 📝 Lệnh thực thi & Kiểm tra:
Chạy tuần tự các cell trong Notebook 1 hoặc chạy lệnh headless:
```bash
python -c "import notebooks.01_delta_basics"
```

#### 📸 CHỨNG CỨ PHẢI LƯU LẠI (Checklist 8đ):
- [ ] **Commit Log:** Terminal/Notebook in ra ít nhất 2 phiên bản commit `v0`, `v1` và thư mục `_lakehouse/scratch/users_delta/_delta_log/` chứa file `00000000000000000000.json`, `00000000000000000001.json` (**4đ**).
- [ ] **Bắt lỗi Schema:** Thông báo `BLOCKED by schema enforcement (expected): ...` xuất hiện khi ghi dữ liệu `age=str` (**2đ**).
- [ ] **Tiến hóa Schema:** Bảng in ra có thêm cột `tier` với giá trị `premium` cho user mới và `null` cho user cũ (**2đ**).
- [ ] **Khối kiểm tra cuối:** In ra `NB1 complete.` và tất cả các mục `[PASS]`.

---

### 🔹 TASK 2: NOTEBOOK `02_optimize_zorder` (12 Điểm)
* **File:** [`notebooks/02_optimize_zorder.py`](file:///c:/Users/ngant/ai_act_lab/lab_18/TRACK2_Day18_2A202601258_TaKimNgan/notebooks/02_optimize_zorder.py) / `.ipynb`
* **Vấn đề giải quyết:** "Bệnh file nhỏ" (Small-file problem) do streaming ingestion tạo ra hàng ngàn file Parquet li ti làm nghẽn metadata và I/O. Z-ORDER clustering giúp nhóm các dòng có cùng giá trị lại gần nhau trong file để engine có thể bỏ qua (file skipping) hàng loạt file không liên quan.

#### 🎯 Mục đích & Điều cần chứng minh:
1. Tái hiện lỗi: Ghi 200 batch nhỏ → sinh ra 200 file Parquet.
2. Đo tốc độ query tìm kiếm `user_id = 4242` trước khi tối ưu (phải scan rất nhiều file).
3. Chạy `OPTIMIZE` (`compact()` gom nhỏ thành lớn) và `z_order(["user_id"])`.
4. Đo tốc độ sau tối ưu: Chứng minh **Speedup ≥ 3×** HOẶC **Files-pruned ratio ≥ 10×**.
5. Đọc trực tiếp min/max stats trong file log để chứng minh chỉ có ~1 file duy nhất chứa `user_id = 4242`.

#### 📝 Lệnh thực thi:
Chạy các cell trong Notebook 2 hoặc:
```bash
python -c "import notebooks.02_optimize_zorder"
```

#### 📸 CHỨNG CỨ PHẢI LƯU LẠI (Checklist 12đ):
- [ ] **Small-file baseline:** In ra `Files before OPTIMIZE: 200` (hoặc ≥ 100 files) (**3đ**).
- [ ] **Hiệu năng vượt trội:** In ra dòng `Speedup: X.Xx` hoặc `Files-pruned ratio: XX.Xx` đạt chuẩn (Speedup ≥ 3× hoặc Pruned ratio ≥ 10×) (**6đ**).
- [ ] **Giảm số file:** Số file giảm rõ rệt (từ 200 xuống còn khoảng 40–50 files với kích thước target 256KB) (**3đ**).
- [ ] **File stats inspect:** Thấy danh sách `file user_id range: [min, max]` và duy nhất file có đánh dấu `← contains target`.
- [ ] **Khối kiểm tra cuối:** In ra `NB2 complete.` với tất cả các mục `[PASS]`.

---

### 🔹 TASK 3: NOTEBOOK `03_time_travel` (12 Điểm)
* **File:** [`notebooks/03_time_travel.py`](file:///c:/Users/ngant/ai_act_lab/lab_18/TRACK2_Day18_2A202601258_TaKimNgan/notebooks/03_time_travel.py) / `.ipynb`
* **Vấn đề giải quyết:** Xử lý sự cố khi pipeline vô tình ghi đè dữ liệu sai/rác (Bad Write/Corruption) mà không cần khôi phục backup hạ tầng vật lý, đồng thời hỗ trợ cập nhật dữ liệu hàng loạt (Upsert / MERGE).

#### 🎯 Mục đích & Điều cần chứng minh:
1. Xây dựng chuỗi phiên bản: `v0` (100k dòng), `v1` (thêm cột tier), `v2` (MERGE upsert 100k dòng: 50k update + 50k insert), `v3` (vô tình nạp 50 dòng rác `score < 0`).
2. Truy vấn Time-travel: Đọc lại chính xác trạng thái tại `version=0` và `version=1`.
3. Khôi phục thảm họa: Gọi `dt.restore(2)` để quay lui trạng thái bảng về đúng `v2`.
4. Kiểm toán: Hành động RESTORE tạo ra commit mới `v4` trong lịch sử mà không xoá lịch sử cũ; kiểm tra không còn dòng nào có `score < 0`.

#### 📝 Lệnh thực thi:
```bash
python -c "import notebooks.03_time_travel"
```

#### 📸 CHỨNG CỨ PHẢI LƯU LẠI (Checklist 12đ):
- [ ] **Lịch sử ≥ 5 versions:** `history()` hiển thị từ `v0` đến `v4`, trong đó có dòng ghi nhận operation `RESTORE` (**4đ**).
- [ ] **MERGE 100K:** Thao tác MERGE ghi nhận thành công trong lịch sử (`MERGE INTO ...`) (**4đ**).
- [ ] **Khôi phục sạch dữ liệu:** Sau restore, câu lệnh đếm `Rows with score<0 after restore` in ra chính xác `0` (**4đ**).
- [ ] **Khối kiểm tra cuối:** `NB3 complete.` và tất cả `[PASS]`.

---

### 🔹 TASK 4: NOTEBOOK `04_medallion` (12 Điểm)
* **File:** [`notebooks/04_medallion.py`](file:///c:/Users/ngant/ai_act_lab/lab_18/TRACK2_Day18_2A202601258_TaKimNgan/notebooks/04_medallion.py) / `.ipynb`
* **Vấn đề giải quyết:** Xây dựng luồng dữ liệu kiến trúc Medallion (Bronze → Silver → Gold) thực tế cho bài toán **LLM Observability** (theo dõi log gọi mô hình, chi phí token, lỗi và độ trễ).

#### 🎯 Mục đích & Điều cần chứng minh:
1. **Bronze (Raw):** Chứa 200,000 dòng log thô ở định dạng JSON dạng string (chứa cả duplicate và lỗi mạng retry).
2. **Silver (Cleaned & Typed):** Parse JSON, chuẩn hóa kiểu dữ liệu, loại bỏ duplicate theo `request_id` → Chứng minh số dòng **Silver < Bronze**.
3. **Gold (Aggregated Metrics):** Tính toán độ trễ p50/p95 (`QUANTILE_CONT`), tổng token, tỷ lệ lỗi và chi phí USD theo từng `(date, model)` trải dài qua **≥ 7 ngày × 3 model** (Claude Haiku, Sonnet, Opus).

#### 📝 Lệnh thực thi:
```bash
python -c "import notebooks.04_medallion"
```

#### 📸 CHỨNG CỨ PHẢI LƯU LẠI (Checklist 12đ):
- [ ] **Thư mục 3 tầng:** Cả 3 tầng bảng tồn tại trên đĩa: `_lakehouse/bronze/`, `_lakehouse/silver/`, `_lakehouse/gold/` (**4đ**).
- [ ] **Dedup thành công:** Terminal in ra `Bronze 200,000 → dedup dropped X,XXX` và `Silver rows < Bronze rows` (**4đ**).
- [ ] **Chỉ số Gold chuẩn:** Bảng Gold có đủ cột `p50_latency_ms`, `p95_latency_ms`, `cost_usd`, `error_rate`; số lượng ngày `Distinct dates >= 7` (**4đ**).
- [ ] **Khối kiểm tra cuối:** `NB4 complete.` và `[PASS]`.

---

### 🔹 TASK 5: NOTEBOOK `05_iceberg_catalog` (13 Điểm)
* **File:** [`notebooks/05_iceberg_catalog.py`](file:///c:/Users/ngant/ai_act_lab/lab_18/TRACK2_Day18_2A202601258_TaKimNgan/notebooks/05_iceberg_catalog.py) / `.ipynb`
* **Vấn đề giải quyết:** Apache Iceberg và kiến trúc **Catalog là Control Plane**. Khắc phục thảm họa của Hive: partition cứng nhắc (quên filter partition bị full-scan tốn tiền tỷ), đổi tên cột làm hỏng bảng, đổi partition layout phải viết lại toàn bộ dữ liệu.

#### 🎯 Mục đích & Điều cần chứng minh:
1. **Tạo bảng qua Catalog:** Không hardcode đường dẫn vật lý; Catalog quản lý metadata location.
2. **Hidden Partitioning:** Định nghĩa partition theo `day(ts)`. Truy vấn chỉ cần filter trên cột gốc `ts` (không cần biết cột `ts_day`), Catalog tự động suy ra partition và đạt **Pruning ratio ≥ 5×**.
3. **Cây Metadata 3 tầng:** Khám phá `metadata.json` → `manifest-list` (avro) → `manifest files` (stats min/max) → `data files` (parquet). Đo tỷ lệ byte metadata / byte data.
4. **Field-ID Evolution:** Đổi tên `latency_ms` thành `latency_millis` nhưng vẫn giữ nguyên `field_id = 4` (thay đổi metadata thuần túy, 0 byte dữ liệu bị ghi lại).
5. **Partition Evolution:** Thêm partition spec mới theo `model`. Dữ liệu cũ và mới với 2 `spec_id` khác nhau cùng tồn tại mà vẫn query trơn tru.

#### 📝 Lệnh thực thi:
```bash
python -c "import notebooks.05_iceberg_catalog"
```

#### 📸 CHỨNG CỨ PHẢI LƯU LẠI (Checklist 13đ):
- [ ] **Tạo bảng qua Catalog:** Bảng được tạo với partition spec `DayTransform(ts)` (**3đ**).
- [ ] **Đo lường Hidden Pruning:** In ra `Pruning ratio: Xx (target ≥ 5×)` khi query với filter `ts` (**5đ**).
- [ ] **Báo cáo Metadata 3 tầng:** In ra số lượng tier 1, tier 2, tier 3 và tỷ lệ `metadata is X.X% of table size` (**1đ**).
- [ ] **Field-ID & Multi-spec:** In ra `field_id` ổn định sau khi rename và `Partition specs in use: [0, 1]` (**4đ**).
- [ ] **Khối kiểm tra cuối:** `NB5 complete.` và `[PASS]`.

---

### 🔹 TASK 6: NOTEBOOK `06_maintenance` (13 Điểm)
* **File:** [`notebooks/06_maintenance.py`](file:///c:/Users/ngant/ai_act_lab/lab_18/TRACK2_Day18_2A202601258_TaKimNgan/notebooks/06_maintenance.py) / `.ipynb`
* **Vấn đề giải quyết:** **4 Job bảo trì bắt buộc (+ Job 5)** của Lakehouse để ngăn ngừa sự cố sập hệ thống và lãng phí chi phí lưu trữ/FinOps.

#### 🎯 Mục đích & Điều cần chứng minh:
1. **Job 1 - Compaction:** Gom 200 file nhỏ streaming thành số lượng file ít hơn ít nhất 10 lần (**≥ 10× fewer files**).
2. **Job 2 - Clustering (Z-ORDER):** Dùng thống kê min/max trong transaction log chứng minh bỏ qua được **≥ 50% số file** khi thực hiện point-query theo `user_id`.
3. **Job 3 - Snapshot Expiry:** Chạy `VACUUM` trên Delta và `expire_snapshots` trên Iceberg (về 3 snapshot).
   > ⚠️ *Bẫy thực tế:* `expire_snapshots` của Iceberg chỉ xóa tham chiếu trong metadata chứ **không xóa file vật lý trên đĩa**.
4. **Job 4 - Orphan Removal:** Tự tạo 3 file orphan do writer bị crash (không nằm trong commit log). Dùng thuật toán hiệu tập hợp (Disk − Live Log) để tìm và xóa sạch 3 file orphan cùng các manifest-list mồ côi của Iceberg.
5. **Job 5 - Checkpoint:** Ghi `*.checkpoint.parquet` và file `_last_checkpoint` để người đọc không phải replay lại 200 file log JSON.

#### 📝 Lệnh thực thi:
```bash
python -c "import notebooks.06_maintenance"
```

#### 📸 CHỨNG CỨ PHẢI LƯU LẠI (Checklist 13đ):
- [ ] **Job 1:** In ra `File reduction: 200 → X (≥ 10× fewer)` (**4đ**).
- [ ] **Job 2:** In ra `skip rate: XX% of files never touched (≥ 50%)` (**3đ**).
- [ ] **Job 3:** `Reclaimed: X.X MB` trên Delta và Iceberg còn đúng 3 snapshots (**3đ**).
- [ ] **Job 4:** In ra `Orphans found: 3` trên Delta và quét sạch các file manifest mồ côi trên Iceberg (**2đ**).
- [ ] **Job 5:** `_last_checkpoint` và file `*.checkpoint.parquet` được sinh ra (**1đ**).
- [ ] **Khối kiểm tra cuối:** `NB6 complete.` và `[PASS]`.

---

### 🔹 TASK 7: NOTEBOOK `07_vectors_multimodal` (13 Điểm)
* **File:** [`notebooks/07_vectors_multimodal.py`](file:///c:/Users/ngant/ai_act_lab/lab_18/TRACK2_Day18_2A202601258_TaKimNgan/notebooks/07_vectors_multimodal.py) / `.ipynb`
* **Vấn đề giải quyết:** Xử lý dữ liệu AI đa phương thức (Ảnh/Media) và Vector Embeddings trong Lakehouse; hiểu hiện tượng khuếch đại đọc ngẫu nhiên và **Lỗi vòng đời (Lifecycle bug / Right-to-Erasure violation)** khi tách biệt Lakehouse và Vector DB.

#### 🎯 Mục đích & Điều cần chứng minh:
1. **Inline vs Pointer:** So sánh lưu byte ảnh trực tiếp trong Parquet vs lưu URI pointer. Chứng minh khi đọc ngẫu nhiên 1 ảnh (`WHERE doc_id = 137`), Parquet phải đọc cả Row Group → **Khuếch đại I/O ≥ 5×** (gây đói GPU).
2. **Lượng tử hóa Vector int8:** Nén vector từ `float32` xuống `int8` giúp giảm dung lượng trên đĩa **≥ 3×** nhưng vẫn giữ được `recall@10 ≥ 0.80` và `topic fidelity ≥ 0.95`.
3. **Semantic Search bằng SQL thuần:** Dùng DuckDB chạy hàm `array_cosine_similarity` ngay trên bảng dữ liệu kết hợp điều kiện quản trị (`WHERE consent_train = true`).
4. **Tái hiện Lifecycle Bug:** Khi người dùng `user_042` yêu cầu xóa dữ liệu (GDPR/Nghị định 13):
   - Lakehouse xóa dòng → Query trả về **0 hit**.
   - Vector DB đồng bộ bên ngoài không nhận được lệnh xóa → Query vẫn trả về **> 0 hit (Vi phạm pháp lý!)**.
   - Giải pháp: Dùng **Change Data Feed (CDF)** để bắt sự kiện `_change_type = 'delete'`.

#### 📝 Lệnh thực thi:
```bash
python -c "import notebooks.07_vectors_multimodal"
```

#### 📸 CHỨNG CỨ PHẢI LƯU LẠI (Checklist 13đ):
- [ ] **Khuếch đại I/O:** In ra `amplification: Xx more bytes than needed (≥ 5×)` (**4đ**).
- [ ] **Nén int8 & Recall:** In ra tỷ lệ nén `int8: X.Xx smaller (≥ 3×)`, `recall@10` và `topic fidelity ≥ 0.95` (**4đ**).
- [ ] **Semantic SQL Search:** Kết quả top-5 tương đồng cùng chủ đề với query (**1đ**).
- [ ] **Lifecycle Bug:** In ra `in_hits = 0` và `ex_hits > 0` cùng sự kiện delete trong CDF (**4đ**).
- [ ] **Khối kiểm tra cuối:** `NB7 complete.` và `[PASS]`.

---

### 🔹 TASK 8: NOTEBOOK `08_agents_provenance` (11 Điểm)
* **File:** [`notebooks/08_agents_provenance.py`](file:///c:/Users/ngant/ai_act_lab/lab_18/TRACK2_Day18_2A202601258_TaKimNgan/notebooks/08_agents_provenance.py) / `.ipynb`
* **Vấn đề giải quyết:** Quản lý vết hành động của AI Agent (Agent Trajectories / Rollouts), thiết kế giao diện **Model Context Protocol (MCP 2026-07-28)** an toàn, và tuân thủ pháp lý **Nguồn gốc dữ liệu huấn luyện (EU AI Act Art. 10 & Luật BV Dữ liệu cá nhân)**.

#### 🎯 Mục đích & Điều cần chứng minh:
1. **Trajectory Medallion:** Lưu vết rollout RL của Agent: Silver phân vùng theo `agent_version` (`policy-v2`, `policy-v3`), Gold tổng hợp hiệu năng (tỷ lệ thành công, chi phí, độ trễ).
2. **Version Pinning:** Ghim số phiên bản bảng (`table_version`) vào metadata đợt train model để bảo đảm tính tái lập 100% (Reproducibility).
3. **Mặt cắt giao tiếp MCP:**
   - Caching danh sách bảng (`tools/list` có TTL): 5 lượt agent gọi chỉ tốn **1 lần đọc catalog thật**.
   - Human-in-the-loop: Gọi thao tác hủy diệt (`delete_rows`) bị chặn và trả về trạng thái `input_required` chờ con người phê duyệt.
   - Long-running tasks: Polling tác vụ quét lớn qua `tasks/get`.
4. **Provenance (EU AI Act Art. 10):** Phân loại toàn bộ dữ liệu thành đúng **4 rổ hợp lệ** (`licensed`, `public_domain`, `scraped_optout_checked`, `synthetic`) và phân vùng theo cột này; cô lập và loại bỏ các dòng `UNCLASSIFIED` khỏi tập huấn luyện.
5. **Thực thi Quyền được xóa bỏ (Right to Erasure):** Xóa toàn bộ dữ liệu của `user_007` và chứng minh được lịch sử sử dụng.

#### 📝 Lệnh thực thi:
```bash
python -c "import notebooks.08_agents_provenance"
```

#### 📸 CHỨNG CỨ PHẢI LƯU LẠI (Checklist 11đ):
- [ ] **Silver Partition & Gold:** Phân vùng đủ 2 policy và Gold tổng hợp cả 2 (**3đ**).
- [ ] **Ghim phiên bản:** Replay tại version đã pin cho ra số dòng khớp tuyệt đối (`Matches what training saw: True`) (**3đ**).
- [ ] **Hành vi chuẩn MCP:** In ra `Actual catalog round-trips: 1` sau 5 lượt; `delete_rows` trả về `input_required` (**3đ**).
- [ ] **4 rổ Art. 10:** Tồn tại đủ 4 thư mục partition trên đĩa; loại trừ `UNCLASSIFIED` ra khỏi `trainable rows` (**2đ**).
- [ ] **Khối kiểm tra cuối:** `NB8 complete.` và `[PASS]`.

---

## 🧪 4. KIỂM THỬ CUỐI CÙNG & TÍNH TÁI LẬP (6 ĐIỂM)

Sau khi chạy xong cả 8 notebook trong giao diện Jupyter Lab, bạn cần chạy 2 lệnh sau từ dòng lệnh để giành trọn **6 điểm Reproducibility**:

### 1. Chạy 22 Unit Tests (2 Điểm)
```bash
make test
# Hoặc trên Windows: .\.venv\Scripts\pytest.exe -q
```
* **Kỳ vọng:** `22 passed in 1.xx s` xanh toàn bộ.

### 2. Chạy Headless toàn bộ 8 Notebooks (4 Điểm)
```bash
make run-all
# Hoặc trên Windows: .\.venv\Scripts\python.exe scripts/run_all.py
```
* **Kỳ vọng:** Cả 8 notebook từ `01_delta_basics` đến `08_agents_provenance` đều in ra `[PASS]` ở tất cả tiêu chí.

---

## ✍️ 5. HƯỚNG DẪN HOÀN THIỆN `submission/REFLECTION.md`

Tạo file [`submission/REFLECTION.md`](file:///c:/Users/ngant/ai_act_lab/lab_18/TRACK2_Day18_2A202601258_TaKimNgan/submission/REFLECTION.md) (yêu cầu ≤ 200 từ) trả lời câu hỏi:
> *Trong "Top 5 Lakehouse Anti-Patterns", đội ngũ/dự án của bạn có nguy cơ mắc phải lỗi nào nhất, và tại sao?*

### 💡 Khung gợi ý 5 Anti-Patterns để bạn chọn:
1. **Small-file disease (Bệnh file nhỏ):** Do streaming Kafka/micro-batch ghi liên tục mà không có cron job chạy Compaction/OPTIMIZE định kỳ.
2. **Hive-style explicit partitioning:** Tạo partition dạng cột thủ công dẫn đến việc dev query quên mệnh đề partition gây full-scan tốn chi phí.
3. **Decoupled Vector DB sync (Lệch vòng đời vector):** Đồng bộ dữ liệu ra ngoài Vector DB dẫn đến khi người dùng yêu cầu xóa (GDPR/Nghị định 13), hệ thống quên xóa vector trong Vector DB.
4. **Unchecked Time-Travel Retention Bloat:** Bật time-travel nhưng không cấu hình `VACUUM` / Expiry định kỳ làm phình chi phí lưu trữ S3/MinIO.
5. **Ungoverned / Unclassified AI Training Data:** Trộn lẫn dữ liệu scrape và licensed không có nhãn nguồn gốc vi phạm EU AI Act Art. 10.

---

## 📦 6. DANH MỤC HỒ SƠ SUBMISSION ĐẦY ĐỦ (CHECKLIST NỘP BÀI)

Trước khi mở Pull Request nộp bài, hãy đối chiếu cây thư mục:

```
Day18-Track2-Lakehouse-Lab/
├── notebooks/                      # 8 notebook đã chạy (GIỮ NGUYÊN OUTPUT CELLS)
│   ├── 01_delta_basics.ipynb
│   ├── 02_optimize_zorder.ipynb
│   ├── 03_time_travel.ipynb
│   ├── 04_medallion.ipynb
│   ├── 05_iceberg_catalog.ipynb
│   ├── 06_maintenance.ipynb
│   ├── 07_vectors_multimodal.ipynb
│   └── 08_agents_provenance.ipynb
├── submission/
│   ├── REFLECTION.md               # Bài luận ngắn (≤ 200 từ)
│   └── screenshots/                # Ảnh chụp chứng minh:
│       └── lakehouse_tree.png      # (Ảnh tree _lakehouse/ + nội dung 1 file _delta_log JSON)
└── checkpoint.md                   # File cẩm nang hướng dẫn này
```

---

## 🏆 7. BẢNG TỔNG KẾT THEO DÕI TIẾN ĐỘ (MASTER CHECKLIST)

| Task | Tên Notebook | Điểm | Mục tiêu cốt lõi cần thấy | Trạng thái |
|:---:|:---|:---:|:---|:---:|
| **NB1** | `01_delta_basics` | 8đ | Thấy JSON commit; Bad schema bị chặn; Cột `tier` được thêm | `[ ]` |
| **NB2** | `02_optimize_zorder` | 12đ | 200 file nhỏ → Speedup ≥ 3x hoặc Prune ≥ 10x; Min/max cô lập 1 file | `[ ]` |
| **NB3** | `03_time_travel` | 12đ | History ≥ 5 ver (có RESTORE); MERGE 100K; `score < 0` về 0 | `[ ]` |
| **NB4** | `04_medallion` | 12đ | 3 tầng Bronze/Silver/Gold; Silver < Bronze (dedup); Gold ≥ 7 ngày | `[ ]` |
| **NB5** | `05_iceberg_catalog` | 13đ | Hidden prune ≥ 5x; Giữ `field_id = 4`; 2 partition spec cùng tồn tại | `[ ]` |
| **NB6** | `06_maintenance` | 13đ | Compaction ít hơn 10x; Cluster skip ≥ 50%; Xóa 3 orphan; Checkpoint có | `[ ]` |
| **NB7** | `07_vectors_multimodal` | 13đ | Amplification ≥ 5x; int8 nén ≥ 3x (recall ≥ 0.8); Tái hiện Lifecycle Bug | `[ ]` |
| **NB8** | `08_agents_provenance` | 11đ | Silver partition `agent_version`; Pin version; MCP cache & human gate; 4 rổ Art 10 | `[ ]` |
| **Test** | `make test` | 2đ | 22/22 pytest pass xanh | `[ ]` |
| **Run** | `make run-all` | 4đ | Chạy 8/8 notebook headless đạt 100% pass | `[ ]` |
| **Reflect**| `REFLECTION.md` | - | Bài luận ≤ 200 từ về Anti-pattern | `[ ]` |
| **Tổng** | **Toàn bộ bài lab** | **100đ** | **Hoàn thành trọn vẹn 100%** | `[ ]` |

---
*Cẩm nang được biên soạn chi tiết cho sinh viên khóa AICB Track 2. Chúc bạn hoàn thành xuất sắc bài lab!*
