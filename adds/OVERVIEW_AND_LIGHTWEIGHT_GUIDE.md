# Day 18 — Lakehouse Lab: Tổng quan & Hướng dẫn chi tiết theo path Lightweight

> File này tóm tắt project và đưa ra **guideline step-by-step** để hoàn thành lab theo
> hướng **Lightweight** (không cần Docker / Spark / Java). Phù hợp với laptop yếu,
> mạng chậm, và muốn focus vào **concept** Lakehouse thay vì vận hành hạ tầng.

---

## 1. Project này làm về cái gì?

Đây là lab thực hành **Data Lakehouse Architecture** (AICB-P2T2 · Day 18) với mục tiêu:

- Xây dựng pipeline **Medallion**: `Bronze → Silver → Gold` trên **Delta Lake**.
- Hiểu các tính năng cốt lõi của Lakehouse: **ACID transactions**, **schema enforcement
  & evolution**, **OPTIMIZE + Z-Order**, **Time Travel / RESTORE**, **MERGE (upsert)**.
- Áp dụng vào use-case thực tế: **LLM observability** (đo p50/p95 latency, cost,
  error rate theo ngày × model).

### Hai con đường (paths)

| Path | Stack | Setup | RAM | Khi nào dùng |
|---|---|---|---|---|
| **Lightweight (default)** | `deltalake` + DuckDB + Polars | `make setup` (~10 s) | ~500 MB | Hầu hết học viên |
| **Spark (Docker)** | PySpark + delta-spark + MinIO | `make spark-up` (~3 min) | ~4 GB | Muốn API y hệt Databricks |

Cả hai paths ghi ra **cùng một Delta Lake on-disk format** → có thể đổi qua lại,
table vẫn đọc được.

### 4 notebook deliverable (cùng cho cả 2 paths)

| Notebook | Skill | Pass khi… |
|---|---|---|
| `01_delta_basics` | Write/read Delta, schema enforcement, transaction log | bad-write bị chặn + cột `tier` được thêm khi `schema_mode="merge"` |
| `02_optimize_zorder` | Small-file problem; OPTIMIZE + Z-order | speedup ≥ 3× **hoặc** files-pruned ≥ 10× |
| `03_time_travel` | versionAsOf, RESTORE, MERGE, `history()` | `history()` ≥ 5 versions (kể cả RESTORE) |
| `04_medallion` | LLM-observability Bronze→Silver→Gold | Silver < Bronze + Gold ≥ 7 ngày × 3 models |

Chấm điểm: xem `rubric.md` (100 pts → Track-2 Daily Lab 30%).

---

## 2. Tại sao chọn Lightweight?

- **Nhanh**: setup ~10 giây (uv: ~2 giây), không tải image Docker vài GB.
- **Nhẹ**: ~500 MB RAM, không cần Java/JVM, không cần MinIO.
- **Đơn giản để debug**: stack trace Python thuần, không bị che bởi Spark executor.
- **Đủ cho deliverable**: 4 notebook đều pass tiêu chí với `deltalake` + DuckDB + Polars.
- **Đổi sang Spark được**: vì cùng định dạng Delta on-disk, tables vẫn đọc lại được.

Trade-off: API không giống PySpark 100% → mỗi notebook lightweight có comment đầu
file chỉ ra **PySpark equivalent** để bạn map khái niệm.

---

## 3. Yêu cầu môi trường

- **Python ≥ 3.10** (`python --version` để check).
- `pip` hoặc tốt hơn là [`uv`](https://github.com/astral-sh/uv) (nhanh hơn ~5×).
- Không cần Docker, không cần Java, không cần MinIO.
- Khoảng **1 GB** ổ cứng trống (venv ~80 MB + sample data ~200K rows).
- Trên Windows: dùng **PowerShell** hoặc **Git Bash**. Lệnh `make` cần
  [GNU Make for Windows](https://gnuwin32.sourceforge.net/packages/make.htm)
  hoặc chạy thủ công các lệnh tương đương (xem mục 5.1).

---

## 4. Quick Start (TL;DR — 4 lệnh)

```bash
make setup    # tạo venv + install deltalake/duckdb/polars (~10 s)
make smoke    # 5-second smoke test — verify stack works
make data     # generate 200K rows Bronze sample (cho NB4)
make lab      # mở Jupyter Lab tại http://localhost:8888
```

Sau khi `make smoke` báo `All checks passed`, mở:
**http://localhost:8888/lab/tree/01_delta_basics.ipynb** và bắt đầu chạy lần lượt
NB1 → NB2 → NB3 → NB4.

---

## 5. Guideline chi tiết — từng bước

### 5.1 Setup môi trường

**Cách A — dùng `make` (Linux/macOS/Git Bash):**

```bash
make setup
```

**Cách B — Windows PowerShell (chạy thủ công, không có `make`):**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install jupyterlab jupytext
```

> Nếu gặp lỗi execution policy khi activate venv:
> `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned`.

**Cách C — dùng `uv` (nhanh nhất):**

```powershell
uv venv
.\.venv\Scripts\Activate.ps1
uv pip install -r requirements.txt jupyterlab jupytext
```

### 5.2 Smoke test (verify stack)

```bash
make smoke
# hoặc thủ công:
python scripts/verify_lite.py
```

Phải thấy `All checks passed`. Nếu fail:
- Kiểm tra Python ≥ 3.10.
- Reinstall: `make clean && make setup`.

### 5.3 Generate sample data cho NB4

```bash
make data
# hoặc:
python scripts/generate_data_lite.py
```

Sinh **200K rows** vào `_lakehouse/bronze/llm_calls_raw/`. Nếu skip bước này,
NB4 sẽ báo `Path does not exist`.

### 5.4 Mở Jupyter Lab

```bash
make lab
# hoặc:
jupyter lab
```

Mở trình duyệt tại `http://localhost:8888`. Notebook live dạng **Jupytext `.py`**
nhỏ gọn — `make lab` tự convert sang `.ipynb`. Edit `.ipynb` trong Jupyter,
Jupytext tự sync lại `.py`.

> **Port 8888 bị chiếm?** Đổi sang 8889 trong `Makefile` hoặc:
> `jupyter lab --port 8889`.

### 5.5 Chạy lần lượt 4 notebook

#### NB1 — `01_delta_basics.ipynb`
- Mục tiêu: viết Delta table, đọc lại, xem `_delta_log/00...0.json`.
- Phải thấy: bad-schema write **bị chặn** → sau đó dùng `schema_mode="merge"` để
  thêm cột `tier` thành công.
- **Screenshot**: cell hiển thị `_delta_log/` + cell evolve schema thành công.

#### NB2 — `02_optimize_zorder.ipynb`
- Mục tiêu: tạo nhiều small files → đo query → `OPTIMIZE + Z-ORDER` → đo lại.
- Pass khi: **speedup ≥ 3×** HOẶC **files-pruned ratio ≥ 10×** (notebook in cả 2 — chụp cái nào pass).
- Nếu cả 2 đều dưới ngưỡng (RAM < 4 GB do DuckDB cache): `make clean && make setup` rồi chạy lại.

#### NB3 — `03_time_travel.ipynb`
- Mục tiêu: MERGE 100K rows → `history()` → `versionAsOf` → `RESTORE` → `history()` lại.
- Pass khi: `history()` **sau RESTORE** cho ≥ 5 versions (gồm cả row RESTORE).
- **Screenshot**: dump `history()` cuối cùng (sau restore).

#### NB4 — `04_medallion.ipynb`
- Mục tiêu: Bronze (raw `make data`) → Silver (dedup, type cast) → Gold (aggregate p50/p95/cost theo `date × model`).
- Pass khi: `Silver_rows < Bronze_rows` (dedup observable) **VÀ** Gold có ≥ **7 ngày × 3 models** với p50/p95/cost_usd/error_rate đầy đủ.
- **Screenshot**: cell in `Silver < Bronze` + cell `Gold.show()` với ≥ 21 rows.

### 5.6 Kiểm tra deliverable trước khi nộp

Checklist:

- [ ] 4 notebook chạy hết, không cell error đỏ.
- [ ] Mỗi notebook có ít nhất 1 screenshot pass criterion (xem mục 5.5).
- [ ] Folder `_lakehouse/{bronze,silver,gold}/...` tồn tại trên disk.
- [ ] File `submission/REFLECTION.md` (≤ 200 từ) viết về anti-pattern slide §5 team dễ vướng.
- [ ] Commit + push lên fork. PR title: `[NXX] Lab18 — <Họ Tên>`.

---

## 6. Troubleshooting (lightweight)

| Triệu chứng | Fix |
|---|---|
| `python3: command not found` | Cài Python 3.10+ → restart shell |
| `port 8888 in use` | `jupyter lab --port 8889` |
| NB2 speedup < 3× | `make clean && make setup` để xoá DuckDB cache |
| NB4 `Path does not exist` | Quên chạy `make data` |
| `pip install deltalake` fail trên Windows | Upgrade pip: `python -m pip install -U pip`; hoặc dùng `uv` |
| `make: command not found` (Windows) | Chạy thủ công các lệnh ở mục 5.1 cách B |
| Notebook không tự convert từ `.py` | Cài Jupytext: `pip install jupytext` rồi mở lại |

---

## 7. Cấu trúc repo (phần liên quan path Lightweight)

```
.
├── Makefile                    # targets: setup / smoke / data / lab / clean
├── requirements.txt            # deltalake + duckdb + polars + jupyter
├── notebooks/                  # ← path lightweight (4 notebook .py)
│   ├── 01_delta_basics.py
│   ├── 02_optimize_zorder.py
│   ├── 03_time_travel.py
│   └── 04_medallion.py
├── scripts/
│   ├── lakehouse.py            # path helper
│   ├── generate_data_lite.py   # sinh Bronze sample
│   └── verify_lite.py          # smoke test
└── _lakehouse/                 # ← sinh ra khi chạy notebook (gitignored)
    ├── bronze/
    ├── silver/
    └── gold/
```

---

## 8. Lệnh hữu ích — cheat sheet

```bash
make setup     # tạo venv + install lightweight stack
make smoke     # smoke test 5s
make data      # sinh 200K rows Bronze
make lab       # mở Jupyter Lab
make clean     # xoá venv + _lakehouse/

# Chạy 1 notebook không cần GUI:
jupyter nbconvert --to notebook --execute notebooks/01_delta_basics.ipynb

# Inspect Delta log nhanh:
ls _lakehouse/bronze/llm_calls_raw/_delta_log/
cat _lakehouse/bronze/llm_calls_raw/_delta_log/00000000000000000000.json
```

---

## 9. Tài liệu tham khảo

- `README.md` — overview gốc + bảng deliverable.
- `rubric.md` — barem chấm 100 pts.
- `BONUS-CHALLENGE.md` — architecture brief (optional, ungraded).
- [delta-rs docs](https://delta-io.github.io/delta-rs/) — API `deltalake` Python.
- [DuckDB + Delta](https://duckdb.org/docs/extensions/delta) — query Delta bằng SQL.
- [Polars](https://docs.pola.rs/) — DataFrame engine thay thế Pandas.

---

**Tóm lại**: chạy `make setup → make smoke → make data → make lab`, sau đó chạy
lần lượt 4 notebook, screenshot đúng cell pass criterion, viết REFLECTION, PR.
Toàn bộ flow lightweight chỉ tốn ~15–30 phút setup + thời gian học concept.
