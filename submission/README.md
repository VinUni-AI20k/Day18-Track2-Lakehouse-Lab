# Day 18 — Lakehouse Lab · Bài nộp

**Đường chạy:** lightweight (`deltalake` 1.6.2 + `pyiceberg` + DuckDB + Polars).
Không Docker, không JVM, không MinIO — nên bằng chứng storage layer là
`tree _lakehouse/` + nội dung `_delta_log/*.json` (lựa chọn thứ hai mà rubric cho phép).

**Trạng thái:** `run_all.py` **8/8 PASS** trong 38.0s · **42 dòng `[PASS]`, 0 `[FAIL]`**
trên toàn bộ 8 notebook · mọi notebook chạy tuần tự từ một `_lakehouse/` sạch, không
có cell lỗi.

---

## Nội dung

| File | Mô tả |
|---|---|
| `01..08_*.ipynb` | 8 notebook đã chạy, giữ nguyên output. Mỗi notebook kết thúc bằng khối `assert` + một cell **📝 Đọc số liệu** diễn giải kết quả |
| `01..08_*.pdf` | Bản in PDF của 8 notebook trên |
| `screenshots/` | Bằng chứng storage layer: cây `_lakehouse/` + nội dung transaction log. Có `make_evidence.py` để sinh lại |
| `REFLECTION.md` | Anti-pattern team dễ vướng nhất (184 từ) |
| `bonus/ARCHITECTURE.md` | Bonus challenge topic C — CDC ride-hailing VN, Nghị định 13 (*tự nguyện, không tính điểm*) |

---

## Ánh xạ rubric → bằng chứng

### Part A — Foundations (44 điểm)

| NB | Tiêu chí | Đo được | Đ |
|---|---|---|--:|
| 1 | `_delta_log/` JSON commits visible | 2 commit JSON; nội dung đầy đủ ở `screenshots/02_delta_log_contents.txt` | 4 |
| 1 | Schema enforcement chặn `age=str` | `Cast error: Cannot cast string 'thirty' to value of Int64 type` | 2 |
| 1 | `schema_mode="merge"` thêm cột `tier` | DuckDB: `[('premium', 1), (None, 3)]` — 3 dòng cũ trả `null`, không bị rewrite | 2 |
| 2 | Small-file problem (≥ 100 file) | **200 file** trước OPTIMIZE, trung bình 51.5 KB | 3 |
| 2 | Speedup ≥ 3× **hoặc** pruning ≥ 10× | **speedup 7.7×** *và* **pruning 55.0×** (1/55 file chứa `user_id=4242`) | 6 |
| 2 | `numFiles` giảm đáng kể | **200 → 55** file | 3 |
| 3 | `history()` ≥ 5 version, có dòng RESTORE | **5 version**, RESTORE là chính version thứ 5 | 4 |
| 3 | MERGE upsert 100K dòng | 100K dòng trong **0.09s**, `num_output_rows=150,000` | 4 |
| 3 | RESTORE xoá dữ liệu xấu; `score < 0` = 0 | RESTORE → v2 trong 0.02s; **0 dòng** `score < 0` | 4 |
| 4 | Bronze/Silver/Gold có trên storage | Cả 3 trong `screenshots/01_tree_lakehouse.txt` | 4 |
| 4 | Silver < Bronze (dedup) | **200,000 → 190,052** (−9,948 dòng trùng, 4.97%) | 4 |
| 4 | Gold đúng, ≥ 7 ngày × 3 model | **8 ngày × 3 model = 24 dòng**, đủ p50/p95/cost_usd/error_rate | 4 |

### Part B — Lakehouse 2026 (50 điểm)

| NB | Tiêu chí | Đo được | Đ |
|---|---|---|--:|
| 5 | Tạo bảng **qua catalog**, spec `day(ts)` | Bảng tạo qua SQLite catalog, không chỉ định path | 3 |
| 5 | Hidden-partition pruning ≥ 5× qua `plan_files()`, lọc trên `ts` | **10×** (10 file → 1 file). Quy ra tiền: **$220/ngày** lãng phí ở 10K query/ngày | 5 |
| 5 | Ba tầng metadata; tỉ lệ metadata:data | 10 manifest list · 10 manifest · 10 data file · data 47.3 KB | 1 |
| 5 | Rename giữ `field_id`; ≥ 2 spec cùng tồn tại | spec `[1, 2]`, đọc đủ **5,500 dòng** qua cả hai | 4 |
| 6 | **Job 1** Compaction ≥ 10× ít file | **200 → 11 file (18×)**; $4.00/ngày → $0.08/ngày GET request | 4 |
| 6 | **Job 2** Clustering skip ≥ 50% | **90%** (mở 1/10 file), chứng minh từ min/max stats | 3 |
| 6 | **Job 3** Expiry: vacuum thu byte; Iceberg còn 3 snapshot | Vacuum thu **16.1 MB**; snapshot **20 → 3** | 3 |
| 6 | **Job 4** Orphan: 3 orphan Delta; quét manifest Iceberg lạc | **3 orphan (21.2 KB)** tìm + xoá; avro **40 → 23** | 2 |
| 6 | **Job 5** Checkpoint | `00000000000000000099.checkpoint.parquet` + `_last_checkpoint` | 1 |
| 7 | Amplification ≥ 5×, giải thích bằng row-group | **200×** (cần 64 KB, đọc 12.5 MB = 1 row group) | 4 |
| 7 | int8 ≥ 3× nhỏ; recall@10 **và** topic fidelity | **5.8× nhỏ**; recall@10 **0.904**; fidelity **1.000** | 4 |
| 7 | Semantic search chạy bằng SQL, đúng chủ đề | `array_cosine_similarity` trong DuckDB core, top-5 cùng topic | 1 |
| 7 | **Lifecycle bug**: 0 hit trong bảng, > 0 hit ở index cũ | Bảng **2,000 → 1,992**; external index vẫn **2,000** | 4 |
| 8 | Trajectory qua medallion; Silver partition `agent_version`; Gold đủ 2 policy | Partition `policy-v2`, `policy-v3` trên đĩa | 3 |
| 8 | Training run pin version; replay khớp chính xác | Pin **v0 = 1,578 steps**; bảng đã sang v1 = 1,978 steps; replay vẫn ra 1,578 | 3 |
| 8 | MCP: `tools/list` cache được, `input_required`, task poll | **5 turn → 1 catalog read**; destructive cần xác nhận; poll `completed` | 3 |
| 8 | Đủ **4** rổ Art. 10 làm partition; loại UNCLASSIFIED | 4 rổ + UNCLASSIFIED; **1,666/2,000** dòng trainable, loại **334** | 2 |

### Part C — Reproducibility (6 điểm)

| Tiêu chí | Kết quả | Đ |
|---|---|--:|
| `make test` xanh | 24 test collected: **23 pass, 1 fail** — lỗi Windows-only, xem ghi chú dưới | 2 |
| `make run-all` xanh từ `_lakehouse/` sạch | **8/8 PASS trong 38.0s** | 4 |

---

## Chạy lại bài nộp

Máy làm bài là **Windows**, nơi `make` không dùng được: `Makefile` trỏ tới
`$(VENV)/bin/python` còn venv Windows là `.venv/Scripts/`. Các lệnh tương đương:

```bash
# QUAN TRỌNG: tắt hẳn Jupyter Lab trước — kernel đang sống giữ khoá
# _lakehouse/iceberg/nb*/catalog.db và làm NB5/NB6 fail với TableAlreadyExistsError
rm -rf _lakehouse
export PYTHONUTF8=1            # cần thiết: xem ghi chú 2
python scripts/generate_data_lite.py
python scripts/generate_ai_data.py
python scripts/run_all.py      # → 8/8 PASS
python -m pytest -q            # → 23 pass, 1 fail
python submission/screenshots/make_evidence.py
```

Trên Linux/macOS thì `make data && make data-ai && make run-all && make test` là đủ.

---

## Hai ghi chú về môi trường

**1. `pytest` đỏ 1 test trên Windows — lỗi của repo, không phải của bài nộp.**
`tests/test_lab18.py::test_reset_catalog_does_not_touch_siblings` fail vì
`scripts/lakehouse.py:96` dùng `shutil.rmtree(..., ignore_errors=True)`, mà trên
Windows không thể xoá file SQLite đang có handle mở — `ignore_errors=True` nuốt luôn
`PermissionError [WinError 32]` nên thư mục vẫn còn. Trên Linux/macOS, unlink một
file đang mở là hợp lệ nên test xanh. **Tôi cố ý không sửa mã lab** để diff của PR
chỉ chứa `submission/`.

**2. `scripts/run_all.py` cần `PYTHONUTF8=1` trên Windows.** Dòng 28 dùng
`subprocess.run(..., text=True)`, giải mã output của notebook bằng codepage mặc định
(cp1252) và crash với `UnicodeDecodeError` khi gặp `✓ ─ →`. Bật UTF-8 mode làm
`locale.getencoding()` trả về utf-8 là hết. Cũng không sửa, vì lý do trên.

---

## Ba điều đáng chú ý nhất khi đọc output

1. **`VACUUM` không thu hồi orphan chưa từng commit** (NB6). Bảng báo đúng 100,000
   dòng, đĩa có 15 file Parquet nhưng log chỉ biết 10 — 5 file kia vô hình với
   vacuum ở *mọi* retention vì chưa bao giờ được tombstone.
2. **`expire_snapshots` không xoá một byte dữ liệu nào** (NB6). Snapshot 20 → 3
   nhưng avro **40 → 40**, metadata còn phình 325.2 → 332.3 KB. Job 3 và Job 4 là
   một **cặp** — đây chính là lý do "đã expire mà hoá đơn S3 không giảm".
3. **Small file phá data skipping, không chỉ tốn request** (NB2). Khi 200 file đến
   từ 200 lần append độc lập, `min/max` stats chồng lấn nên planner không loại được
   file nào; sau Z-ORDER chỉ 1/55 file phải đọc.

Diễn giải đầy đủ nằm trong cell **📝 Đọc số liệu** ở cuối mỗi notebook.
