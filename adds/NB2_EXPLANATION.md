# NB2 — Small-File Problem & OPTIMIZE + Z-order: Giải thích chi tiết

## Notebook này làm về cái gì?

NB2 giải quyết **Small-File Problem** — vấn đề phổ biến nhất trong Data Lakehouse
khi streaming ingestion tạo ra hàng trăm/ngàn file nhỏ. Notebook chứng minh cách
`OPTIMIZE` (compact) + `Z-ORDER` cải thiện hiệu năng query **đáng kể**.

---

## Giải thích từng phần

### Cell 1 — Setup & Import

```python
import _setup, time, random
import polars as pl, duckdb
from deltalake import DeltaTable, write_deltalake
from lakehouse import path, reset

table_path = path("scratch", "events_smallfiles")
reset(table_path)
```

Tạo table mới tại `_lakehouse/scratch/events_smallfiles/`.

---

### Cell 3 — Tạo Small-File Problem (200 file nhỏ)

```python
for batch in range(200):
    rows = pl.DataFrame({...})  # 5,000 rows mỗi batch
    write_deltalake(table_path, rows.to_arrow(), mode="append" / "overwrite")
# → Files before OPTIMIZE: 200
```

**Ý nghĩa:**
- Mô phỏng **streaming ingestion**: 200 batch × 5K rows = **1 triệu rows** ghi thành **200 file nhỏ**.
- Mỗi file chứa `event_id`, `kind`, `user_id` (1–100K), `payload` (~200 bytes/row).
- `TARGET_USER = 4242`: "cây kim" để tìm trong 200 "đống rơm" file.

> **Small-File Problem là gì?** Khi mỗi lần ghi tạo 1 file mới, sau thời gian
> bạn có hàng trăm/ngàn file rất nhỏ. Query phải mở từng file → overhead I/O lớn,
> metadata nặng, query chậm. Đây là anti-pattern phổ biến nhất trong Lakehouse.

---

### Cell 5 — Benchmark BEFORE (chưa optimize)

```python
def bench(label, runs=3):
    dt_local = DeltaTable(table_path)
    tbl = dt_local.to_pyarrow_table(
        filters=[("user_id", "=", 4242), ("kind", "=", "purchase")]
    )
    # → median time
```

**Kết quả:** `BEFORE OPTIMIZE  count=5  median=587.7 ms`

- Query tìm `user_id=4242` phải scan qua **tất cả 200 file** vì data phân tán ngẫu nhiên.
- `filters=[...]`: delta-rs dùng **min/max stats** trong log để skip file, nhưng trước
  Z-order, user_id nằm rải rác → hầu như không skip được file nào.

---

### Cell 7 — OPTIMIZE + Z-ORDER

```python
TARGET_SIZE = 256 * 1024  # 256 KB — giữ ~50 files để Z-order có gì để skip

dt.optimize.compact(target_size=TARGET_SIZE)     # gộp file nhỏ
dt.optimize.z_order(["user_id"], target_size=TARGET_SIZE)  # sắp xếp theo user_id
# → Files after: 55 (was 200)
```

**Hai bước:**

1. **`compact()`** — gộp 200 file nhỏ thành ít file lớn hơn (200 → ~55).
   - Giảm overhead mở file.
   - `target_size=256KB` giữ đủ nhiều file để Z-order có hiệu quả.

2. **`z_order(["user_id"])`** — sắp xếp lại data theo `user_id` bằng Z-order curve.
   - Sau Z-order, mỗi file chứa 1 **dải user_id liên tục** (vd. file 1: [1, 1851],
     file 2: [1851, 3696], ...).
   - Query `user_id=4242` chỉ cần đọc **1 file** thay vì 200.

> **Z-order curve là gì?** Thuật toán ánh xạ nhiều chiều (multi-dimensional) thành
> 1 chiều, sao cho giá trị gần nhau trong không gian gốc vẫn gần nhau trên disk.
> Kết quả: data cùng user_id nằm cùng file → file-skipping hiệu quả.

---

### Cell 9 — Benchmark AFTER

```
AFTER OPTIMIZE+ZORDER  count=5  median=67.8 ms

Speedup: 8.7×  (target ≥ 3×)     ← PASS ✅
File reduction: 200 → 55  (4× fewer)
```

**Cải thiện:** Query nhanh hơn **8.7 lần** nhờ:
- Ít file hơn (200 → 55) → ít overhead I/O.
- Z-order cho phép skip 54/55 file → chỉ đọc 1 file chứa user_id=4242.

---

### Cell 11 — Inspect file-level stats (chứng minh cơ chế)

```
file user_id range: [  3696,   5534] ← contains target
... (54 files khác không chứa 4242)

──── Z-order deliverable metrics ────
  Speedup (wall-clock):     8.7×   (target ≥ 3×)
  Files-pruned ratio:      55.0×   (target ≥ 10×)
  [1 of 55 files cover user_id=4242]
```

**Ý nghĩa:**
- Sau Z-order, mỗi file có **min/max user_id range không chồng lấp**.
- `user_id=4242` chỉ nằm trong 1 file (range [3696, 5534]).
- **Files-pruned ratio = 55.0×**: engine skip 54 file, chỉ đọc 1 → 55× ít I/O.
- **Đây là metric đáng tin cậy hơn** speedup (không bị ảnh hưởng bởi cache/CPU).

---

## Tổng kết — NB2 dạy gì?

| # | Khái niệm | Cell | Một câu tóm tắt |
|---|---|---|---|
| 1 | **Small-File Problem** | 3 | Streaming tạo nhiều file nhỏ → query chậm |
| 2 | **Benchmark Before** | 5 | Đo baseline: 587.7 ms, scan hết 200 file |
| 3 | **OPTIMIZE compact** | 7 | Gộp file nhỏ thành file lớn, giảm I/O overhead |
| 4 | **Z-ORDER** | 7 | Sắp xếp data theo user_id → file-skipping hiệu quả |
| 5 | **Benchmark After** | 9 | 67.8 ms, speedup 8.7× |
| 6 | **Min/Max Stats** | 11 | Chứng minh Z-order tạo range không chồng lấp → prune 54/55 file |

---

## Checklist NB2 — Đã pass ✅

- [x] **Speedup ≥ 3×**: đạt **8.7×** ✅
- [x] **Files-pruned ratio ≥ 10×**: đạt **55.0×** ✅ (cả 2 metric đều pass!)
- [x] File count giảm đáng kể: **200 → 55** ✅
- [x] Stats inspection: chỉ **1/55 file** chứa `user_id=4242` ✅
- [x] Screenshot: cell in số liệu speedup + pruned ratio ✅
