# NB1 — Delta Lake Basics: Giải thích chi tiết

## Notebook này làm về cái gì?

NB1 dạy bạn **4 khái niệm cốt lõi** của Delta Lake — format lưu trữ mở cho Data
Lakehouse. Thay vì dùng Spark (nặng, cần JVM), path lightweight dùng thư viện
`deltalake` (Rust binding cho Python) + Polars + DuckDB để thao tác cùng định dạng
Delta on-disk.

---

## Giải thích từng phần

### Cell 1 — Setup & Import

```python
import _setup          # thêm scripts/ vào sys.path
import polars as pl    # DataFrame engine (thay thế Pandas)
from deltalake import DeltaTable, write_deltalake
from lakehouse import path, reset  # helper tạo đường dẫn _lakehouse/...

table_path = path("scratch", "users_delta")  # → _lakehouse/scratch/users_delta/
reset(table_path)  # xoá table cũ nếu có, đảm bảo chạy lại idempotent
```

- `path()`: tạo đường dẫn chuẩn bên trong `_lakehouse/`.
- `reset()`: xoá folder cũ để notebook chạy lại không bị conflict.

---

### Cell 3 — Write a Delta table

```python
df = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["alice", "bob", "charlie"],
    "age": [30, 25, 35],
    "city": ["Hanoi", "HCMC", "Danang"],
})
write_deltalake(table_path, df.to_arrow(), mode="overwrite")
```

**Ý nghĩa:**
- Tạo DataFrame 3 dòng với 4 cột (`id`, `name`, `age`, `city`).
- `df.to_arrow()`: convert sang Apache Arrow format (cầu nối giữa Polars ↔ Delta).
- `write_deltalake(...)`: ghi ra **Delta table** trên disk.
- `mode="overwrite"`: ghi đè toàn bộ (nếu table đã tồn tại).

**Kết quả trên disk:**
```
_lakehouse/scratch/users_delta/
├── _delta_log/
│   └── 00000000000000000000.json   ← transaction log (phiên bản v0)
└── part-00000-...-c000.snappy.parquet  ← data file (Parquet)
```

> **Khái niệm Delta Lake:** Khác với Parquet thường, Delta Lake thêm folder
> `_delta_log/` chứa JSON log ghi lại mọi thay đổi (ACID transaction).
> Đây chính là thứ biến Parquet thành "table có transaction".

---

### Cell 5 — Read back + Inspect transaction log

```python
dt = DeltaTable(table_path)
print(pl.from_arrow(dt.to_pyarrow_table()))
# History:
#   v0  WRITE  {num_added_files: 1, num_added_rows: 3, ...}
```

**Ý nghĩa:**
- `DeltaTable(path)`: mở Delta table đã ghi.
- `dt.to_pyarrow_table()`: đọc toàn bộ data → Arrow → Polars DataFrame.
- `dt.history()`: xem lịch sử thay đổi (giống `DESCRIBE HISTORY` trong Databricks).
- **v0 = WRITE**: phiên bản đầu tiên, ghi 3 rows vào 1 file.

> **Tương đương Spark:**
> `spark.read.format("delta").load(path)` ↔ `DeltaTable(path).to_pyarrow_table()`

---

### Cell 7 — Schema Enforcement (bảo vệ schema)

```python
bad = pl.DataFrame({"id": [4], "name": ["dan"], "age": ["thirty"], "city": ["Hue"]})
#                                                  ^^^ string thay vì int!
write_deltalake(table_path, bad.to_arrow(), mode="append")
# → BLOCKED! Cannot cast string 'thirty' to Int64
```

**Ý nghĩa:**
- Cố gắng append dữ liệu có `age` kiểu `string` ("thirty") vào table có `age` kiểu `int64`.
- Delta Lake **từ chối** (raise Exception) → data không bị ghi sai schema.
- Đây là **Schema Enforcement** — tính năng quan trọng ngăn "data pollution".

> **Tại sao quan trọng?** Trong production, nếu upstream đổi schema (vd. field từ
> int → string), Delta Lake chặn ngay thay vì để data hỏng lan xuống pipeline.

---

### Cell 9 — Schema Evolution (mở rộng schema có chủ đích)

```python
new = pl.DataFrame({
    "id": [4], "name": ["dan"], "age": [28], "city": ["Hue"],
    "tier": ["premium"],  # ← cột MỚI, chưa có trong schema cũ
})
write_deltalake(table_path, new.to_arrow(), mode="append", schema_mode="merge")
```

**Kết quả:**
```
shape: (4, 5)
┌─────┬─────────┬─────┬────────┬─────────┐
│ id  ┆ name    ┆ age ┆ city   ┆ tier    │
│ 1   ┆ alice   ┆ 30  ┆ Hanoi  ┆ null    │  ← row cũ, tier = null
│ 2   ┆ bob     ┆ 25  ┆ HCMC   ┆ null    │
│ 3   ┆ charlie ┆ 35  ┆ Danang ┆ null    │
│ 4   ┆ dan     ┆ 28  ┆ Hue    ┆ premium │  ← row mới có tier
└─────┴─────────┴─────┴────────┴─────────┘
```

**Ý nghĩa:**
- `schema_mode="merge"`: cho phép Delta **tự động thêm cột mới** vào schema.
- Các row cũ không có cột `tier` → hiển thị `null`.
- Đây là **Schema Evolution** — mở rộng schema khi có nhu cầu hợp lệ.

> **Phân biệt 2 khái niệm:**
> - **Schema Enforcement** = chặn data sai type → bảo vệ chất lượng.
> - **Schema Evolution** = thêm cột mới khi opt-in → linh hoạt mở rộng.

---

### Cell 11 — Bonus: Query bằng DuckDB (zero-copy)

```python
import duckdb
duckdb.sql(f"SELECT tier, count(*) FROM delta_scan('{table_path}') GROUP BY 1")
```

**Kết quả:**
```
┌─────────┬──────────────┐
│  tier   │ count_star() │
│ premium │            1 │
│ NULL    │            3 │
└─────────┴──────────────┘
```

**Ý nghĩa:**
- `delta_scan()`: DuckDB đọc trực tiếp Delta table (không cần copy data).
- Kết quả: 2 nhóm tier — chứng minh schema evolution hoạt động.
- **Zero-copy**: DuckDB đọc thẳng Arrow memory từ Delta, cực nhanh.

> **Ứng dụng thực tế:** Trong Lakehouse, bạn có thể viết data bằng Spark/Delta
> rồi query bằng DuckDB/Trino/Presto mà không cần di chuyển data.

---

## Tổng kết — NB1 dạy gì?

| # | Khái niệm | Cell | Một câu tóm tắt |
|---|---|---|---|
| 1 | **Delta Write** | 3 | Ghi DataFrame thành Delta table (Parquet + transaction log) |
| 2 | **Transaction Log** | 5 | `_delta_log/` JSON ghi lại mọi thao tác → ACID |
| 3 | **Schema Enforcement** | 7 | Chặn data sai kiểu → bảo vệ data quality |
| 4 | **Schema Evolution** | 9 | Thêm cột mới khi opt-in (`schema_mode="merge"`) |
| 5 | **Cross-engine query** | 11 | DuckDB đọc Delta trực tiếp → interoperability |

---

## Checklist NB1 — Đã pass ✅

- [x] `_delta_log/` chứa JSON files (history v0 WRITE hiện ra)
- [x] Schema enforcement chặn bad write (`Cannot cast string 'thirty' to Int64`)
- [x] `schema_mode="merge"` thêm cột `tier` thành công (4 rows × 5 cols)
- [x] DuckDB query trả về 2 tier groups (`premium: 1`, `NULL: 3`)
