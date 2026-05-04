# NB3 — Time Travel + MERGE Upsert: Giải thích chi tiết

## Notebook này làm về cái gì?

NB3 dạy **3 tính năng quan trọng** của Delta Lake mà Parquet thường không có:
- **MERGE (upsert)**: cập nhật row cũ + chèn row mới trong 1 thao tác atomic.
- **Time Travel**: đọc lại data ở bất kỳ version cũ nào.
- **RESTORE**: rollback table về version trước khi có sự cố.

Đây là những tính năng khiến Delta Lake được gọi là "table format có ACID transaction".

---

## Giải thích từng phần

### Cell 1 — Setup

```python
table_path = path("scratch", "customers_tt")
reset(table_path)
```

Tạo table tại `_lakehouse/scratch/customers_tt/`, xoá data cũ nếu có.

---

### Cell 3 — Build version history (4 versions: v0 → v3)

#### v0 — Initial load (100K rows)
```python
v0 = pl.DataFrame({
    "customer_id": list(range(100_000)),
    "status":      ["active"] * 100_000,
    "score":       [i % 1000 for i in range(100_000)],
})
write_deltalake(table_path, v0.to_arrow(), mode="overwrite")
```
- Ghi 100K customers với 3 cột: `customer_id`, `status`, `score`.
- Đây là **version 0** trong transaction log.

#### v1 — Schema Evolution (thêm cột `tier`)
```python
v1 = v0_data.with_columns(
    pl.when(pl.col("score") > 800).then("gold").otherwise("silver").alias("tier")
)
write_deltalake(table_path, v1.to_arrow(), mode="overwrite", schema_mode="overwrite")
```
- Tính cột `tier` dựa trên `score`: >800 → "gold", còn lại → "silver".
- `schema_mode="overwrite"`: cho phép thay đổi schema hoàn toàn.

#### v2 — MERGE upsert (100K rows: 50K update + 50K insert)
```python
updates = pl.DataFrame({
    "customer_id": list(range(50_000, 150_000)),  # 50K chồng lấp + 50K mới
    "status": ["vip"] * 100_000,
    "score":  [999] * 100_000,
    "tier":   ["platinum"] * 100_000,
})
DeltaTable(table_path)
    .merge(source=updates.to_arrow(),
           predicate="t.customer_id = s.customer_id",
           source_alias="s", target_alias="t")
    .when_matched_update_all()      # customer 50K–99K: update → vip/platinum
    .when_not_matched_insert_all()  # customer 100K–149K: insert mới
    .execute()
```

**Output:** `MERGE 100K rows: 0.60s`

**Ý nghĩa:**
- **MERGE = UPDATE + INSERT trong 1 transaction** (còn gọi là "upsert").
- `predicate`: join key — match trên `customer_id`.
- `when_matched_update_all()`: nếu key đã tồn tại → update tất cả cột.
- `when_not_matched_insert_all()`: nếu key chưa tồn tại → insert row mới.
- Kết quả: table có **150K rows** (100K cũ + 50K mới, trong đó 50K bị update).

> **Tại sao MERGE quan trọng?** Trong CDC (Change Data Capture), upstream gửi
> mix update + insert. MERGE xử lý cả hai atomic, không cần tách thành 2 bước
> riêng → tránh data inconsistency.

#### v3 — Bad data (mô phỏng sự cố)
```python
bad = pl.DataFrame({
    "customer_id": range(50),
    "status": [None] * 50,
    "score":  [-1] * 50,      # ← score âm = data sai
    "tier":   ["UNKNOWN"] * 50,
})
write_deltalake(table_path, bad.to_arrow(), mode="append")
```
- Append 50 rows với `score=-1` và `status=None` → mô phỏng pipeline bị lỗi.
- Đây là lý do cần **RESTORE** ở bước sau.

---

### Cell 5 — history() (audit trail)

```
v 3  WRITE          — bad data append
v 2  MERGE          — upsert 100K (50K update + 50K insert)
v 1  WRITE          — schema evolution (thêm tier)
v 0  WRITE          — initial 100K load
```

**Ý nghĩa:**
- `history()` hiển thị **toàn bộ lịch sử thay đổi** của table.
- Mỗi version ghi lại: operation, thời gian, số file thêm/xoá, số row.
- Tương đương `DESCRIBE HISTORY` trong Databricks SQL.

> **Audit trail** giúp trả lời: "Ai thay đổi gì, lúc nào, bao nhiêu row?"
> — yêu cầu bắt buộc trong compliance (GDPR, Decree 13, SOX).

---

### Cell 7 — Time Travel queries

```python
v0_count = DeltaTable(table_path, version=0).to_pyarrow_table().num_rows  # → 100000
v1_cols  = DeltaTable(table_path, version=1).schema().to_pyarrow().names
# → ['customer_id', 'status', 'score', 'tier']
```

**Ý nghĩa:**
- `DeltaTable(path, version=N)`: đọc table **đúng như nó ở version N**.
- v0: 100K rows, chưa có cột `tier`.
- v1: đã có cột `tier` (schema evolution).
- **Không cần backup/snapshot** — Delta tự lưu tất cả version nhờ transaction log.

> **Tương đương Spark:** `spark.read.format("delta").option("versionAsOf", 0).load(path)`

---

### Cell 9 — RESTORE (rollback về v2)

```python
dt = DeltaTable(table_path)
dt.restore(2)   # rollback về trạng thái ở v2
# → RESTORE → v2: 0.04s (target < 30s)
# → Rows with score<0 after restore: 0 (expected 0)
```

**Ý nghĩa:**
- `restore(2)`: đưa table **trở lại trạng thái v2** (sau MERGE, trước bad data).
- RESTORE tạo ra **version mới (v4)** — không xoá lịch sử cũ.
- Bad rows (`score=-1`) từ v3 **đã biến mất** → data clean trở lại.
- Thời gian: 0.04s (rất nhanh vì chỉ thay đổi metadata, không copy data).

> **RESTORE vs DELETE:** RESTORE hoàn toàn trên transaction log (instant),
> không cần scan/filter/rewrite data. An toàn hơn và nhanh hơn nhiều.

---

### Cell 11 — Final history (sau RESTORE)

```
v 4  RESTORE        ← version mới ghi nhận rollback
v 3  WRITE          ← bad data (đã bị "undo")
v 2  MERGE
v 1  WRITE
v 0  WRITE

Total versions: 5  (target ≥ 5)
```

**Ý nghĩa:**
- RESTORE **không xoá history** — nó thêm 1 version mới (v4) có nội dung giống v2.
- Toàn bộ audit trail còn nguyên → compliance-friendly.
- **5 versions** (v0–v4) đáp ứng yêu cầu ≥ 5.

> **Screenshot cần chụp:** Chính cell này — hiển thị 5 versions bao gồm RESTORE.

---

## Tổng kết — NB3 dạy gì?

| # | Khái niệm | Cell | Một câu tóm tắt |
|---|---|---|---|
| 1 | **MERGE (upsert)** | 3 | Update + Insert atomic trong 1 transaction (0.60s cho 100K rows) |
| 2 | **Version history** | 5 | `history()` ghi lại mọi thao tác → audit trail |
| 3 | **Time Travel** | 7 | Đọc lại data ở bất kỳ version cũ nào |
| 4 | **RESTORE** | 9 | Rollback table về version trước, instant, không mất history |
| 5 | **Full audit** | 11 | Sau RESTORE vẫn giữ nguyên toàn bộ lịch sử (5 versions) |

---

## Checklist NB3 — Đã pass ✅

- [x] `history()` hiển thị **≥ 5 versions** bao gồm RESTORE (v0–v4) ✅
- [x] MERGE 100K hoàn thành trong **0.60s** (target < 60s) ✅
- [x] RESTORE trong **0.04s** (target < 30s) + xoá sạch bad rows (`score<0 = 0`) ✅
