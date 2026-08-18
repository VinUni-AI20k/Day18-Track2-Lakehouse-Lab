# Storage-layer evidence (lightweight path)

Rubric Submission #2 cho phép hai lựa chọn; lab này chạy trên **đường lightweight**
(`deltalake` + `pyiceberg`, không Docker/MinIO), nên bằng chứng là lựa chọn thứ hai:
`tree _lakehouse/` **kèm nội dung một file `_delta_log/*.json`**.

| File | Nội dung | Phủ tiêu chí |
|---|---|---|
| `01_tree_lakehouse.txt` | Cây thư mục `_lakehouse/` — 100 thư mục, 1,152 file, 98.4 MB | Bronze/Silver/Gold tồn tại trên tầng lưu trữ (NB4); partition `date=` (NB4), `agent_version=` (NB8), `provenance_bucket=` × 5 rổ Art. 10 (NB8); catalog Iceberg tách riêng theo notebook (`iceberg/nb5`, `nb6`, `nb8`) |
| `02_delta_log_contents.txt` | Toàn bộ nội dung 2 commit của `scratch/users_delta`, tách theo action | Transaction log là JSON thật (NB1); `metaData.schemaString` v0 có 4 cột → v1 có thêm `tier` (schema evolution opt-in); `add.stats` chứa `minValues`/`maxValues` — chính là thống kê mà NB2/NB6 dùng để prune file |

Sinh lại bất cứ lúc nào sau `make run-all` bằng script kèm theo:

```bash
python submission/screenshots/make_evidence.py
```

## Đọc gì trong `02_delta_log_contents.txt`

* **`[protocol]`** — `minReaderVersion: 1` / `minWriterVersion: 2`: hợp đồng tương thích
  giữa reader và writer, thứ cho phép Spark và delta-rs cùng đọc một bảng.
* **`[metaData].schemaString`** — schema nằm **trong log**, không nằm trong file Parquet.
  Đây là lý do `schema_mode="merge"` là thao tác *metadata-only*: v1 ghi lại schema mới
  có `tier`, các file Parquet cũ không hề bị viết lại.
* **`[add].stats`** — `numRecords` + `minValues`/`maxValues` per-file. Planner đọc đúng
  phần này để loại file trước khi mở Parquet; đó là nguồn gốc con số pruning 55× ở NB2
  và skip-rate 90% ở NB6.
