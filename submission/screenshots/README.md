# Bằng chứng lưu trữ — đường lightweight

Thư mục này **không chứa ảnh chụp màn hình**, và đó là đúng yêu cầu.

`rubric.md` cho hai lựa chọn thay thế nhau, tuỳ đường chạy:

> `submission/screenshots/` — at least one of:
> * MinIO console showing `_delta_log/` + bucket layout (**Spark path**), **or**
> * `tree _lakehouse/` plus the contents of one `_delta_log/*.json` (**lightweight path**)

Ảnh chụp màn hình chỉ áp dụng cho nhánh Spark (chụp console MinIO). Bài này chạy
**đường lightweight** (`deltalake` + `pyiceberg` + DuckDB, không Docker, không MinIO),
nên bằng chứng tương ứng là văn bản. Tên thư mục `screenshots/` giữ nguyên theo rubric.

## Nội dung

| File | Là gì |
|---|---|
| `lakehouse_tree.txt` | Output của `tree _lakehouse/` — 101 thư mục, 1.152 file. Thấy rõ layout `bronze/ silver/ gold/ scratch/ iceberg/` và các thư mục `_delta_log/` |
| `lakehouse_files.txt` | Danh sách phẳng cùng dữ liệu, sinh bằng lệnh `python3 -c "…os.walk…"` mà đề bài trên LMS đưa |
| `delta_log_sample.json` | Nội dung đầy đủ `_lakehouse/scratch/users_delta/_delta_log/00000000000000000000.json` — commit đầu tiên của bảng Delta ở NB1 |

## Đọc `delta_log_sample.json` thế nào

Bốn dòng JSON, mỗi dòng là một *action* — đây chính là cấu trúc transaction log mà NB1
mở ra xem:

| Dòng | Action | Nói lên điều gì |
|---|---|---|
| 1 | `commitInfo` | ai ghi, lúc nào, bằng engine gì (`delta-rs.py-1.6.2`), và metrics của phép ghi |
| 2 | `protocol` | phiên bản reader/writer tối thiểu cần để đọc bảng |
| 3 | `metaData` | schema và cấu hình bảng |
| 4 | `add` | file Parquet được thêm, kèm `size`, `modificationTime` và `stats` (min/max mỗi cột) |

Chính trường `stats` ở action `add` là thứ làm nên file-skipping đo được ở NB2:
pruning ratio 55× nhờ đúng 1 trong 55 file có dải `user_id` chứa giá trị cần tìm.

## Tái lập

```bash
make clean && make setup && make data && make data-ai && make run-all

tree _lakehouse/ > submission/screenshots/lakehouse_tree.txt
python3 -c "import os; [print(f'{r}/{f}') for r,d,fs in os.walk('_lakehouse') for f in fs]" \
  > submission/screenshots/lakehouse_files.txt
cat _lakehouse/scratch/users_delta/_delta_log/00000000000000000000.json \
  > submission/screenshots/delta_log_sample.json
```

Tên file Parquet chứa UUID sinh ngẫu nhiên mỗi lần ghi, nên danh sách file sẽ **khác**
sau mỗi lần chạy lại. Số lượng (1.152 file / 101 thư mục) thì ổn định.
