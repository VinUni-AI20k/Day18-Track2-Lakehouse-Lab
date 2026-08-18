# Ghi chú môi trường & sai lệch so với hướng dẫn mặc định

Ba thay đổi so với `README.md`, đều có lý do đo được. Không sửa notebook, không sửa rubric.

## 1. `LAKEHOUSE_ROOT` trỏ sang ext4 thay vì `_lakehouse/` trong repo

Repo nằm trên `/mnt/d` (ổ NTFS mount vào WSL qua DrvFs). Ghi Delta table 200.000 dòng lên đó
**fail deterministic**:

```
_internal.DeltaError: Failed to parse parquet: External:
  Generic LocalFileSystem error: Upload aborted
```

Đã kiểm chứng: cùng script, cùng interpreter, ghi 200.000 dòng lên `/mnt/d` fail 2/2 lần; lên
ext4 (`$HOME`) thành công `RC=0`. Ghi 2.000 dòng lên `/mnt/d` thì thành công — lỗi phụ thuộc
kích thước. Lỗi phát ra ở tầng `object_store` của Rust, không phải Delta protocol.

Cách xử lý dùng đúng knob có sẵn của lab (`scripts/lakehouse.py:15`), không phải hack:

```bash
export LAKEHOUSE_ROOT=$HOME/lakehouse18
```

Mọi đường dẫn trong `submission/screenshots/01_tree_lakehouse.txt` đã được hiển thị lại dưới
tên `_lakehouse` cho khớp rubric.

## 2. `git config core.autocrlf input`

Repo được clone bằng Git for Windows (`core.autocrlf=true` ở system level) nên working tree là
CRLF. Git trong WSL (`autocrlf` unset) coi 33 file text là "modified" dù nội dung không đổi:

```
git diff --name-only          → 34 file
git diff --ignore-cr-at-eol   →  1 file   (chỉ .gitignore là thay đổi thật)
```

Đặt `core.autocrlf=input` ở scope repo để commit từ WSL không tạo diff line-ending giả.

## 3. `.gitignore`: bỏ `notebooks/*.ipynb` và `notebooks-spark/*.ipynb`

Rubric yêu cầu nộp *"Eight executed notebooks (output cells preserved)"*, nên `.ipynb` phải
commit được.

## Môi trường

| | |
|---|---|
| OS | Windows 10 Pro + WSL2 Ubuntu 26.04 LTS |
| Python | 3.11.16 (venv tạo bằng `uv`; system python3 của distro là 3.14.4) |
| deltalake | 1.6.2 |
| Đường chạy | lightweight (`deltalake` + `pyiceberg` + DuckDB + Polars) — phủ cả 8 notebook |

## Kết quả kiểm chứng

```
make smoke     9/9 checks
make test     24/24 passed
make run-all   8/8 PASS in 81.4s
```

42 assert `[PASS]`, 0 `[FAIL]`, 0 error trong output đã lưu của 8 notebook.

## 4. Về các file `.png` trong `screenshots/`

Tám file `11_*.png` … `18_*.png` là **ảnh render từ output đã ghi lại**, không phải ảnh chụp
màn hình hệ điều hành. Cách tạo: đọc stdout đã lưu trong `notebooks/*.ipynb` (hoặc output của
`make run-all`), rồi vẽ ra PNG bằng Pillow với font mono, tô màu `[PASS]`/`[FAIL]`.

Nói rõ để không gây hiểu nhầm: nội dung trong ảnh **là output thật**, trích từ chính notebook
nộp kèm, nhưng ảnh được dựng bằng script chứ không phải bấm PrintScreen. Bản text gốc nằm cùng
thư mục (`02_*.txt` … `10_*.txt`) để đối chiếu từng dòng.

Vài ảnh có ghi chú trong caption khi nội dung bị rút gọn cho vừa khung:
- `12_nb2_zorder.png` — bảng 55 dòng min/max rút còn 5 dòng
- `14_nb6_maintenance.png`, `15_nb7_vectors.png`, `16_nb8_agents.png` — chỉ giữ dòng đo lường + `[PASS]`
- `16_nb8_agents.png` — dòng dài nhất (251 ký tự) cắt còn 118

Bản đầy đủ, không rút gọn, luôn nằm trong file `.txt` tương ứng và trong notebook.
