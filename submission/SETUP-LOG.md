# Setup Log — Day 18 Lakehouse Lab

Ghi lại quá trình setup môi trường để viết báo cáo.

## Môi trường

- Máy: Windows 11, project ở `E:\AIinAction\Lab\Day18-Track2-2A202601031-LETHIYENNHI`
- Lab yêu cầu Unix (Makefile dùng `.venv/bin/python`, bash) — dùng **WSL (Ubuntu)**
- Quyết định cuối: làm việc **toàn bộ tại `/mnt/e/AIinAction/Lab/Day18-Track2-2A202601031-LETHIYENNHI`** (đúng thư mục đã `git clone`), không chuyển sang home directory của WSL.

## Sự cố 1 — Setup nhầm trên Windows native

- Vô tình chạy `python -m venv .venv` + cài requirements trực tiếp trên PowerShell (Windows venv, layout `Include/`, `Lib/`, không có `bin/`)
- Không tương thích Makefile (đòi `.venv/bin/python` kiểu Unix)
- Thử xoá bằng `rm -rf .venv` trong PowerShell, lỗi nguyên văn:
  ```
  (.venv) PS E:\AIinAction\Lab\Day18-Track2-2A202601031-LETHIYENNHI> rm -rf .venv
  Remove-Item : A parameter cannot be found that matches parameter name 'rf'.
  At line:1 char:4
  + rm -rf .venv
  +    ~~~
      + CategoryInfo          : InvalidArgument: (:) [Remove-Item], ParameterBindingException
      + FullyQualifiedErrorId : NamedParameterNotFound,Microsoft.PowerShell.Commands.RemoveItemCommand
  ```
  Lý do: `rm` trong PowerShell chỉ là alias của `Remove-Item`, không nhận flag kiểu Unix (`-rf`).
- **Fix:** `deactivate` rồi `Remove-Item -Recurse -Force .venv` để xoá venv Windows

## Sự cố 2 — `deltalake` ghi file lỗi trên `/mnt/e` (DrvFs)

- Từ WSL, đứng tại `/mnt/e/AIinAction/Lab/...`, chạy `make setup` — thành công (venv + jupytext convert 8 notebook `.py` → `.ipynb`), nhưng **rất chậm** (I/O qua 9p/DrvFs protocol khi ghi hàng nghìn file nhỏ của site-packages)
- `make smoke` — **PASS** (9/9 check offline, vì ghi ít dữ liệu)
- `make data` — **FAIL**, lỗi nguyên văn:
  ```
  Traceback (most recent call last):
    File "/mnt/e/AIinAction/Lab/Day18-Track2-2A202601031-LETHIYENNHI/scripts/generate_data_lite.py", line 100, in <module>
      main(n)
    File "/mnt/e/AIinAction/Lab/Day18-Track2-2A202601031-LETHIYENNHI/scripts/generate_data_lite.py", line 89, in main
      write_deltalake(out, df.to_arrow(), mode="overwrite")
    File "/mnt/e/AIinAction/Lab/Day18-Track2-2A202601031-LETHIYENNHI/.venv/lib/python3.12/site-packages/deltalake/writer/writer.py", line 149, in write_deltalake
      write_deltalake_rust(
  _internal.DeltaError: Failed to parse parquet: External: Generic LocalFileSystem error: Upload aborted
  make: *** [Makefile:43: data] Error 1
  ```
  Nguyên nhân gốc: `/mnt/e` không phải filesystem Linux thật mà là **DrvFs** — WSL dịch truy cập ổ NTFS Windows qua giao thức **9p**. DrvFs thiếu một số thao tác file POSIX chuẩn (đặc biệt: ghi file tạm rồi `rename` atomic). Thư viện `deltalake` (Rust, qua crate `object_store`) bắt buộc dùng đúng kiểu ghi atomic này để đảm bảo tính ACID của Delta Lake. Ghi nửa chừng trên DrvFs bị hỏng → lỗi "Upload aborted". `make smoke` không lộ lỗi vì ghi ít dữ liệu hơn `make data` (200K dòng Bronze).

## Sự cố 3 — venv trên WSL thiếu `pip`

- Thử hướng khác: copy project ra `~/lab18` (native ext4) bằng `rsync` (loại trừ `.venv`, `_lakehouse`, `.pytest_cache`, `__pycache__`)
- `cd ~/lab18 && make setup` — **FAIL**:
  ```
  /bin/sh: 2: .venv/bin/pip: not found
  make: *** [Makefile:25: setup] Error 127
  ```
  Nguyên nhân: bản Ubuntu/WSL cài sẵn thiếu gói `python3-venv` đầy đủ (không có `ensurepip`), venv tạo ra rỗng, không có `pip`. Không có `uv` cài sẵn để venv fallback qua đó.
- **Fix:** `sudo apt update && sudo apt install -y python3-pip python3.12-venv`

## Quyết định cuối — ở lại `/mnt/e`, dùng symlink né lỗi DrvFs

Không muốn tách project ra khỏi thư mục đã `git clone` (`~/lab18` là bản sao rời, sợ lệch nhánh/khó push). Thay vào đó: giữ nguyên code, `.venv`, git tại `/mnt/e/...`, chỉ **dời phần ghi dữ liệu Delta/Iceberg** (nơi thực sự đụng bug DrvFs) ra ext4 bằng symlink — code không đổi gì (`scripts/lakehouse.py` mặc định đọc/ghi vào `_lakehouse/` ngay trong repo).

```bash
cd /mnt/e/AIinAction/Lab/Day18-Track2-2A202601031-LETHIYENNHI
rm -rf _lakehouse                       # dọn rác từ lần lỗi Sự cố 2
mkdir -p ~/lakehouse-data               # nơi lưu thật, trên ext4 native
ln -s ~/lakehouse-data _lakehouse       # _lakehouse/ vẫn "nằm" trong repo, chỉ là symlink
sudo apt update && sudo apt install -y python3-pip python3.12-venv
make setup
```

`_lakehouse/` và `.venv/` đều đã có trong `.gitignore`, nên symlink không ảnh hưởng gì tới git — giảng viên clone/chạy trên máy họ hoàn toàn không thấy hay phụ thuộc vào cách setup local này.

## Kết quả cuối cùng — mọi lệnh chạy sạch tại `/mnt/e/...`

```
make smoke     → ✓ 9/9 check offline pass
make data      → Wrote 200,000 rows → .../_lakehouse/bronze/llm_calls_raw (qua symlink, không còn lỗi Upload aborted)
make data-ai   → docs 2,000 rows, blobs 200 files, traces 1,578 steps — OK
make test      → tất cả pytest pass
make lab       → Jupyter Lab chạy tại http://localhost:8888
```

## Bài học

- Lab dùng `deltalake`/`pyiceberg` ghi file theo cơ chế atomic (temp file → rename) — filesystem phải hỗ trợ đầy đủ POSIX semantics. `/mnt/*` trong WSL (DrvFs qua 9p) không đáp ứng được, gây lỗi ghi ngầm dù `make setup`/`make smoke` vẫn xanh (lỗi chỉ lộ khi ghi dữ liệu thật, khối lượng lớn hơn).
- Symlink một thư mục con ra ext4 native là cách né lỗi mà không phải di chuyển toàn bộ project — giữ nguyên vị trí git clone, chỉ cô lập đúng phần I/O có vấn đề.
- Ubuntu/WSL có thể thiếu `ensurepip` mặc định trong `python3-venv` — cần cài thêm `python3-pip` + `python3.<version>-venv` thủ công.
