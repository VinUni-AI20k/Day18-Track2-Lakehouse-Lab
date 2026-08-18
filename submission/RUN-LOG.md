# Nhật ký chạy lab — Day 18 Lakehouse

Ghi lại toàn bộ quá trình từ máy trắng đến `8/8 PASS`, kèm những chỗ vấp và những
chỗ số liệu không khớp với tài liệu. Output nguyên văn ở `submission/baseline/`.

**Ngày chạy:** 2026-08-18 · **Máy:** macOS 23.6.0 (Darwin), Apple silicon
**Môi trường:** Python 3.11.9 · deltalake 1.6.2 · duckdb 1.5.5 · pyiceberg 0.11.1 · polars · pyarrow

---

## Cách tái lập kết quả trên máy mới

```bash
git clone <repo> && cd Day18-Track2-Lakehouse-Lab
make setup                      # lần đầu ~7 phút (pip tải wheel); sau đó ~34s
make smoke                      # 9/9 check
make data && make data-ai       # sinh Bronze + corpus + agent traces
make test                       # 24 passed
make run-all                    # 8/8 PASS

# Muốn có số liệu để nộp (run-all không in số của notebook PASS):
rm -rf _lakehouse && make data && make data-ai
for nb in notebooks/0*.py; do
  .venv/bin/python "$nb" > "submission/baseline/nb-$(basename $nb .py).txt" 2>&1
done
```

---

## Giai đoạn 0 — Khảo sát trước khi chạy

Điều đầu tiên cần xác định: **bài này thuộc loại nào?** Quy trình chuẩn cho một
repo bàn giao là "chẩn đoán và khắc phục", nên tôi kiểm xem có gì để sửa không:

```bash
grep -rn "TODO\|FIXME\|YOUR CODE\|raise NotImplementedError" notebooks/ tests/ scripts/
# → 0 kết quả
```

Không có chỗ trống nào. Cả 8 notebook (110–475 dòng mỗi file) đã hiện thực đầy đủ.
Bảy trong tám kết thúc bằng một khối `assert` trên tiêu chí đậu của chính nó — NB4 là
ngoại lệ, xem mục "Bốn chỗ số liệu lệch tài liệu" § 4.

**Kết luận điều chỉnh phạm vi:** đây không phải bài sửa lỗi mà là bài **chạy — đo —
giải thích**. Chu trình chẩn đoán 5 bước vì thế không áp dụng cho toàn bài; tôi chỉ
dùng nó cho những chỗ số liệu thực tế lệch với tài liệu (mục "Bốn chỗ lệch" bên dưới).

Xác định công cụ phản hồi chính, theo thứ tự tin cậy **code > rubric > README**:

| Nguồn | Vai trò | Tin đến đâu |
|---|---|---|
| `Makefile` | định nghĩa mọi lệnh | cao nhất — đây là thứ thật sự chạy |
| `scripts/run_all.py` | cổng chấm chính (8 notebook headless) | cao nhất |
| `tests/test_lab18.py` | 24 pytest | cao nhất |
| `rubric.md` | thang điểm 100 | tin, trừ chỗ mâu thuẫn code |
| `README.md` / mô tả LMS | hướng dẫn | thấp nhất — đã phát hiện lỗi thời |

Ranh giới sửa file: `tests/` và `scripts/` là hạ tầng chấm điểm → **không đụng vào**.
Chỉ tạo mới trong `submission/`.

---

## Giai đoạn 1 — Dựng môi trường

### Khó khăn 1: `make setup` mất ~7 phút thay vì ~20 giây, và không in gì cả

Makefile ưu tiên `uv`, fallback về `pip`:

```make
@command -v uv >/dev/null 2>&1 && uv pip install --python $(PY) -r requirements.txt \
  || $(PIP) install -q -r requirements.txt
```

Máy này không có `uv`, nên rơi vào nhánh `pip install -q`. Cờ `-q` nghĩa là **không
có một dòng tiến trình nào** trong lúc tải ~180 MB wheel. Trong 7 phút đó, dấu hiệu
duy nhất cho biết nó còn sống là kiểm thủ công:

```bash
ps aux | grep "[p]ip install"        # tiến trình còn chạy
du -sh .venv                          # 25M → tăng dần
```

Đây **không phải lỗi** — Makefile xử lý đúng. Nhưng nó là một cái bẫy UX: người
mới sẽ tưởng máy treo và Ctrl-C giữa chừng, để lại một venv hỏng dở. README ghi
"~20s pip / ~4s uv"; con số 20s đó chỉ đúng khi wheel đã nằm sẵn trong cache pip.

**Rút ra:** nếu muốn nhanh thì `brew install uv` trước khi `make setup`. Nếu đã lỡ
chạy pip thì cứ để yên, đừng ngắt.

**Bổ sung sau khi chạy lại (xem Giai đoạn 4):** lần dựng thứ hai chỉ mất **34 giây**,
vì wheel đã nằm trong cache pip. Vậy con số "~20s pip" trong README không sai — nó chỉ
mô tả lần chạy *thứ hai* trở đi. Lần đầu trên máy trắng, hãy dự trù vài phút.

`make setup` kết thúc bằng exit code 0 và tự sinh 9 file `.ipynb` từ Jupytext.

---

## Giai đoạn 2 — Baseline

Chạy đúng thứ tự rubric yêu cầu, lưu nguyên output.

| Lệnh | Kết quả | Thời gian |
|---|---|---|
| `make smoke` | 9/9 check offline | 9.9s |
| `make data` | 200.000 dòng Bronze; 190.052 request_id duy nhất (9.948 bản trùng có chủ đích) | 2.1s |
| `make data-ai` | 2.000 doc (dim=256) · 200 blob (12.5 MB) · 1.578 bước / 300 phiên | 0.3s |
| `make test` | **24 passed** | 1.59s |
| `make run-all` | **8/8 PASS** | 15.1s |

Không một notebook nào đỏ ở lần chạy đầu tiên. Tức là **không có bug nào để chẩn
đoán** — điều này cần nói thẳng thay vì bịa ra một sự cố cho khớp quy trình.

### Khó khăn 2: cổng chấm xanh nhưng không cho tôi một con số nào

`make run-all` in đúng 8 dòng `PASS` và thời gian, hết. Đọc `scripts/run_all.py`
thấy lý do:

```python
proc = subprocess.run([sys.executable, str(nb)], capture_output=True, text=True)
if proc.returncode == 0:
    print(f"  PASS  {nb.name:<32} {dt:6.1f}s")     # stdout bị vứt
else:
    failures.append((nb.name, proc.stdout[-1500:], proc.stderr[-1500:]))
```

stdout chỉ được giữ lại khi notebook **thất bại**. Hợp lý cho CI, nhưng vô dụng khi
bạn cần bằng chứng để nộp — mà rubric thì chấm chính những con số nằm trong stdout đó.

**Cách xử lý:** chạy từng notebook riêng, redirect ra file. Nhân tiện làm luôn cho
đúng: xoá sạch `_lakehouse/` rồi sinh lại dữ liệu, vì lần `run-all` đầu chạy trên
trạng thái đã tích luỹ từ bước `make data` — số đo trên trạng thái tích luỹ không
diễn giải được.

```bash
rm -rf _lakehouse && make data && make data-ai
for nb in notebooks/0*.py; do .venv/bin/python "$nb" > submission/baseline/nb-$(basename $nb .py).txt 2>&1; done
```

8/8 PASS lần nữa, và lần này có đủ số.

---

## Giai đoạn 3 — Số đo, và cách đọc chúng

### NB2 — Z-ORDER: hai chỉ số, chỉ một cái đáng tin

```
Files before OPTIMIZE: 200
BEFORE OPTIMIZE   count=5  median=  73.0 ms
Files after OPTIMIZE+ZORDER: 55
AFTER OPTIMIZE+ZORDER  count=5  median=   7.5 ms
Speedup: 9.8×          Files-pruned ratio: 55.0×  [1 of 55 files cover user_id=4242]
```

Máy này cho speedup 9.8×, vượt ngưỡng 3×. Nhưng con số **đáng lập luận** là 55×:
sau Z-ORDER, đúng một file duy nhất có dải `[3696, 5534]` chứa `user_id = 4242`;
54 file còn lại bị loại chỉ bằng min/max trong log, không cần mở.

Vì sao không nộp mỗi speedup: nó là *hệ quả*, và phụ thuộc page cache. Chạy lại lần
hai khi 200 file nhỏ đã nằm trong RAM thì speedup có thể tụt xuống dưới 3× mà chẳng
có gì sai. Files-pruned ratio đọc thẳng từ metadata nên tất định.

### NB6 — cặp job bị hiểu nhầm nhiều nhất

```
BASELINE (the mess)   files= 200  data=  10.1 MB   avg file size: 51.5 KB
AFTER compaction      files=  11  data=  16.1 MB   ← data bytes TĂNG
```

Compaction ghi file mới **trước khi** file cũ được thu hồi, nên có một khoảng bạn
trả tiền cho cả hai. Sau vacuum mới xuống 6.2 MB.

Phần Iceberg là chỗ tôi thấy có giá trị nhất:

```
before expiry  snapshots= 20  avro= 40  metadata=337.6 KB
after expiry   snapshots=  3  avro= 40  metadata=345.4 KB
avro files on disk: 40 → 40  (deleted: 0)
```

Expire 17 snapshot mà **không xoá một file avro nào**, và metadata còn *to ra*
(vì expiry ghi thêm một `metadata.json`). Phải chạy tiếp sweep mới thu hồi được
37.0 KB từ 17 manifest list mồ côi. Đây chính là lời giải cho câu than phiền
"team em expire snapshot đều mà hoá đơn S3 không giảm".

### NB7 — hai phép đo cho hai kết luận ngược nhau

Cùng một câu hỏi "có nên nhét blob vào bảng không", hai pattern truy cập, hai đáp án:

| Pattern | Inline blob | Pointer | Kết luận |
|---|---|---|---|
| `SELECT topic, count(*) GROUP BY topic` | đọc 1.2 KB / 12.5 MB | đọc 1.2 KB / 2.4 KB | blob **miễn phí** — projection pushdown |
| `WHERE doc_id = 137` (1 dòng) | đọc 12.5 MB (cả row group) | đọc 64 KB | amplification **200×** |

Bài học không phải "blob tốt" hay "blob xấu" mà là **câu trả lời phụ thuộc pattern
truy cập, và phải đo**. Lời khuyên "đừng bao giờ để blob trong bảng" sai với vế đầu.

Về quantization: int8 nhỏ hơn 5.8× (2.6 MB → 451.9 KB), recall@10 = 0.904 nhưng
topic fidelity = 1.000. Nghĩa là 10% "mất" chỉ là hoán đổi giữa các láng giềng gần
tương đương — recall theo ID chính xác **đánh giá thấp** chất lượng quantization
cho RAG.

---

## Bốn chỗ số liệu lệch tài liệu

Đây là phần áp dụng chu trình chẩn đoán thật sự. Cả bốn đều **không** làm đỏ assert nào.

### 1. Tài liệu ghi 22 test, thực tế 24

**Triệu chứng:** `rubric.md` ("make test green (22 tests)"), `README.md` ("22 pytest")
và mô tả bài trên LMS ("Kỳ vọng: 22 passed") đều nói 22.

**Điều tra:**
```bash
$ grep -c "^def test_" tests/test_lab18.py
24
$ .venv/bin/pytest -p no:cacheprovider | tail -1
24 passed in 1.59s
```

**Cơ chế:** tài liệu lỗi thời so với code — commit `24a5391 "Lab v2: 4→8 notebooks"`
mở rộng lab nhưng con số trong văn bản không được cập nhật theo.

**Xử lý:** giữ nguyên test, ghi đúng 24 vào báo cáo. Không sửa test cho khớp tài liệu —
cái quyết định điểm là `pytest`, không phải câu văn.

### 2. `VACUUM would reclaim 211 tombstoned files (0 B)` — dung lượng luôn bằng 0

**Triệu chứng:** NB6 báo dry-run sẽ thu hồi 211 file nhưng **0 B**, rồi ngay dòng sau
lần vacuum thật thu hồi 16.1 MB. Hai con số không thể cùng đúng.

**Điều tra** — tái lập tối thiểu trên một bảng tạm:
```python
doomed = DeltaTable(t).vacuum(retention_hours=0, dry_run=True, enforce_retention_duration=False)
print(repr(doomed[0]))          # 'part-00000-9e08...-c000.snappy.parquet'
print(os.path.isabs(doomed[0])) # False
print(Path(doomed[0]).exists()) # False
```

**Cơ chế:** `vacuum(dry_run=True)` trả về **tên file trần**, không phải đường dẫn.
`du()` trong `scripts/lakehouse.py` thấy path không tồn tại thì trả 0 — nên
`sum(du(f) for f in doomed)` luôn ra 0 bất kể có bao nhiêu byte.

**Phân loại:** lỗi *tường thuật*, không phải lỗi *logic*. Số file (211) đúng; assert
dùng `before_vacuum > du(TABLE)` nên vẫn đo đúng 16.1 MB. Nhưng ai chép dòng in ra
vào báo cáo sẽ viết một câu sai.

**Phòng ngừa:** một phép kiểm rẻ — assert rằng dung lượng dry-run báo ≈ dung lượng
thật thu hồi được — sẽ bắt được ngay.

### 3. `→ 5 files you pay for and cannot see` — thực tế chỉ 3

**Triệu chứng:** NB6 cấy 3 orphan nhưng báo có 5 file "vô hình", rồi `find_orphans()`
lại tìm ra đúng 3.

**Điều tra:**
```
$ ls _lakehouse/scratch/maint_events/_delta_log/*.parquet
00000000000000000099.checkpoint.parquet
00000000000000000199.checkpoint.parquet
00000000000000000203.checkpoint.parquet
$ count_files(T) → 13   |   len(file_uris()) → 10
```

**Cơ chế:** `count_files()` dùng `Path(target).rglob("*.parquet")` — quét **cả**
`_delta_log/`. delta-rs tự tạo checkpoint mỗi 100 commit, nên sau 200 micro-batch
đã có sẵn 2 file `*.checkpoint.parquet` trong log. Chúng bị đếm như data file.
Đó là 2 file "dư". Chúng là **metadata hợp lệ**, không phải rác.

`find_orphans()` có `if "_delta_log" in f.parts: continue` nên loại đúng, tìm ra 3.

**Phân biệt rõ:** 3 file là *orphan thật* (rác cần xoá); 2 file là *checkpoint* (tài
sản cần giữ). Gộp chung hai loại vào một con số là chỗ dễ viết sai nhất trong cả bài.

### 4. NB4 không có khối kiểm tra cuối bài như 7 notebook còn lại

**Triệu chứng:** đếm số dòng `[PASS]` in ra từ baseline:

```
nb-01  4    nb-05  5
nb-02  3    nb-06  9
nb-03  4    nb-07  7
nb-04  0    ← duy nhất
nb-08 10
```

**Điều tra:**
```bash
$ grep -l "complete\." notebooks/0*.py | wc -l
7                          # thiếu đúng 04_medallion.py
$ grep -n "^assert" notebooks/04_medallion.py
87:assert silver_n < bronze_n, (
147:assert n_dates >= 7, (
```

**Cơ chế:** `rubric.md` mô tả cấu trúc lab là *"each notebook ends in an `assert` block
over its own pass criteria"*. NB4 không theo mẫu đó — nó có 2 câu `assert` rời nằm giữa
bài và kết thúc bằng một **checklist markdown**, tức là văn bản chứ không phải phép kiểm.

**Hệ quả:** hai tiêu chí rubric của NB4 không được máy nào kiểm:
"Bronze, Silver, Gold all present on the storage layer" và "cost_usd, error_rate populated".
Trên thực tế chúng đúng (24 dòng Gold đều có `cost_usd` và `error_rate` khác 0 — kiểm bằng
mắt trong `nb-04_medallion.txt`), nhưng nếu một thay đổi sau này làm hỏng chúng thì
`make run-all` vẫn xanh.

**Không tự ý sửa:** thêm khối `checks` vào NB4 sẽ làm bài lệch khỏi repo gốc mà rubric
chấm. Ghi nhận và kiểm bằng mắt là phản ứng đúng ở đây.

**Tổng số phép kiểm thực tế:** 42 dòng `[PASS]` + 2 assert trần trong NB4 = **44**,
không phải một con số tròn trịa nào cả.

---

## Giai đoạn 4 — Kiểm bản clone sạch (Part C, 4 điểm)

Đây là mục rubric cho điểm cao mà ít người kiểm: `make run-all` phải xanh **từ một
`make setup` sạch**, không phải từ máy đã chạy sẵn.

```bash
make clean      # xoá CẢ .venv lẫn _lakehouse/
make setup && make smoke && make data && make data-ai && make test && make run-all
```

| Bước | Kết quả |
|---|---|
| `make clean` | xoá `.venv _lakehouse notebooks/.ipynb_checkpoints .pytest_cache` |
| `make setup` | ✓ **34s** (cache pip ấm — lần đầu ~7 phút) |
| `make smoke` | 9/9 |
| `make data` / `data-ai` | 200.000 dòng / 2.000 doc · 200 blob · 1.578 bước |
| `make test` | **24 passed** in 1.53s |
| `make run-all` | **8/8 PASS** in 14.5s |

Part C xanh. Output đầy đủ ở `submission/baseline/05-clean-machine.txt`.

Con số ổn định giữa hai lần chạy độc lập (15.1s → 14.5s; 24 passed cả hai lần), nên
kết quả không phải may mắn một lần.

---

## Những gì tôi KHÔNG làm được / không kiểm chứng

Nói rõ để báo cáo trung thực:

- **Không chạy đường Spark.** Cần Docker hoặc Apple `container` (~6 GB RAM, ~2 GB pull).
  Rubric chấp nhận bằng chứng từ đường lightweight, và đường lightweight phủ đủ 8/8
  notebook trong khi Spark chỉ phủ 4. Mọi khẳng định trong báo cáo này chỉ áp dụng
  cho đường lightweight.
- **Không kiểm chứng con số chi phí.** Các số `$220/ngày` (NB5), `$990/tháng` (NB6),
  `$4.00/ngày` (NB6) là phép ngoại suy từ giá niêm yết cắm cứng trong notebook, không
  phải đo trên hệ thống thật. Bảng giá token trong NB4 được chính notebook ghi chú là
  `Illustrative cost model — NOT canonical pricing`.
- **Chưa đo lại nhiều lần.** Speedup NB2 (9.8×) đo một lần, `n=3` runs nội bộ. Muốn
  khẳng định nó ổn định thì phải chạy lại vài lượt — mà đó chính là lý do rubric cho
  dùng files-pruned ratio thay thế.

---

## Tóm tắt

| Hạng mục | Kết quả |
|---|---|
| Notebook | 8/8 PASS · 44 phép kiểm |
| Unit test | 24 passed (tài liệu ghi 22) |
| `make run-all` | xanh, 15.1s |
| Part C (clone sạch) | xanh — setup 34s, run-all 14.5s |
| Bug thật trong logic lab | 0 |
| Lỗi tường thuật số liệu tìm được | 2 (NB6) |
| Chỗ lệch cấu trúc | 1 (NB4 thiếu khối kiểm tra cuối) |
| Chỗ tài liệu lỗi thời | 1 (số lượng test) |
| Khó khăn thực tế | setup chậm & im lặng khi thiếu `uv`; cổng chấm không xuất số liệu |
