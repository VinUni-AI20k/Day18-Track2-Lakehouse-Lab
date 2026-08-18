# Bài nộp — Lab Lakehouse Ngày 18 (Track 2)

**Nguyễn Quang Tường** · 2A202601597 · đường lightweight · Windows 11 / Python 3.11.9

```bash
make clean && make setup && make smoke && make data && make data-ai && make test && make run-all
```

| Cổng | Kết quả |
|---|---|
| `make smoke` | 9/9 |
| `make test` | 24/24 |
| `make run-all` | **8/8 trong 30,6 s** (lần chạy sạch trước đó: 37,0 s sau `make clean` + `make setup`) |

Toàn bộ chuỗi trên nằm trong một log liên tục:
[`logs/05_clean_rebuild.log`](logs/05_clean_rebuild.log).

> **Trên Windows** cần đặt `PYTHONIOENCODING=utf-8` trước khi chạy — notebook in
> `✓`/`→`/`⚠` còn console mặc định dùng cp1252. Máy này không có `make`
> (Makefile viết theo đường dẫn Unix `.venv/bin/python`), nên các target được
> chạy bằng lệnh Python tương đương; log ghi rõ từng bước.

## Nội dung

| Đường dẫn | Là gì |
|---|---|
| [`FINDINGS.md`](FINDINGS.md) | **Deliverable chính.** Từng tiêu chí rubric kèm con số đo được *và* cách đọc con số đó. |
| [`REFLECTION.md`](REFLECTION.md) | Bài reflection về Top-5 anti-pattern (200 từ). |
| [`logs/`](logs/) | Stdout thô: 5 cổng `make` và output từng notebook. |
| [`screenshots/`](screenshots/) | Bằng chứng đường lightweight (4 file — xem bảng ngay dưới). |
| [`bonus/ARCHITECTURE.md`](bonus/ARCHITECTURE.md) | Bonus tuỳ chọn: LLM observability ở quy mô 1 tỉ request/ngày. |
| [`../notebooks/*.ipynb`](../notebooks/) | Tám notebook đã thực thi, giữ nguyên output cell, **không có output lỗi nào**. |

### Thư mục `screenshots/`

| File | Nguồn |
|---|---|
| `lakehouse_tree.txt` | đúng lệnh `os.walk('_lakehouse')` trong đề bài (1.152 dòng) |
| `delta_log_sample.json` | đúng lệnh `cat …/users_delta/_delta_log/00000000000000000000.json` |
| `01_tree_lakehouse.txt` | bản cây thư mục có định dạng + chú giải ba tầng medallion |
| `02_delta_log_commits.txt` | 3 commit `_delta_log` có chú giải (commit 0, schema evolution, OPTIMIZE+ZORDER) |

Hai file đầu tạo đúng theo lệnh trong đề; hai file sau là bản đọc được cho người chấm.

## Bản đồ rubric → bằng chứng

### Phần A — Nền tảng (44 điểm)

| # | Tiêu chí | Bằng chứng | Đo được |
|---|---|---|---|
| 1 | `_delta_log/` có commit JSON | `screenshots/02_…` bc. 1–2 | 2 commit |
| 1 | Schema enforcement chặn `age=str` | `logs/nb_01…` | `Cast error`, **không sinh commit** |
| 1 | `schema_mode="merge"` thêm `tier` | `logs/nb_01…` | có `tier`, dòng cũ = NULL |
| 2 | ≥ 100 file trước OPTIMIZE | `logs/nb_02…` | **200** |
| 2 | Speedup ≥ 3× **hoặc** pruning ≥ 10× | `logs/nb_02…`, bc. 3 | **9,0×** và **55×** |
| 2 | `numFiles` giảm rõ rệt | `logs/nb_02…` | 200 → 55 |
| 3 | ≥ 5 version, có cả RESTORE | `logs/nb_03…` | **5**, RESTORE = v4 |
| 3 | MERGE 100K thành công | `logs/nb_03…` | 0,07 s · 50K update + 50K insert |
| 3 | RESTORE; `score<0` = 0 | `logs/nb_03…` | **0** |
| 4 | Bronze/Silver/Gold trên storage | `screenshots/01_tree…` | đủ cả ba |
| 4 | Silver < Bronze | `logs/nb_04…` | 200.000 → **190.052** |
| 4 | Gold ≥ 7 ngày × 3 model | `logs/nb_04…` | **8 × 3 = 24 dòng** |
| 4 | Khối assert cuối in `NB4 complete.` | `logs/nb_04…` | **4/4 PASS** (bổ sung — xem ghi chú cuối) |

### Phần B — Lakehouse 2026 (50 điểm)

| # | Tiêu chí | Đo được |
|---|---|---|
| 5 | Tạo bảng qua catalog, spec `day(ts)` | `SqlCatalog`, spec `1000: ts_day: day(2)` |
| 5 | Hidden-partition pruning ≥ 5× khi lọc `ts` | **10×** (10 file → 1) |
| 5 | Ba tầng metadata + tỉ lệ byte | 10 list / 10 manifest / 10 file; **metadata = 284,8% data** |
| 5 | `field_id` giữ nguyên qua rename; ≥ 2 spec | `latency_millis` giữ **id 4**; `spec_id [1,2]`; 5.500 dòng đọc được |
| 6 | Job 1 compaction ≥ 10× | 200 → 11 (**18×**) |
| 6 | Job 2 clustering skip ≥ 50% | **90%** (1/10 file) |
| 6 | Job 3 expiry | Delta 16,1 → 6,2 MB; Iceberg 20 → **3** snapshot |
| 6 | Job 4 orphan | **3** orphan Delta + **17** manifest list Iceberg bị bỏ rơi, quét sạch |
| 6 | Job 5 checkpoint | `*.checkpoint.parquet` + `_last_checkpoint` |
| 7 | Khuếch đại random-access ≥ 5× | **200×** (row group 12,5 MB so với blob 64 KB) |
| 7 | int8 nhỏ ≥ 3×; recall + topic fidelity | **5,8×**; recall@10 **0,904**, fidelity **1,000** |
| 7 | Semantic search chạy bằng SQL | top-5 đều `topic=storage` |
| 7 | Tái hiện lỗi lifecycle | trong bảng **0** hit, index ngoài **8** hit |
| 8 | Medallion cho trajectory | Silver partition `policy-v2`/`policy-v3`; Gold phủ cả hai |
| 8 | Ghim version, replay khớp | v0 đã ghim = **1.578** bước sau khi bảng lên v1 |
| 8 | Bề mặt MCP | 5 lượt → **1** lần đọc catalog; `input_required`; task hoàn tất |
| 8 | Đủ 4 rổ Điều 10; loại UNCLASSIFIED | 4 rổ + partition UNCLASSIFIED; **1.666/2.000** dùng được |

### Phần C — Khả năng tái lập (6 điểm)

`make test` 24/24 · `make run-all` 8/8 chạy từ môi trường sạch hoàn toàn. Xem mục
cuối `FINDINGS.md` để biết thay đổi source duy nhất cần thiết: một lỗi khoá file
trên Windows trong `scripts/lakehouse.py:reset_catalog()` khiến hàm này âm thầm
trở thành no-op.

## Hai phát hiện "đi ngược niềm tin phổ biến"

Cả hai nằm ở `FINDINGS.md` → NB6, và đều tái hiện từ output của chính máy này:

1. **`VACUUM` không thu hồi orphan chưa từng commit.** 3 file lùi ngày 30 ngày vẫn
   sống sót ở mọi mức retention — delta-rs chỉ thu hồi thứ đã bị log *tombstone*,
   mà file chưa từng commit thì chưa từng bị tombstone.
2. **`expire_snapshots` không xoá file nào.** Snapshot 20 → 3, file avro 40 → 40,
   và metadata trên đĩa còn *phình* 336,0 → 343,7 KB. Job 3 và Job 4 là một cặp.

`FINDINGS.md` còn xử lý một điểm vênh thứ ba, nhỏ hơn, nằm trong chính output của
NB6 — dòng "5 files you pay for and cannot see" đã tính nhầm 2 checkpoint tự động
của Delta thành orphan, và đó là lý do job quét chỉ xoá đúng 3 file.
