# Day 18 — Bảng bằng chứng chấm điểm

**Học viên:** Nguyễn Hoàng Thảo Tiên · **Mã:** 2A202601650 · **Track 2 — Day 18 Lakehouse Lab**
**Ngày chạy:** 18/08/2026 · **Đường chạy:** lightweight (không Docker, không JVM, offline)

| Môi trường | Giá trị |
|---|---|
| Python | 3.14.6 (arm64, macOS 24.6.0) |
| deltalake | 1.6.2 (delta-rs) |
| pyiceberg | 0.11.1 (SqlCatalog trên SQLite) |
| duckdb / polars / pyarrow / numpy | 1.5.5 / 1.43.2 / 25.0.1 / 2.5.2 |

Chuỗi lệnh đã chạy đúng như rubric yêu cầu:

```
make setup && make smoke && make data && make data-ai && make test && make run-all
```

Kết quả: `smoke` 9/9 ✓ · `test` **24 passed** · `run-all` **8/8 PASS in 9.9s**.
Toàn bộ transcript: [`screenshots/00_make_gates.txt`](screenshots/00_make_gates.txt).
Output thô của từng notebook: [`outputs/`](outputs/). Notebook đã chạy giữ output cells: [`../notebooks/*.ipynb`](../notebooks/).

> Ghi chú trung thực: rubric ghi "22 pytest", suite trong repo hiện tại có **24** test — cả 24 đều xanh.

---

## Part A — Foundations (44 điểm)

### NB1 `01_delta_basics` — 8 điểm

| Tiêu chí | Số đo | Đọc số này thế nào |
|---|---|---|
| `_delta_log/` JSON commits (4đ) | 2 file: `...0000.json` (WRITE/Overwrite), `...0001.json` (WRITE/Append) | Mỗi commit là **một dòng JSON per action**, không phải một object: `commitInfo` + `protocol` + `metaData` + `add`. Đó là lý do append-only log này an toàn với ghi song song — writer chỉ thêm file mới, không sửa file cũ. |
| Schema enforcement chặn `age=str` (2đ) | `Exception: Cast error: Cannot cast string 'thirty' to value of Int64 type` | Lỗi đến từ **tầng Rust khi cast Arrow**, tức là bị chặn *trước khi* có bất kỳ byte nào vào bảng. Data lake thuần Parquet sẽ nhận file này và bung ra thành lỗi lúc đọc, 3 tháng sau, ở dashboard của người khác. |
| `schema_mode="merge"` thêm `tier` (2đ) | `tier` xuất hiện trong `schemaString` của v1; 3 dòng cũ = `null`, dòng mới = `premium`; DuckDB thấy 2 nhóm `[('premium',1),(None,3)]` | Schema evolution là **metadata-only**: v1 ghi lại `metaData` mới nhưng **không rewrite** parquet của v0. Đọc file cũ với schema mới → cột thiếu thành `null`. Không backfill, không migration job. |

Bằng chứng: [`screenshots/02_delta_log_nb1_users_delta.png`](screenshots/02_delta_log_nb1_users_delta.png) — so hai `schemaString` v0 vs v1 sẽ thấy đúng một field được thêm.

### NB2 `02_optimize_zorder` — 12 điểm

| Tiêu chí | Số đo | Đọc số này thế nào |
|---|---|---|
| Tái hiện small-file ≥ 100 file (3đ) | **200 file** từ 200 lần append (1M dòng, 5K/batch) | Đây không phải code sai. Đây là một job streaming trigger 5 giây chạy qua đêm. Mỗi commit đều đúng; **sự tích luỹ** mới là bug. |
| Speedup ≥ 3× **hoặc** pruned ≥ 10× (6đ) | speedup **12.4×** (62.6 ms → 5.0 ms) **và** files-pruned **55.0×** (1/55 file chứa `user_id=4242`) | Đạt cả hai, nhưng **55× mới là con số nên tin**. Speedup wall-clock trên SSD laptop có page cache và nhiễu CPU; files-pruned đọc trực tiếp min/max trong log nên **tất định**. Rubric cho chọn một — chọn con số tất định thì lập luận mới đứng được trước design review. |
| `numFiles` giảm rõ (3đ) | 200 → **55** | Không phải 200→1. `target_size=256 KB` được đặt **có chủ đích**: nếu compact về 1 file thì Z-ORDER không còn gì để prune, và bài học file-skipping biến mất. Sản xuất nhắm 128–512 MB. |

Log ranges sau Z-ORDER: `[1,1851] [1851,3696] [3696,5534]← chứa target ... [99535,100000]` — 55 dải **gần như không chồng nhau**. Trước Z-ORDER, mỗi file chứa user_id random 1–100.000 nên mọi dải đều phủ 4242 → stats vô dụng, engine buộc phải đọc hết. **Z-ORDER không làm query nhanh hơn; nó làm stats trở nên hữu ích.**

### NB3 `03_time_travel` — 12 điểm

| Tiêu chí | Số đo | Đọc số này thế nào |
|---|---|---|
| `history()` ≥ 5 version kể cả RESTORE (4đ) | 5 version: v0 WRITE · v1 WRITE · v2 MERGE · v3 WRITE · **v4 RESTORE** | RESTORE **không xoá** v3. Nó ghi v4 với nội dung của v2. Rollback vì thế **tự nó là một transaction có audit trail** — ngược với `UPDATE`/`DELETE` trên OLTP, nơi trạng thái cũ biến mất. |
| MERGE upsert 100K dòng (4đ) | 0.06s · `num_output_rows=150000`, `num_target_rows_updated=50000`, `num_target_rows_inserted=50000`, `num_target_files_scanned=1` | 50K update + 50K insert từ 100K dòng source — đúng semantics upsert. `files_scanned=1` vì bảng chỉ có 1 file: ở scale thật con số này là chỉ báo quan trọng nhất của chi phí MERGE. |
| RESTORE xoá bad data, `score<0` = 0 (4đ) | RESTORE 0.00s; `score<0` đếm được **0** dòng (trước đó 50 dòng `score=-1, status=NULL`) | RESTORE là **metadata-only**: chỉ đổi tập file được tham chiếu, không rewrite dữ liệu → 0.00s bất kể bảng lớn cỡ nào. Đây là lý do "rollback 30 phút" trong SLA là khả thi. |

### NB4 `04_medallion` — 12 điểm

| Tiêu chí | Số đo | Đọc số này thế nào |
|---|---|---|
| Bronze/Silver/Gold có trên storage (4đ) | `_lakehouse/bronze/llm_calls_raw` · `_lakehouse/silver/llm_calls` (8 partition `date=`) · `_lakehouse/gold/llm_daily_metrics` (8 partition `date=`) | Xem [`screenshots/01_tree_lakehouse.png`](screenshots/01_tree_lakehouse.png). Bronze giữ `raw_json` **nguyên trạng** — đó là hợp đồng của Bronze: nếu logic parse ở Silver sai, ta replay lại được, không phải đi xin lại dữ liệu từ upstream. |
| Silver < Bronze do dedup (4đ) | 200.000 → **190.052** (−9.948 = **−4,97%**) | 9.948 dòng là **retry cùng `request_id`**, không phải rác. Dedup bằng `ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts) = 1` giữ lần *đầu*. Nếu bỏ qua bước này, cost dashboard sẽ **thổi phồng chi tiêu ~5%** — và không ai phát hiện, vì con số vẫn "trông hợp lý". |
| Gold đúng ≥ 7 date × 3 model (4đ) | **24 dòng = 8 date × 3 model**, đủ `p50/p95/cost_usd/error_rate` | Ví dụ 2026-04-07: haiku p50 558 ms / $46.27 · sonnet p50 1391 ms / $343.06 · opus p50 3069 ms / $285.16. `error_rate` 4,9–6,2%. **Đọc kỹ:** sonnet đắt hơn opus dù rẻ hơn 5×/token — vì volume gấp ~6×. Đây chính là lý do Gold phải là `(date, model)` chứ không phải một con số tổng: tổng chi phí không nói cho bạn biết cần tối ưu cái gì. |

---

## Part B — Lakehouse 2026 (50 điểm)

### NB5 `05_iceberg_catalog` — 13 điểm

| Tiêu chí | Số đo | Đọc số này thế nào |
|---|---|---|
| Tạo bảng **qua catalog**, spec `day(ts)` (3đ) | `SqlCatalog` → `create_table('lake.llm_events')`; spec `[1000: ts_day: day(2)]`; format-v2 | Điểm đáng nói là thứ **không** làm: không chọn path. Catalog sở hữu layout. Chính lớp gián tiếp đó cho phép catalog về sau vend credential, áp row-filter, và plan scan hộ client. |
| Hidden-partition pruning ≥ 5× (5đ) | filter trên **`ts`** → **10 file → 1 file = 10×**, 500 dòng | `ts_day` **không phải cột ta insert** — nó được derive từ transform lưu trong metadata. Người dùng Hive quên `WHERE dt=...` sẽ đọc cả 10 file: ở 512 MB/file và $5/TB scan thì **4,5 GB thừa/query = $0,022**, × 10.000 query/ngày = **$220/ngày** cho đúng một predicate bị quên. Hidden partitioning **xoá bỏ cơ hội quên** — đó mới là feature, không phải "nhanh hơn". |
| 3 tầng metadata + tỉ lệ metadata:data (1đ) | metadata.json → 10 manifest list → 10 manifest file → 10 data file; data 47,3 KB vs metadata 134,7 KB = **285%** | 285% là con số *nực cười* — và đó là bài học: ở 500 dòng/file, metadata át data. Ở 512 MB/file tỉ lệ này ~0,1%. **Small file phạt bạn hai lần**: nhiều data file *và* nhiều metadata để plan. Đây là động lực đằng sau server-side planning của Iceberg 1.11. |
| Rename giữ `field_id`; ≥ 2 spec cùng tồn tại (4đ) | `latency_ms → latency_millis` giữ **`field_id=4`**; `spec_id` in use = **[1, 2]**; 5.500 dòng đọc được qua cả hai spec | Parquet định danh cột theo **vị trí**, Hive theo **tên** — cả hai vỡ khi rename/reorder. Iceberg gán ID số vĩnh viễn, tên chỉ là nhãn → rename là metadata-only, **0 file bị rewrite**. Partition evolution cũng vậy: file cũ nằm im ở spec 1 (`ts_day=`), file mới ghi theo spec 2 (`ts_day=/model_id=`), một bảng đọc được cả hai. Trên Hive đây là migration rewrite toàn bảng. |

### NB6 `06_maintenance` — 13 điểm

Bối cảnh: 200 micro-batch → 100.000 dòng, **file trung bình 51,5 KB** (mục tiêu sản xuất 128–512 MB).
Chi phí request thuần: 200 file × 50.000 query/ngày = **10.000.000 GET/ngày = $4,00/ngày**; nếu 4 file thì $0,08/ngày. **Chi phí không tuyến tính theo bytes mà theo số file.**

| Job | Số đo trước → sau | Đọc số này thế nào |
|---|---|---|
| **J1 Compaction** (4đ) | **200 → 11 file (18×)**; `filesAdded=11, filesRemoved=200`; data **10,1 MB → 16,1 MB** | Dung lượng **tăng** sau compaction. Không phải bug: file mới được ghi trước, file cũ chỉ bị *tombstone* chứ chưa xoá. **Bạn trả tiền hai lần, trong một khoảng thời gian** — phải tính vào budget, và đây chính là lý do J1 và J3 phải chạy thành cặp. |
| **J2 Clustering** (3đ) | point query `user_id=12345`: **11/11 file → 1/10 file = skip 90%** | Đo bằng **chất lượng stats** (`min.user_id ≤ target ≤ max.user_id` từ `get_add_actions`), không bằng đồng hồ — nên tất định. Trước clustering mọi dải min/max đều chồng nhau ⇒ stats *không chứng minh được gì* ⇒ engine buộc đọc hết. |
| **J3 Expiry** (3đ) | Delta: 211 file tombstoned, **thu hồi 16,1 MB**, 11 → 10 file. Iceberg: **20 → 3 snapshot** | Delta: `retention_hours=0` (chỉ để thấy được trong lab) — **time travel về v0 mất vĩnh viễn**. Đó là cái giá vừa trả. Sản xuất ≥ 168h. |
| **J4 Orphans** (2đ) | Trên đĩa 15 parquet vs trong log 10 → **5 file trả tiền mà không thấy**; `find_orphans()` xoá **3 file (21,2 KB)**; Iceberg: **17 manifest list mồ côi (37,1 KB)** bị quét, metadata 344,2 → **307,1 KB** | Chi tiết đáng chú ý: có **5** file lạ nhưng chỉ **3** bị xoá — 2 file còn lại mới hơn `min_age_hours=24` nên **age guard giữ lại**. Guard đó không phải tuỳ chọn: bỏ nó đi là bạn xoá file mà một writer đang commit dở, và làm hỏng bảng. |
| **J5 Checkpoint** (1đ) | `00000000000000000203.checkpoint.parquet` + `_last_checkpoint` = `{"version":203,...,"numOfAddFiles":10}` | Trong `_delta_log/` có **3** checkpoint: v99 và v199 do delta-rs **tự tạo mỗi 100 commit**, v203 do `create_checkpoint()` gọi tay. Notebook in ra file `...099` (kết quả glob đầu tiên) — nhưng `_last_checkpoint` mới là thứ reader thật sự đọc, và nó trỏ v203. Cold reader vì thế nạp 1 checkpoint + vài JSON thay vì replay 204 JSON. |

**FinOps (§12):** managed compaction cho 500 GB / 2.000.000 file, chạy hằng ngày = $750/mo (per-GB) + $240/mo (per-object) = **$990/mo**, trong đó **24% hoá đơn do số file quyết định, không phải dung lượng**. "Fully managed" ≠ miễn phí: bảng small-file bệnh nhất chính là bảng đắt nhất khi auto-compact. Sửa trigger interval của writer rẻ hơn thuê người dọn.

### NB7 `07_vectors_multimodal` — 13 điểm

| Tiêu chí | Số đo | Đọc số này thế nào |
|---|---|---|
| Amplification random-read ≥ 5× (4đ) | inline: 1 row group / 200 dòng / **12,5 MB**; pointer: **64,0 KB**/GET → **200×** | Đơn vị I/O của Parquet là **row group**, không phải row. Lấy 1 frame phải đọc + giải nén cả group. Ở 1.000 random fetch/giây để feed GPU, **chính con số 200× này là bài toán GPU-starvation** — không phải kích thước file. Đây là thứ mà "3–35× faster random access" của Lance nói tới. |
| int8 ≥ 3× nhỏ hơn; recall@10 **và** topic fidelity (4đ) | 2,6 MB → **451,9 KB = 5,8×** (tiết kiệm 83%); **recall@10 = 0,904**; **topic fidelity = 1,000** | 5,8× > 4× lý thuyết vì Parquet nén int8 tốt hơn float32. Quan trọng hơn: recall@10 mất ~10% **ID chính xác** nhưng **100% kết quả vẫn đúng chủ đề** — "miss" chỉ là đảo chỗ giữa các neighbour tương đương. Vậy nên **exact-ID recall đánh giá thấp chất lượng quantization cho RAG**; báo cáo một mình nó sẽ dẫn tới quyết định sai. |
| Semantic search bằng SQL (1đ) | `array_cosine_similarity` (DuckDB core, offline); top-5 **đều** topic `storage`, sim 1.000 / 0.779 / 0.777 / 0.776 / 0.768; 8,1 ms cho 2.000 vector | Scaling honest: 100K → ~406 ms (borderline), 1M → ~4.055 ms = **không phải serving path**. Kết luận đúng là: lakehouse = **system-of-record**, vector DB = **derived index có thể build lại**. |
| **Lifecycle bug tái hiện** (4đ) | `user_042` xin xoá 8 doc → lakehouse **0 hit**, external index **8 hit** | Đây là **compliance bug**, không phải stale cache. Index sẽ tiếp tục trả nội dung của `user_042` vào prompt RAG cho tới lần sync sau — và nếu sync là one-way upsert (trường hợp phổ biến) thì **mãi mãi**, vì *delete* đúng là thao tác mà pipeline sync hay quên. Cách đúng: đọc **CDF** — đo được **8 delete event** mang đúng `doc_id` cần evict. Tốt nhất là không cần sync: giữ vector trong hàng, lifecycle do chính bảng cưỡng chế. |

Bonus quan sát (README nêu là điều slide chưa nói): ghi `fixed_size_list<float>[256]` xuống Delta, đọc lên thành `list<element: float>` — Delta protocol **không có kiểu vector cố định chiều**, phải `emb::FLOAT[256]` lúc query. Đó chính là lý do Hudi 1.2 thêm cột `VECTOR(dim, type)` hạng nhất.

### NB8 `08_agents_provenance` — 11 điểm

| Tiêu chí | Số đo | Đọc số này thế nào |
|---|---|---|
| Trajectory qua medallion; Silver partition `agent_version`; Gold 2 policy (3đ) | 1.578 step; partition trên đĩa `agent_version=policy-v2` và `=policy-v3`; Gold 2 dòng: v2 success 0,760 / v3 0,753 (cost ~$0,069/trajectory, ~5,26 step) | Trajectory khác dữ liệu supervised ở một điểm làm đổi thiết kế storage: **phân phối dữ liệu dịch khi policy tốt lên**, nên dataset tĩnh là vô dụng. Partition theo `agent_version` để **drop hoặc retrain trên rollout của một policy mà không đụng policy kia**. Và đọc số cho đúng: 0,760 vs 0,753 với n=150 là **nhiễu, không phải cải thiện** — kết luận "v3 tệ hơn" ở đây là sai thống kê. |
| Training run pin version; replay khớp (3đ) | pin `table_version=0`, `n_steps_seen=1578`; rollout mới → v1 / 1.978 step; replay tại v0 → **1.578 step, khớp = True** | **Một số nguyên** là toàn bộ khác biệt giữa một run tái lập được và một câu chuyện. Không có nó, "policy này train trên dữ liệu nào?" không có câu trả lời — và Annex IV cũng không. |
| MCP surface (3đ) | 5 lượt agent → **1 catalog round-trip** (`ttlMs=60000, cacheScope=session`); `delete_rows` → `resultType=input_required` trước khi được approve, sau approve → `ok`; `submit_scan` → poll 3 lần → `completed {rows: 300}` | Ba thay đổi của MCP 2026-07-28, mỗi cái giải một bài toán data-platform cụ thể: cacheable list để catalog 50.000 bảng không tự liệt kê lại mỗi turn; `input_required` để **agent không thể tự phê duyệt** (cổng thuộc protocol, không thuộc model); tasks/poll cho job Spark 40 phút — **đúng shape với plan-id của Iceberg 1.11 server-side planning**, và đó không phải trùng hợp. Metering theo header `Mcp-Name` cho phép gateway bill per-tool mà không parse body. |
| 4 rổ Art. 10 thành partition; loại UNCLASSIFIED (2đ) | Partition: `licensed` (675, 33,8%) · `public_domain` (333) · `scraped_optout_checked` (327) · `synthetic` (331) · **`UNCLASSIFIED` (334, 16,7%)**; trainable **1.666/2.000** | 4 rổ = **một cột governed + một partition key**, không phải trang Confluence. `UNCLASSIFIED` **không được** âm thầm thành rổ mặc định — nó là audit finding. Vì provenance là partition, "loại mọi thứ ta không bảo vệ được" là một **partition prune**, không phải full scan và cầu nguyện. |
| (Erasure, ngoài rubric) | `user_007`: 8 → 0 dòng; version 0 → 1; truy được đã dùng ở rổ nào: 5 UNCLASSIFIED, 1 licensed, 1 synthetic, 1 scraped | Căng thẳng cần nói thẳng: **v0 vẫn còn dòng đã xoá**. "Chúng tôi có time travel" và "chúng tôi tôn trọng quyền được xoá" **xung đột trực tiếp** trừ khi retention window là một quyết định *được viết ra* — không phải giá trị mặc định. Xoá chỉ hoàn tất khi J3 (NB6) expire các version đó. |

---

## Part C — Reproducibility (6 điểm)

| Tiêu chí | Số đo |
|---|---|
| `make test` xanh (2đ) | **24 passed in 0.66s** (rubric ghi 22; suite hiện tại 24) |
| `make run-all` xanh từ `make setup` sạch (4đ) | **8/8 PASS in 9.9s** — venv dựng lại từ đầu trong session này, Python 3.14.6 |

`make smoke` 9/9 ✓, hoàn toàn offline: không API key, không Docker, không JVM, không tải model, không tải DuckDB extension.

---

## Ba phát hiện đi ngược niềm tin phổ biến

Rubric nói: submission nhận ra và giải thích được một trong các phát hiện này chứng tỏ đã **đọc output của chính mình**. Cả ba đều tái hiện được trên máy này.

**1. `VACUUM` không dọn orphan chưa từng commit.** Sau khi trồng 3 file do "writer crash" (mtime −30 ngày), `vacuum(retention_hours=0, dry_run=True)` báo 211 file — nhưng **3 orphan vẫn nằm trên đĩa**. Lý do: `deltalake` (Rust) chỉ thu hồi file đã bị **tombstone trong log**; file chưa từng được commit thì log không biết nó tồn tại. VACUUM của Spark *có thêm* pass liệt kê thư mục — nên slide xếp VACUUM vào orphan removal là đúng với Spark, nhưng **đừng bao giờ giả định engine của bạn làm pass đó**. Phải tự chạy phép hiệu tập hợp `files_on_disk − files_referenced`, kèm age guard.

**2. `expire_snapshots` của Iceberg chỉ đụng metadata.** 20 → 3 snapshot, nhưng **0 file avro bị xoá** và metadata còn *phình từ 336,5 KB lên 344,2 KB* (expiry ghi thêm một `metadata.json`). Đây không phải bug: việc của expiry là làm file trở nên **unreferenced**; **xoá** file là một job khác — Job 4. Trong Iceberg Java/Spark hai job này thường được chain sẵn; trên đường Python bạn phải tự chain, nếu không storage **không bao giờ giảm**. Chain xong: 17 manifest list mồ côi, thu hồi 37,1 KB, metadata về 307,1 KB. **J3 và J4 là một cặp** — và đây chính là lời giải cho "chúng tôi expire snapshot suốt mà hoá đơn S3 không giảm".

**3. Delta không có kiểu vector cố định chiều.** Ghi `fixed_size_list<float>[256]`, đọc lên nhận `list<element: float>`; phải cast `emb::FLOAT[256]` mới bind được `array_cosine_similarity`. Thiếu kiểu này là lý do Hudi 1.2 thêm `VECTOR(dim, type)` hạng nhất.

---

## Thư mục bài nộp

```
submission/
├── EVIDENCE.md                      ← file này: 17 tiêu chí → số đo → cách đọc số
├── REFLECTION.md                    ← ≤ 200 từ, Top 5 Lakehouse Anti-Patterns
├── outputs/*.txt                    ← output thô trích từ 8 notebook đã chạy
├── screenshots/
│   ├── 00_make_gates.{txt,png}      ← smoke 9/9 · test 24 · run-all 8/8
│   ├── 01_tree_lakehouse.{txt,png}  ← tree _lakehouse/ (bronze/silver/gold + iceberg + partition)
│   ├── 02_delta_log_nb1_users_delta.{txt,png}  ← nội dung 2 commit JSON, thấy schema evolution
│   └── 03_delta_log_silver_stats.{txt,png}     ← add action + min/max stats = cơ chế file pruning
└── bonus/
    ├── ARCHITECTURE.md              ← architecture brief (Topic A: LLM observability 1B req/ngày)
    └── poc/                         ← PoC chạy được cho phần khó nhất của design
```

8 notebook đã chạy giữ output nằm ở `notebooks/*.ipynb`. Lưu ý: `.gitignore` của repo bỏ qua
`notebooks/*.ipynb` (repo dùng Jupytext `.py` làm source), nên chúng được thêm bằng `git add -f`.
