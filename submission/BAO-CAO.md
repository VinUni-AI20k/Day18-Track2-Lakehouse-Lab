# Day 18 — Lakehouse Lab: Báo cáo kết quả đo

Cả tám notebook đã chạy headless (`make run-all` → **8/8 PASS**, 31.5s) và chạy
lại trong Jupyter với output được giữ nguyên. `make test` → **24 passed**.

**Môi trường:** Python 3.12.3 · deltalake 1.6.2 · pyiceberg 0.11.1 · duckdb 1.5.5
· polars 1.43.2 · pyarrow 25.0.1 — hoàn toàn offline, không JVM, không API key.

---

## Checkpoint 1 — Delta Lake Basics & Transaction Log

| Thao tác | Mục tiêu | Đo được |
|---|---|---|
| Ghi bảng Delta `mode="overwrite"` | tạo được bảng | `_lakehouse/scratch/users_delta`, 3 dòng |
| Đọc log JSON + `dt.history()` | có commit JSON | **2 file** `00000000000000000000.json`, `…0001.json` |
| Ghi sai kiểu `age="thirty"` | bị chặn | `Cast error: Cannot cast string 'thirty' to value of Int64 type` |
| Thêm cột `tier` với `schema_mode="merge"` | cột được thêm | `tier` xuất hiện trong schema |
| DuckDB nhóm theo `tier` | 2 nhóm | `[('premium', 1), (None, 3)]` |

**Đọc kết quả:** commit v0 chứa đúng ba loại action — `protocol` (phiên bản
reader/writer), `metaData` (schema), và `add` (một file Parquet kèm `stats`
min/max). Đây là bằng chứng "bảng" chỉ là Parquet + log JSON. Schema enforcement
chặn ghi lỗi **mặc định**; muốn tiến hoá schema phải **chủ động opt-in** bằng
`schema_mode="merge"` — đó là ranh giới giữa an toàn và linh hoạt.

→ `NB1 complete.`

---

## Checkpoint 2 — Small-Files Problem & OPTIMIZE + Z-ORDER

| Thao tác | Mục tiêu | Đo được |
|---|---|---|
| 200 micro-batch | ≥ 100 file | **200 file** Parquet nhỏ |
| Benchmark BEFORE (`user_id=4242`) | — | median **176.1 ms** |
| `compact(target_size=256KB)` + `z_order(["user_id"])` | giảm file | 200 → **55 file** |
| Benchmark AFTER | — | median **16.1 ms** |
| **Speedup** | ≥ 3× | **10.9×** |
| **Files-pruned ratio** (từ minValues/maxValues) | ≥ 10× | **55×** — chỉ **1/55** file chứa `user_id=4242` |

**Đọc kết quả:** Đề bài cho phép đạt **một trong hai**; bài này đạt **cả hai**.
Nhưng con số đáng tin hơn là files-pruned: wall-clock phụ thuộc cache SSD và tải
máy, còn tỷ lệ file bị prune đọc thẳng từ `minValues`/`maxValues` trong commit
log nên **tất định**. Giá trị thật của Z-ORDER không phải "chạy nhanh hơn" mà là
**file-skipping**: sau khi co-locate theo `user_id`, khoảng [min,max] của mỗi
file hẹp lại và không chồng lấn, nên engine loại được 54/55 file *trước khi đọc
một byte dữ liệu nào*.

→ `NB2 complete.`

---

## Checkpoint 3 — Time Travel, ACID MERGE & Rollback

| Thao tác | Mục tiêu | Đo được |
|---|---|---|
| v0 → v1 → v2 → v3 | dựng lịch sử | v0 100K dòng · v1 thêm `tier` · v2 MERGE · v3 inject `score=-1` |
| `MERGE INTO` 100K dòng | thành công | **0.09s** — 50K updated + 50K inserted → 150K dòng |
| Time-travel v0 / v1 | đọc được | v0 = **100,000 dòng**; v1 schema = `[customer_id, status, score, tier]` |
| `dt.restore(2)` | tạo commit mới | **0.01s**, sinh v4 |
| Đếm `score < 0` | = 0 | **0** |
| `history()` | ≥ 5 phiên bản | **5** — WRITE, WRITE, MERGE, WRITE, **RESTORE** |

**Đọc kết quả:** RESTORE **không xoá lịch sử** — nó là một transaction mới (v4)
trỏ trạng thái bảng về v2. Nghĩa là vẫn audit được ai rollback, lúc nào, và dữ
liệu lỗi v3 vẫn nằm trong lịch sử cho tới khi retention hết hạn. Đây chính là
mâu thuẫn mà NB8 nêu lại: "có time travel" và "tôn trọng quyền xoá" xung đột
trực tiếp nếu cửa sổ retention không phải một quyết định có chủ đích.

→ `NB3 complete.`

---

## Checkpoint 4 — Medallion Architecture cho AI Observability

| Tầng | Thao tác | Đo được |
|---|---|---|
| **Bronze** | đọc `llm_calls_raw` | **200,000 dòng** |
| **Silver** | trích JSON, chuẩn hoá kiểu, khử trùng lặp `ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts)`, partition theo `date` | **190,052 dòng** (loại **9,948** bản trùng), **8 partition** `date=` |
| **Gold** | join bảng giá token, `z_order(["model"])` | **8 ngày × 3 model = 24 dòng** |

Cột Gold: `p50_latency_ms`, `p95_latency_ms`, `total_prompt_tokens`,
`total_completion_tokens`, `error_rate`, `cost_usd` — tất cả đều có giá trị khác 0.

**Đọc kết quả:** Bronze giữ **nguyên trạng** (kể cả bản retry trùng) vì Bronze là
bản sao bất biến của nguồn — nếu logic parse sai, ta chạy lại từ Bronze chứ không
mất dữ liệu. 9,948 dòng bị loại chính là các lần retry sinh cùng `request_id`;
nếu không dedup, mọi con số chi phí ở Gold sẽ **thổi phồng ~5%**. Đó là lý do
dedup phải nằm ở Silver, không phải ở dashboard.

→ `NB4 complete.` (4/4 điều kiện PASS)

---

## Checkpoint 5 — Apache Iceberg & Catalog as Control Plane

| Thao tác | Mục tiêu | Đo được |
|---|---|---|
| Tạo `lake.llm_events` qua Catalog API (`CAT="nb5"`) | qua catalog | tạo xong, **không tự chọn path** |
| Partition ẩn `days(ts)` + append 10 ngày | có spec | spec `ts_day = day(ts)`, **10 snapshot / 10 data file / 5,000 dòng** |
| `plan_files()` lọc trên cột `ts` gốc | **≥ 5×** | **10×** (10 → **1** file) |
| Cây metadata 3 tầng | báo cáo tỷ lệ | metadata.json → **10** manifest list → **10** manifest → **10** data file; metadata = **282.5%** dung lượng data |
| Đổi tên `latency_ms` → `latency_millis` | giữ field_id | vẫn **`field_id = 4`**, **không** rewrite file nào |
| Tiến hoá partition (thêm `model`) | ≥ 2 spec | `spec_id = [1, 2]`, đọc đủ **5,500 dòng** |

**Đọc kết quả:** Tỷ lệ 10× có được vì filter đặt trên **`ts`** — cột thật — còn
`ts_day` do Iceberg tự suy ra từ transform lưu trong metadata. Một người dùng
Hive quên `WHERE dt=...` sẽ đọc cả 10 file: ở quy mô 512 MB/file và $5/TB, đó là
**~$220/ngày** với 10,000 query/ngày. Hidden partitioning không "nhắc" người dùng
nhớ predicate — nó **xoá bỏ cơ hội quên**.

Tỷ lệ metadata 282.5% nhìn phi lý, nhưng đó là hệ quả của bảng đồ chơi 10
dòng/file. Ở 512 MB/file tỷ lệ này ~0.1%. Bài học ngược lại mới quan trọng:
**small files phạt bạn hai lần** — vừa nhiều data file, vừa phình metadata phải
plan qua.

→ `NB5 complete.`

---

## Checkpoint 6 — Five Table Maintenance Jobs

Bảng phân mảnh: **200 micro-batch → 200 file**, kích thước trung bình **51.5 KB**
(mục tiêu production: 128–512 MB).

| Job | Thao tác | Mục tiêu | Đo được |
|---|---|---|---|
| **1 — Compaction** | `compact()` | ≥ 10× ít file | **200 → 11 file (18×)** |
| **2 — Clustering** | `z_order(["user_id"])` | skip ≥ 50% | **90%** file không phải đụng tới |
| **3 — Expiry** | Delta `vacuum()` · Iceberg `expire_snapshots()` | thu hồi byte · còn 3 snapshot | thu hồi **16.1 MB** · **20 → 3** snapshot |
| **4 — Orphan Removal** | hiệu tập hợp *Disk ∖ Log* | 3 file rác | **3** orphan Delta (21.2 KB) + **17** manifest list mồ côi Iceberg (36.7 KB) |
| **5 — Checkpointing** | `create_checkpoint()` | có file | `00000000000000000199.checkpoint.parquet` + `_last_checkpoint` ✓ |

**9/9 kiểm thử [PASS].** → `NB6 complete.`

### Hai phát hiện đi ngược niềm tin phổ biến

1. **`VACUUM` KHÔNG dọn được orphan chưa từng commit.** Sau khi tạo 3 file rác và
   set mtime lùi 30 ngày, `vacuum(retention_hours=0)` tìm thấy **0/3**. Lý do:
   delta-rs chỉ thu hồi file đã bị **tombstone** trong log; file do job crash để
   lại chưa từng vào log nên log không biết nó tồn tại. Phép hiệu
   *Disk ∖ Log* tìm ra cả 3.
2. **`expire_snapshots` của Iceberg KHÔNG xoá file nào.** Snapshot 20 → 3 nhưng
   số file avro giữ nguyên **40**, và metadata còn **phình** 332.9 KB → 340.4 KB
   (vì expiry ghi thêm một `metadata.json` mới). Expiry chỉ làm file trở nên
   *không được tham chiếu*; **xoá** là việc của Job 4. Đây chính xác là lý do các
   team báo "đã expire snapshot mà hoá đơn S3 không giảm" — **Job 3 và Job 4 là
   một cặp.**

---

## Checkpoint 7 — Vectors, Multimodal & Lifecycle Traps

| Thao tác | Mục tiêu | Đo được |
|---|---|---|
| Inline blob vs pointer URI (200 frame) | — | tổng dung lượng gần như bằng nhau |
| **Random-access amplification** | ≥ 5× | **200×** (đọc cả row group **12.5 MB** để lấy **64 KB**) |
| Lượng tử hoá int8 | ≥ 3× nhỏ hơn | **5.8×** (tiết kiệm **83%**) |
| **Recall@10** (int8 vs float32) | ≥ 0.80 | **0.904** |
| **Topic Fidelity** | ≥ 0.95 | **1.000** |
| Semantic search bằng `array_cosine_similarity()` | đúng chủ đề | top-5 cùng topic với query |
| **Lifecycle bug** | 0 in-table / > 0 external | **0** vs **8** ← tái hiện thành công |
| Change Data Feed bắt delete | ≥ 1 | **8** delete event = đúng 8 doc bị xoá |

**Đọc kết quả:** Lời khuyên "đừng bao giờ để blob trong bảng" **sai** với truy vấn
phân tích — column pruning khiến cột `blob` gần như không tốn gì cho
`SELECT topic, count(*)`. Chỗ thật sự vỡ là **random single-row access**: đơn vị
I/O của Parquet là **row group**, không phải row, nên lấy 1 frame phải đọc và
giải nén cả nhóm → khuếch đại 200×. Ở tốc độ 1,000 frame/giây để nuôi GPU, đây
chính là bài toán GPU-starvation.

Về int8: recall@10 = 0.904 nghĩa là mất ~10% doc ID *chính xác*, nhưng topic
fidelity = 1.000 nghĩa là **100% kết quả vẫn đúng chủ đề** — các "miss" chỉ là
hoán vị giữa những láng giềng tương đương. Với RAG, recall theo ID **đánh giá
thấp** chất lượng lượng tử hoá.

**Lifecycle bug** là phần đắt giá nhất: xoá `user_042` khỏi lakehouse thành công
(0 hit), nhưng external vector index **vẫn trả về 8 doc** cho tới lần sync sau —
và nếu sync là one-way upsert (trường hợp phổ biến) thì là **vĩnh viễn**, vì
delete chính là thao tác các pipeline sync hay quên. Đó là lỗi **tuân thủ pháp
lý**, không phải lỗi kỹ thuật.

→ `NB7 complete.`

---

## Checkpoint 8 — Agent Trajectories & EU AI Act Provenance

| Thao tác | Mục tiêu | Đo được |
|---|---|---|
| Silver partition theo `agent_version` | 2 partition | `policy-v2`, `policy-v3` — **1,578 step** |
| Gold tổng hợp theo policy | 2 dòng | success_rate / avg_steps / cost per policy |
| Ghim `table_version` vào training run + replay | khớp 100% | pinned **v0 → 1,578 step**, khớp **100%** kể cả sau khi rollout mới đổ về |
| MCP: cache `tools/list` | 5 turn → 1 read | **1** catalog round-trip |
| MCP: `input_required` trước lệnh destructive | có chặn | `resultType: input_required` → sau khi duyệt mới `ok` |
| MCP: async task polling | hoàn tất | `working → working → completed` |
| 4 rổ bản quyền Art. 10 | đủ 4 partition | `licensed`, `public_domain`, `scraped_optout_checked`, `synthetic` |
| Cách ly UNCLASSIFIED | loại khỏi tập train | **334** dòng bị loại; **1,666/2,000** dòng dùng được |
| Right-to-erasure `user_007` | = 0 | **8 → 0** dòng (v0 → v1) |

**10/10 kiểm thử [PASS].** → `NB8 complete.`

**Đọc kết quả:** Một số nguyên `table_version` là toàn bộ khác biệt giữa "một run
tái lập được" và "một câu chuyện kể lại". Sáu tháng sau, auditor hỏi "model này
train trên dữ liệu nào?" — không có pin thì không có câu trả lời, và Annex IV
cũng không có.

334 dòng UNCLASSIFIED **không phải sai số làm tròn mà là một audit finding**:
trộn dữ liệu scraped và licensed vào một rổ không nhãn chính là kiểu thất bại
kiểm toán 2026. Khi provenance là **partition key**, câu lệnh "loại mọi thứ
không bảo vệ được" trở thành một phép partition prune, không phải full-table scan
kèm cầu nguyện.

---

## Bằng chứng đính kèm

| File | Nội dung |
|---|---|
| `notebooks/0[1-8]_*.ipynb` | 8 notebook đã chạy, giữ nguyên output, **0 cell lỗi** |
| `screenshots/tree_lakehouse.txt` | cây thư mục `_lakehouse/` — **1,152 file, 98.4 MB** |
| `screenshots/delta_log_commit.txt` | toàn bộ commit `00000000000000000000.json` (`protocol` / `metaData` / `add` + stats min/max) |
| `REFLECTION.md` | bài suy ngẫm về Top 5 Lakehouse Anti-Patterns |

---

## Phụ lục — Đường Spark/Docker (tuỳ chọn)

`rubric.md` ghi rõ hai đường đều ghi ra **cùng định dạng Delta trên đĩa**, nên
bằng chứng từ đường nào cũng được tính; NB5–NB8 vốn chỉ chạy đường lightweight.
Bài này nộp bằng chứng đường lightweight cho cả 8 checkpoint, và **có chạy thêm
đường Spark để đối chứng**:

| Hạng mục | Kết quả |
|---|---|
| `make spark-up` | MinIO + Spark/Jupyter **healthy**; 4 bucket `lakehouse`/`bronze`/`silver`/`gold` đã tạo |
| `make spark-smoke` (`scripts/verify.py`) | **PASS** (exit 0) — 4/4 bước |
| ↳ Boot Spark with Delta | OK — Spark 3.5.0 + `delta-spark_2.12:3.2.0` |
| ↳ Write Delta → MinIO `s3a://lakehouse/_smoke` | OK qua S3A |
| ↳ Read back | 10 dòng |
| ↳ Time travel `versionAsOf 0` | v0 vẫn **10 dòng** sau khi append |
| ↳ `DESCRIBE HISTORY` | **≥ 2 version** |

**Ghi chú vận hành:** lần chạy đầu mất ~12 phút, gần như toàn bộ nằm ở một JAR
duy nhất — `aws-java-sdk-bundle-1.12.262.jar` (~280 MB) tải hết **652 giây**.
Sau khi vào cache Ivy (`~/.cache/ivy`, volume riêng) thì lần sau khởi động nhanh.
Spark cũng cảnh báo `Total allocation exceeds 95.00% of heap memory` với
`-Xmx1g` — đúng như README nói đường Spark cần ~6 GB RAM, trong khi đường
lightweight chỉ cần ~600 MB và chạy cả 8 notebook trong **31.5 giây**.

Đó chính là lý do đường lightweight là mặc định: cùng một định dạng bảng, cùng
một bài học, ít hơn hai bậc độ lớn về chi phí khởi động.
