# Ngày 18 — Kết quả đo được & cách đọc các con số

**Học viên:** Nguyễn Quang Tường · **MSSV:** 2A202601597 · **Đường chạy:** lightweight (`deltalake` + `pyiceberg` + DuckDB)
**Môi trường:** Windows 11, Python 3.11.9, delta-rs 1.6.2, pyiceberg 0.11.1, duckdb 1.5.5, polars 1.43.2, pyarrow 25.0.1, numpy 2.4.6

Mọi con số dưới đây đều copy từ lần chạy trên máy này. Stdout thô của từng
notebook nằm ở [`logs/`](logs/); tám notebook đã thực thi, **giữ nguyên output
cell**, nằm ở [`../notebooks/`](../notebooks/).

## Trạng thái các cổng chấm điểm

| Cổng | Kết quả |
|---|---|
| `make setup` | venv dựng lại từ số 0, gate Python 3.10–3.14 pass (3.11.9) |
| `make smoke` | 9/9 check pass, hoàn toàn offline |
| `make test` | **24/24 pytest xanh** (rubric ghi 22; bộ test đã tăng lên 24) |
| `make data` | 200.000 dòng Bronze, seed 9.948 bản trùng |
| `make data-ai` | 2.000 doc (dim 256) · 200 blob (12,5 MB) · 1.578 bước trajectory |
| `make run-all` | **8/8 notebook pass trong 30,6 s** (lần chạy sạch: 37,0 s) |

Toàn bộ chuỗi trên chạy liên tiếp **từ `make clean`** (xoá cả `.venv` lẫn
`_lakehouse`) — log đầy đủ ở [`logs/05_clean_rebuild.log`](logs/05_clean_rebuild.log).
Đây chính là điều kiện Part C yêu cầu: *run-all xanh từ một `make setup` sạch*.

---

# Phần A — Nền tảng

## NB1 — Delta Lake căn bản

`_delta_log/` chứa 2 commit JSON. Điều đáng đọc ở đây: **transaction log CHÍNH LÀ
cái bảng, còn parquet chỉ là đống byte mà nó trỏ tới.** Commit 0 mang `protocol` +
`metaData` + một `add`; commit 1 mang một `metaData` *mới* có thêm cột `tier` và
một `add` nữa — và **không hề ghi lại file của v0**. Schema evolution tốn 0 byte
di chuyển dữ liệu.

Lệnh ghi sai kiểu bị chặn với `Cast error: Cannot cast string 'thirty' to value of
Int64 type`, và **không sinh ra commit nào cả**. Nửa sau mới là phần quan trọng:
enforcement xảy ra *trước khi* log bị đụng tới, nên không có trạng thái ghi dở nào
phải đi dọn. Một data lake không có log sẽ nhận file đó, rồi 6 tháng sau mới chết
lúc đọc, trong query của người khác.

DuckDB sau đó thấy 2 nhóm tier — `[('premium', 1), (None, 3)]` — chứng minh 3 dòng
cũ đọc lên thành `tier=NULL` mà không cần job backfill nào.

## NB2 — Small files, OPTIMIZE + Z-ORDER

| Chỉ số | Trước | Sau |
|---|---:|---:|
| Số file | 200 | 55 |
| Point query (median) | 130,2 ms | 14,5 ms |

**Speedup 9,0× · tỉ lệ file bị prune 55×.** Rubric chấp nhận *một trong hai*; em
báo cáo cả hai vì chúng đo hai thứ khác nhau và chỉ một cái đáng tin.

Con số wall-clock là thật nhưng nhiễu — nó đo lẫn cả OS page cache, và dao động
trong khoảng 9,0×–11,9× qua các lần chạy lại trên máy này **trong khi số file
không đổi một bit**. Còn **55× thì tất định và chứng minh được từ byte trên đĩa**:
commit OPTIMIZE (`screenshots/02_delta_log_commits.txt`, bằng chứng 3) có 55 action
`add`, mỗi cái mang `stats.minValues.user_id` / `maxValues.user_id` với khoảng giá
trị hẹp và gần như không chồng lấn — `[1,1851]`, `[1851,3696]`, `[3696,5534]`, …
Đúng **một** trong 55 file có thể chứa `user_id=4242`. Engine bỏ qua 54 file còn
lại mà không cần mở.

Đó là toàn bộ cơ chế, và nó giải thích vì sao Z-ORDER vô dụng trên bảng chỉ có 1
file: không còn gì để skip thì stats hẹp cũng chẳng mua được gì. Đó cũng là lý do
notebook giới hạn `target_size` ở 256 KB — nếu để target 512 MB như production thì
1 triệu dòng gộp vào đúng 1 file và **phá luôn** phép đo, trong khi nhìn vẫn như
thành công.

Chú ý `filesRemoved: 67` chứ không phải 200 so với `filesAdded: 55`: `compact()`
đã chạy trước, nên Z-ORDER đang ghi lại 67 file đã compact, không phải 200 file gốc.

## NB3 — Time travel, MERGE, RESTORE

MERGE 100K dòng: **0,07 s**. Metrics trong history: `num_output_rows: 150000`,
`num_target_rows_updated: 50000`, `num_target_rows_inserted: 50000`,
`num_target_rows_copied: 50000` — tỉ lệ update/insert 50/50 trên bảng 100K, ra
150K dòng. Đáng chú ý là `num_target_files_scanned: 1` và
`num_target_files_skipped_during_scan: 0`: chỉ có 1 file nên không có gì để skip,
MERGE phải ghi lại cả bảng. Ở quy mô production, đây đúng là lý do phải cluster
theo merge key — vẫn là cơ chế file-skipping của NB2, nhưng áp cho đường ghi.

RESTORE về v2: **0,01 s**, và số dòng `score < 0` về **0**. Sở dĩ 0,01 s vì đây là
thao tác *metadata* — nó ghi một commit mới (v4) khai báo file nào đang sống, còn
file dữ liệu xấu của v3 đơn giản là thôi được tham chiếu. Không xoá gì, không copy
gì. Đó cũng là lý do bản rollback là v4 chứ không phải v3: **cái sai vẫn nằm trong
history, vĩnh viễn audit được.**

`history()` cuối cùng = **5 version** (`WRITE, WRITE, MERGE, WRITE, RESTORE`).

## NB4 — Medallion Bronze → Silver → Gold

* Bronze **200.000** → Silver **190.052**: dedup bỏ **9.948 dòng (4,97%)**, khớp
  gần như chính xác tỉ lệ retry 5% mà generator seed vào.
* Gold: **24 dòng = 8 ngày × 3 model** (yêu cầu ≥ 7 ngày × 3 model).

Con số 8 ngày không phải lỗi và đáng giải thích: generator trải 200K dòng trên một
*khoảng* 7 ngày bắt đầu từ `2026-04-01T00:00:00Z`, nên dòng cuối rơi vào
`2026-04-08T00:00:00Z` — một khoảng 7 ngày chạm vào 8 ngày lịch. Partition
2026-04-08 vì thế rất nhỏ (4,86 triệu prompt token so với 32,5 triệu của 04-07),
và đây đúng là kiểu partition ngày-lẻ khiến dashboard "so hôm nay với hôm qua"
nói dối.

Kiểm tra tính hợp lý của Gold: error_rate rơi vào **0,042–0,062** so với generator
sinh 5% status khác `ok`; p50/p95 tách bạch theo model (haiku 574/1118 ms · sonnet
1404/2783 ms · opus 2946/6058 ms) vì generator cho latency là hàm của số token
*output* — decoding tự hồi quy, không phải kích thước prompt. Thứ tự chi phí cũng
theo đúng hình đó: opus đắt gấp ~6 lần sonnet mỗi ngày trên khoảng 1/6 lượng token.

---

# Phần B — Lakehouse 2026

## NB5 — Iceberg và catalog như control plane

**Tỉ lệ pruning 10× (10 file → 1)**, lọc trên **`ts`**, không hề lọc trên `ts_day`.

Đây là con số cần *cách đọc*, chứ không chỉ cần giá trị. `ts_day` không phải một
cột — nó không tồn tại trong bất kỳ dòng nào. Nó là một *transform* `day(ts)` ghi
trong partition spec, và planner tự suy ra giá trị partition từ predicate của
người dùng trên cột thật. Người dùng Hive phải *biết* có cột dẫn xuất đó và phải
*nhớ* lọc theo nó; quên là đọc cả 10 file.

Quy đổi theo mô hình của notebook — 512 MB/file, $5/TB scan, 10K query/ngày — một
predicate bị quên đó tốn **$220/ngày**. Hidden partitioning không làm cho lỗi đó
rẻ đi; **nó xoá bỏ luôn cơ hội phạm lỗi.**

**Metadata chiếm 284,8% kích thước bảng** (134,7 KB metadata so với 47,3 KB data,
trên 20 avro + 12 json). Ở mức 10 dòng/file thì con số này nghe vô lý, và đó chính
là điểm cần thấy: small files phạt bạn *hai lần* — nhiều file dữ liệu phải mở hơn,
**và** nhiều metadata phải plan qua trước khi mở. Tỉ lệ này là lý do Iceberg v4
đang thiết kế lại cây metadata, và lý do bản 1.11 đẩy scan planning về phía server.

**Field ID sống sót qua rename.** `latency_ms` → `latency_millis` giữ nguyên
`field_id=4`; thêm `tier` được cấp `field_id=6`; toàn bộ 5.000 dòng cũ đọc lên với
`tier=NULL`. Tên chỉ là nhãn; ID mới là danh tính. Parquet khớp theo vị trí, Hive
khớp theo tên — cả hai đều gãy khi rename/reorder, và đó là lý do "team đổi tên
một cột rồi dashboard lặng lẽ trả về null" là câu chuyện thời Hive chứ không phải
của Iceberg.

**Hai partition spec cùng tồn tại**: `spec_id` `[1, 2]` trên các data file, và
**5.500 dòng đọc được qua cả hai**. File cũ nằm nguyên tại chỗ. Ở Hive đây là một
cuộc migration ghi lại cả bảng; ở đây nó là một commit metadata.

## NB6 — Bốn job maintenance bắt buộc

### Job 1 — Compaction: 200 → 11 file (**18×**)

Kích thước file trung bình ở baseline là **51,5 KB** so với mục tiêu production
128–512 MB. Không ai viết sai code — 200 commit hoàn toàn đúng từ một stream
trigger ngắn tạo ra tình trạng này. Ở 50K full scan/ngày, đó là 10 triệu GET =
**$4,00/ngày chỉ riêng phí request**, so với $0,08/ngày sau khi compact.

Dòng không nên đọc lướt: **số byte dữ liệu TĂNG**, 10,1 MB → 16,1 MB. Compaction
ghi file mới *trước khi* file cũ được thu hồi, nên trong chốc lát bạn trả tiền cho
cả hai bản. Một job compaction chạy trên bucket gần đầy có thể thất bại *chính vì*
nó là job compaction.

### Job 2 — Clustering: 90% số file có thể bỏ qua

Trước khi cluster, point query `user_id=12345` phải mở **11/11** file. Sau
Z-ORDER: **1/10** — tỉ lệ skip 90% (yêu cầu ≥ 50%).

Đo bằng min/max stats từ `get_add_actions(flatten=True)` chứ không bấm giờ, và đó
là lựa chọn đúng: dữ liệu chưa cluster có các khoảng min/max *chồng lấn*, nên file
nào cũng "có thể" chứa giá trị cần tìm và stats chẳng chứng minh được gì.
Clustering không làm stats *xuất hiện* — nó làm stats **có tính chọn lọc**.

### Job 3 — Expiry, và phát hiện đi ngược niềm tin phổ biến

Delta `VACUUM` thu hồi **16,1 MB → 6,2 MB**. Bình thường.

`expire_snapshots` của Iceberg mới là chỗ đáng chú ý:

| | trước | sau |
|---|---:|---:|
| snapshot | 20 | **3** |
| file avro trên đĩa | 40 | **40** |
| metadata (byte) | 336,0 KB | **343,7 KB** |

**Snapshot giảm 20 → 3 mà không một file nào bị xoá. Metadata trên đĩa còn
*phình ra*.** Đây không phải bug — hợp đồng của expiry là làm cho file *mất tham
chiếu*, và việc ghi `metadata.json` mới để ghi nhận điều đó tốn thêm byte. Xoá file
là một job khác.

### Job 4 — Orphan, và niềm tin thứ hai bị bác bỏ

3 file "writer bị crash" được cắm vào, lùi ngày 30 ngày. `VACUUM` dry-run với
`retention_hours=0` báo 211 file — và **không cái nào trong đó là orphan**; chúng
nằm nguyên trên đĩa ở mọi mức retention.

Lý do rất cơ học: delta-rs chỉ thu hồi những file mà transaction log đã
**tombstone**. File chưa từng được commit thì chưa từng bị tombstone, nên log
không biết nó tồn tại. VACUUM của Spark có thêm bước liệt kê thư mục; delta-rs thì
không. Phép hiệu tập hợp *file trên đĩa − file được metadata sống tham chiếu* tìm
ra đủ 3 file (21,2 KB) và xoá chúng.

Áp đúng phép hiệu đó sang phía Iceberg tìm được **17 manifest list bị bỏ rơi
(37,1 KB)**; quét sạch chúng đưa avro 40 → 23 và metadata 343,7 KB → **306,6 KB** —
**lần đầu tiên xuống dưới mức 336,0 KB trước khi expire.** Job 3 và Job 4 là một
**cặp**: expiry đứng một mình thu hồi đúng bằng 0, và đó là toàn bộ lời giải thích
cho câu "bọn tôi expire snapshot rồi mà hoá đơn S3 không giảm".

### Một điểm vênh trong chính output của NB6, và cách giải quyết

NB6 in ra:

```
Parquet files on disk:      15
Parquet files in the log:   10
→ 5 files you pay for and cannot see
```

...rồi tìm và xoá đúng **3**, và báo `After removal — on disk: 12`. Khoảng chênh 2
file này lặp lại y hệt qua 3 lần chạy liên tiếp, và nó là **lỗi phép đo, không
phải 2 orphan không bị phát hiện**.

`count_files()` glob `*.parquet` trên *toàn bộ* thư mục bảng, **bao gồm cả**
`_delta_log/`. delta-rs tự động ghi checkpoint mỗi 100 commit, nên một lần ingest
200 commit đã kịp ghi `00000000000000000099.checkpoint.parquet` và
`00000000000000000199.checkpoint.parquet` từ trước khi orphan được cắm vào:

```
10 file dữ liệu sống + 3 orphan cắm vào + 2 checkpoint tự động = 15
```

`find_orphans()` mới là hàm làm đúng — nó bỏ qua `_delta_log` một cách tường minh
(`if "_delta_log" in f.parts: continue`), nên trả về 3. Checkpoint là metadata hợp
lệ, được `_last_checkpoint` tham chiếu; tính chúng là rác vô hình mới là cái sai.
**Con số headline đã thổi phồng vấn đề đúng bằng số checkpoint** — và bản thân điều
đó chính là bài học của Job 4: một job quét orphan chỉ tốt bằng đúng định nghĩa
"được tham chiếu" của nó, và một job hiểu ngược sẽ xoá mất checkpoint của bạn.

(Liên quan: Job 5 in `Checkpoint written: 00000000000000000099.checkpoint.parquet`,
nhưng `create_checkpoint()` thực tế ghi ra **v203** — `_last_checkpoint` chứa
`{"version":203,"size":223,"sizeInBytes":36291,"numOfAddFiles":10}`. Câu lệnh glob
lấy checkpoint có số nhỏ nhất trong ba cái, không phải cái vừa ghi.)

### Job 5 — Checkpoint

204 entry JSON mà một reader nguội lẽ ra phải replay, gộp lại thành 1 checkpoint
223 dòng / 36 KB liệt kê 10 add-file. `_last_checkpoint` có mặt.

### FinOps

Managed compaction cho 500 GB / 2.000.000 file, chạy hằng ngày: **$990/tháng**,
trong đó **$240 (24%) là thành phần tính theo số object**. Thành phần đó bị chi
phối bởi *số lượng file*, không phải dung lượng — nên cái bảng đắt nhất khi thuê
auto-compaction lại chính là cái bảng có writer trigger cấu hình sai. Sửa
trigger interval rẻ hơn trả tiền cho người đi dọn sau lưng mình.

## NB7 — Multimodal và vector

### Khuếch đại đọc ngẫu nhiên: 200×

Bảng inline-blob là 1 row group gồm 200 dòng / **12,5 MB**. Lấy đúng một frame
(`doc_id=137`) buộc phải đọc và giải nén nguyên cả row group: **12,5 MB để lấy về
64 KB — khuếch đại 200×** (yêu cầu ≥ 5×).

Cơ chế nằm ở chỗ **đơn vị I/O của Parquet là row group, không phải row.** Đây cũng
là lý do lời khuyên phổ biến sai một nửa: chính bảng inline đó không tốn gì cho
truy vấn *phân tích* — `SELECT topic, count(*) GROUP BY topic` chỉ đọc **1,2 KB
trên tổng 12,5 MB**, vì projection pushdown của định dạng cột không bao giờ chạm
vào cột blob. Blob inline không "xấu"; nó xấu **cho truy cập ngẫu nhiên từng
dòng**, và hoàn toàn ổn cho scan. Ở mức 1.000 lượt lấy frame ngẫu nhiên/giây để
nuôi GPU, con số 200× đó *chính là* vấn đề GPU chết đói, và cũng chính là điều mà
tuyên bố "random access nhanh hơn 3–35×" của Lance đang nói tới.

### Lượng tử hoá int8: nhỏ hơn 5,8×, recall 0,904, độ trung thực chủ đề 1,000

2,6 MB → **451,9 KB trên đĩa (tiết kiệm 83%, tức 5,8×)** — tốt hơn mức lý thuyết 4×
vì giá trị int8 còn nén tiếp trong Parquet tốt hơn phần định trị của float32.

recall@10 so với ground truth float32: **0,904**. Độ trung thực chủ đề: **1,000**.

Phải có cả hai số mới đọc đúng được. Recall theo ID chính xác coi việc hoán đổi
giữa hai tài liệu liên quan ngang nhau là một lần trượt, nên nó *đánh giá thấp*
chất lượng lượng tử hoá cho RAG. Khoảng 10% ID "bị mất" đã được thay bằng tài liệu
**cùng chủ đề trong 100% trường hợp** — tập kết quả vẫn đúng chủ đề. Với một
pipeline RAG, đó mới là chỉ số quyết định câu trả lời. Dù vậy em vẫn sẽ đo cả hai
trên corpus thật: đánh đổi này phụ thuộc corpus, và một corpus có phân biệt tinh vi
*bên trong* cùng một chủ đề sẽ không hành xử như vậy.

### Semantic search giờ là một câu SQL

Top-5 cho `storage-note-00007` đều là **`topic=storage`** (sim 1,000 / 0,779 /
0,777 / 0,776 / 0,768). Brute force trên 2.000 vector: 13,7 ms. Ngoại suy: 100K →
~686 ms (biên), **1 triệu → ~6.860 ms — không phải đường serving.** Đó là ranh giới
trung thực: brute force trong bảng dành cho truy vấn ngữ nghĩa mang tính phân tích
và cho việc đo recall offline. Vector DB là **index dẫn xuất, dựng lại được**; còn
lakehouse là **system of record**.

Truy vấn có filter mới là lý lẽ thật sự cho việc giữ vector trong bảng: "tìm tài
liệu tương tự **mà chúng ta có quyền train**" là một câu SQL duy nhất với mệnh đề
`WHERE consent_train AND license <> 'unknown'`, bởi vì vector và các cột governance
nằm trong cùng một dòng. Không phải đối soát ID, không phải hỏi "vector này còn
hợp lệ không?"

### Lỗi lifecycle — tái hiện được

Một job sync ban đêm dựng index ngoài gồm 2.000 vector. `user_042` thực hiện quyền
được xoá; 8 doc bị xoá khỏi lakehouse.

* Doc đã xoá còn truy hồi được từ lakehouse: **0**
* Doc đã xoá còn truy hồi được từ index ngoài: **8** ← vi phạm

Index đó sẽ vẫn hồn nhiên trả nội dung ấy vào prompt RAG cho tới lần sync kế tiếp —
và nếu sync là upsert một chiều, vốn là trường hợp phổ biến, thì **là mãi mãi**, vì
delete đúng là thao tác mà các pipeline sync hay quên. Đây không phải bug hiệu
năng; ngay khi việc xoá trở thành một yêu cầu pháp lý, nó thành bug tuân thủ.

Cơ chế lan truyền đúng là Change Data Feed: `load_cdf()` trả về **8 dòng, tất cả
`_change_type='delete'`**, mang theo đúng các `doc_id` cần trục xuất. Index *đăng
ký nhận* sự kiện delete thay vì đoán. Tốt nhất vẫn là không cần sync.

### Một cái bẫy đáng ghi lại

Bọn em ghi xuống `fixed_size_list<float>[256]`, nhưng đọc lên thành
`list<element: float>`. Delta protocol không có kiểu vector cố định chiều, chỉ có
`array<element>`, nên các hàm mảng cố định của DuckDB không bind được nếu thiếu ép
kiểu tường minh `emb::FLOAT[256]`. Đây đúng là lý do Hudi 1.2 bổ sung cột
`VECTOR(dim, type)` hạng nhất.

## NB8 — Agent, MCP, provenance

**Trajectory.** 1.578 bước Bronze → Silver partition theo `agent_version`
(`policy-v2`, `policy-v3` trên đĩa) → Gold phủ cả hai policy. Partition key mới là
điểm mấu chốt: bạn có thể bỏ hoặc train lại trên rollout của một policy mà không
đụng tới policy kia — điều này quan trọng vì phân phối dữ liệu trajectory *dịch
chuyển khi policy tốt lên*, nên một dataset tĩnh tự nó sẽ cũ đi.

Gold cho thấy policy-v2 đạt success 0,760 so với policy-v3 là 0,753, cùng 5,26
bước trung bình, tổng chi phí $10,37 so với $10,39. Đọc cho trung thực: **chênh
lệch đó là nhiễu**, mỗi nhánh chỉ 150 trajectory và cùng sinh từ một generator.
Bảng có đúng hình dạng để so sánh; nhưng tự nó chưa cho phép kết luận.

**Ghim version.** Training run ghi lại `table_version: 0` ứng với 1.578 bước. Sau
đó rollout mới đổ về (v1, 1.978 bước). Replay tại version đã ghim trả về **1.578 —
khớp chính xác**. Một số nguyên duy nhất là khác biệt giữa một lần chạy tái lập
được và một câu chuyện kể lại, và đó cũng chính là hợp đồng MLflow ↔ Delta. Thiếu
nó, câu hỏi "policy này train trên dữ liệu nào?" không có đáp án, và Annex IV cũng
vậy.

**Bề mặt MCP 2026-07-28.**

* Cacheable list: 5 lượt agent → **1 lần gọi catalog**, quảng bá bằng
  `{'ttlMs': 60000, 'cacheScope': 'session'}`. Ở quy mô 50.000 bảng, đây là khác
  biệt giữa một catalog dùng được và một catalog tự liệt kê lại chính nó mỗi lượt.
* Human-in-the-loop: `delete_rows` trả về `resultType: input_required` và chỉ trả
  `ok` sau khi có `_meta={"confirmed": True}`. **Agent không thể tự phê duyệt —
  cái cổng đó thuộc về protocol, không thuộc về model.** Một guardrail đặt trong
  system prompt là một lời đề nghị; cái này là một chốt kiểm soát.
* Tasks: `submit_scan` → `working`, `working`, `completed` (300 dòng). Đúng hình
  dạng mà server-side planning của Iceberg 1.11 dùng khi trả về plan-id. Hai
  protocol hội tụ về một hình dạng không phải trùng hợp — cả hai đều là công việc
  chạy lâu nằm sau một request phi trạng thái.
* Metering theo từng tool dựa trên header `Mcp-Name` cho phép gateway tính tiền
  theo tool mà không cần parse body.

**Provenance — EU AI Act Điều 10, có hiệu lực từ 2/8/2026.**

| rổ | số dòng | % |
|---|---:|---:|
| licensed | 675 | 33,8 |
| UNCLASSIFIED | **334** | **16,7** |
| public_domain | 333 | 16,7 |
| synthetic | 331 | 16,6 |
| scraped_optout_checked | 327 | 16,4 |

Cả bốn rổ hợp lệ đều tồn tại thành partition trên đĩa, cộng thêm UNCLASSIFIED là
rổ thứ năm. **1.666/2.000 dòng dùng được để train; 334 dòng bị loại** vì
`license=unknown` không đạt yêu cầu về nguồn gốc của Điều 10.

Quyết định thiết kế đáng bảo vệ: UNCLASSIFIED là một **rổ thật, không phải giá trị
mặc định**. Một biểu thức `CASE` có nhánh `ELSE` dễ dãi sẽ lặng lẽ quét 334 dòng đó
vào `scraped_optout_checked` và tạo ra một corpus không ai bảo vệ nổi. Provenance
là một cột được quản trị cộng một partition key — nhờ vậy "loại bỏ mọi thứ ta không
bảo vệ được" là một phép prune partition, chứ không phải quét toàn bảng rồi cầu
nguyện.

**Mâu thuẫn mà lab này làm cho cụ thể.** Xoá theo yêu cầu của `user_007` gỡ đi 8
dòng (v0 → v1). Nhưng **v0 vẫn còn chứa chúng** — đó chính là *ý nghĩa* của time
travel. "Chúng tôi hỗ trợ time travel" và "chúng tôi tôn trọng quyền được xoá" mâu
thuẫn trực tiếp, trừ khi cửa sổ retention là một quyết định được viết ra và có
người chịu trách nhiệm. Việc xoá chỉ hoàn tất khi retention đã hết hạn các version
đó (NB6, Job 3); và việc xoá chỉ *chứng minh được* nhờ provenance đã ghi lại dữ
liệu của chủ thể nằm ở những rổ nào: `scraped_optout_checked` 1, `licensed` 1,
`synthetic` 1, `UNCLASSIFIED` 5.

---

# Phần C — Khả năng tái lập

`make test` 24/24 · `make run-all` 8/8 trong 37,0 s, chạy nối tiếp ngay sau
`make clean` + `make setup` (dựng lại venv từ số 0). Log đầy đủ:
[`logs/05_clean_rebuild.log`](logs/05_clean_rebuild.log).

## Bổ sung khối assert còn thiếu của NB4

NB4 là notebook **duy nhất** trong lab không kết thúc bằng một khối `assert`: bảy
notebook kia đều in `NBx complete.`, còn NB4 chỉ có một checklist markdown bốn gạch
đầu dòng mà máy không kiểm chứng. Trong đó, hai điều kiện — *cả ba bảng tồn tại trên
đĩa* và *cost_usd & error_rate khác 0* — chưa hề được assert ở bất kỳ đâu, nên
`make run-all` (vốn là cổng chấm điểm) chưa thực sự kiểm tra chúng.

Đã bổ sung khối assert vào cuối [`notebooks/04_medallion.py`](../notebooks/04_medallion.py)
đúng theo bốn gạch đầu dòng đó và theo style của bảy notebook còn lại:

```
  [PASS] bronze/silver/gold all on disk
  [PASS] silver dedup dropped rows
  [PASS] gold ≥ 7 dates × 3 models
  [PASS] cost_usd & error_rate non-zero

NB4 complete.
```

Nhờ vậy cả 8/8 notebook hiện đều tự kiểm chứng tiêu chí đậu của chính mình, đúng như
README của lab mô tả về `make run-all`.

## Một thay đổi trong source code là bắt buộc để đạt được điều đó trên Windows

`make test` ban đầu fail 1/24 ở `test_reset_catalog_does_not_touch_siblings`.
Nguyên nhân gốc, đã xác nhận bằng repro trực tiếp: `lakehouse.reset_catalog()` gọi
`shutil.rmtree(..., ignore_errors=True)` lên thư mục đang chứa file SQLite của một
`SqlCatalog` **còn sống**. POSIX cho phép unlink file đang mở nên trên Linux/macOS
việc này chạy tốt. Windows từ chối với `PermissionError [WinError 32]`, và
`ignore_errors=True` **nuốt lỗi trong im lặng** — `reset_catalog` trở thành một
hàm không làm gì, và lệnh `create_table` kế tiếp sẽ chết với
`TableAlreadyExistsError`.

Cách sửa ở [`scripts/lakehouse.py`](../scripts/lakehouse.py): ghi nhớ SQLAlchemy
engine đã cấp cho từng tên catalog và gọi `engine.dispose()` trước khi rmtree. Vài
dòng, không đổi hành vi trên POSIX, và làm cho việc chạy lại notebook trên Windows
là *an toàn thật* thay vì an toàn nhờ may mắn.

Workaround duy nhất không đụng source: `PYTHONIOENCODING=utf-8`, vì notebook in ra
`✓` / `→` / `⚠` còn console Windows mặc định dùng cp1252.
