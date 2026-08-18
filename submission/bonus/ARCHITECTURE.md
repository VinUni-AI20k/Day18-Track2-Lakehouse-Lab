# Architecture Brief — LLM Observability ở 1 tỷ requests/ngày

**Topic A** · Nguyễn Hoàng Thảo Tiên (2A202601650) · Day 18 Track 2 · 18/08/2026
Vai: *architect on-call*. Mọi con số dưới đây đều có phép tính kèm theo; những con số có ghi
`NBx` là **đo trực tiếp trong lab này** (NB2, NB5, NB6, NB7, NB8), không trích từ blog.

---

## 1. Problem statement

Một API foundation-model log mọi request/response: **1 tỷ req/ngày, ~5 KB/req → 5 TB/ngày raw**,
peak ~35K event/s. Bốn ràng buộc:

1. Dashboard **cost & latency theo tenant**, refresh **5 phút** (p95 < 2s).
2. **Payload đầy đủ giữ 7 ngày** cho incident review; sau đó chỉ aggregate 1 năm.
3. **PII redact trước khi bất kỳ ai đọc** — kể cả on-call.
4. **Storage ≤ $5.000/tháng**, cap cứng của CFO.

Khó không ở dung lượng: nén zstd 4× giữ 7 ngày chỉ **$165/tháng**, dưới cap 30 lần.
Khó ở ba mâu thuẫn:

- (2) đòi giữ **payload thô** — chính là dữ liệu vi phạm (3) nếu redact sai một lần.
- (1) đòi ghi liên tục 5 phút → **small file**; ở scale này hoá đơn do **số file** quyết định,
  không phải byte: **$7.741/tháng tiền GET** (§5) đã **vượt cap**, còn bytes chỉ $228.
- (3) đòi xoá được, nhưng time travel — thứ dùng để rollback — **giữ lại đúng cái vừa xoá**
  (NB8: v0 vẫn còn dòng đã delete).

Quyết định trung tâm không phải "format nào" mà là **layout file và retention window nào làm
rollback và erasure cùng khả thi**.

---

## 2. Architecture (một diagram)

```
                    1B req/ngày · ~5 KB/req · 5 TB/ngày raw · peak ~35K ev/s
                                             │
┌─────────────────────── INGESTION PATH ─────┼────────────────────────────────────────────┐
│   API edge ──► Kafka  topic=llm.events  64 partitions  retain 24h                       │
│                          │                                                             │
│                          ▼   Spark Structured Streaming, trigger = 60s                  │
│                 ┌──────────────────────────────────┐                                   │
│                 │ TOKENIZE PII (HMAC, pre-write)   │──► token vault (KMS, audit riêng)  │
│                 │ + cột redaction_version          │    token→plaintext, TTL 30 ngày    │
│                 └───────────────┬──────────────────┘                                   │
└─────────────────────────────────┼───────────────────────────────────────────────────────┘
                                  ▼
  BRONZE  llm_events_raw       48h  │  2.5 TB │ REPLAY BUFFER cho bug ở Silver/Gold; ĐÃ tokenize
          partition: event_hour     │  $59/mo │ ⚠ bug ở chính tokenizer thì phải replay từ Kafka (24h)
                                  │ CDF
                                  ▼
  SILVER  llm_events            7d  │  7.0 TB │ typed columns + payload cho incident review
          partition: event_hour(168)│ $165/mo │ cluster: (tenant_id, model)
          compact mỗi giờ → 512 MB  │         │ deletion vectors ON · VACUUM retain 24h
                                  │ CDF (micro-batch 5 phút)
                                  ▼
  GOLD    tenant_5min_metrics    1y │ 0.18 TB │ khoá: (bucket_5min, tenant_id, model)
          p50/p95/cost/error_rate   │   $4/mo │ KHÔNG user_id, KHÔNG payload
          + cột silver_version (pin)│         │ → tầng 1 năm KHÔNG chứa gì phải xoá
                                  │
┌─────────────────────────────────┼──── QUERY PATH ───────────────────────────────────────┐
│  Trino ──► GOLD   ──► dashboard 5 phút · p95 < 2s · scan ~14 file                       │
│  Trino ──► SILVER ──► incident review, chỉ on-call, row-filter theo tenant từ catalog    │
│  REST catalog (Polaris / Unity) = CONTROL PLANE: credential vending + row filter + audit │
└──────────────────────────────────────────────────────────────────────────────────────────┘
  MAINTENANCE — cron, không tuỳ chọn (4+1 job của NB6)
  J1 compact giờ vừa đóng · J2 cluster đêm · J3 VACUUM 24h · J4 orphan set-diff · J5 checkpoint
```

---

## 3. Bảy quyết định chính, kèm phương án đã loại

### D1 — Table format: **Delta Lake 4.x** (bật UniForm để engine khác đọc như Iceberg)

**Loại Iceberg** vì đường xoá của chúng tôi cần row-level delete lan truyền xuống derived
consumer, và trên đường Python `pyiceberg` không ghi được positional delete → mỗi lần scrub PII
trở thành một job Spark riêng. Nặng hơn: NB6 đo được `expire_snapshots` là **metadata-only**
(20→3 snapshot, **0 file avro bị xoá**, metadata còn phình 336,5→344,2 KB) — muốn giảm storage
phải tự chain thêm orphan sweep. Iceberg mạnh ở partition evolution và engine neutrality, nhưng
ở đây **chúng tôi sở hữu writer duy nhất**, nên hai ưu thế đó gần như không sinh giá trị.
**Loại Hudi** vì MOR tối ưu cho upsert nặng, còn workload này append-heavy; đổi lại là timeline +
compaction mode — quá nhiều bề mặt vận hành cho một platform team 3 người.
**Loại Parquet + Hive metastore** vì không có ACID: compaction chạy song song với reader sẽ cho
kết quả sai, và quên partition predicate là full scan — NB5 đo được **$220/ngày** cho đúng một
predicate bị quên ở 10K query/ngày.

### D2 — Partitioning: **`event_hour`** + liquid clustering trên **`(tenant_id, model)`**

Toán: 1B/24 = **41,7 triệu dòng/giờ** ≈ 42 GB nén/giờ → **~82 file 512 MB/giờ**, và cả cửa sổ
7 ngày là 168 partition ≈ **14.336 file**. Kích thước file đúng *do thiết kế*, không do may mắn.
**Loại partition theo `tenant_id`**: 5.000 tenant × 168 giờ = **840.000 partition**, mỗi partition
~8.300 dòng ≈ 2 MB → đây chính là anti-pattern over-partitioning, small file *sinh ra từ layout*.
**Loại partition `(date, tenant_id)`**: cùng bệnh, nhỏ hơn một bậc.
**Loại "chỉ clustering, không partition"**: khi đó xoá dữ liệu hết hạn là rewrite toàn bảng thay vì
**drop partition (metadata-only)** — và retention là thao tác chạy mỗi ngày, không thể đắt.

### D3 — Retention/tiering: **Bronze 48h → Silver 7 ngày → Gold 1 năm**

**Loại giữ Bronze 7 ngày**: cộng $165/tháng thì còn chịu được, nhưng cái đắt là *compliance* —
Bronze là nơi mọi lượt replay/debug đọc vào, giữ payload chưa xử lý ở đó 7 ngày là mở rộng
blast radius đúng vào nhóm người không nên có quyền đó.
**Loại Glacier cho payload 7 ngày**: retrieval mất phút→giờ, mà lý do duy nhất payload tồn tại là
incident review — tức là truy cập *gấp*. Tier sai mục đích.
**Loại giữ raw 1 năm ở S3 IA**: 1.825 TB × $0,0125/GB = **$23.360/tháng**, gấp **4,7×** cap.
Ngay cả bản "rẻ" cũng không cứu được nếu retention sai.

**Quyết định phụ, quan trọng hơn vẻ ngoài — `VACUUM` retention = 24h (không phải default 7 ngày).**
Compaction mỗi giờ tombstone gần như toàn bộ Silver mỗi ngày; giữ tombstone 30 ngày cộng thêm
**29 TB = $683/tháng**, gấp 4× chính Silver. Đánh đổi: time travel chỉ còn 24h — **chấp nhận**,
vì đường rollback thật của chúng tôi là **replay**: Bronze 48h cho bug ở Silver/Gold,
Kafka 24h cho bug ở chính tokenizer (D4). Replay tái lập được; time travel chỉ *nhìn lại* được.

### D4 — PII: **tokenize deterministic trong job ingestion, TRƯỚC lần ghi đầu tiên**

**Loại redact ở Silver**: Bronze sẽ giữ PII thô 48h, xem D3.
**Loại masking lúc query (dynamic view)**: chỉ bảo vệ đường SQL; ai có quyền đọc object store là
đi vòng được. Ở 5 TB/ngày, dựa vào kỷ luật IAM là một cược tồi.
**Loại tokenize non-deterministic**: sẽ không join được cùng một user qua hai request → mất luôn
khả năng điều tra incident theo người dùng.
Đánh đổi đã chấp nhận: tokenize pre-write nghĩa là **regex sai thì sai luôn trong dữ liệu** — và
Bronze *không* cứu được, vì Bronze đã tokenize nên nó chứa đúng phần rò rỉ đó. Nguồn replay duy nhất
của text chưa tokenize là **Kafka, retention 24h**. Hệ quả thiết kế, không phải chi tiết: **cửa sổ
hồi phục là 24h chứ không phải 48h**, nên canary PII phải chạy *inline trên từng micro-batch* và
fail fast — một canary chạy hằng đêm là vô nghĩa ở đây. Bù thêm bằng cột **`redaction_version`**
để đo phủ và rollback theo version — cùng ý tưởng "pin version" mà NB8 dùng cho training run.
PoC ở §7 chạy đúng đường này (chính PoC là thứ phơi ra rằng Bronze không phải nguồn replay).

### D5 — Compaction: **compact ngay khi một giờ vừa đóng**, cluster hằng đêm; **self-managed**

**Loại compaction mỗi ngày**: xem §5 — với file streaming, chỉ riêng tiền GET đã **$7.741/tháng**,
vượt cap, trong khi bytes chỉ $228.
**Loại managed auto-compaction**: NB6 tính ra ở 500 GB/2 triệu file, hoá đơn $990/tháng có
**24% là thành phần per-object**; metering là per-GB *và* per-1K-object, nên **bảng bệnh nhất
lại là bảng đắt nhất khi để nó tự dọn**. Ở file count của chúng tôi thành phần đó áp đảo. Sửa
trigger interval của writer rẻ hơn thuê người dọn sau.
**Loại compact liên tục (streaming compaction)**: tranh commit với writer, và không có ranh giới
idempotent. "Giờ đã đóng" cho ta một job **bounded và chạy lại được** — chính tính chất cứu chúng
tôi ở FM1.

### D6 — Đường serving 5 phút: **incremental rollup từ CDF của Silver vào Gold**

**Loại re-aggregate cửa sổ mỗi lần refresh**: 288 lần/ngày × scan giờ gần nhất = compute lãng phí
và p95 phụ thuộc volume — đúng thứ SLA không cho phép.
**Loại một OLAP store riêng (Druid/ClickHouse)**: nó thành **system-of-record thứ hai với lifecycle
riêng** — đúng cái bug mà NB7 biến thành vi phạm tuân thủ (lakehouse 0 hit, index cũ **8 hit**).
Khi đó erasure và redaction phải lan truyền tới hai nơi, và nơi thứ hai là nơi sẽ quên *delete*.
Nếu về sau p95 buộc phải xuống dưới 200 ms, OLAP store được thêm vào như **derived index build lại
được từ Gold**, không bao giờ là nguồn sự thật.

### D7 — Catalog: **REST catalog (Polaris/Unity) làm control plane**

**Loại path-based access**: mọi consumer nhận credential mức bucket, và câu trả lời cho "ai đã đọc
payload của tenant X" nằm ở CloudTrail chứ không phải một bảng query được. Catalog vend credential
theo scope + áp row filter theo tenant, nên audit là một câu SQL. Đây đúng là dịch chuyển 2026 mà
NB5 nói: catalog không còn là tra cứu name→path, nó là **query planner và security boundary**.

---

## 4. Failure modes: 3 giờ sáng thì cái gì vỡ

### FM1 — Kafka backfill dội 6 giờ event muộn vào một giờ đã compact xong

*3h sáng:* mạng một region hồi phục, consumer lag tuột về 0 bằng cách đẩy 6 giờ dữ liệu vào
partition `event_hour` đã đóng. Gold của các bucket quá khứ **đổi số sau khi đã báo cáo**.
**Detect:** job ingestion emit `commits_per_closed_hour`; alert khi một hour-partition đã đóng nhận
commit mới. Song song: assert Gold immutability — bucket 5 phút đã final mà `silver_version` tăng.
**Rollback:** compaction là idempotent theo giờ → chạy lại đúng các giờ bị ảnh hưởng; rollup Gold
recompute từ CDF của Silver cho đúng các bucket đó.
**Gắn với Day 18 — time travel + version pin:** mỗi dòng Gold mang `silver_version` mà nó đọc
(chính hợp đồng NB8 dùng cho training run). Nhờ vậy "con số này đến từ dữ liệu nào" luôn có đáp án,
và ta replay đúng phần cần thay vì rebuild cả ngày.

### FM2 — Deploy regex redaction lỗi, số điện thoại chưa tokenize chảy vào Silver

*3h sáng:* bản mới xử lý `0912...` nhưng bỏ sót `+84912...`.
**Detect:** canary trên mỗi micro-batch — sample 10K dòng, chạy detector PII **chặt hơn/chậm hơn**,
alert nếu hit > 0; cộng alert khi `redaction_version` xuất hiện giá trị mới.
**Rollback (4 bước, đã demo trong PoC §7):** (1) dừng writer; (2) `DELETE ... WHERE
redaction_version = '<bad>'` trên **cả Silver và Bronze** — cả hai đều nhiễm, vì tokenize xảy ra
trước lần ghi đầu; với deletion vectors đây là metadata op, không rewrite; (3) replay từ **Kafka
(24h)** bằng version đã sửa — Bronze *không* dùng được làm nguồn ở đây, đây chính là điều PoC phơi ra;
(4) `VACUUM` retention 0 trên các partition đó để **xoá vật lý** file đã rò, rồi purge bucket Gold
tương ứng. Ràng buộc cứng rút ra: **toàn bộ 4 bước phải xong trong 24h**, nếu không Kafka đã hết
retention và dữ liệu sạch không còn nguồn để dựng lại.
**Gắn với Day 18 — xung đột time travel ↔ erasure:** NB8 đo được v0 **vẫn còn** dòng đã delete.
Bước (4) mới là bước làm rollback *hoàn tất*, và nó chỉ khả thi vì D3 đã chọn retention 24h.
Nếu để default 7 ngày, PII đã rò còn đọc được qua time travel suốt một tuần.

### FM3 — Compaction chết giữa đường; storage leo và không ai thấy vì sao

*3h sáng:* job bị OOM sau khi ghi file mới, trước khi commit. File nằm trên đĩa, **không metadata
nào trỏ tới** → vô hình với `history()`, với `files()`, với dashboard.
**Detect:** job đêm chạy phép hiệu tập hợp `files_on_disk − files_referenced_by_log` (đúng
`find_orphans` của NB6, kèm age guard 24h), alert khi orphan bytes > 1% table bytes. Job này tồn tại
**vì NB6 đã đo**: trồng 3 orphan 30 ngày tuổi, `VACUUM` ở mọi retention báo 211 file nhưng
**không hề thấy 3 file đó** — delta-rs chỉ thu hồi file đã bị tombstone trong log.
**Rollback:** xoá orphan quá age guard; chạy lại compaction (idempotent). Age guard không tuỳ chọn:
NB6 cũng cho thấy trong 5 file lạ chỉ 3 file đủ tuổi để xoá — 2 file còn lại có thể đang được
một writer commit dở.

### FM4 — Dashboard của một tenant từ 2s lên 90s

*3h sáng:* một tenant tăng volume 50× sau khi launch; clustering drift, `(tenant_id, model)` không
còn co-locate, số file phải mở tăng vọt.
**Detect:** p95 theo tenant + `files_scanned/query` từ query log của Trino; alert khi
files_scanned vượt 3× median của tenant đó. Đây là phiên bản production của phép đo NB6 —
skip rate tụt từ **90%** xuống là dấu hiệu, và nó tất định, không nhiễu như wall-clock.
**Rollback/fix:** re-cluster đúng khoảng giờ đó (bounded); dài hạn tách tenant "cá voi" ra prefix
partition riêng, vì một tenant chiếm >5% volume đã phá giả định phân bố của clustering.

### FM5 — Yêu cầu quyền được xoá tới, deadline 30 ngày, mà Gold giữ 1 năm

*Không phải sự cố 3h sáng mà là án treo:* nếu tầng 1 năm chứa dữ liệu mức người dùng thì mỗi
erasure request thành một job rewrite lịch sử, và bạn sẽ trượt deadline.
**Thiết kế để câu hỏi không tồn tại:** Gold **chỉ** aggregate theo `(bucket_5min, tenant_id, model)`
— không `user_id`, không payload. Nên erasure không bao giờ chạm Gold. Thủ tục còn 2 bước:
xoá bản ghi trong **token vault** (phá liên kết token→danh tính, không thể đảo) và Silver tự hết hạn
trong ≤ 7 ngày.
**Detect:** erasure request là *dòng trong một bảng governed* có đồng hồ SLA; job hằng ngày assert
mọi request đang mở đều < 25 ngày tuổi. Cách phân rổ provenance của NB8 áp trực tiếp: bốn rổ là
**một cột governed + một partition key**, không phải một trang Confluence.

---

## 5. Back-of-envelope cost — show the math

Giá list dùng chung: S3 Standard **$0,023/GB-tháng**, S3 IA **$0,0125/GB-tháng**,
GET **$0,0004/1K**, PUT **$0,005/1K**, compute spot **$0,02/vCPU-giờ**. Nén zstd **4×** trên
JSON/text (bảo thủ; log JSON thường 5–8×).

**Storage**

| Tầng | Phép tính | Dung lượng | $/tháng |
|---|---|---:|---:|
| Bronze 48h | 5 TB/ngày ÷ 4 × 2 ngày | 2,50 TB | **$59** |
| Silver 7 ngày | 5 ÷ 4 × 0,8 (typed cols) × 7 | 7,00 TB | **$165** |
| Gold 1 năm | 288 bucket × 5.000 tenant × 3 model × 120 B × 365 | 0,18 TB | **$4** |
| | | **9,68 TB** | **$228** |

**$228/tháng so với cap $5.000 — dư 22×.** Đó không phải tin tốt, đó là tín hiệu rằng cap
*không phải* bài toán bytes. Hai cách quen thuộc để phá cap:

| Phương án ngây thơ | Phép tính | $/tháng |
|---|---|---:|
| Giữ raw 1 năm, S3 Standard | 1.825 TB × 1024 × $0,023 | **$42.982** (8,6× cap) |
| Giữ raw 1 năm, S3 IA | 1.825 TB × 1024 × $0,0125 | **$23.360** (4,7× cap) |
| VACUUM retention 30 ngày thay vì 24h | +29 TB tombstone | **+$683** |

**Con số thật sự quyết định kiến trúc — số file, không phải số byte**

| Layout | Số file trong cửa sổ 7 ngày | $/full-scan | 1.000 incident query/ngày |
|---|---:|---:|---:|
| File streaming (60s × 64 partition) | 645.120 | $0,2580 | **$7.741/tháng → VƯỢT CAP** |
| Sau compaction 512 MB | 14.336 | $0,0057 | **$172/tháng** |

**45× chênh lệch, và nó nằm hoàn toàn ở tiền request.** Đây là dạng production của phép đo NB6:
200 file × 50.000 query/ngày = 10 triệu GET = $4,00/ngày, so với 4 file = $0,08/ngày. Kết luận
mang sang được: *ở lakehouse, hoá đơn co giãn theo số file; trigger interval của writer là một
quyết định FinOps, không phải một tham số kỹ thuật.*

**Compute**

| Thành phần | vCPU-giờ/tháng | $/tháng |
|---|---:|---:|
| Ingest + tokenize (60 vCPU liên tục) | 43.200 | $864 |
| Compaction mỗi giờ | 1.500 | $30 |
| Rollup 5 phút (288 lần/ngày × 8 vCPU × 30s) | 576 | $12 |
| Cluster hằng đêm | 2.400 | $48 |
| Trino serving (16 vCPU) | 11.520 | $230 |
| | | **$1.184** |

**Tổng $1.412/tháng** (storage $228 + compute $1.184), storage chiếm **16%** cap.
Thành phần đắt nhất là **tokenization**, không phải lưu trữ — nên nếu cần cắt chi phí, chỗ để cắt
là chi phí CPU của redaction (Aho-Corasick thay vì regex chain, hoặc NER chỉ chạy trên field
free-text), chứ không phải rút ngắn retention. Rút retention là cắt vào yêu cầu (2) để tiết kiệm
$165 — sai chỗ.

---

## 6. Tuần đầu build gì (MVP slice)

Không build cả hệ. Slice nhỏ nhất **chứng minh được phần kinh tế và phần rollback** — hai thứ
quyết định kiến trúc này sống hay chết. Dashboard là phần dễ, để sau.

| Ngày | Việc | Xong nghĩa là |
|---|---|---|
| 1–2 | 1 tenant, 1 model: Kafka → Spark streaming, tokenize HMAC pre-write + cột `redaction_version`, ghi Bronze (48h) và Silver (partition `event_hour`, cluster `(tenant_id, model)`) | dữ liệu chảy, PII **không** tồn tại ở dạng thô ở bất kỳ tầng nào |
| 3 | J1 compaction "giờ vừa đóng" + J4 canary orphan set-diff | hai job mà **sự thiếu vắng của chúng chính là FM3 và §5** |
| 4 | Rollup CDF → Gold 5 phút + 1 panel Trino (p50/p95/cost theo tenant) | đường query end-to-end, có `silver_version` pin |
| 5 | Đo 3 số quyết định: (a) file/giờ sau compaction ≤ 100; (b) $/full-scan của một incident query; (c) canary bắt được một regex **cố tình làm sai** | kiến trúc có bằng chứng, không chỉ có sơ đồ |

**Không làm trong tuần 1:** multi-region, HSM cho token vault, tiering sang IA, row filter per-tenant
trong catalog, và OLAP store. Tất cả đều thêm được sau **mà không** đổi layout — đó là tiêu chí
chọn thứ tự này.

Điều kiện để tuyên bố kiến trúc "đã chứng minh": số (b) phải cho thấy chi phí request nằm dưới
**$200/tháng** ở 1.000 query/ngày, và số (c) phải đỏ trước khi tôi sửa regex. Nếu (c) không đỏ,
canary vô dụng và FM2 vẫn là án treo.

---

## 7. PoC — [`poc/redaction_rollback.py`](poc/redaction_rollback.py)

PoC demo **phần khó nhất**, không phải phần dễ: FM2 end-to-end trên Delta thật.

1. Bronze 5.000 event có PII (2 định dạng SĐT Việt Nam) → tokenize bằng **regex v1 thiếu sót**
   (bỏ sót `+84…`) → Silver `redaction_version='v1'`.
2. **Canary** phát hiện leak > 0 — đúng cơ chế detect của FM2.
3. Rollback: `DELETE ... WHERE redaction_version='v1'` trên **cả hai** tầng → replay từ
   **Kafka** bằng **v2** → leak = 0. PoC chạy ra 5.000 delete event và 0 rò rỉ.
4. Chứng minh phần mà người ta thường bỏ qua: **time travel vẫn đọc được PII đã rò** ở version cũ,
   rồi `VACUUM` retention 0 làm nó **biến mất vật lý** (đọc version cũ ném lỗi).
5. **CDF** phát ra đúng các `delete` event mang `event_id` để derived consumer evict — cơ chế NB7
   chỉ ra là cách duy nhất đúng nếu buộc phải có index dẫn xuất.

Chạy: `.venv/bin/python submission/bonus/poc/redaction_rollback.py` (offline, ~2s, kết thúc bằng
khối `assert` như các notebook của lab). Output đã chạy: [`poc/OUTPUT.txt`](poc/OUTPUT.txt).

---

## 8. Cái tôi vẫn chưa chắc

Ba điểm tôi sẽ mang vào design review dưới dạng câu hỏi, không phải kết luận:

1. **Nén 4× là giả định, chưa đo.** Nếu prompt/response thực tế nén 2× (nhiều base64/ảnh inline),
   Silver thành 14 TB = $330/tháng — vẫn dưới cap, nên giả định này *không* làm sập kiến trúc.
   Nhưng nó đổi kích thước file, và kích thước file là thứ mọi con số §5 đứng trên.
2. **5.000 tenant là con số tôi tự đặt** để tính Gold. Nếu là 100.000 tenant, Gold thành 3,6 TB/năm
   và câu hỏi partition-vs-cluster ở D2 phải xét lại — lúc đó bucket theo `tenant_id` có thể thắng.
3. **Deletion vectors trên Delta ở scale này tôi chưa vận hành.** FM2 bước (2) giả định delete là
   metadata op; nếu reader path của Trino chưa hỗ trợ tốt, bước đó thành rewrite và cửa sổ rollback
   dài ra. Đây là thứ MVP tuần 1 nên đo, không nên tin.
