# Architecture Brief — LLM Observability ở quy mô 1 tỉ request/ngày

**Topic A** · Nguyễn Quang Tường · 2A202601597 · Bonus Track 2, Ngày 18

---

## 1. Đề bài

Một team API foundation-model log lại mọi request/response: **1 tỉ req/ngày,
~5 KB mỗi cái = 5 TB/ngày raw**, trải trên ~10.000 tenant và ~5 dòng model.
Bốn yêu cầu, và chúng đánh nhau:

1. **Dashboard cost & latency theo tenant, refresh mỗi 5 phút.** Độ tươi đẩy hệ
   thống về phía commit nhỏ và dày.
2. **Giữ đủ prompt/response 7 ngày** để incident review, sau đó chỉ giữ aggregate
   trong 1 năm. Hai lớp retention trên cùng một tập dòng.
3. **PII phải được redact trước khi bất kỳ ai đọc** — không phải kiểu "analyst
   được dặn là đừng nhìn".
4. **Tổng chi phí storage ≤ $5.000/tháng.**

Chỗ khó không nằm ở dung lượng: 5 TB/ngày nén còn ~830 GB/ngày và chỉ tốn ~$134/tháng
để nằm trên S3. Chỗ khó là yêu cầu (1) chính là một **cỗ máy sinh small files**
chĩa vào cái bảng mà yêu cầu (2) bắt phải giữ 7 ngày, trong khi yêu cầu (3) đòi
vùng landing phải không đọc được với chính những người vận hành nó. Thoả mãn (1)
một cách ngây thơ — mỗi request một object — tốn **$5.000/ngày chỉ riêng phí PUT**,
đốt sạch ngân sách tháng trước khi lưu được một byte nào.

*(196 từ)*

---

## 2. Kiến trúc

```
                        ┌──────────── CONTROL PLANE ────────────┐
                        │  Apache Polaris (REST catalog)        │
                        │  · cấp credential ngắn hạn, scoped    │
                        │  · policy row/column theo tenant      │
                        │  · lối DUY NHẤT để lấy được path      │
                        └───────────────┬───────────────────────┘
      inference edge                    │ (mọi engine bên dưới auth ở đây)
           │                            │
           ▼                            ▼
  ┌─────────────────┐          ┌────────────────────────────────────────────┐
  │ Kafka  64 parts │          │        ĐƯỜNG QUERY / SERVING               │
  │ 1 tỉ msg/ngày   │          │                                            │
  └────────┬────────┘          │  Grafana ──► Trino ──► gold.metrics_5m     │
           │                   │   (refresh 5 phút, ~200 MB/ngày scan)      │
           ▼                   │                                            │
  ┌──────────────────────┐     │  Incident review ──► Trino ──► silver      │
  │ REDACT + TYPE        │     │   (cred theo tenant; CHỈ thấy token)       │
  │ Flink, tumble 5 phút │     │                                            │
  │ · NER + regex spans  │     │  Break-glass ──► KMS Decrypt ──► plaintext │
  │ · FPE-encrypt spans  │     │   (duyệt 2 người, mọi lần gọi đều audit)   │
  │ · parse usage/latency│     └────────────────────────────────────────────┘
  └───┬──────────────┬───┘
      │              │  byte thô, chỉ 24h, khoá KMS CHỈ role này dùng được
      │              └──────────► s3://quarantine/   (không human IAM nào)
      ▼
┌───────────────────────────────────────────────────────────────────────────┐
│  DELTA LAKEHOUSE  (medallion)                                             │
│                                                                           │
│  BRONZE  bronze.calls_redacted        SILVER  silver.calls                │
│  part: dt/hour                        part: dt/hour                       │
│  payload đủ, PII → FPE token          cột typed, dedup theo request_id    │
│  830 GB/ngày · xoá cứng sau 7 ngày    CLUSTER BY (tenant_id, ts)          │
│  288 commit/ngày × 64 file            400 GB/ngày · xoá cứng sau 7 ngày   │
│           │                                    │                          │
│           └────────────────┬───────────────────┘                          │
│                            ▼                                              │
│              GOLD  gold.metrics_5m   part: dt                             │
│              (tenant, model, bucket 5 phút) → p50/p95/p99,                │
│              tokens_in/out, cost_usd, error_rate                          │
│              ~220 MB/ngày · giữ 365 ngày · CLUSTER BY (tenant_id)         │
└───────────────────────────────────────────────────────────────────────────┘
                            ▲
                  ┌─────────┴──────────────────────────────────┐
                  │  MAINTENANCE (một Airflow DAG, nối chuỗi)  │
                  │  mỗi giờ : OPTIMIZE bronze/silver (512 MB) │
                  │  mỗi ngày: VACUUM 168h  ─►  QUÉT ORPHAN    │
                  │  mỗi ngày: checkpoint + compact log        │
                  │  Cảnh báo SLO: files-per-GB > 4            │
                  └────────────────────────────────────────────┘
```

---

## 3. Các quyết định chính, kèm phương án đã loại

### D1 — Gom batch theo cửa sổ 5 phút, không phải mỗi request một object

**Chọn:** Flink tumbling window 5 phút × 64 partition Kafka → 288 commit/ngày,
18.432 object/ngày, mỗi cái ~45 MB.

* **Loại: mỗi request một object.** 1 tỉ PUT/ngày × $0,005/1.000 = **$5.000/ngày**.
  Ngân sách là $5.000/*tháng*. Chỉ riêng quyết định này đã là ranh giới giữa khả
  thi và phi lý, và nó là lập luận về chi phí *request*, không phải storage.
* **Loại: gom theo giờ.** Còn rẻ hơn nữa (24 commit/ngày), nhưng SLA dashboard là
  5 phút. Gom theo giờ tức là thoả mãn ngân sách bằng cách vi phạm yêu cầu sản phẩm.

Cửa sổ 5 phút được chọn *vì nó chính là SLA*, và mọi thứ phía sau được thiết kế để
hấp thụ số file nó sinh ra, chứ không phải để giảm số file đó đi.

### D2 — Delta Lake, không phải Iceberg, không phải Hudi, không phải Parquet + Hive

**Chọn:** Delta, kèm sinh metadata kiểu UniForm cho reader Iceberg bên ngoài.

* **Loại: Iceberg.** Thật sự rất sát, và NB5 cho thấy hidden partitioning có
  ergonomics tốt hơn. Nhưng bài học NB7 quyết định: đường incident-review và đường
  quyền-được-xoá đều cần **change feed**, mà Delta CDF hôm nay là hạng nhất. Bản
  tương đương của Iceberg chưa chín trên các engine bọn em chạy.
* **Loại: Hudi.** Upsert tốt nhất nhóm, nhưng bọn em không cần — đây là log
  append-only. Trả phí phức tạp MoR cho một workload không có update là đánh đổi tồi.
* **Loại: Parquet thuần + Hive metastore.** Không có ACID, nên một batch 5 phút
  fail sẽ để lại file dở mà query vẫn nhìn thấy. Ở 288 batch/ngày, tỉ lệ fail 1% là
  3 lần dashboard đọc sai mỗi ngày.

### D3 — Partition theo `dt/hour`; cluster theo `tenant_id`. **Không** partition theo tenant

**Chọn:** `PARTITION BY (dt, hour)` + `CLUSTER BY (tenant_id, ts)`.

* **Loại: partition theo `tenant_id`.** Đây là đáp án sai đầy quyến rũ — vì hot
  path *đúng là* "lọc theo tenant". Với 10.000 tenant × 168 giờ, nó tạo 1,68 triệu
  partition mà phần lớn chỉ chứa vài KB. NB5 đã đo hệ quả: metadata lên tới
  **284,8% kích thước bảng** trên một bảng small-file. Bạn sẽ tốn cho việc plan
  query nhiều hơn cho việc chạy nó.
* **Loại: chỉ partition theo ngày.** 830 GB/ngày mỗi partition buộc mọi lần refresh
  dashboard 5 phút phải plan qua cả một ngày file.

Clustering cho đúng khả năng skip đó mà không nổ metadata. NB2 đã chứng minh cơ chế
trên đúng hình dạng này: sau Z-ORDER, **1 trong 55 file** có thể chứa key cần tìm,
suy ra từ min/max stats trong log — prune 55× với *không một* partition phát sinh.
NB6 đo được **90% số file có thể bỏ qua** cho một point query từ cùng cơ chế.

### D4 — Mã hoá bảo toàn định dạng (FPE) tại biên, không dùng token vault, không hash

**Chọn:** phát hiện span PII (regex + NER), thay bằng **ciphertext FPE tất định**
dưới một khoá KMS. Detokenise = giải mã, canh bởi cổng break-glass.

* **Loại: token vault (DynamoDB token → plaintext).** Giả sử 5% request có chứa PII
  → 50 triệu token mới/ngày → 1,5 tỉ lượt ghi/tháng. Với giá on-demand, đó là
  **~$940/tháng, tức 19% toàn bộ ngân sách**, chỉ để lưu một ánh xạ mà FPE cho
  không, đổi lấy một khoá KMS. Nó còn biến vault thành một single point of failure
  phải backup, replicate, và tự nó lại phải phân quyền.
* **Loại: hash SHA-256.** Không đảo ngược được, nên incident review — chính là lý
  do tồn tại của việc giữ payload 7 ngày — trở thành bất khả. Lại còn yếu: hash một
  số điện thoại bị rainbow-table dễ dàng trên không gian 10 chữ số.
* **Loại: redact rồi bỏ luôn.** Cùng nhược điểm chí mạng, và không cứu lại được.

Chốt kiểm soát ở đây là *kiến trúc*, không phải quy trình: byte thô chỉ hạ cánh
xuống `s3://quarantine/` dưới một khoá KMS mà danh sách grant chỉ có **một** role —
job redaction của Flink. Không một IAM principal người nào giải mã được, nên
"redact trước khi bất kỳ ai đọc" được cưỡng chế bởi key policy chứ không bởi
tập huấn.

### D5 — Xoá cứng sau 7 ngày. Không tier payload xuống Glacier

**Chọn:** Bronze/Silver lifecycle-delete ở mốc 7 ngày; chỉ Gold sống tới 365 ngày.

* **Loại: tier Bronze xuống Glacier Instant Retrieval trong một năm.** 830 GB/ngày
  × 365 ngày = 303 TB ở trạng thái ổn định × $0,004/GB-tháng = **$1.212/tháng**
  (~$606/tháng nếu tính trung bình trong năm đầu đang tăng dần) — vừa túi tiền, và
  đúng là bản năng sai. Mỗi ngày giữ thêm là một ngày PII mà bọn em phải chứng minh
  lại consent, phải thực thi quyền được xoá xuyên qua, và phải khai báo theo Điều 10.
  **Storage thì rẻ; trách nhiệm pháp lý thì không.**
* **Loại: giữ tất cả ở Standard.** 303 TB × $0,023 = **$6.968/tháng ở trạng thái ổn
  định** — riêng nó đã bằng 139% toàn bộ ngân sách, cho dữ liệu mà sản phẩm không cần.

Cửa sổ retention là một *quyết định được viết ra và có người sở hữu*, đúng như điều
NB8 đã chỉ ra: time travel và quyền được xoá mâu thuẫn nhau cho tới khi có ai đó
cam kết một con số.

### D6 — Tự vận hành compaction trên spot, không mua managed auto-compaction

**Chọn:** `OPTIMIZE` mỗi giờ (target 512 MB) + `VACUUM 168h` mỗi ngày trên spot worker.

* **Loại: managed auto-compaction.** Mô hình của NB6 áp thẳng vào đây: tính tiền
  theo GB *và* theo 1.000 object, trong đó thành phần object chiếm **24% hoá đơn**
  và bị chi phối bởi số lượng file. Bọn em sinh 18.432 file/ngày *một cách có chủ
  đích* (D1), nên sẽ phải trả một khoản phụ trội tỉ lệ thuận với đúng quyết định
  mình vừa cân nhắc kỹ. Tự compact 830 GB/ngày chỉ tốn ~2,3 core-giờ công việc thật.
* **Loại: không compact, dựa hết vào clustering.** Clustering không làm giảm *số
  lượng* file. 129K file trong cửa sổ 7 ngày biến mọi query dashboard thành bài
  toán plan metadata, và NB6 đã định giá phần phí request: **$4,00/ngày so với
  $0,08/ngày** cho cùng lượng dữ liệu sau khi compact.

### D7 — Nối expiry với quét orphan trong cùng một DAG

**Chọn:** `VACUUM` và phép hiệu *trên-đĩa trừ được-tham-chiếu* là hai task của cùng
một DAG, và DAG fail nếu một trong hai bị bỏ qua.

* **Loại: chỉ chạy VACUUM.** NB6 đã đo trúng cái bẫy. `VACUUM` của Delta chỉ thu
  hồi file mà log đã **tombstone**; ba file crashed-writer cắm vào sống sót qua
  vacuum ở *mọi* mức retention, vì file chưa từng commit thì chưa từng bị tombstone.
  Phía Iceberg, `expire_snapshots` đưa snapshot 20 → 3 mà xoá **0** file, trong khi
  metadata trên đĩa còn *phình* 336,0 KB → 343,7 KB.
* **Loại: dọn tay theo quý.** Ở 1 tỉ req/ngày, cứ 1.000 batch có một writer chết là
  để lại ~100 orphan/ngày, vô hình với `history()` và với mọi dashboard, nhưng vẫn
  bị tính tiền hằng tháng.

### D8 — Apache Polaris làm control plane, không phải Glue, không phải grant S3 trực tiếp

**Chọn:** REST catalog; mọi engine (Trino, Flink, Spark) xin credential ngắn hạn,
có scope, từ nó.

* **Loại: Glue + IAM bucket policy.** Grant theo prefix, nên "analyst của tenant A
  chỉ thấy dòng của tenant A" đòi phải partition theo tenant — đúng thứ D3 đã loại
  vì lý do metadata. Catalog có thể cưỡng chế row filter mà không áp đặt layout vật lý.
* **Loại: Unity Catalog.** Sản phẩm tốt; nhưng sai phụ thuộc. Một control plane do
  vendor quản lý đặt lên toàn bộ hạ tầng đa engine sẽ tái tạo đúng thứ lock-in mà
  topic F của chính đề bài này sinh ra để thoát khỏi.

---

## 4. Các chế độ hỏng

### F1 — Bão small-file (cú page lúc 3 giờ sáng)

**Kịch bản:** một lần deploy đặt checkpoint interval của Flink xuống 30 giây "cho
nhanh recovery". Số file đi từ 18K → 184K/ngày. Không có lỗi nào. Dashboard chậm
dần từng giờ; đến sáng thì trượt SLA 5 phút và dòng phí request trên hoá đơn S3 đã
gấp ba.

**Phát hiện:** chỉ số `files_per_GB` cho từng bảng, do DAG maintenance phát ra.
Cảnh báo khi > 4. Đây là chỉ báo *sớm* — nó kêu trước khi latency kịp xấu vài giờ.

**Rollback:** dừng writer, trả trigger interval về cũ, chạy `OPTIMIZE` ngoài lịch.
Rẻ, vì compaction là idempotent và bảng vẫn đọc được suốt quá trình — commit
compaction chỉ đổi xem file nào đang sống.

### F2 — Rò rỉ PII do deploy redaction hỏng *(Ngày 18: time travel + quyền được xoá)*

**Kịch bản:** một thay đổi regex khiến nó ngừng khớp một định dạng số điện thoại.
Số điện thoại thật hạ cánh vào `bronze.calls_redacted`, nơi analyst *có* quyền đọc.

**Phát hiện:** một bản ghi canary mang các mẫu PII đã biết được bơm vào mọi
partition Kafka ở từng cửa sổ; một job phía sau assert rằng canary đi ra phải ở
dạng đã token hoá. Nếu không, pipeline dừng trong vòng một cửa sổ (≤ 5 phút). Cộng
thêm một lớp lưới độc lập thứ hai: quét regex mẫu trên 10K dòng Bronze mỗi giờ.

**Rollback:** `RESTORE bronze.calls_redacted TO VERSION <bản-tốt-cuối>` — chỉ vài
giây, vì đó là một commit metadata (NB3 đo được 0,01 s). **Rồi tới phần các team
hay quên:** restore chỉ làm những dòng xấu *mất tham chiếu*. NB8 nói thẳng điều
này — version xấu vẫn còn chứa chúng, và time travel sẽ vẫn phục vụ nó. Sự cố chưa
đóng cho tới khi `VACUUM` với retention đủ ngắn để thật sự xoá đã chạy **và** job
quét orphan (D7) xác nhận chúng đã rời đĩa. Nạp lại cửa sổ đó từ
`s3://quarantine/` — đó chính là lý do quarantine tồn tại.

### F3 — Schema drift làm nghẽn ingestion

**Kịch bản:** một dòng model mới thêm `usage.cache_read_tokens`. Schema enforcement
của Delta từ chối batch — một cách chính xác, như NB1 đã cho thấy: lệnh ghi fail
*trước* mọi commit, nên không có trạng thái dở dang nào.

**Phát hiện:** bộ đếm batch-bị-từ-chối > 0 trong hai cửa sổ liên tiếp; consumer lag
của Kafka tăng mà throughput không giảm.

**Rollback:** batch được định tuyến sang topic DLQ thay vì chặn dòng chảy (fail
open ở khâu giao nhận, fail closed ở khâu schema). Một người review rồi áp
`schemaMode="merge"`, sau đó replay DLQ. Evolution vẫn phải **opt-in** — phương án
ngược lại, tự động merge bất cứ field nào producer nghĩ ra, chính là cách một bảng
fact phình lên 400 cột không ai giải thích nổi.

### F4 — "Đã expire snapshot mà hoá đơn không giảm"

**Kịch bản:** task quét trong DAG bị tắt trong một sự cố không liên quan rồi không
ai bật lại. Storage tăng ~100 orphan/ngày mãi mãi, vô hình với mọi metric cấp bảng.

**Phát hiện:** đối chiếu **byte trên đĩa** (S3 inventory) với **byte được metadata
sống tham chiếu**, hằng ngày. Lệch > 5% thì page. Đây là metric *duy nhất* nhìn
thấy orphan, chính vì orphan được định nghĩa bằng sự vắng mặt của chúng trong metadata.

**Rollback:** bật lại và chạy job quét. Ràng buộc DAG sao cho expiry và quét không
thể lên lịch độc lập với nhau.

### F5 — Tính lại chi phí hồi tố

**Kịch bản:** giá một model thay đổi. Lần rebuild Gold join với bảng giá *hiện tại*
và âm thầm ghi đè `cost_usd` của 12 tháng. Đối soát của bộ phận tài chính vỡ, và
không ai nói được con số đã đổi lúc nào.

**Phát hiện:** mỗi lần rebuild Gold đều diff với version trước của bảng; bất kỳ
thay đổi nào lên một ngày đã chốt sẽ làm job fail.

**Rollback:** giá nằm trong một dimension SCD-2, join theo
`ts BETWEEN valid_from AND valid_to`, nên đổi giá là thêm dòng mới chứ không bao
giờ là update. `DESCRIBE HISTORY` cộng version đã ghim cộng run id trả lời được
"chúng ta đã tính tiền bao nhiêu và tuyên bố điều đó khi nào" — đúng hợp đồng ghim
version mà NB8 dùng cho training run.

---

## 5. Ước tính chi phí

**Nén.** 5 TB/ngày JSON, zstd ~6× → **830 GB/ngày** Bronze. Silver bỏ `raw_json`
để lấy cột typed → **400 GB/ngày**. Gold: ~10K tenant × 10% hoạt động mỗi bucket ×
5 model × 288 bucket/ngày ≈ 1,44 triệu dòng/ngày × ~150 B → **220 MB/ngày**.

### Storage (S3 Standard $0,023/GB-tháng; IA $0,0125)

| Tập dữ liệu | Dung lượng ổn định | Phép tính | $/tháng |
|---|---:|---|---:|
| Quarantine (raw, 24 h) | 830 GB | 830 × 0,023 | **$19** |
| Bronze đã redact (7 ngày) | 5,81 TB | 5.810 × 0,023 | **$134** |
| Silver typed (7 ngày) | 2,80 TB | 2.800 × 0,023 | **$64** |
| Gold (365 ngày, 30 ngày hot → IA) | 80 GB | 6,6 × 0,023 + 73 × 0,0125 | **$1** |
| Metadata, log, checkpoint | ~50 GB | 50 × 0,023 | **$1** |
| | | **Cộng storage** | **$219** |

### Request

| Khoản | Phép tính | $/tháng |
|---|---|---:|
| PUT khi ingest | 288 batch × 64 file × 3 lớp × 30 ngày = 1,66 tr × $0,005/1k | **$8** |
| PUT khi compaction | (830 GB ÷ 512 MB) × 30 ngày ≈ 49K × $0,005/1k | **$1** |
| GET từ dashboard | 288 lần refresh/ngày × ~40 file × 30 ngày = 346K × $0,0004/1k | **$1** |
| | **Cộng request** | **$10** |

> So với phương án đã loại ở D1: 1 tỉ PUT/ngày × $0,005/1.000 = **$5.000/ngày =
> $150.000/tháng**. Một quyết định gom batch đáng giá gấp ~685 lần toàn bộ hoá đơn
> storage.

### Compute (Graviton spot, ~$0,04/vCPU-giờ)

| Job | Phép tính | $/tháng |
|---|---|---:|
| Flink redact + type | 1 tỉ/ngày ÷ 86.400 = 11,6K req/s trung bình; ~1K req/s/vCPU → 12 vCPU trung bình, cấp 48 cho peak 3× → 48 × 730 × 0,04 | **$1.402** |
| Compaction + vacuum + quét | đọc+ghi 830 GB/ngày ≈ 2,3 core-giờ; cấp 8 vCPU × 4 h × 30 × 0,04 | **$38** |
| Gold rollup 5 phút | 4 vCPU × 288 lần × 1 phút = 19 vCPU-giờ/ngày × 30 × 0,04 | **$23** |
| Trino cho dashboard | 8 vCPU × 730 × 0,04 | **$234** |
| | **Cộng compute** | **$1.697** |

### Tổng

| | $/tháng |
|---|---:|
| Storage | $219 |
| Request | $10 |
| Compute | $1.697 |
| KMS (khoá FPE + break-glass) | ~$30 |
| **Tổng** | **≈ $1.956** |

**So với trần $5.000/tháng: dùng 39%, còn dư 2,5 lần.** Ngân sách bị chặn bởi
*compute*, không phải storage — storage chỉ chiếm 11%. Bản năng lao vào tier byte
xuống Glacier là đang tối ưu dòng nhỏ nhất trên hoá đơn.

---

## 6. Thứ em sẽ dựng đầu tiên — lát cắt MVP một tuần

**Lát cắt:** một partition Kafka, một shard tenant, ~1% lưu lượng (10 triệu
req/ngày), chạy đầu-cuối. Mục tiêu không phải quy mô — mà là chứng minh hai điều
đắt tiền nếu phát hiện muộn.

| Ngày | Sản phẩm bàn giao |
|---|---|
| 1 | Kafka → Flink tumbling 5 phút → `bronze.calls_redacted` trên Delta. Partition `dt/hour`. |
| 2 | FPE redaction + **bucket quarantine với KMS grant chỉ một principal**. Chứng minh role analyst nhận `AccessDenied` trên dữ liệu thô — dưới dạng một test trong CI. |
| 3 | Silver dedup theo `request_id` + `CLUSTER BY (tenant_id, ts)`. Gold rollup 5 phút. Một panel Grafana trên dữ liệu thật. |
| 4 | DAG maintenance: `OPTIMIZE` → `VACUUM 168h` → quét orphan, nối chuỗi, kèm cảnh báo `files_per_GB` và cảnh báo lệch đĩa-vs-metadata. |
| 5 | Canary: bơm PII tổng hợp mỗi cửa sổ, assert nó đi ra ở dạng token hoá, dừng pipeline nếu không. Thêm đường DLQ + schema-drift. |
| 6–7 | Load test lên 100 triệu req/ngày trên đúng shard đó; ghi lại files/GB, p95 dashboard, và $/1 triệu request. Viết phần ngoại suy lên 1 tỉ. |

**Vì sao chọn lát cắt này.** Nó cố ý đẩy lên trước hai khẳng định mà cả thiết kế
dựa vào, và là hai thứ một bộ slide không thể kiểm chứng:

1. **Redaction được cưỡng chế bởi key policy, không phải bởi quy ước.** Nếu role
   analyst đọc được quarantine thì yêu cầu (3) không đạt, và kiến trúc là *sai*,
   chứ không phải mới làm dở. Test đó rẻ vào ngày 2 và thảm hoạ vào tháng thứ 6.
2. **DAG maintenance giữ được SLO số file dưới một trigger interval thật.** Mọi con
   số chi phí ở §5 đều giả định file 512 MB. Nếu `OPTIMIZE` mỗi giờ không đuổi kịp
   một writer 5 phút ở mức 1% lưu lượng, thì ở 100% cũng không, và D1 phải được đàm
   phán lại với SLA dashboard — cuộc trao đổi đáng có ở tuần 1 hơn là sau khi
   pipeline đã gánh tải thật.

Những thứ **cố ý không** có trong tuần 1: multi-tenancy trong Polaris, detokenise
break-glass, dimension giá SCD-2, interop Iceberg. Đều là việc thật; nhưng không
cái nào có thể phủ định kiến trúc.
