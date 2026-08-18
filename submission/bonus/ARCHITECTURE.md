# Architecture Brief — CDC từ ride-hailing Việt Nam sang Lakehouse

**Topic C** · Bonus Challenge Day 18 · *không tính điểm*

---

## 1. Problem statement

Oracle production là source of truth cho `trips`, `payments`, `driver_profile`,
`driver_locations`. Debezium đọc redo log, đẩy CDC event sang lakehouse.

**Quy mô:** 100 triệu chuyến/năm (≈ 274K/ngày), **30K writes/giây peak**, trung
bình ~9K/giây → **≈ 780 triệu event/ngày**. Firehose không phải `trips` mà là
`driver_locations`: ping 4 giây/lần × ~20 phút = ~300 row/chuyến, **85% khối
lượng ghi**.

**Ràng buộc cứng:**

| | |
|---|---|
| Dashboard | ≤ 60 giây kể từ source commit |
| Ad-hoc query | p95 < 1 giây |
| Nghị định 13/2023/NĐ-CP | SĐT, CCCD, GPS của tài xế + hành khách |
| Audit | mọi lần đọc PII phải truy vết được |
| Late data | mất mạng ở tỉnh xa, event muộn hàng giờ là thường ngày |

**Vì sao khó:** ba ràng buộc kéo ngược nhau. Freshness 60 giây đẩy về micro-batch
ngắn, mà micro-batch ngắn đẻ ra small file — thứ giết p95 < 1 giây. Late data đòi
MERGE (random write), trong khi object storage chỉ giỏi append tuần tự. Và Nghị
định 13 cấm đúng cách rẻ nhất để làm cả hai: đổ raw xuống đĩa rồi tính sau.

*(≈ 180 từ)*

---

## 2. Kiến trúc

```
  ORACLE (source of truth)                      NGHỊ ĐỊNH 13 BOUNDARY
  trips · payments · driver_profile          ═══════════╗
  driver_locations                                      ║  raw PII KHÔNG BAO GIỜ
        │ redo log                                      ║  chạm đĩa lakehouse
        ▼                                               ║
  ┌───────────┐   ┌─────────┐   ┌──────────────────┐    ║   ┌──────────────────┐
  │ Debezium  │──▶│  Kafka  │──▶│ Spark Structured │────╫──▶│ TOKENIZER (KMS)  │
  │ connector │   │ 7d ret. │   │ Streaming 30s    │    ║   │ HMAC-SHA256+salt │
  └───────────┘   └─────────┘   └────────┬─────────┘    ║   └────────┬─────────┘
                                         │              ║            │ token
                        ┌────────────────▼──────────────╫────────────▼─────────┐
   BRONZE               │  bronze.cdc_raw        (Delta, append-only)          │
   append-only          │  partition: event_date(+07) · zstd-3 · 78 GB/ngày    │
   30d hot → Glacier IR │  PII đã token-hoá · schema evolution bật             │
                        └────────────────┬─────────────────────────────────────┘
                                         │ Delta CDF (không diff thủ công)
                        ┌────────────────▼─────────────────────────────────────┐
   SILVER               │ trips_current   SCD-1, MERGE guard src.op_ts>tgt.op_ts│
   typed · dedup        │ trips_history   SCD-2, append-only từ CDF            │
   MERGE + late-data    │ locations_30s   downsample 4s→30s sau 90 ngày        │
                        │ ZORDER (city_id, trip_id) · watermark 6h             │
                        │ _late_arrivals  backfill hằng đêm qua MERGE          │
                        └────────────────┬─────────────────────────────────────┘
                                         │
                        ┌────────────────▼─────────────────────────────────────┐
   GOLD                 │ trip_metrics_5m · driver_daily · city_hourly         │
   aggregates           │ KHÔNG chứa token PII — chỉ số liệu tổng hợp          │
                        └────────────────┬─────────────────────────────────────┘
                                         │
   QUERY PATH           Superset ─────────┤ Trino (ad-hoc, p95<1s)
                        (dashboard 60s)   └ Notebook DS (chỉ Gold + Silver-noPII)
                                          ▲
   CONTROL PLANE        Unity Catalog OSS ─┘  column-level grant · audit log mọi
                                              lần đọc cột token (Nghị định 13)

   MAINTENANCE (cron, 4 job bắt buộc — xem §4 FM2/FM3)
   Job1 OPTIMIZE 2h/lần · Job2 ZORDER hằng đêm · Job3 VACUUM+expiry · Job4 ORPHAN SWEEP
                                                   └── Job3 và Job4 luôn chạy THÀNH CẶP
```

---

## 3. Sáu quyết định chính, kèm phương án đã loại

### QĐ-1 · Table format: **Delta Lake 3.x**

Chọn Delta vì hai thứ khớp trực tiếp với bài toán CDC: **Change Data Feed** biến
Bronze→Silver→Gold thành incremental thật (không phải diff thủ công hai snapshot),
và **deletion vectors** khiến MERGE của CDC không phải rewrite cả file dữ liệu —
đúng chỗ đau khi có 30K writes/giây.

* **Loại Hudi.** Về lý thuyết Hudi MOR (merge-on-read) là format *hợp CDC nhất*:
  write amplification thấp nhất trong ba format. Loại vì **gánh nặng vận hành**,
  không phải vì kỹ thuật kém — MOR bắt buộc phải chạy compaction service riêng,
  và nếu nó chết thì read path xuống cấp âm thầm (giống hệt FM2 bên dưới nhưng
  không có metric sẵn). Team 6 người không nuôi nổi thêm một stateful service.
* **Loại Iceberg.** Hidden partitioning của Iceberg là thứ tôi tiếc nhất — NB5 đo
  được pruning **10×** khi filter đặt trên `ts` chứ không phải `ts_day`, tức là
  analyst *không thể* quên partition predicate. Nhưng row-level upsert trên
  Iceberg v2 sinh **equality delete file**, và read path chậm dần tuyến tính theo
  số delete file cho tới lần compact kế tiếp. Với tần suất MERGE của CDC, đó là
  đánh đổi sai chiều.
* **Điều kiện đảo ngược:** nếu sau này bắt buộc multi-engine + vendor-neutral
  catalog (Snowflake/BigQuery cùng đọc), Iceberg thắng và ta migrate qua
  UniForm/XTable. Ghi lại điều kiện này để lần review sau không phải cãi lại từ đầu.

### QĐ-2 · Catalog & governance: **Unity Catalog OSS**

Nghị định 13 không chỉ hỏi "ai được đọc" mà còn hỏi "**ai đã đọc, lúc nào**".
Đó là yêu cầu về *catalog*, không phải về *format*.

* **Loại Hive Metastore.** Không có column-level ACL và **không có audit log**.
  Muốn đáp ứng yêu cầu audit thì phải tự dựng proxy trước mọi engine — mà DuckDB
  hay Trino đọc thẳng file trên S3 là bypass sạch sẽ. Kiến trúc mà quyền có thể
  bị đi vòng qua thì không phải kiến trúc bảo mật.
* **Loại Glue + Lake Formation.** Đáp ứng được về kỹ thuật nhưng khoá cứng vào
  AWS, trong khi phần lớn dữ liệu nhạy cảm này có khả năng phải giữ trong nước
  (data localization) — nghĩa là một ngày nào đó sẽ có một cụm on-prem hoặc
  VNG/Viettel Cloud, và Glue không đi theo được.

### QĐ-3 · Partitioning: **`event_date` theo giờ VN (+07) + ZORDER `(city_id, trip_id)`**

Partition theo *ngày Việt Nam* chứ không phải ngày UTC: mọi báo cáo, mọi định
nghĩa "ngày" trong luật và mọi câu hỏi của business đều theo giờ VN. Partition
theo UTC nghĩa là mọi query của analyst đều phải quét 2 partition, mãi mãi.

* **Loại partition theo giờ.** 24× số partition → 8.760 partition/năm/bảng, và
  với micro-batch 30 giây thì mỗi partition-giờ nhận 120 file. Đây **chính xác**
  là bài toán NB2/NB6 đo được: 200 file nhỏ khiến chi phí GET request lên
  **$4.00/ngày** cho một bảng chỉ 10 MB; sau compaction còn 11 file → **$0.08/ngày**.
  Nhân con số đó với quy mô thật thì partition theo giờ là tự bắn vào chân.
* **Loại partition theo `city_id`.** Skew nặng — TP.HCM và Hà Nội chiếm ~60%
  lưu lượng → partition lệch, straggler task, và một partition đủ lớn để không
  bao giờ compact xong trong cửa sổ đêm.
* **Vì sao ZORDER thay vì partition thêm cấp:** NB2 đo được **pruning 55×** cho
  point query sau khi Z-ORDER co-locate `user_id` — cơ chế là `min/max` stats
  per-file trong transaction log, không phải là thư mục. Ta được lợi ích của
  partition mà không phải trả giá bằng số lượng thư mục.

### QĐ-4 · PII: **deterministic tokenization ngay tại Bronze landing**

`phone`, `cccd`, `driver_id` được thay bằng `HMAC-SHA256(value, key_quý ‖ salt_bảng)`
**trước khi** chạm đĩa lakehouse. Key nằm trong KMS, bảng ánh xạ token↔raw nằm
trong một vault riêng ngoài lakehouse, truy cập qua service có audit.

* **Loại "lưu raw + column mask lúc query".** Nghị định 13 điều chỉnh việc *lưu
  trữ* dữ liệu cá nhân, không chỉ việc *truy cập*. Một bản backup rò rỉ là vi phạm
  dù mask có bật hay không. Và như đã nói ở QĐ-2, mask ở tầng engine luôn có
  đường đi vòng.
* **Loại envelope encryption theo cột.** Giữ được khả năng giải mã hai chiều,
  nhưng mọi join trên số điện thoại đều phải giải mã trước → key phải phát tán
  tới mọi compute node. Deterministic token cho phép **join mà không cần giải mã**,
  đó là lý do chọn nó thay vì mã hoá.
* **Đánh đổi thừa nhận thẳng:** token deterministic dễ bị dictionary attack —
  không gian số ĐT Việt Nam chỉ ~10⁹, một GPU quét hết trong vài giờ nếu lộ key.
  Giảm thiểu bằng salt riêng theo bảng + rotate key theo quý. Cái giá phải trả là
  join xuyên kỳ rotate sẽ vỡ → xem FM4.
* **GPS:** không token hoá (vô nghĩa), mà **giảm độ chính xác** xuống geohash-7
  (~150 m) ở Silver. Toạ độ đầy đủ chỉ tồn tại ở Bronze và hết hạn sau 30 ngày.

### QĐ-5 · Late data & lịch sử: **SCD-1 qua MERGE có guard + SCD-2 append-only từ CDF**

`trips_current` là SCD Type 1, cập nhật bằng
`MERGE ... WHEN MATCHED AND src.op_ts > tgt.op_ts THEN UPDATE`. Guard `op_ts` là
toàn bộ cách xử lý late data: một event đến muộn 6 tiếng **không thể** ghi đè
trạng thái mới hơn, vì điều kiện MERGE tự loại nó.

* **Loại append-only + view "latest" bằng window function.** Đơn giản và đúng,
  nhưng mỗi query phải quét toàn bộ lịch sử của key rồi `ROW_NUMBER()` — không có
  cách nào đạt p95 < 1 giây trên 100 triệu chuyến.
* **Loại full daily snapshot.** Rewrite toàn bảng mỗi đêm; và tệ hơn, không trả
  lời được câu hỏi "trạng thái chuyến này lúc 14:23" — mà đó chính là câu hỏi mọi
  incident review đều hỏi.
* **Watermark 6 giờ** cho streaming; event đến sau đó rơi vào `_late_arrivals` và
  được backfill bằng MERGE hằng đêm. Guard `op_ts` đảm bảo backfill an toàn kể cả
  khi chạy lại nhiều lần (idempotent).

### QĐ-6 · Lifecycle: **Bronze 30 ngày hot → Glacier IR; locations downsample sau 90 ngày**

* **Loại "giữ tất cả ở S3 Standard".** 26 TB × $23/TB-tháng = **$598/tháng** so
  với **$105/tháng** ở Glacier Instant Retrieval. Cùng SLA truy xuất (mili-giây),
  chênh 5,7×.
* **Loại "xoá Bronze sau 30 ngày".** Rẻ hơn nữa, nhưng mất khả năng replay CDC khi
  phát hiện bug transform — và đó chính là FM1. Bronze là chi phí bảo hiểm, giữ
  365 ngày ở tier lạnh.

---

## 4. Failure modes — chuyện gì hỏng lúc 3 giờ sáng

### FM1 · Debezium replay: connector reset offset, 4 giờ CDC event bị apply lại

*Kịch bản:* Kafka Connect restart sau khi ops thay disk, offset commit bị mất,
connector đọc lại từ đầu retention. `trips_current` an toàn — guard `op_ts` ở QĐ-5
loại hết event cũ. Nhưng `trips_history` là **append-only từ CDF**, nên nó nuốt
trọn: ~12 triệu dòng trùng, và mọi số liệu SCD-2 sai từ 3 giờ sáng cho đến khi
có người phát hiện.

*Detect:* canary query mỗi 10 phút —
`count(*) - count(DISTINCT trip_id, op_ts, op_type)`, alert khi > 0. Bổ sung:
đọc `DESCRIBE HISTORY`, alert khi `numOutputRows` của một commit vượt 3× median
7 ngày. (NB3 cho thấy `history()` giữ đủ metric để làm việc này mà không cần
hệ thống monitoring riêng.)

*Rollback:* `RESTORE VERSION AS OF <v>` về commit ngay trước replay. Điểm mấu
chốt đo được ở NB3: **RESTORE là một commit mới, không phải xoá lịch sử** —
`history()` sau restore vẫn ≥ 5 version và có cả dòng RESTORE. Với auditor, điều
đó nghĩa là ta chứng minh được cả sự cố lẫn cách sửa, thay vì có một khoảng
trống đáng ngờ. Sau restore, replay lại từ Bronze với dedup key
`(trip_id, op_ts, op_type)`. **Điều kiện tiên quyết là Bronze còn sống** — đó là
lý do QĐ-6 không xoá Bronze.

### FM2 · Compaction chết lúc 2h sáng, 9h sáng dashboard đứng

*Kịch bản:* job OPTIMIZE fail vì spot instance bị thu hồi. Không ai được báo.
Micro-batch 30 giây tiếp tục chạy → sau 7 giờ, mỗi partition có ~840 file nhỏ.
Analyst mở dashboard lúc 9h, query p95 nhảy từ <1 giây lên hàng chục giây.

*Detect:* metric `avg_file_size = numFiles / totalBytes` theo bảng, alert khi
file trung bình < 32 MB. Metric thứ hai tinh hơn: **skip rate** — NB6 đo được sau
clustering thì point query chỉ phải mở 1/10 file (**90% skip**); khi skip rate
tụt xuống dưới 50% nghĩa là stats đã loãng, kể cả khi số file trông vẫn ổn.

*Rollback:* không có gì để rollback — chạy OPTIMIZE khẩn cấp, nhưng **phải
throttle**: compaction đọc và ghi lại toàn bộ partition, chạy full-speed lúc 9h
sáng sẽ tranh I/O với đúng những query đang chậm.

### FM3 · "Đã expire snapshot rồi mà hoá đơn S3 vẫn không giảm"

*Kịch bản:* FinOps hỏi vì sao chi phí storage tăng 40% trong quý dù team đã bật
snapshot expiry. Đây là failure mode **âm thầm nhất** vì không có gì đỏ cả.

*Cơ chế — hai phát hiện đo được trong lab, cả hai đều ngược với niềm tin phổ biến:*

1. **`expire_snapshots` chỉ đụng metadata.** NB6 đo: 20 → 3 snapshot, nhưng số
   file avro trên đĩa **40 → 40**, và metadata còn *phình ra* 325.3 KB → 332.5 KB.
   Không một byte dữ liệu nào được giải phóng.
2. **`VACUUM` không thu hồi orphan chưa từng commit.** File do job crash để lại
   chưa bao giờ vào transaction log → vô hình với vacuum ở **mọi** retention.
   NB6 đo: 5 file trên đĩa mà log không biết, bảng vẫn báo đúng 100.000 dòng.

Ở 30K writes/giây, streaming task bị kill giữa chừng là chuyện hằng ngày, và mỗi
lần như vậy để lại file mồ côi. Sau vài tháng là hàng TB trả tiền cho dữ liệu
**không ai đọc được và không công cụ chuẩn nào nhìn thấy**.

*Detect:* job đối chiếu tập hợp — liệt kê object trên S3, trừ đi tập file trong
transaction log, alert khi phần chênh > 1% dung lượng bảng.

*Rollback:* đây là vấn đề quy trình chứ không phải sự cố. Nguyên tắc rút ra:
**Job 3 (expiry) và Job 4 (orphan sweep) phải chạy thành cặp, sweep sau expiry.**
Chạy expiry một mình tạo ra đúng cảm giác "đã dọn dẹp" mà không dọn gì cả.

### FM4 · Key rotation làm vỡ join

*Kịch bản:* rotate key tokenization đầu quý (QĐ-4). Từ 00:00, cùng một số điện
thoại sinh ra token khác → join `trips × driver_profile` mất match cho mọi dữ
liệu xuyên kỳ. Dashboard "chuyến theo tài xế" rỗng dần.

*Detect:* canary join-rate — tỉ lệ match của join chính, alert khi tụt > 5% so
với ngày trước.

*Rollback:* giữ **hai key version song song** trong một quý (cột `token_v_n` và
`token_v_n1`), join theo `COALESCE`. Chi phí là một cột thừa; cái tránh được là
một sự cố dữ liệu im lặng kéo dài ba tháng.

---

## 5. Ước lượng chi phí (back-of-envelope)

**Giả định khối lượng:** 780 triệu event/ngày × 400 B envelope = 312 GB/ngày
logical; zstd-3 nén ~4× → **78 GB/ngày trên đĩa**.

### Storage (giá S3 ap-southeast-1)

| Tầng | Dung lượng | Đơn giá | $/tháng |
|---|---|---|---|
| Bronze hot (30 ngày) | 78 GB × 30 = 2,34 TB | S3 Standard $23/TB-th | **$54** |
| Bronze lạnh (ngày 31–365) | 78 GB × 335 = 26,1 TB | Glacier IR $4/TB-th | **$105** |
| Silver `trips`/`payments` (365 ngày) | ~5 GB/ngày × 365 = 1,8 TB | S3 Standard | **$41** |
| Silver `locations_30s` (downsample 8×) | 66 GB/ngày ÷ 8 × 275 ngày = 2,3 TB | S3 Standard | **$53** |
| Gold aggregates | ~50 GB | S3 Standard | **$1** |
| | | **Storage** | **≈ $254** |

### Request cost — khoản người ta hay quên

NB6 đo trực tiếp: full-scan trên bảng 200 file tốn **$4.00/ngày** chỉ riêng GET
request; sau compaction còn 11 file → **$0.08/ngày**. Với ~40 bảng và ~10K query
/ngày, khoản này dao động từ **~$30/tháng** (giữ compaction kỷ luật) tới
**~$1.500/tháng** (để small file tích tụ). Chênh lệch 50× này **hoàn toàn nằm ở
kỷ luật vận hành**, không nằm ở kiến trúc — đó là lý do 4 maintenance job nằm
trong sơ đồ §2 chứ không phải trong phần "sẽ làm sau".

### Compute

| Hạng mục | Cấu hình | $/tháng |
|---|---|---|
| Spark Structured Streaming (24/7) | 6 × r6g.2xlarge spot ≈ $0.18/h | **$780** |
| Maintenance job (OPTIMIZE/ZORDER/vacuum/sweep) | ~3 h/ngày, 4 node on-demand | **$300** |
| Trino (ad-hoc, p95 < 1s) | 3 × r6g.xlarge on-demand | **$550** |
| Kafka (MSK, retention 7 ngày) | 3 broker kafka.m5.large | **$390** |
| | **Compute** | **≈ $2.020** |

**Tổng ≈ $2.300/tháng** ở trạng thái vận hành kỷ luật; **≈ $3.800/tháng** nếu
buông compaction. Storage chỉ chiếm 11% — **chi phí thật nằm ở compute streaming
24/7 và ở request cost**, không nằm ở số TB. Đây là lý do tôi không tối ưu thêm
tier lạnh: cắt hết $105 của Glacier cũng chỉ bằng 4% hoá đơn.

---

## 6. Sẽ build gì trước — slice MVP một tuần

**Chỉ bảng `trips`. Bỏ hẳn `driver_locations`** — nó là 85% khối lượng nhưng 0%
rủi ro kiến trúc, và đưa nó vào tuần 1 sẽ biến mọi bug thành bug quy mô.

Đường đi đầu-cuối, hẹp nhất có thể:

```
Oracle.trips ─ Debezium ─ Kafka(1 topic) ─ Spark 30s ─ tokenize(phone)
              └─▶ bronze.cdc_trips ─CDF─▶ silver.trips_current ─▶ gold.trip_metrics_5m ─▶ 1 Superset chart
```

Kèm đúng ba thứ phụ trợ, không hơn:
1. `tokenize()` UDF cho `phone` (QĐ-4) — phần khó nhất về tuân thủ
2. Canary duplicate-check của FM1 — phần khó nhất về đúng đắn
3. Cron OPTIMIZE 2 giờ/lần — phần khó nhất về chi phí

**Slice này chứng minh được ba điều, và đó là toàn bộ mục đích của nó:**
(a) đạt 60 giây end-to-end với micro-batch 30 giây; (b) event đến muộn không ghi
đè trạng thái mới hơn — test bằng cách bơm tay một event `op_ts` cũ; (c) sau một
lần replay sai, `RESTORE` đưa bảng về đúng trạng thái và `history()` vẫn giữ
nguyên vết.

**Chưa làm trong tuần 1:** SCD-2 `trips_history`, downsample locations, tiering
Glacier, orphan sweep, key rotation, multi-city. Tất cả đều quan trọng — nhưng
không cái nào trong số đó có thể chứng minh hay bác bỏ kiến trúc, mà đó mới là
việc của một MVP.
