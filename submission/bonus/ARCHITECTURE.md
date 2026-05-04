# Bonus Architecture: Lakehouse CDC cho Ride-Hailing Việt Nam, tuân thủ Nghị định 13

## 1. Problem statement

Một công ty ride-hailing tại Việt Nam cần stream thay đổi từ Oracle production vào lakehouse phục vụ analytics: khoảng **100 triệu chuyến/năm**, peak **30K writes/giây** trong các đợt khuyến mãi hoặc mưa bão. PII của tài xế và hành khách gồm số điện thoại, số định danh cá nhân, và GPS chính xác nằm trong phạm vi **Nghị định 13/2023/NĐ-CP**, nên dữ liệu nhạy cảm phải được token hóa trước khi con người hoặc BI tool có thể đọc. Analyst cần dashboard refresh trong **60 giây** từ source commit và ad-hoc query **p95 < 1 giây** theo city, district, service tier, và trip status. Bài toán khó vì Debezium có thể gửi event lệch thứ tự khi mạng ở tỉnh xa reconnect, dimension cần SCD Type 2, deletion/right-to-erasure phải chứng minh được, và mọi lần đọc PII đều phải audit mà không làm chậm query path bình thường.

## 2. Architecture diagram

```text
Oracle OLTP
  | redo logs
  v
Debezium Connect -> Kafka topics: trips, drivers, riders, payments
  | Avro + Schema Registry, source commit LSN, event_ts, op
  v
+---------------- BRONZE ----------------+      +---------------- SECURITY / OPS ----------------+
| Delta append-only cdc_raw_*             |----->| pii_read_audit Delta table + SIEM              |
| partition: ingest_date/hour             |      | OpenLineage events -> Marquez/DataHub          |
| HMAC tokenization trước khi commit      |      | expectations, lag SLO, schema registry alerts  |
| raw encrypted quarantine: 24h, no BI    |      +------------------------------------------------+
+------------------+---------------------+
                   |
                   | streaming MERGE, dedup by table + pk + source_lsn,
                   | late event rule: apply only if src.source_ts > tgt.source_ts
                   v
+---------------- SILVER ----------------------------------------------+
| trip_events_current: Delta + CDF enabled, deletion vectors enabled    |
| dim_driver_scd2, dim_rider_scd2, dim_vehicle_scd2                     |
| gps_grid_7 hoặc ward_id thay vì GPS chính xác cho analytics thường    |
| constraints: non-null ids, valid event order, schema evolution gated   |
+------------------+---------------------------------------------------+
                   |
                   | CDF incremental jobs mỗi 30s; OPTIMIZE hourly
                   v
+---------------- GOLD ------------------------------------------------+
| fact_trip_minute_city, fact_trip_hour_district, revenue_margin_daily   |
| marts cho BI: city_id/date/service_tier/status                         |
| Z-order/liquid clustering by tenant/city_id + trip_date + status       |
+------------------+---------------------------------------------------+
                   |
       +-----------+--------------+
       v                          v
 Superset/PowerBI dashboards   Trino/DuckDB ad-hoc
 p95 freshness < 60s           PII chỉ đọc qua governed views/UDFs
```

## 3. Quyết định chính và alternatives đã loại

### Quyết định 1: Table format

Tôi chọn **Delta Lake cho Bronze, Silver, và Gold** vì workload này cần ACID `MERGE`, Change Data Feed, time travel, deletion vectors, và streaming support trưởng thành trong cùng một table format. Silver có thể consume Debezium event theo cách idempotent bằng source LSN, còn Gold có thể refresh từ CDF thay vì scan lại toàn bộ Silver.

Tôi loại **plain Parquet trên object storage** vì không có transaction log, không có native time travel, và không đảm bảo concurrent streaming `MERGE`; failure mode rõ nhất ở đây là batch CDC bị ghi một nửa trong giờ peak. Tôi loại **Apache Iceberg làm format chính** vì Iceberg rất mạnh cho multi-engine reads và hidden partitioning, nhưng Delta CDF và MERGE ergonomics phù hợp hơn với bài toán CDC analytics 60 giây trong stack của lab. Tôi sẽ xem lại Iceberg nếu platform chuyển thành Trino-first và Spark streaming không còn là writer chính.

### Quyết định 2: Catalog và governance

Tôi chọn **Unity Catalog hoặc một central lakehouse catalog tương đương có column masks, row filters, và audit hooks**, kết hợp OpenLineage events gửi sang Marquez/DataHub. Ownership, PII classification, và masking policy phải nằm ngoài notebook để Spark, Trino, và BI tools đều thấy cùng một rule.

Tôi loại **Hive Metastore-only governance** vì nó quản lý table metadata nhưng không tự cung cấp column-level security, PII classification, hoặc policy enforcement đủ mạnh. Tôi loại **application-side access control only** vì analyst còn query qua BI tool, notebook, và Trino; governance phải được enforce ở data access layer, không chỉ trong một app.

### Quyết định 3: Bronze landing và tokenization

Tôi chọn **tokenization ngay tại Bronze landing trước khi Delta commit**. Số điện thoại và national ID được chuyển thành deterministic HMAC-SHA256 token kèm `token_version`; GPS chính xác được chuyển thành khóa địa lý vận hành như geohash/grid hoặc ward. Một restricted encrypted quarantine giữ raw payload tối đa 24 giờ cho ingestion debugging, với break-glass access được ghi vào `pii_read_audit`.

Tôi loại **tokenize ở Silver** vì raw PII lúc đó đã tồn tại trong Bronze durable table và có thể bị đọc bởi bất kỳ ai có quyền Bronze. Tôi loại **random token không deterministic** vì join tài xế/hành khách, duplicate detection, và fraud pattern cần identifier ổn định nhưng vẫn pseudonymous. Tôi loại **đặt reversible encryption key cạnh lakehouse** vì một lỗi permission trong catalog có thể biến thành data breach; token lookup và key rotation nên nằm sau một privacy service riêng.

### Quyết định 4: CDC merge và late-data semantics

Tôi chọn **Debezium vào Kafka, Bronze append-only, rồi streaming `MERGE` vào Silver bằng source LSN và source timestamp guard**. Mỗi Silver row lưu `source_lsn`, `source_ts`, `ingest_ts`, và `record_hash`. Merge rule: chỉ update khi incoming event mới hơn current row, hoặc khi LSN chứng minh đây là retry của cùng source transaction. SCD Type 2 dimensions sẽ close row cũ và open row mới khi thông tin tài xế/hành khách thay đổi.

Tôi loại **last-write-wins theo ingestion timestamp** vì khi mạng ở tỉnh xa hồi phục, event source cũ có thể đến sau event mới và làm hỏng trip state. Tôi loại **daily batch CDC load** vì SLA dashboard là 60 giây từ source commit. Tôi loại **mutate Bronze records** vì Bronze phải là forensic record của cái gì đã đến và đến lúc nào; Silver mới là nơi state nghiệp vụ được sửa đúng.

### Quyết định 5: Partitioning, clustering, và file layout

Tôi chọn **date/hour partitions ở Bronze, date partitions cộng clustering/Z-order ở Silver và Gold**. Bronze partition theo `ingest_date` và `ingest_hour` để late arrivals không phải rewrite source partitions cũ. Silver trip tables partition theo `trip_date` và cluster theo `city_id`, `service_tier`, `status`, và `driver_token`. Gold marts hẹp hơn, được pre-aggregate theo phút/giờ và cluster theo `city_id`, `trip_date`, và `status`.

Tôi loại **partition Silver theo driver hoặc rider** vì cardinality quá cao sẽ tạo small files và làm chậm metadata planning. Tôi loại **chỉ partition theo date mà không clustering** vì ad-hoc query p95 < 1 giây thường filter theo city/status/service tier, không chỉ theo ngày. Tôi loại **partition Bronze theo source event date** vì late events sẽ liên tục reopen partitions cũ và làm ingestion khó dự đoán.

### Quyết định 6: Retention, deletion, và lifecycle

Tôi chọn **hot/warm/cold lifecycle theo layer và access pattern**. Bronze CDC hot trong 30 ngày, sau đó chuyển sang infrequent-access storage trong 13 tháng. Silver current-state tables hot trong 13 tháng; SCD2 history cũ chuyển warm sau 90 ngày. Gold aggregates hot trong 24 tháng vì nhỏ và phục vụ dashboard. Delta `VACUUM` giữ tối thiểu 14 ngày cho table thường và 30 ngày cho Silver tables nhạy cảm để time-travel rollback vẫn hữu ích.

Tôi loại **vacuum quá gắt sau một ngày** vì lỗi schema hoặc merge có thể không bị phát hiện ngay, trong khi rollback cần historical versions. Tôi loại **giữ mọi raw payload hot mãi mãi** vì vừa tăng PII exposure vừa tăng storage cost mà không cải thiện analytics phổ biến. Tôi loại **hard-delete token khỏi Bronze ngay khi có erasure request** vì auditability vẫn quan trọng; deletion vectors cộng tombstone/deny-list table cho phép chứng minh và enforce erasure trong khi vẫn giữ transaction history đến khi hết legal retention.

### Quyết định 7: Serving strategy

Tôi chọn **Gold marts cho dashboard và governed Silver views cho ad-hoc analysis**. Dashboard đọc aggregate tables nhỏ được refresh mỗi 30 giây từ CDF. Ad-hoc users dùng Silver views đã row-filter và tokenized; break-glass exact PII retrieval bắt buộc có purpose code và ghi vào `pii_read_audit`.

Tôi loại **query trực tiếp Bronze cho dashboard** vì Bronze trộn inserts, updates, deletes, và duplicates, nên không thể đạt p95 < 1 giây trong peak CDC. Tôi loại **copy dữ liệu sang warehouse riêng làm system of record** vì tạo thêm một governance surface và làm yếu khả năng time-travel/debug. Warehouse cache có thể tồn tại cho một số Gold tables, nhưng Delta vẫn là governed record.

## 4. Failure modes và rollback

1. **Late CDC replay overwrite trip state mới hơn.** Detection: expectation checks so sánh tính monotonic của `source_ts` theo primary key và alert khi một merge cố apply event cũ hơn. Rollback: dùng Delta time travel restore Silver table về version trước bad merge, replay Bronze cho LSN range bị ảnh hưởng với guard `src.source_ts > tgt.source_ts` đã sửa, rồi regenerate Gold từ CDF.

2. **Deploy tokenization service làm đổi nhầm HMAC salt hoặc token version.** Detection: Bronze quality checks theo dõi token stability trên sample identity cố định và alert khi cùng source identity map ra nhiều token trong cùng `token_version`. Rollback: pause Kafka consumers, time-travel Bronze về Delta version tốt gần nhất, redeploy tokenization config cũ, rồi replay từ Kafka offsets sau good version. `token_version` giúp phát hiện mixed-version rõ ràng.

3. **Schema evolution từ Oracle thêm nullable column làm hỏng BI downstream.** Detection: Schema Registry compatibility checks và Delta expectations fail Silver promotion job trước khi field mới tới Gold. Rollback: giữ Bronze append-only với column mới, pin Silver readers vào schema cũ, tạo compatibility view với default/null-safe column, và chỉ promote sau khi Gold contracts được cập nhật. Ở đây schema evolution là quy trình có kiểm soát, không phải side effect vô tình.

4. **PII audit table unavailable khi có break-glass read.** Detection: governed PII UDF kiểm tra audit-write success trước khi trả unmasked value. Rollback: fail closed; không trả PII cho tới khi audit table writable. Nếu cần emergency access vì business continuity, security approve offline export và export manifest phải được backfill vào `pii_read_audit` trước khi đóng incident.

5. **Small files từ peak traffic làm p95 ad-hoc query vượt 1 giây.** Detection: file-count metrics và query history alert khi Gold table scan planning time hoặc files-per-partition vượt threshold. Rollback: chạy targeted OPTIMIZE/Z-order trên city/date partitions bị ảnh hưởng, tạm route dashboard về Gold version trước đó bằng time travel, và tăng compaction frequency trong promotion windows.

## 5. Ước lượng chi phí back-of-envelope

Giả định Debezium payload trung bình 2 KB sau Avro compression và tokenization. Peak 30K writes/giây chỉ xuất hiện ở một số thời điểm; với **100M trips/năm**, giả sử 20 lifecycle events cho mỗi trip trên trips, payments, driver/rider updates, và status changes: **2B CDC events/năm**, tương đương **5.5M/ngày** trung bình. Thêm surge và operational headroom 5x: **27.5M events/ngày**. Storage math dùng giá object storage gần S3: Standard **$23/TB-tháng**, Infrequent Access **$12.5/TB-tháng**, Glacier-like archive **$4/TB-tháng**.

Raw Bronze:

- 27.5M events/ngày x 2 KB = **55 GB/ngày raw compressed**.
- 30 ngày hot = 1.65 TB x $23 = **$38/tháng**.
- 13 tháng warm, trừ tháng hot: khoảng 18.4 TB x $12.5 = **$230/tháng**.

Silver:

- Current và SCD2 history khoảng 1.3x Bronze sau normalized columns, stats, và metadata: 55 GB/ngày x 1.3 = **71.5 GB/ngày**.
- 90 ngày hot = 6.4 TB x $23 = **$147/tháng**.
- 10 tháng warm = 21.5 TB x $12.5 = **$269/tháng**.

Gold:

- Aggregates khoảng 5% Silver: 3.6 GB/ngày.
- 24 tháng = 2.6 TB x $23 = **$60/tháng**.

Operational overhead:

- Delta logs, checkpoints, audit tables, lineage metadata, và quarantine headroom: **$150/tháng**.

Estimated storage total: **$894/tháng**, làm tròn thành **$1.2K/tháng** khi tính thêm object request cost và region replication cho Gold tables quan trọng.

Compute:

- Kafka Connect/Debezium: 3 medium workers khoảng $180/tháng mỗi worker = **$540/tháng**.
- Streaming Silver jobs: 4 autoscaled Spark workers trung bình 12 giờ/ngày ở $0.60/giờ = 4 x 12 x 30 x $0.60 = **$864/tháng**.
- Gold CDF refresh jobs: 2 workers trung bình 8 giờ/ngày ở $0.60/giờ = **$288/tháng**.
- OPTIMIZE/compaction: 6 workers x 2 giờ/ngày x 30 x $0.60 = **$216/tháng**.
- BI/query warehouse: small always-on cluster cộng bursts = **$1.5K/tháng**.

Estimated compute total khoảng **$3.4K/tháng**. Tổng storage + compute khoảng **$4.6K/tháng** trước enterprise support và network egress. Các cost-control chính là CDF incremental processing, Gold pre-aggregation, tránh high-cardinality partitions, và chuyển Bronze/Silver cũ sang warm storage.

## 6. MVP một tuần sẽ build trước

Slice nhỏ nhất có thể ship là một city, ba CDC topics, và một dashboard mart:

1. Ingest Debezium-like events cho `trips`, `drivers`, và `riders` vào Bronze Delta với deterministic HMAC tokenization cho phone và ID fields, kèm restricted raw quarantine path.
2. Build Silver `trip_events_current` với idempotent merge theo source LSN và late-data protection bằng `source_ts`; build `dim_driver_scd2` cho thay đổi hồ sơ tài xế.
3. Enable Delta CDF trên Silver và tạo Gold `fact_trip_minute_city` với p50/p95 pickup latency, completion rate, gross bookings, và cancellation counts.
4. Add `pii_read_audit` và governed view trả masked fields mặc định, đồng thời fail closed nếu audit logging unavailable.
5. Run ba test: duplicate CDC replay không đổi counts, late older updates không overwrite trip state mới hơn, và bad Silver merge có thể rollback bằng time travel rồi replay từ Bronze.

Success criteria trong tuần: p95 Gold refresh dưới 60 giây trên synthetic day 10M events, ad-hoc city/status query dưới 1 giây trên Gold, mọi PII reveal đều sinh audit row, và có documented rollback bằng Delta table history.
