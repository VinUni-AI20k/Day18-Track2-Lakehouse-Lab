# Bonus Architecture Brief — Vietnamese Ride-Hailing CDC Lakehouse

**Student:** Nguyễn Đức Khang — 2A202600588  
**Topic:** C. CDC từ ride-hailing Việt Nam → Lakehouse, tuân thủ Nghị định 13  
**Scope:** Oracle production DB → Debezium CDC → Delta Lakehouse cho analytics, fraud, operations dashboard

---

## 1. Problem statement

Một nền tảng ride-hailing Việt Nam cần đưa dữ liệu chuyến xe từ Oracle production sang lakehouse để phục vụ dashboard vận hành, phân tích nhu cầu, fraud monitoring và báo cáo tài chính. Quy mô thiết kế: **100 triệu chuyến/năm**, peak **30K writes/giây** trong giờ cao điểm, dữ liệu gồm chuyến xe, tài xế, hành khách, thanh toán và GPS. PII như số điện thoại, CCCD/CMND, địa chỉ và tọa độ GPS thuộc phạm vi **Nghị định 13/2023/NĐ-CP**, nên người phân tích không được đọc raw PII nếu không có mục đích hợp lệ và audit trail.

SLA nghiệp vụ: dashboard refresh trong **60 giây** từ lúc source commit; ad-hoc query p95 **< 1 giây** cho dữ liệu 90 ngày gần nhất. Sự kiện đến muộn xảy ra thường xuyên vì tài xế mất mạng ở tỉnh xa. Thiết kế khó ở chỗ phải giữ đúng thứ tự thay đổi CDC, xử lý late data, vẫn cho analyst query nhanh, đồng thời không làm rò PII.

---

## 2. Architecture diagram

```text
Oracle OLTP
(trips, drivers, riders, payments)
    |
    | redo log / binlog
    v
Debezium Connect  --->  Schema Registry  --->  DLQ topic
    |
    | Kafka topics: trips_cdc, driver_cdc, rider_cdc, payment_cdc
    v
Streaming Bronze Writer
- append-only raw CDC envelopes
- HMAC tokenization for phone/id
- GPS coarse geohash for analytics
- raw PII encrypted, restricted path
    |
    v
+-------------------------- DELTA LAKEHOUSE --------------------------+
|                                                                     |
| Bronze:                                                             |
|   bronze.cdc_raw_events                                             |
|   partition: ingest_date, source_table                              |
|   columns: op, source_ts, ingest_ts, lsn, table, payload_json,       |
|            pii_token_map_ref, schema_id, quality_status             |
|                                                                     |
| Silver:                                                             |
|   silver.trips_current      -- latest state by trip_id              |
|   silver.trips_history      -- SCD Type 2                           |
|   silver.driver_dim_scd2    -- driver attributes, tokenized PII      |
|   silver.rider_dim_scd2     -- rider attributes, tokenized PII       |
|   silver.pii_access_audit   -- every approved PII read              |
|   MERGE rule: update only if src.source_ts > tgt.source_ts           |
|                                                                     |
| Gold:                                                               |
|   gold.city_hourly_demand                                           |
|   gold.driver_supply_metrics                                        |
|   gold.payment_reconciliation                                       |
|   gold.fraud_signal_daily                                           |
|   clustered by city_id, event_date, tenant/business_unit             |
|                                                                     |
+---------------------------------------------------------------------+
    |                                      |
    | Delta CDF                            | Catalog + policies
    v                                      v
BI dashboards / Trino / DuckDB       Unity/Polaris-style catalog
Ops alerting                         Column policy: PII hidden by default
Fraud notebooks                      Lineage: OpenLineage + Marquez
```

---

## 3. Main architecture decisions

### Decision 1 — Table format: Delta Lake

Tôi chọn **Delta Lake** cho Bronze, Silver và Gold vì lab cần ACID `MERGE`, time travel, Delta Change Data Feed và schema evolution. CDC không phải batch append đơn giản: một trip có thể tạo, cập nhật, huỷ, hoàn tiền, rồi nhận late GPS event. Silver cần `MERGE WHEN MATCHED AND src.source_ts > tgt.source_ts` để tránh event cũ ghi đè trạng thái mới.

Tôi loại **raw Parquet on S3** vì không có transaction log. Khi stream job fail giữa chừng, analyst có thể đọc nửa batch và dashboard sai. Tôi cũng loại **plain Kafka retention làm source phân tích dài hạn** vì Kafka phù hợp transport 3–7 ngày, không phù hợp ad-hoc query 1 năm và time travel theo version bảng.

### Decision 2 — Ingestion: Debezium + Kafka + streaming writer

Tôi chọn **Debezium đọc redo log Oracle vào Kafka**, sau đó một streaming job ghi Bronze. Debezium giữ thứ tự thay đổi theo partition key và có metadata như `source_ts`, `lsn`, `op`. Kafka giúp tách production Oracle khỏi lakehouse; nếu Silver job chậm, Oracle không bị query analytics kéo xuống.

Tôi loại **nightly batch extract** vì dashboard cần refresh trong 60 giây. Tôi loại **trigger trực tiếp từ Oracle sang lakehouse** vì coupling quá chặt: khi lakehouse lỗi, production database bị ảnh hưởng. Với CDC, production chỉ trả log stream, còn retry/backpressure nằm ở Kafka và stream writer.

### Decision 3 — Medallion layout: raw immutable Bronze, governed Silver, query-optimized Gold

Tôi chọn **Bronze append-only**, **Silver chuẩn hóa + SCD2**, **Gold aggregate cho dashboard**. Bronze giữ CDC envelope nguyên bản để debug replay. Silver mới là nơi áp late-event rule, tokenization, type validation và dedup theo `(source_table, primary_key, lsn)`. Gold chứa dữ liệu đã giảm kích thước: city-hour, driver supply, payment reconciliation.

Tôi loại thiết kế **dashboard đọc thẳng Bronze** vì raw CDC có insert/update/delete envelope, dễ double count chuyến xe. Tôi cũng loại thiết kế **chỉ có một bảng trips lớn** vì trộn raw, cleaned và aggregate làm lineage mờ: khi số chuyến ở dashboard lệch, team không biết lỗi nằm ở source, CDC, dedup hay aggregate.

### Decision 4 — Partitioning and clustering

Tôi chọn partition chính theo **event_date** cho Silver/Gold, thêm **city_id** ở Gold nếu cardinality thành phố ổn định. Hot query phổ biến là “Hà Nội, hôm nay, theo giờ” hoặc “Hồ Chí Minh, 7 ngày gần nhất”. Với Delta, tôi dùng clustering/Z-order trên `city_id`, `driver_id`, `event_date` cho bảng lớn như `silver.trips_current` và `gold.city_hourly_demand`.

Tôi loại partition theo **driver_id** vì cardinality quá cao, tạo small files. Tôi loại partition theo **ingest_date** cho Silver vì late event làm query theo ngày chuyến xe phải scan nhiều ngày ingest. Bronze vẫn dùng `ingest_date` vì mục tiêu Bronze là replay và vận hành stream, không phải dashboard theo event time.

### Decision 5 — PII governance: tokenization at Bronze boundary

Tôi chọn **tokenization ngay tại Bronze writer** cho số điện thoại, CCCD/CMND và rider/driver id nhạy cảm. Token dùng HMAC-SHA256 với key trong KMS. Analyst chỉ thấy token ổn định để join, không thấy raw PII. Raw PII nếu bắt buộc giữ thì nằm ở encrypted restricted path, TTL ngắn, access qua approval workflow và ghi `silver.pii_access_audit`.

Tôi loại **mask PII ở dashboard layer** vì dữ liệu nhạy cảm vẫn đã nằm trong Silver/Gold và có thể bị đọc bằng notebook. Tôi cũng loại **hash thường không salt** vì có thể brute-force số điện thoại Việt Nam. HMAC với key quản lý trong KMS giảm rủi ro dictionary attack và vẫn join được qua token.

### Decision 6 — Catalog, lineage, and access control

Tôi chọn **central catalog + OpenLineage/Marquez**. Catalog quản lý schema, table ownership, column policy và tags như `pii.direct`, `pii.tokenized`, `financial`. OpenLineage ghi job nào đọc bảng nào, cột nào tạo ra cột nào. Khi Risk hỏi “ai đọc PII tuần này?” hoặc “Gold metric này lấy từ source nào?”, team trả lời bằng metadata, không grep code.

Tôi loại **folder convention only** vì đặt tên đúng không đủ để enforce quyền. Tôi loại **spreadsheet data dictionary** vì nhanh lỗi thời: CDC schema đổi liên tục, nếu metadata không đi cùng job thì audit không đáng tin.

### Decision 7 — Retention and lifecycle

Tôi chọn retention theo lớp:

```text
Bronze raw CDC: 30 ngày hot + 180 ngày cold archive
Restricted raw PII: 7 ngày, encrypted, approval-only
Silver current/history: 2 năm hot/warm
Gold aggregates: 5 năm warm/cold
```

Bronze cần đủ dài để replay sự cố và backfill. Silver/Gold giữ lâu hơn vì phục vụ analytics, tài chính và audit. Tôi dùng object lifecycle: Standard cho 30 ngày đầu, Infrequent Access cho dữ liệu 31–180 ngày, Glacier/Archive cho dữ liệu hiếm đọc.

Tôi loại **giữ full raw PII 1 năm** vì chi phí và rủi ro pháp lý không đáng. Tôi cũng loại **xoá Bronze sau 24 giờ** vì khi phát hiện bug trong Silver merge, team không còn nguồn replay đáng tin.

---

## 4. Failure modes and rollback

### Failure mode 1 — Late event overwrites newer trip state

**Scenario 3 AM:** tài xế ở vùng mất mạng gửi lại event “trip accepted” sau khi trip đã hoàn thành. Nếu merge không kiểm tra timestamp, `silver.trips_current` bị rollback trạng thái từ `completed` về `accepted`.

**Detection:** metric `late_event_overwrite_attempts` tăng; data quality query tìm record có `src.source_ts < tgt.source_ts` nhưng vẫn update.  
**Rollback:** dùng Delta time travel restore Silver về version trước batch lỗi, sửa merge condition thành `src.source_ts > tgt.source_ts`, rồi replay Bronze từ `lsn` đã checkpoint. Đây là failure mode gắn trực tiếp với **time travel + MERGE** của Day 18.

### Failure mode 2 — Schema evolution breaks Silver job

**Scenario 3 AM:** team mobile thêm field `discount_breakdown` dạng nested JSON vào payment event. Bronze append được, nhưng Silver cast lỗi vì schema cũ expect `discount_amount` numeric.

**Detection:** stream job fail, DLQ topic tăng, Great Expectations check báo field mismatch.  
**Rollback:** Bronze vẫn append raw envelope với `quality_status='schema_pending'`. Silver job skip schema mới vào quarantine table. Sau khi cập nhật parser, replay only quarantined Bronze records. Không restore Bronze vì Bronze là immutable log.

### Failure mode 3 — PII tokenization key misconfiguration

**Scenario 3 AM:** KMS alias trỏ nhầm key mới, cùng một số điện thoại sinh token khác. Join giữa trips và rider_dim giảm đột ngột.

**Detection:** join success rate `trips_current -> rider_dim_scd2` rơi từ 99.8% xuống dưới 95%; key version trong Bronze metadata đổi bất thường.  
**Rollback:** dừng Bronze writer, đưa KMS alias về key đúng, restore Silver affected tables về version trước khi key lệch, replay Bronze records có `key_version` sai. Audit ghi rõ khoảng thời gian và số record bị ảnh hưởng.

### Failure mode 4 — Small files make dashboard p95 > 1 second

**Scenario 3 AM:** peak 30K writes/s tạo nhiều micro-batches, Gold table có hàng chục nghìn file nhỏ theo giờ. Dashboard thành phố load 8–12 giây.

**Detection:** Delta table file count/file size metric vượt threshold; Trino query p95 > 1s trong 15 phút.  
**Rollback:** không rollback dữ liệu. Chạy compaction khẩn cho partitions hot trong 7 ngày, sau đó bật lịch OPTIMIZE mỗi 30 phút cho Gold hot partitions. Nếu cần, tăng micro-batch interval từ 10s lên 30s để giảm số file.

---

## 5. Cost back-of-envelope

Assumptions:

```text
CDC raw event size after JSON envelope: ~2 KB/event average
100M trips/year, each trip ~12 CDC events across trip/payment/GPS/status
Total CDC events/year: 1.2B
Raw Bronze volume/year: 1.2B × 2 KB = 2.4 TB/year before compression
Parquet/ZSTD compression: ~4× smaller -> 0.6 TB/year Bronze storage
Silver normalized/history: ~1.0 TB/year
Gold aggregates: ~0.1 TB/year
Keep operational headroom ×3 for GPS-heavy records and replay copies
Estimated managed lake size: ~5 TB active/warm
```

Storage estimate using S3-like pricing:

```text
Hot 2 TB Standard:        $23/TB-month × 2 TB  = $46/month
Warm 2 TB IA:             $12.5/TB-month × 2 TB = $25/month
Cold 1 TB Glacier:        $4/TB-month × 1 TB    = $4/month
Metadata/log overhead 20%:                           ~$15/month
Storage subtotal:                                   ~$90/month
```

Compute estimate:

```text
Streaming CDC jobs: 4 workers × $0.20/hour × 24 × 30 = $576/month
Silver/Gold compaction + OPTIMIZE: 2 hours/day × $2/hour × 30 = $120/month
BI/ad-hoc query engine: ~$300/month for small team usage
Lineage/catalog/audit services: ~$150/month equivalent infra
Compute subtotal: ~$1,146/month
```

Total rough monthly cost:

```text
Storage ~$90 + Compute ~$1,146 = ~$1,236/month
Add 30% buffer for retries, backfills, dev/staging: ~$1,607/month
```

This is below a hypothetical **$5K/month** analytics budget. The main risk is not raw storage; it is compute waste from small files, unbounded ad-hoc queries and repeated backfills. That is why compaction, partition design and Gold aggregates matter.

---

## 6. One-week MVP slice

The first week should prove the risky parts, not build every table.

### Day 1–2: CDC skeleton

- Use sample Oracle/Debezium topic for `trips_cdc` and `driver_cdc`.
- Write Bronze Delta table with `op`, `source_ts`, `ingest_ts`, `lsn`, `payload_json`, `schema_id`.
- Add checkpointing and DLQ for malformed events.

### Day 3: Tokenization boundary

- Implement HMAC tokenization for phone and CCCD/CMND fields.
- Store `key_version` and `token_method` in Bronze metadata.
- Prove analyst query can join on token without seeing raw PII.

### Day 4: Silver MERGE with late-event guard

- Build `silver.trips_current` using `MERGE`.
- Add rule: update only when `src.source_ts > tgt.source_ts`.
- Create a test with one late event and prove it does not overwrite newer state.

### Day 5: Gold dashboard table

- Build `gold.city_hourly_demand` with city, hour, completed trips, cancel rate and GMV.
- Optimize hot partitions and measure query p95 on 7-day filter.

### Day 6–7: Failure drill and review

- Run a replay from Bronze after intentionally breaking Silver parser.
- Restore Silver via Delta time travel.
- Show lineage from Gold metric back to Silver and Bronze.
- Document what data analyst can read, what requires approval, and where audit rows are written.

MVP success criteria:

```text
- Bronze receives CDC events continuously.
- Silver handles late events correctly.
- PII is tokenized before analyst access.
- Gold dashboard table refreshes within 60 seconds on sample load.
- One restore/replay drill is demonstrated end-to-end.
```

---

## 7. Design review summary

I would defend this design because it separates three concerns that are easy to mix up:

1. **Truth capture:** Bronze stores immutable CDC facts for replay.
2. **Correct state:** Silver applies ordering, dedup, SCD2 and PII controls.
3. **Fast consumption:** Gold serves dashboard and ad-hoc analytics without scanning raw CDC.

The trade-off is operational complexity: Kafka, streaming jobs, catalog, lineage and compaction all need owners. I accept that cost because the alternative is worse: fast dashboards built on unclear data, PII spread across notebooks, and no reliable rollback when a CDC merge bug appears at 3 AM.
