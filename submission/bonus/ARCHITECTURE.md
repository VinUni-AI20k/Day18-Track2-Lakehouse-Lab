# Architecture Brief — Topic C: CDC Ride-Hailing VN → Lakehouse (Nghị định 13/2023/NĐ-CP)

## 1. Problem statement

Production Oracle DB của một app gọi xe VN cần chảy sang Lakehouse qua Debezium
CDC để phục vụ analytics. Quy mô: **100 triệu chuyến/năm** (~274K chuyến/ngày),
mỗi chuyến sinh ~250 sự kiện (GPS ping mỗi 5s trong chuyến trung bình 20 phút +
~10 sự kiện trạng thái) → **~68.5 triệu sự kiện/ngày**, đỉnh **30K writes/giây**
giờ cao điểm. Dữ liệu chứa PII của tài xế/hành khách (SĐT, CMND, GPS) thuộc
phạm vi **Nghị định 13/2023/NĐ-CP**. Yêu cầu: dashboard analyst refresh trong
**60 giây** kể từ khi Oracle commit; ad-hoc query **p95 < 1s**. Sự kiện đến
muộn (late-arriving) xảy ra thường xuyên do mất sóng ở tỉnh xa — có thể trễ
hàng giờ. Cái khó: vừa đạt độ trễ near-real-time, vừa đảm bảo state đúng khi
sự kiện đến sai thứ tự, vừa chặn PII rò rỉ ngay từ điểm landing đầu tiên,
trong khi hệ thống nguồn (Oracle OLTP) không được phép chịu thêm tải đọc.

## 2. Architecture diagram

```
 Oracle OLTP (trips, drivers, riders, payments)
        │  (redo log, không query trực tiếp)
        ▼
 Debezium connector (LogMiner) ──► Kafka (topic/table, 7-day retention,
        │                                  partition theo trip_id % N)
        │
        ├─────────────────────────────► Schema Registry (Avro, backward-compat)
        ▼
 ┌─────────────────────── Structured Streaming job (30s micro-batch) ───────────────┐
 │  1. Tokenize PII tại landing (SĐT/CMND → HMAC token; GPS giữ nguyên,      │
 │     chỉ mask ở Silver/Gold view)                                          │
 │  2. Ghi Bronze (append-only, 1:1 với Kafka offset — chưa merge)           │
 └────────────────────────────────────────────────────────────────────────────┘
        ▼
 Bronze (Delta)  _lakehouse/bronze/{trips,gps_pings,payments}
   partition: ingest_date (landing day, KHÔNG phải event ts)
        │  MERGE INTO ... WHEN MATCHED AND src.ts > tgt.ts  (late-data guard)
        ▼
 Silver (Delta, SCD Type 2)  trips_current + trips_history
   partition: event_date (từ ts gốc) — cho phép re-partition khi late data tới
        │  aggregate + PII masked view (row/column security)
        ▼
 Gold (Delta, Z-ORDER theo region/hour)  trips_by_region_hour, driver_kpi_daily
        │
        ▼
 Query path: BI dashboard (DuckDB/Trino) đọc Gold — p95 < 1s (Gold nhỏ, cached)
             Analyst ad-hoc trên Silver qua masked view — audit table ghi mọi lần đọc PII
```

## 3. Quyết định chính (kèm alternatives đã loại)

**(1) Table format: Delta Lake.** Tôi chọn **Delta** vì `MERGE INTO` +
`Change Data Feed` là first-class, khớp trực tiếp với pattern CDC upsert +
SCD2 cần ở Silver. Tôi loại **Iceberg** vì tuy V3 (deletion vectors) đã ổn
định về mặt kỹ thuật, hệ sinh thái tooling CDC-merge quanh Iceberg ở quy mô
30K writes/giây liên tục còn ít case study production hơn Delta — rủi ro vận
hành cao hơn giá trị portability. Tôi loại **Hudi** vì team không có kinh
nghiệm vận hành Hudi's compaction service riêng biệt (khác cơ chế OPTIMIZE
đơn giản của Delta mà NB2/NB6 đã minh hoạ).

**(2) Ingestion: Debezium (LogMiner) + Kafka**, không JDBC polling. Polling
định kỳ (`SELECT ... WHERE updated_at > last_poll`) sẽ thêm tải đọc lặp lại
lên Oracle OLTP đang phục vụ 30K writes/giây — rủi ro làm chậm hệ thống
production. Tôi cũng loại **Oracle GoldenGate**: license cost cao và vendor
lock-in, trong khi Debezium open-source đã đủ đáp ứng LogMiner-based CDC ở
throughput này.

**(3) Streaming engine: Spark Structured Streaming**, không Flink. Team đã
vận hành Spark cho batch Silver/Gold (giống stack lab), nên dùng chung engine
giảm chi phí vận hành hai runtime khác nhau. Tôi loại **Flink**: latency
thấp hơn (sub-second) nhưng SLA yêu cầu chỉ là 60s — sub-second là
over-engineering không cần thiết, đổi lấy thêm một hệ thống phải học/vận
hành. Tôi loại **ksqlDB**: mạnh cho stream-to-stream join đơn giản nhưng
không có ACID table + time travel cần cho audit/incident replay.

**(4) Partitioning: Bronze theo `ingest_date` (ngày landing), Silver theo
`event_date` (ngày gốc từ Oracle).** Tôi loại partition Bronze theo
`event_date`: vì late data có thể đến trễ hàng giờ/ngày, ghi vào đúng
partition ngày cũ liên tục sẽ tạo **small-files problem y hệt NB2** (mỗi
micro-batch trễ tạo một file mới trong một partition đã "đóng băng" ở Gold).
Bronze append theo ngày landing tránh việc này; việc sắp xếp lại theo
event_date chỉ xảy ra một lần ở bước MERGE sang Silver.

**(5) PII: tokenize (HMAC + vault lookup) ngay tại điểm landing (trong job
Structured Streaming, trước khi ghi Bronze)**, không lưu raw rồi mask bằng
view. Tôi loại "raw + view-level masking": Nghị định 13 yêu cầu hạn chế tối
đa bề mặt lưu trữ dữ liệu định danh — nếu raw PII vẫn nằm trên đĩa ở Bronze,
mọi lỗi cấu hình access-control (quên bật RLS trên một view mới) sẽ lộ dữ
liệu thật ngay lập tức. Tokenize sớm nghĩa là kể cả khi access-control sai,
dữ liệu lộ ra vẫn chỉ là token vô nghĩa nếu không có vault riêng.

**(6) Late-data handling: `MERGE INTO silver_trips t USING bronze_batch s ON
t.trip_id = s.trip_id WHEN MATCHED AND s.ts > t.ts THEN UPDATE ... WHEN NOT
MATCHED THEN INSERT`**, không dùng daily full-reprocess batch. Tôi loại
reprocess-toàn-bảng-mỗi-ngày: ở 68.5M sự kiện/ngày, re-scan toàn bộ Silver
mỗi đêm để "sửa" vài nghìn bản ghi trễ là lãng phí compute lớn so với một
MERGE có điều kiện `ts` chỉ chạm đúng những dòng cần cập nhật.

## 4. Failure modes (3 giờ sáng)

**a) Oracle redo log switch làm Debezium connector rớt offset (tie: schema
evolution/time travel — Day 18 §2, §3).** Triệu chứng: Kafka consumer lag
tăng vọt, dashboard staleness vượt 60s. Detect: alert trên
`kafka_consumer_lag > 5 min`. Rollback: Kafka retention 7 ngày cho phép
connector tự resume từ offset cuối cùng đã commit; nếu Bronze bị ghi sai
schema trong lúc reconnect, dùng `RESTORE TABLE bronze.trips VERSION AS OF
<v_tốt_cuối>` (giống NB3) rồi replay Kafka từ offset tương ứng — không cần
full re-ingest từ Oracle.

**b) DBA thêm cột mới vào bảng Oracle `trips` không báo trước → Debezium
phát field mới → job MERGE Silver crash vì schema mismatch.** Detect: job
alert khi Structured Streaming exception do schema validation. Rollback:
Bronze dùng `schema_mode="merge"` (như NB1) để **luôn nhận được** field mới
mà không crash; nhưng Silver MERGE job chỉ auto-evolve cột **additive**,
breaking change (đổi kiểu, xoá cột) bị chặn cứng và cần review thủ công
trước khi cho field đi tiếp — tránh corrupt SCD2 lịch sử.

**c) Mất sóng diện rộng ở một tỉnh 6 giờ → khi có mạng lại, hàng loạt GPS
event với `ts` cũ ùa về cùng lúc, tạo micro-burst > 30K/giây trong vài
phút.** Detect: throughput alert trên Kafka topic + kiểm tra
`valid_to < valid_from` sau MERGE (data-quality check trên SCD2). Rollback:
guard `WHEN MATCHED AND s.ts > t.ts` (mục 3.6) đảm bảo state hiện tại không
bị ghi đè bởi dữ liệu cũ hơn dù đến sau; nếu burst vượt khả năng cluster xử
lý trong 60s SLA, cho phép **SLA degrade có kiểm soát** (backpressure, xử lý
dần trong 5–10 phút) thay vì drop event — trip GPS trace không được phép mất
vì phục vụ điều tra sự cố.

## 5. Ước lượng chi phí (back-of-envelope, tháng)

| Hạng mục | Tính toán | $/tháng |
|---|---|---:|
| Kafka (3× broker CDC, retention 7 ngày, ~700GB) | 3 × \$0.21/hr × 730h | ~\$460 |
| Spark Structured Streaming cluster (4 worker, 24/7) | 4 × \$0.30/hr × 730h | ~\$876 |
| Bronze storage (68.5M evt/ngày × 300B/evt ≈ 20.5GB raw/ngày, nén parquet ~2:1 → ~10GB/ngày, giữ 90 ngày = 0.9TB) | 0.9TB × \$23/TB-tháng | ~\$21 |
| Silver + Gold storage (SCD2 history + aggregates, ước ~1.5× Bronze) | 1.35TB × \$23/TB-tháng | ~\$31 |
| Compaction/OPTIMIZE job định kỳ (batch, không 24/7) | ~2h/ngày × \$1/h × 30 | ~\$60 |
| **Tổng ước lượng** | | **~\$1,450/tháng** |

So với budget \$5–8K/tháng thấy ở các topic khác trong đề bài, con số này
còn dư nhiều headroom — hợp lý vì raw event nhỏ (GPS ping ~300B) chứ không
phải log LLM 5KB/req như Topic A.

## 6. MVP tuần đầu (slice nhỏ nhất chứng minh kiến trúc work)

Không build cả 3 luồng (trips, gps_pings, payments) và không nhắm SLA 60s
ngay. Tuần 1 chỉ làm:

1. Debezium → Kafka cho **một bảng duy nhất** (`trips`, bỏ qua GPS ping tần
   suất cao).
2. Structured Streaming job **micro-batch 15 phút** (nới lỏng so với target
   60s) ghi Bronze **có tokenize PII tại landing**.
3. MERGE sang Silver SCD2 với late-data guard (`ts > t.ts`).
4. Một bảng Gold duy nhất: `trips_by_region_hour`.

Slice này đã chứng minh đủ 3 mechanism khó nhất — CDC ingestion, tokenization
sớm, và late-data-safe MERGE — trước khi đầu tư siết SLA xuống 60s và mở
rộng sang GPS/payments ở tuần sau.

---

*PoC đi kèm tại `submission/bonus/poc/tokenize_and_late_merge_demo.py`:
demo hàm tokenize HMAC cho SĐT/CMND (mục 3.5) và late-data-safe MERGE
(mục 3.6) trên dữ liệu giả lập, không cần Oracle/Kafka thật.*
