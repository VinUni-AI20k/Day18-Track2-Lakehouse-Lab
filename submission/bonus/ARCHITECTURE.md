# Kiến Trúc Lakehouse CDC Cho Nền Tảng Ride-Hailing Việt Nam (Tuân Thủ Nghị Định 13/2023/NĐ-CP & Luật Dữ Liệu 2026)

**Tác giả:** Đinh Quốc Việt — Senior Data Platform Architect  
**Hệ thống:** Real-time CDC Ingestion & Analytics Lakehouse  
**Quy mô:** 100 triệu chuyến xe/năm, 30.000 writes/giây ở peak, SLA dashboard < 60s, SLA ad-hoc query p95 < 1s  

---

## 1. Problem Statement (Bài Toán & Thách Thức)

Nền tảng gọi xe công nghệ xử lý **100 triệu chuyến đi/năm**, chịu tải đỉnh **30.000 writes/giây** (GPS ping, cập nhật trạng thái cuốc xe, thanh toán). Toàn bộ dữ liệu tác nghiệp nằm trên hệ thống OLTP (Oracle DB / PostgreSQL), cần đồng bộ CDC sang Lakehouse cho analytics và real-time dashboards với các ràng buộc khắt khe:

1. **SLA Độ Trễ (Latency):** Dashboard tài chính & điều vận phải cập nhật trong vòng **60 giây** kể từ khi source commit; ad-hoc analytics query đạt **p95 < 1s**.
2. **Tuân Thủ Pháp Lý:** Dữ liệu cá nhân (Số điện thoại, CMND/CCCD, định vị GPS tài xế/hành khách) thuộc phạm vi điều chỉnh của **Nghị định 13/2023/NĐ-CP** và Luật Bảo vệ Dữ liệu Cá nhân: bắt buộc ẩn danh hóa/tokenization trước khi ghi xuống vùng phân tích, lưu trữ audit log toàn bộ lượt truy cập PII, và đáp ứng quyền yêu cầu xóa dữ liệu (Right-to-Erasure) trong 72 giờ.
3. **Dữ Liệu Đến Muộn (Late-Arriving Data):** Do kết nối mạng không ổn định ở các tỉnh xa/vùng sâu, sự kiện GPS và hoàn thành cuốc xe thường xuyên bị trễ hàng chục phút đến vài giờ, dễ dẫn đến hiện tượng ghi đè sai lệch thứ tự trạng thái (out-of-order state regression).

---

## 2. Architecture Diagram (Sơ Đồ Kiến Trúc Toàn Diện)

```
       [ Production OLTP ]
   (Oracle DB / PostgreSQL)
     [ 30k writes/s peak ]
              │
              │ Debezium CDC (LogMiner / WAL)
              ▼
      [ Apache Kafka / Redpanda Cluster ]
   (Topics: raw.ride_events, raw.driver_gps, raw.users)
              │
              │ Spark Structured Streaming (Trigger: 10s)
              │ + In-Flight Tokenization Engine (KMS Salted HMAC)
              ▼
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                         STORAGE LAYER (S3 / MinIO)                          │
 │                                                                             │
 │ ┌─────────────────────────────────────────────────────────────────────────┐ │
 │ │ BRONZE LAYER (Append-Only Raw CDC Log)                                  │ │
 │ │ • Raw CDC payloads + Change Data Feed (CDF) enabled                     │ │
 │ │ • PII columns tokenized with deterministic HMAC SHA-256 (Salted)        │ │
 │ │ • Reversible PII encrypted with AES-GCM (Envelope Encryption via KMS)   │ │
 │ │ • Partitioned by: ingestion_date                                        │ │
 │ └────────────────────────────────────┬────────────────────────────────────┘ │
 │                                      │ Delta MERGE (Micro-batch 30s)        │
 │                                      │ WHEN MATCHED AND src.ts > tgt.ts     │
 │                                      │ WHEN NOT MATCHED THEN INSERT         │
 │                                      ▼                                      │
 │ ┌─────────────────────────────────────────────────────────────────────────┐ │
 │ │ SILVER LAYER (Conformed, De-duplicated, Governed State)                 │ │
 │ │ • Fact: silver.ride_trips (SCD Type 1 current state)                    │ │
 │ │ • Fact: silver.trip_state_transitions (SCD Type 2 history audit)        │ │
 │ │ • Partition: day(start_time), province_id                               │ │
 │ │ • Z-Order / Liquid Clustering: (driver_token, passenger_token, h3_res7) │ │
 │ └────────────────────────────────────┬────────────────────────────────────┘ │
 │                                      │ Continuous Aggregations (1-5 min)   │
 │                                      ▼                                      │
 │ ┌─────────────────────────────────────────────────────────────────────────┐ │
 │ │ GOLD LAYER (Business Aggregates & Feature Marts)                        │ │
 │ │ • gold.dispatch_metrics_5m (p50/p95 wait time, surge multiplier)        │ │
 │ │ • gold.driver_hourly_earnings (real-time payout tracking)               │ │
 │ │ • Partition: date, h3_cell_res7                                         │ │
 │ └─────────────────────────────────────────────────────────────────────────┘ │
 └──────────────────────────────────────┬──────────────────────────────────────┘
                                        │
 ┌──────────────────────────────────────┴──────────────────────────────────────┐
 │                     CONTROL PLANE & GOVERNANCE LAYER                        │
 │                                                                             │
 │ • Apache Polaris REST Catalog (Universal Catalog & Access Control)          │
 │ • Data Contract & Schema Registry (Strict schema enforcement on Bronze)     │
 │ • Audit & Lineage Service (OpenLineage + Marquez: Audit every PII read)     │
 │ • Maintenance Orchestrator: Hourly Compaction + Daily Orphan Sweep          │
 └──────────────────────────────────────┬──────────────────────────────────────┘
                                        │
                                        ▼
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                         QUERY & CONSUMPTION LAYER                           │
 │                                                                             │
 │ • Real-time Operations Dashboard (DuckDB / Trino over Gold, p95 < 500ms)   │
 │ • Ad-hoc Analytics & Investigation (Trino / DuckDB over Silver, p95 < 1s)   │
 │ • ML Feature Store (Feast / Delta CDF subscriber for ETA & Pricing models) │
 │ • Compliance Audit API (Decree 13 Right-to-Erasure & Access Logs)           │
 └─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Các Quyết Định Kiến Trúc & Alternatives Đã Loại Bỏ

### Quyết định 1: Định dạng Bảng Lưu trữ (Table Format)
- **Lựa chọn:** **Delta Lake 4.x (với Change Data Feed - CDF và Deletion Vectors)**.
- **Loại bỏ Apache Hudi 1.2:** Mặc dù Hudi hỗ trợ Merge-on-Read (MoR) mạnh cho write-heavy workloads, hệ sinh thái engine của Hudi (đặc biệt khi query từ DuckDB/Polars/Trino native pure-Python) cồng kềnh hơn nhiều, đòi hỏi Java bundle phức tạp và chi phí vận hành catalog cao hơn Delta.
- **Loại bỏ Apache Iceberg 1.6:** Iceberg có REST Catalog xuất sắc, nhưng cơ chế CDC row-level UPSERT thông qua Equality Deletes gây khuếch đại (amplification) I/O lớn khi đọc nếu không chạy compaction liên tục ở tần suất 30s. Delta 4.x với Deletion Vectors và Change Data Feed native cung cấp hiệu năng MERGE và streaming downstream ổn định hơn cho workload CDC 30k writes/s.

### Quyết định 2: Chiến lược Xử lý Dữ liệu Đến Muộn (Late-Arriving & Out-of-Order Events)
- **Lựa chọn:** **Conditional Delta MERGE với biểu thức thời gian `WHEN MATCHED AND src.event_ts > tgt.event_ts THEN UPDATE` kết hợp tách bảng SCD Type 2**.
- **Loại bỏ Native Ingestion Upsert (Blind Overwrite):** Ghi đè mù quáng theo primary key sẽ khiến trạng thái cuốc xe bị thụt lùi khi gói tin mạng đến muộn (ví dụ: trạng thái `PICKING_UP` gửi trễ từ vùng mất sóng sẽ ghi đè lên trạng thái `COMPLETED` đã commit trước đó).
- **Loại bỏ Full Table Rewrite / Micro-partition Replacement:** Chi phí compute và write amplification tăng theo cấp số nhân ($O(N)$ dung lượng partition) khi mỗi micro-batch 10s phải quét và viết lại toàn bộ file Parquet.

### Quyết định 3: Mô hình Bảo vệ Dữ liệu Cá nhân (PII & Tuân thủ Nghị định 13/2023/NĐ-CP)
- **Lựa chọn:** **Dual-Zone In-Flight Tokenization tại cổng Ingestion Bronze (Salted HMAC SHA-256 + Envelope KMS Encryption)**. Dữ liệu nhận diện (số điện thoại, CCCD) được băm thành Token một chiều không thể đảo ngược để phân tích hành vi; dữ liệu cần phục hồi cho hỗ trợ khách hàng được mã hóa đối xứng bằng khóa KMS lưu trong Vault riêng biệt có phân quyền nghiêm ngặt.
- **Loại bỏ View-based Dynamic Masking:** Mặt nạ động trên tầng truy vấn (Dynamic Data Masking) tiềm ẩn lỗ hổng bảo mật: dữ liệu thô nhạy cảm vẫn nằm trần trên storage S3/Parquet; bất kỳ ai có quyền truy cập storage bucket trực tiếp đều có thể trích xuất toàn bộ PII mà không qua engine SQL.
- **Loại bỏ Transparent Database Encryption (TDE):** TDE chỉ bảo vệ dữ liệu at-rest ở cấp độ đĩa/block, hoàn toàn vô hiệu khi dữ liệu được đọc lên bởi query engine hoặc truyền qua pipeline downstream.

### Quyết định 4: Chiến lược Phân vùng và Gom nhóm Dữ liệu (Partitioning & Clustering)
- **Lựa chọn:** **Coarse Partitioning theo `day(start_time)` và `province_id` kết hợp Z-Order / Liquid Clustering trên `(driver_token, passenger_token, h3_res7)`**.
- **Loại bỏ Deep Physical Directory Partitioning (`year/month/day/hour/province/service_type`):** Tạo ra hàng trăm nghìn thư mục vật lý dẫn đến hiện tượng small-file problem nghiêm trọng, làm tê liệt catalog khi listing và planning metadata.
- **Loại bỏ Unpartitioned Flat Table:** Thiếu vắng partition pruning sẽ buộc mọi câu truy vấn phân tích hàng ngày phải quét qua 100% dung lượng bảng, làm tăng chi phí S3 GET và vi phạm SLA p95 < 1s.

### Quyết định 5: Lớp Catalog & Control Plane (Governance & Access Control)
- **Lựa chọn:** **Apache Polaris REST Catalog hỗ trợ Universal Table Registration & Credential Vending**. Catalog đóng vai trò là single control plane duy nhất quản lý xác thực (ABAC), cấp temporary scoped token (S3 IAM credentials) trực tiếp theo từng bảng, ngăn chặn rò rỉ quyền truy cập storage.
- **Loại bỏ AWS Glue Catalog thuần túy:** Bị vendor lock-in vào hệ sinh thái AWS, không hỗ trợ tốt scan planning offload và hạn chế khả năng tương thích đa engine mã nguồn mở (DuckDB, Spark, Trino).
- **Loại bỏ Quản lý theo File Path trực tiếp (Uncataloged Lake):** Mất hoàn toàn khả năng quản trị tập trung, không có audit trail truy cập metadata, và không thể thực thi các chính sách bảo mật dữ liệu cấp cột/dòng.

### Quyết định 6: Quy trình Bảo dưỡng Bảng (Table Maintenance & FinOps Cadence)
- **Lựa chọn:** **2-Tier Coordinated Maintenance: Compaction vi mô định kỳ 30 phút (target size 128 MB) + Daily Z-Order Re-clustering + Orphan Sweep với 24h safety guard + Snapshot Retention 168 giờ (7 ngày)**.
- **Loại bỏ Immediate Vacuum (`retention_hours = 0`):** Phá vỡ ACID snapshot isolation của các long-running reader đang chạy, đồng thời triệt tiêu hoàn toàn khả năng rollback time-travel khi xảy ra sự cố dữ liệu.
- **Loại bỏ Unmanaged Storage (Không chạy maintenance):** Tích tụ hàng triệu file nhỏ từ streaming 10s làm chi phí S3 request tăng vọt gấp 15 lần và khiến truy vấn dashboard bị timeout.

---

## 4. Kịch bản Xử lý Sự cố Lúc 3 Giờ Sáng (Failure Modes & Recovery)

### Failure Mode 1: Bão Dữ Liệu Đến Muộn Làm Lệch Trạng Thái Cuốc Xe (Out-of-Order CDC Flood)
- **Hiện tượng (3:00 AM):** Cột sóng viễn thông tại một tỉnh miền Trung được khôi phục sau sự cố mất điện, đẩy 500.000 sự kiện CDC tích lũy từ 4 giờ trước ồ ạt đổ về Kafka. Dashboard vận hành báo cáo số lượng cuốc xe `IN_PROGRESS` tăng đột biến bất thường do các event cũ ghi đè lên các cuốc xe đã `COMPLETED`.
- **Cơ chế Phát hiện (Detection):** Alert giám sát streaming kích hoạt khi tỷ lệ `src.event_ts < (current_time - 30 minutes)` vượt ngưỡng 5% trong 3 micro-batches liên tiếp.
- **Kế hoạch Rollback & Khắc phục:**
  1. Điều kiện Delta MERGE `WHEN MATCHED AND src.event_ts > tgt.event_ts` tự động loại bỏ các bản ghi cập nhật cũ hơn trạng thái hiện tại trên Silver.
  2. Nếu phát hiện bảng Gold bị tính sai do sliding window aggregation: Thực hiện `RESTORE TABLE silver.ride_trips TO VERSION AS OF <last_good_version>` (tận dụng Time-Travel Day 18).
  3. Kích hoạt Spark backfill job phát lại dữ liệu từ Kafka offset tương ứng với bộ lọc `WHERE event_ts >= trigger_window_start`.

### Failure Mode 2: Spark Executor OOM Crash Để Lại Hàng Nghìn Uncommitted Orphan Files
- **Hiện tượng (3:30 AM):** Cụm Spark streaming gặp sự cố OOM khi xử lý batch đột biến, khiến 12 executor bị giết đột ngột khi đang ghi dở các file Parquet. Các file này nằm trôi nổi trên S3, không được commit vào `_delta_log`, gây lãng phí dung lượng và làm sai lệch thống kê lưu trữ của CFO.
- **Cơ chế Phát hiện (Detection):** Cronjob kiểm toán dung lượng phát hiện độ lệch giữa kích thước thực tế trên S3 (`du(S3)`) và dung lượng ghi nhận trong Delta transaction log (`sum(add_actions.size)`) vượt quá 150 GB.
- **Kế hoạch Rollback & Khắc phục:**
  1. `VACUUM` tiêu chuẩn của Delta không dọn các file chưa từng được commit (do delta-rs/Spark chỉ xóa tombstone).
  2. Kích hoạt script dọn dẹp Orphan chuyên dụng: Quét danh sách file vật lý trên S3 (`s3.rglob("*.parquet")`), trừ đi tập hợp các file đang hoạt động trong phiên bản mới nhất (`dt.file_uris()`).
  3. Áp dụng **Safety Guard 24 giờ**: Chỉ xóa các file uncommitted có thời gian tạo `st_mtime < (now - 24h)` để tuyệt đối không xóa nhầm file của các writer đang ghi dở dang.

### Failure Mode 3: Yêu Cầu Xóa Dữ Liệu Khẩn Cấp (Decree 13 Right-to-Erasure) Bị Rò Rỉ Qua Time-Travel
- **Hiện tượng (4:15 AM):** Nhận được yêu cầu xóa dữ liệu khẩn cấp từ khách hàng theo Nghị định 13. Kỹ sư thực hiện lệnh `DELETE FROM silver.ride_trips WHERE passenger_token = 'token_xyz'`. Tuy nhiên, kiểm toán viên phát hiện dữ liệu của khách hàng vẫn có thể truy vấn được thông qua `DeltaTable(path, version=current_version - 1)`.
- **Cơ chế Phát hiện (Detection):** Job kiểm toán tuân thủ tự động chạy `SELECT count(*) FROM silver.ride_trips VERSION AS OF n WHERE passenger_token = 'token_xyz'` trên tất cả snapshot còn hiệu lực và cảnh báo vi phạm SLA 72 giờ.
- **Kế hoạch Rollback & Khắc phục:**
  1. *Bước 1 (Soft Delete & CDF Broadcast):* Thực hiện `DELETE` trên bảng Silver $\to$ Delta Change Data Feed phát ra sự kiện `_change_type = 'delete'` để đồng bộ xóa lập tức trên các downstream caches / Feature Store.
  2. *Bước 2 (Staged Quarantine):* Ghi nhận ID yêu cầu vào bảng `audit.erasure_requests`.
  3. *Bước 3 (Physical Erasure & Log Rewriting):* Thiết lập chính sách retention ngắn hạn cho partition chứa bản ghi: chạy `VACUUM silver.ride_trips RETAIN 72 HOURS` sau khi hết thời hạn khiếu nại quy định, đảm bảo mọi Parquet file chứa dữ liệu cũ bị tiêu hủy vật lý hoàn toàn khỏi S3.

---

## 5. Ước Lượng Chi Phí Back-of-Envelope (FinOps Math)

### A. Giả định Dữ liệu & Quy mô Lưu trữ
- **Số cuốc xe:** 100.000.000 chuyến/năm $\approx$ 274.000 chuyến/ngày.
- **Dung lượng raw CDC (GPS pings + state updates):** Trung bình 50 sự kiện/chuyến $\times$ 1 KB/event $\approx$ 50 KB/chuyến.
- **Lượng dữ liệu phát sinh hàng ngày:** $274.000 \times 50\text{ KB} \approx 13,7\text{ GB/ngày raw}$.
- **Dữ liệu 1 năm (Silver nén Snappy Parquet tỷ lệ 4:1):** $\approx 1,25\text{ TB/năm}$.
- **Dữ liệu tổng cộng 3 năm (kể cả Bronze CDC log & Gold):** $\approx 10\text{ TB}$.

### B. Chi phí Lưu trữ S3 (Storage Costs)
- **S3 Standard (Hot tier - 90 ngày gần nhất $\approx 2\text{ TB}$):**  
  $$2\text{ TB} \times \$0,023/\text{GB-tháng} \times 1.024\text{ GB/TB} = \$47,10/\text{tháng}$$
- **S3 Standard-Infrequent Access (Warm tier - 90 đến 365 ngày $\approx 3\text{ TB}$):**  
  $$3\text{ TB} \times \$0,0125/\text{GB-tháng} \times 1.024\text{ GB/TB} = \$38,40/\text{tháng}$$
- **S3 Glacier Flexible (Cold tier - trên 1 năm $\approx 5\text{ TB}$):**  
  $$5\text{ TB} \times \$0,0036/\text{GB-tháng} \times 1.024\text{ GB/TB} = \$18,43/\text{tháng}$$
- **Tổng chi phí Storage:** $\approx \mathbf{\$103,93/\text{tháng}}$.

### C. Chi phí Compute (Ingestion, Compaction & Querying)
- **Streaming Ingestion & Tokenization (EKS / ECS Fargate):**  
  2 workers $\times$ 2 vCPU $\times$ 8 GB RAM chạy 24/7 $\approx \$0,08/\text{giờ} \times 730\text{ giờ} \times 2 = \$116,80/\text{tháng}$.
- **Scheduled Maintenance & Compaction Jobs:**  
  Chạy 15 phút mỗi giờ trên Spot instances (4 vCPU, 16 GB RAM) $\approx \$0,04/\text{giờ} \times (0,25 \times 730) \approx \$7,30/\text{tháng}$.
- **Interactive Query Engine (Serverless Trino / DuckDB on Demand):**  
  Trung bình 5.000 queries/ngày, quét trung bình 100 MB/query (nhờ Z-Order & Partition pruning) $\to 500\text{ GB/ngày} = 15\text{ TB/tháng}$ scan.  
  $$15\text{ TB} \times \$5,00/\text{TB scan} = \$75,00/\text{tháng}$$.
- **Tổng chi phí Compute:** $\approx \mathbf{\$199,10/\text{tháng}}$.

### D. Chi phí API Requests & Network
- S3 PUT/POST (Streaming micro-batches + compaction): $\approx 3.000.000\text{ requests/tháng} \times \$0,005/1.000 = \$15,00/\text{tháng}$.
- S3 GET (Queries & Dashboards): $\approx 10.000.000\text{ requests/tháng} \times \$0,0004/1.000 = \$4,00/\text{tháng}$.
- **Tổng chi phí Request/Network:** $\approx \mathbf{\$19,00/\text{tháng}}$.

### E. Tổng Hóa Đơn Hàng Tháng (Total Monthly TCO)
$$\text{TCO Tổng} = \$103,93 + \$199,10 + \$19,00 = \mathbf{\$322,03/\text{tháng}}$$
*Mức chi phí cực kỳ tối ưu cho quy mô 100 triệu giao dịch/năm, thấp hơn 90% so với việc lưu trữ toàn bộ trên Cloud Data Warehouse truyền thống.*

---

## 6. Lộ Trình Triển Khai MVP Trong 1 Tuần (1-Week MVP Build Slice)

Để chứng minh tính khả thi của kiến trúc trước Senior Review, team triển khai lát cắt mỏng nhất (thinnest end-to-end slice) trong 5 ngày làm việc:

| Ngày | Hạng mục Công việc | Deliverable Đo lường Được |
|---|---|---|
| **Day 1** | Dựng Kafka topic + CDC Simulator & Module HMAC Tokenization PII | Sinh luồng giả lập 10.000 events/phút; kiểm chứng số điện thoại/CCCD bị ẩn danh 100% trước khi chạm Bronze. |
| **Day 2** | Xây dựng pipeline Bronze $\to$ Silver bằng Delta Ingestion Engine | Script Delta MERGE với điều kiện `src.ts > tgt.ts`; kiểm tra độ trễ ghi < 30s. |
| **Day 3** | Tái hiện dữ liệu đến muộn (Late-Arriving Data) & Kiểm chứng Idempotency | Bắn 1.000 events trễ 2 giờ; chứng minh trạng thái trên Silver không bị thoái lui. |
| **Day 4** | Xây dựng Gold Aggregations & Tích hợp DuckDB SQL Dashboard | Bảng Gold tính toán p50/p95 latency theo khu vực địa lý H3; test latency query ad-hoc p95 < 200ms. |
| **Day 5** | Triển khai PoC Right-to-Erasure (Nghị định 13) & Table Maintenance | Demo luồng xóa khách hàng: Delta DELETE $\to$ CDF verification $\to$ Audit log $\to$ Compaction. |

---

## 7. Kết Luận

Kiến trúc Lakehouse này giải quyết triệt để bài toán giao thoa giữa **hiệu năng xử lý real-time quy mô lớn** và **tuân thủ pháp lý nghiêm ngặt của Việt Nam (Nghị định 13/2023/NĐ-CP)**. Bằng cách áp dụng các nguyên lý cốt lõi của Lakehouse hiện đại (Delta Change Data Feed, Conditional Time-based MERGE, In-Flight Tokenization, và Coordinated Table Maintenance), hệ thống đảm bảo vận hành ổn định, sẵn sàng mở rộng gấp 10 lần với chi phí dưới $350/tháng.
