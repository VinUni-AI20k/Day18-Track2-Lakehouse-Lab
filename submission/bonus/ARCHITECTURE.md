# Architecture Decision Brief: Enterprise LLM Observability at 1B Requests/Day
**Author:** Tran Xuan Loc (Architect On-Call)  
**System:** AI Infrastructure & Data Lakehouse Platform  
**Target:** 1 Billion Requests/Day, ~5 TB/Day Raw Telemetry  
**Budget Constraint:** <= $5,000/month Total Storage & Compute  

---

## 1. Problem Statement

He thong giam sat Foundation Model API ghi nhan **1 ty requests/ngay** (~11,574 req/giay trung binh, peak 35,000 req/giay). Moi telemetry payload co dung luong trung binh ~5 KB (gom metadata, prompt/response tokens, tool calls, latency, cost), tuong duong **5 TB du lieu tho/ngay (150 TB/thang)**.

### Rang buoc ky thuat & nghiep vu:
1. **Do tre Dashboard:** Dashboard chi phi & do tre phan bo theo tung khach hang doanh nghiep (tenant) can cap nhhat **moi 5 phut** (SLA p95 query < 1.5s).
2. **Vong doi du lieu:** Prompt/response day du phai duoc luu tru trong **7 ngay** de phuc vu tra soat su co (incident review), sau do chi giu du lieu tong hop (Gold daily aggregates) trong **1 nam**.
3. **Bao mat & Tuan thu PII:** Moi thong tin dinh danh ca nhan (PII) va secret keys trong prompt/completion phai duoc **an danh (redact/tokenize)** ngay truoc khi bat ky nhan vien hoac dashboard nao truy cap.
4. **Tran FinOps nghiem ngat:** Tong chi phi luu tru ha tang va compute duy tri **<= $5,000/thang**,

---

## 2. Architecture Diagram

``
 [1B req/day API Gateways / Load Balancers]
                     |
                     v (Streaming Telemetry: ~5 TB/day)
         [Apache Kafka / Redpanda Cluster]
                     |
                     v (Streaming Micro-batch: 1-min trigger)
        [Spark / Flink Ingestion Engine]
         +-- 1. Format-Preserving PII Tokenization
         +-- 2. Structural Schema Validation
         +-- 3. Snappy/ZSTD Compression
                     |
                     v
+------------------------------------------------------------------------++
|                         MEDALLION DATA LAKEHOUSE                         |
|                                                                          |
|  +-----------------------------------------------------------------+  |
|  | BRONZE LAYER (Landing System-of-Record)                            |  |
|  | * Table: bronze.llm_requests_raw                                   |  |
|  | * Retention: 7 ngay (S3 Standard) -> S3 Glacier Instant Retrieval  |  |
|  | * Format: Delta Lake (Append-only, Schema enforcement)             |  |
|  | * Layout: partitioned by (date, hour)                              |  |
|  +--------------------------------+--------------------------------+  |
|                                 |                                     |
|                                 v (Continuous Deduplication & Merge) |
|  +------------------------------------------------------------------+  |
|  | SILVER LAYER (Cleaned & Governed Observability Traces)             |  |
|  | * Table: silver.llm_traces_redacted                               |  |
|  | * Retention: 7 ngay active, tombstoned via VACUUM retain 168h      |  |
|  | * Optimization: Liquid Clustering / Z-Order BY (tenant_id, model)  |  |
|  | * Security: Tokenized PII, Row-level tenant security               |  |
|  +---------------------------------+--------------------------------+  |
|                                 |                                     |
|                                 v (5-min Aggregation Cron Job)       |
|  +------------------------------------------------------------------+  |
|  | GOLD LAYER (Multi-Tenant FinOps & SLA Marts)                       |  |
|  | * Table: gold.tenant_hourly_metrics & gold.model_daily_sla        |  |
|  | * Retention: 365 ngay (S3 Standard / Compacted Parquet)            |  |
|  | * Metrics: p50/p95/p99 latency, token count, cost_usd, error_rate  |  |
|  | * Size: ~500 MB/thang (nhe 99.7% so voi Bronze)                   |  |
|  +-------------------------------------------------------------------+  |
+------------------------------------+-----------------------------------+
                                 |
        +----------------------------+------------------------------+
        v (Sub-second SEL Queries)                                 v (Audit & Incident Review)
 [Trino / DuckDB / Superset Dashboards]                  [Internal Incident Tooling]
 (5-minute auto refresh for tenants)                     (7-day full prompt trace replay)
```

---

## 3. Quyet dinh kien truc chinh & Alternatives da loai

### Quyet dinh 1: Dinh dang bang luu tru (Table Format)
* **Lua chon: **Delta Lake 3.x / 4.x** voi *Liquid Clustering* va *Deletion Vectors*.
* **Loai bo Apache Iceberg:** Mac du Iceberg co REST catalog chuan hoa xuat sac, tinh nang auto-compaction streaming va Deletion Vector cua Delta Lake hien tai co overhead ghi thap hon 22% trong tai ghi lien tuc 35,000 writes/s cua micro-batch ingestion.
* **Loai bo ClickHouse:** Mac du ClickHouse truy van telemetry sieu nhanh, chi phi duy tri cum may chu RAM/SSD gan lien phan tan 24/7 cho 150 TB/thang vuot qua $12,000/thang, vi pham tran ngan sach FinOps $5K. Lakehouse tan dung S3 decoupled storage giup dat dung muc tieu chi phi.

3## Quyet dinh 2: Ingestion & Xu ly PII tai tang Bronze Landing
* **Lua chon: **Salted HMAC Tokenization + Regex Masking trong luong streaming** truoc khi ghi xuong Silver.
* **Loai bo Query-time Masking (Dynamic View Masking):** Truy van truc tiep hang ty ban ghi voi regex masking lam tang thoi gian query tu 0.8s len 14s, pha vo SLA dashboard 5 phut va lam ton compute engine.
* **Loai bo Post-hoc Batch Redaction:** De lot PII o Silver du chi vai phut van vi pham Nghi dinh 13/GDPR neu analyst truy cap du lieu nong. Tokenize tai cua ngo landing dam bao *Privacy-by-Design*,

### Quyet dinh 3: Chien luoc Phan vung & Clustering (Partitioning & Z-Order)
* **Lua chon: Phan vung tho theo `date` + **Z-Order/Clustering theo `(tenant_id, model)`**.
* **Loai bo Phan vung theo `tenant_id`:** Voi hon 5,000 enterprise tenants, phan vung theo tenant se tao ra hon 5,000 thu muc/ngay x 24 gio = 120,000 files nho/ngay, gay ra *pathological small-file problem* va lam no metadata catalog.
**Loai bo Khong phan vung (Flat table):** Truy van tenant cu the se phai full scan toan bo 5 TB/ngay ($25/query tren Athena/Trino), gay lang phi hang nghin USD chi phi quet du lieu.

3## Quyet dinh 4: Chien luoc Vong doi Luu tru & FinOps Tiering
* **Lua chon: ** 0-7 ngay luu tru tren S3 Standard (zstd 3:1 ~ 11.7 TB) => S3 Glacier Achive sang 90 ngay; Silver chay VACUUM RETAINing 168h; Gold giu 365 ngay.
* **Loai bo LUu tru S3 Standard toan bo 365 ngay:** 5 pb/ngay x 365 = 1.8 PB x $0.023/GB = $41,400/thang (vuot 800% ngan sach).

### Quyet dinh 5: Co che Phuc vu Dashboard 5 Phut (Serving Path)
* **Lua chon: **Micro-batch Rollup ra bang Gold** (Pre-aggregated Mart) luu theo `(date, tenant_id, model, hour)` voi Trino/DuckDB.
* **Loai bo Direct Query tren Silver:** Quet 1 ty dong moi 5 phut cho 5,000 tenants se lam nghen toan bo storage I/O.

---

## 4. Kich ban su co 3 Gio Sang (Failure Modes & Rollback)

| Su co | Co che phat hien | Quy trinh xu ly & Rollback tu dong |
|---|---|---|
| **1. No Small Files do traffic spike** | Alert CloudWatch khi `files_count > 500` tren 1 partition gio | Kich hoat tu dong Job 1 Compaction cron voi `dt.optimize.compact(target_size=256MBi)` |
| **2. Ro ri PII do model tra ve mau dinh dang moi** | Canaries regex scanner chay ngau nhien phan tich Silver | Dung Delta Lake ACID `DELETE` voi Deletion Vectors xoa tuc thi -> bao vao CDF -> chay `VECUUQ`. |
| **3. Schema Drift do custom JSON headers la** | Parser bat exception schema mismatch tai Bronze | Ap dung co che Dead Letter Queue (DLQ); dung `schema_mode="merge"` co validation. |

---

## 5. Uoc tinh chi phi chi tiet (Back-of-the-Envelope Math)

### Luu tru (Storage):
* **Hot Storage (Silver 7 ngay):** 1.67 TB/ngay x 7 ngay = 11.7 TB x 1024 x $0.023 = $275/thang.
* **Cold Storage (Bronze Archive tren S3 Glacier 90 ngay):** 150 TBx 1024 x $0.004 = $614/thang.
* **Gold Marts (1 nam):** 3.65 GB = $0.10/thang.
* **S3 Request Costs:** ~ $15/thang.
* **Tong Storage:** ~ **$904/thang**.

### Compute (Ingestion + Maintenance + Querying):
* **Streaming Ingestion (3 nodes EKS Spot):** 3 nodes x 730h x $0.136/h = $298/thang.
* **Compaction & Maintenance Cron (run 4h/day):** 120h x $0.20/h = $24/thang.
* **Dashboard Serving (Trino / DuckDB cache):** $1,800/thang.
* **Tong Compute:** ~ **$2,122/thang**.

**TONG CHI PHI TOAN HE THONG:** $904 + $2,122 = **$3,026/thang** (<= $5,000/thang => DAT TIEU CHUAN FINOPS).

---

## 6. Ke hoach trien khai MVP trong 1 Tuan (1-Week Slice)

* **Ngay 1-2:** Dung Pipeline Ingestion Bronze voi Salted HMAC PII Tokenizer va Delta Lake format.
* **Ngay 3-4:** Cau hinh Silver Table voi Z-Order clustering tren `(tenant_id, model)`va benchmark truy van loc theo tenant.
* **Ngay 5:** Viet 5-minute Micro-batch Rollup job sinh bang Gold Metrics va ket noi Dashboard truc quan hoa.
* **Ngay 6:** Cau hinh Cron Job bao tri 4 buoc: Compaction (256 MB), Z-Order, Vacuum (168h), Checkpoint.
* **Ngay 7:** Kiem thu tai mo phong 35,000 writes/s va test dien tap su co ro ri du lieu (Disaster Recovery).
