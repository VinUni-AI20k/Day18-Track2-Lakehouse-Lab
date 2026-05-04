# Architecture Decision Record  
## Topic F — Catalog Migration: Databricks Unity Catalog → Apache Polaris  
### Zero-Downtime, 500 Tables, 20 Teams, 6 Tuần

**Họ và tên:** Dương Quang Đông — 2A202600445  
**Ngày:** 2026-05-04  
**Track:** Lakehouse Lab — Day 18 Bonus Challenge

---

## 1. Problem Statement

Team nhận nhiệm vụ thoát vendor lock-in khỏi Databricks Unity Catalog. Bối cảnh:

- **500 Delta tables** tổng cộng, trải dài từ raw ingest đến Gold aggregates
- **20 teams** phụ thuộc, query qua **4 engines khác nhau**: Spark, Trino, DuckDB, Snowflake
- **Yêu cầu cứng:** zero query downtime trong suốt migration — không được phép dashboard bị gián đoạn
- **Time-travel semantics phải được preserve:** các audit query cần rewind về snapshot cũ vẫn phải hoạt động sau migration
- **Deadline: 6 tuần** (hard — leadership đã thông báo cho tất cả stakeholders)
- **Đích đến:** Apache Polaris (REST Catalog, Iceberg-native, Apache incubating), vendor-neutral hoàn toàn

Tại sao khó: Delta Lake và Iceberg dùng hai transaction log hoàn toàn khác nhau (`_delta_log/` JSON commits vs Iceberg `metadata.json → manifest list → manifests`). Không có công cụ nào convert hai log này hoàn hảo trong thời gian thực — phải có một dual-read window, và window đó tạo ra surface area cho divergence giữa hai systems.

---

## 2. Architecture Diagram

```
╔══════════════════════════════════════════════════════════════════════╗
║                   PHASE 1 — DUAL-READ WINDOW (Tuần 1–4)              ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  [Delta tables in S3/ADLS]                                           ║
║       │                                                              ║
║       ├──[UniForm layer]──────────────────────────────────────────►  ║
║       │   Delta 3.0+ exposes Iceberg REST endpoint natively          ║
║       │   Iceberg clients đọc được mà không cần copy data            ║
║       │                                                              ║
║       └──[Apache XTable sync daemon]────────────────────────────►    ║
║           Đọc _delta_log/ → generate Iceberg metadata.json           ║
║           Push snapshot history → Apache Polaris Catalog             ║
║                                                                      ║
║  ┌─────────────────────────────────────────────────────────┐         ║
║  │              APACHE POLARIS CATALOG (EKS)               │         ║
║  │  - 500 tables registered (Iceberg format)               │         ║
║  │  - Full snapshot history (time travel preserved)         │        ║
║  │  - RBAC: per-team namespace isolation                   │         ║
║  └────────────────────┬────────────────────────────────────┘         ║
║                       │                                              ║
║          ┌────────────┼────────────┬────────────┐                    ║
║          ▼            ▼            ▼            ▼                    ║
║       [Spark]      [Trino]     [DuckDB]  [Snowflake]                 ║
║                                                                      ║
╠══════════════════════════════════════════════════════════════════════╣
║                    PHASE 2 — CUTOVER (Tuần 5–6)                      ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  1. Validate: mỗi table — checksum data + snapshot IDs khớp          ║
║  2. Flip catalog pointer: DNS/connection string → Polaris            ║
║  3. Disable Unity Catalog write access (read-only 2 tuần buffer)     ║
║  4. XTable daemon tắt sau khi xác nhận zero divergence               ║
║                                                                      ║
║  INGESTION PATH (sau cutover):                                       ║
║  [Producers: Spark/Flink] → write Iceberg native → Polaris           ║
║                                                                      ║
║  QUERY PATH (sau cutover):                                           ║
║  [4 engines] → Polaris REST Catalog → Iceberg metadata → S3 data     ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝

SCHEMA VALIDATION MONITOR (chạy liên tục trong migration window):
  every 5 min: compare Delta schema vs Iceberg schema → alert on drift
  every 30 sec: synthetic probe query → Polaris → alert on downtime
  every 10 min: XTable sync lag check → alert if lag > 10 min
```

---

## 3. Quyết Định Kiến Trúc Chính

### Decision 1 — Table Format Đích: **Iceberg** (không phải Hudi, không phải giữ Delta)

**Chọn: Apache Iceberg**

**Loại Hudi:** Hudi dùng Merge-On-Read (MOR) model — write nhanh hơn nhưng read phải merge log files tại query time, latency cao hơn với 4 engines đa dạng. Quan trọng hơn, Hudi thiếu Hidden Partitioning — khi migrate 500 tables từ Delta partition layout sang Hudi, phải rewrite partition scheme cho từng table, rủi ro data corruption và tốn thêm ít nhất 3 tuần ngoài timeline 6 tuần. Branching/Tagging: Hudi không hỗ trợ (×) — mất tính năng mà Nessie trên Iceberg cung cấp.

**Loại "giữ Delta + vendor lock-in chấp nhận":** Mục tiêu của leadership là thoát Databricks billing dependency. Delta với UniForm vẫn cần Delta transaction log làm source of truth — Databricks Unity Catalog vẫn là control plane. Không đạt mục tiêu.

**Loại "Delta + open-source Delta Standalone":** Delta Standalone không hỗ trợ đầy đủ Snowflake native read; phải dùng Snowflake External Tables với manual refresh — không đáp ứng yêu cầu real-time query từ 20 teams.

**Lý do chọn Iceberg:** Multi-engine native (default, không cần adapter), Hidden Partitioning cho phép query optimizer tự chọn partition strategy, Nessie branching cho staging environment, và quan trọng nhất — Apache Polaris (catalog đích) được thiết kế native cho Iceberg REST Catalog spec.

---

### Decision 2 — Migration Toolchain: **Apache XTable** (không phải manual conversion, không phải Iceberg `add_files`)

**Chọn: Apache XTable** (formerly OneTable, donated by dbt Labs/Onehouse, Apache incubating)

**Loại manual conversion (viết script tự parse `_delta_log/`):** 500 tables × avg 50 snapshots/table = 25,000 snapshot objects cần được reconcile. Manual script không thể đảm bảo correctness với edge cases: Delta checkpoint files (`.checkpoint.parquet`) phải được đọc đúng thứ tự, tombstone records cho deleted files phải được map sang Iceberg equality deletes. Một lỗi nhỏ trong parsing logic làm hỏng time-travel semantics — không recoverable without full re-migration.

**Loại Iceberg `add_files` stored procedure (Spark):** Procedure này chỉ register data files hiện tại vào một Iceberg snapshot mới — không preserve lịch sử. Một table có 200 commits Delta history sẽ chỉ còn 1 Iceberg snapshot. Vi phạm yêu cầu time-travel semantics.

**Loại Delta-to-Iceberg converter trong Databricks (nếu có):** Vendor tool, tạo dependency mới vào Databricks runtime — đi ngược mục tiêu.

**XTable được chọn vì:** Convert toàn bộ snapshot history, maintain deletion vector semantics, support incremental sync (chạy daemon theo dõi Delta log và push Iceberg metadata khi có commit mới). Python API dễ integrate vào pipeline.

---

### Decision 3 — Dual-Read Strategy: **UniForm** (không phải dual-write, không phải hard cutover)

**Chọn: Delta UniForm** (Delta 3.0+, Databricks 12.2+)

**Loại dual-write (ghi đồng thời vào cả Delta và Iceberg):** Write latency tăng gấp đôi cho tất cả producers. Nguy hiểm hơn: nếu Delta write thành công nhưng Iceberg write fail (network timeout, catalog downtime), hai systems diverge — không có atomic cross-format transaction. Rollback phức tạp, có thể mất data.

**Loại hard cutover (cắt Delta, switch toàn bộ sang Iceberg trong một đêm):** 500 tables × validation time = không thể complete trong một maintenance window. Nếu validation phát hiện bug sau khi flip, rollback đồng nghĩa với downtime kéo dài. "Zero downtime" requirement bị vi phạm.

**UniForm được chọn vì:** Delta tables expose Iceberg REST endpoint natively, không copy data, không tăng write latency. Iceberg clients (Trino, DuckDB, Snowflake) đọc được ngay từ tuần 1 trong khi XTable sync hoàn thiện metadata history ở background. Delta vẫn là source of truth trong migration window — rollback chỉ cần tắt Polaris pointer.

---

### Decision 4 — Catalog Đích: **Apache Polaris** (không phải Project Nessie standalone, không phải AWS Glue)

**Chọn: Apache Polaris** (Snowflake donated, Apache incubating 2024)

**Loại Project Nessie standalone:** Nessie cung cấp branching/tagging tốt (Git-like) nhưng thiếu enterprise-grade RBAC out-of-the-box. 20 teams cần namespace isolation với per-team read/write permissions, audit log cho compliance. Nessie cần significant custom middleware để đạt được điều này. Ngoài ra Nessie là catalog-only — không có built-in storage credential vending (Polaris có, theo Iceberg REST Catalog spec v2).

**Loại AWS Glue Catalog:** Vendor lock-in mới (AWS). Glue Catalog không hỗ trợ đầy đủ Iceberg REST Catalog spec — Snowflake và DuckDB cần REST Catalog endpoint chuẩn. Glue dùng proprietary API, cần adapter layer cho từng engine.

**Loại Hive Metastore (HMS):** Legacy, không hỗ trợ Iceberg natively, maintenance burden cao. Trino cần HMS bridge. DuckDB không có HMS connector.

**Polaris được chọn vì:** REST Catalog spec native, RBAC per-namespace, credential vending cho direct S3 access (engines không cần Databricks credential), active Apache community, và Snowflake đã integrate Polaris native — giải quyết Snowflake engine requirement mà không cần External Tables.

---

### Decision 5 — Migration Order: **Criticality-based batching** (không phải alphabetical, không phải reverse-dependency)

**Chọn: Migrate theo tier, Gold trước → Silver → Bronze**

**Logic:** Gold tables là những gì dashboard đang đọc. Nếu Gold migrate thành công và stable, 20 teams có thể chuyển query engine sang Polaris ngay — giảm dual-read window cost. Bronze và Silver ít ai query trực tiếp, có thể migrate sau mà không ảnh hưởng SLA.

**Loại alphabetical:** Không minimize blast radius. Table `aaa_raw_events` (Bronze) migrate trước `zzz_revenue_dashboard` (Gold) — wrong priority.

**Loại reverse-dependency (downstream tables trước):** Đúng về mặt lý thuyết nhưng cần build dependency graph đầy đủ cho 500 tables × 20 teams — ít nhất 2 tuần chỉ để map dependencies. Không fit 6-week timeline.

**Loại "all at once":** 500 tables migrate parallel → nếu một batch fail, không biết table nào bị ảnh hưởng, rollback toàn bộ là thảm họa.

**Batching plan:** 10 Gold tables (tuần 1 — pilot), 40 Gold tables còn lại (tuần 2), 150 Silver tables (tuần 3–4), 300 Bronze tables (tuần 5), cutover + validation (tuần 6).

---

### Decision 6 — Partitioning Post-Migration: **Giữ nguyên layout Delta, refactor sau** (không phải re-partition khi migrate)

**Chọn: Preserve existing partition layout, migrate as-is**

**Loại re-partition khi migrate (dùng Iceberg Hidden Partitioning ngay):** Iceberg Hidden Partitioning cho phép thay đổi partition strategy mà không phải rewrite data — nhưng chỉ cho *new data*. Để backfill existing 500 tables với new partition scheme, phải rewrite toàn bộ data files. Với 50 TB data, rewrite cost ~$200 một lần (S3 PUT) + 2–3 tuần compute. Ngoài timeline.

**Loại "không đổi gì cả vĩnh viễn":** Lãng phí lợi ích của Iceberg Hidden Partitioning. Sau migration stable (tuần 8+), nên schedule partition optimization.

**Quyết định này defer intentionally:** Zero-downtime migration là mục tiêu tuần 1–6. Partition optimization là optimization task, không phải migration blocker.

---

## 4. Failure Modes (Kịch Bản 3 Giờ Sáng)

### Failure 1 — XTable Sync Lag → Stale Iceberg Metadata  
*(Day 18 concept: Time Travel)*

**Kịch bản:** Producer Spark job commit 10,000 rows vào Delta table lúc 2:47 AM. XTable daemon bị stuck do S3 throttling — Iceberg metadata trên Polaris chưa được update. Trino query của team analytics lúc 3:02 AM không thấy data mới → báo cáo revenue thiếu 10,000 records.

**Detection:**
```python
# Synthetic validator chạy mỗi 5 phút
delta_last_commit = delta_log.get_last_commit_timestamp(table)
polaris_last_snapshot = polaris_catalog.get_latest_snapshot(table).committed_at
lag_minutes = (delta_last_commit - polaris_last_snapshot).total_seconds() / 60

if lag_minutes > 10:
    pagerduty.alert(f"XTable lag {lag_minutes:.1f}min on {table}")
```

**Rollback:** Trino/DuckDB/Snowflake clients tạm redirect về UniForm endpoint (Delta vẫn serve Iceberg REST, sync lag không ảnh hưởng). Page on-call engineer. XTable daemon force-sync: `xtable sync --table <name> --force --from-snapshot <delta_version>`. Sau khi sync complete, validate bằng `SELECT COUNT(*) FROM iceberg_table` vs `SELECT COUNT(*) FROM delta.table` — phải khớp.

---

### Failure 2 — Schema Evolution Conflict: Delta Type Promotion vs Iceberg Type Strictness  
*(Day 18 concept: Schema Evolution)*

**Kịch bản:** Team A ALTER TABLE thêm column `retry_count TINYINT` vào Delta table lúc 3:15 AM. XTable sync generate Iceberg schema với `retry_count: int` (Iceberg không có TINYINT — auto-promote). Snowflake đọc Iceberg schema thấy `int`, nhưng team B có pipeline đang CAST `retry_count` sang TINYINT range validation → throw overflow error vì Iceberg returns int.

**Detection:**
```sql
-- Schema drift monitor, chạy mỗi 5 phút
SELECT 
    table_name,
    column_name,
    delta_type,
    iceberg_type
FROM schema_comparison_view
WHERE delta_type != iceberg_type
    OR (delta_type IS NULL AND iceberg_type IS NOT NULL)
    OR (delta_type IS NOT NULL AND iceberg_type IS NULL);
-- Alert nếu có row nào
```

**Rollback:** 
1. Immediate: block write đến table bằng Polaris table-level lock
2. Revert Delta schema: `RESTORE TABLE delta.table TO VERSION AS OF <N-1>` — Delta time travel cho phép rollback schema change
3. XTable re-sync sau khi Delta schema revert
4. Coordinate với Team A để apply schema change đúng cách (Iceberg-compatible type)

---

### Failure 3 — Apache Polaris Catalog Downtime → Tất Cả Iceberg Queries Fail  
*(Day 18 concept: ACID + Catalog availability)*

**Kịch bản:** EKS node running Polaris bị OOM kill lúc 3:30 AM. Kubernetes restart pod nhưng startup time 45 giây. Trong 45 giây đó, 4 engines nhận `catalog unavailable` → 20 teams báo cáo dashboard trắng.

**Detection:**
```bash
# Synthetic probe chạy mỗi 30 giây
curl -f -s --max-time 5 \
  "https://polaris.internal/catalog/v1/namespaces" \
  -H "Authorization: Bearer $TOKEN" \
  || pagerduty_alert "Polaris catalog DOWN"
```

**Rollback (trong 45 giây, tự động):**
```yaml
# Kubernetes HPA config — đã được pre-configured
spec:
  replicas: 3  # 3 pods, anti-affinity across AZs
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 0  # zero-downtime pod replacement
```
Nếu outage kéo dài > 2 phút: Traffic failover về UniForm endpoint (Delta còn sống, serve Iceberg REST). Config này được pre-tested trong tuần 1 pilot. On-call engineer không cần làm gì manual — automation xử lý trong SLA.

---

## 5. Ước Lượng Chi Phí (Back-of-Envelope)

### Storage
```
Data size: 500 tables × avg 100 GB = 50 TB
S3 Standard: $0.023/GB-tháng × 50,000 GB = $1,150/tháng
(Không đổi so với Delta — Iceberg dùng cùng Parquet files)

Iceberg metadata overhead:
  metadata.json + manifest lists: ~1 MB/table × 500 = 500 MB
  manifests: ~10 MB/table × 500 = 5 GB
  Total metadata: ~5.5 GB → $0.127/tháng ≈ $0 (rounding)

XTable sync temp files: ~2% data size = 1 TB → $23/tháng (tắt sau 6 tuần)
```

### Compute (Migration Phase — 6 tuần)
```
XTable sync jobs (one-time per table):
  500 tables × avg 10 min = 5,000 min = 83 CPU-hours
  EC2 c5.2xlarge ($0.34/hr) × 83 hr = $28 one-time

XTable daemon (ongoing sync trong migration window):
  1× EC2 c5.large ($0.085/hr) × 24hr × 42 days = $86 total

Schema validation monitor:
  Lambda 5-min interval: 12/hr × 24hr × 42 days × $0.0000002/ms × 500ms = $0.06 (negligible)

Validation jobs (pre-cutover):
  500 tables × COUNT(*) compare: ~2 hr Spark cluster = $15 one-time
```

### Apache Polaris Hosting (Post-Migration, Ongoing)
```
EKS: 3× m5.xlarge ($0.192/hr) = $0.576/hr × 720 hr/tháng = $415/tháng
  (3 replicas for HA, anti-affinity across AZs)

ALB + NAT Gateway: ~$50/tháng

Total Polaris infra: ~$465/tháng
```

### Summary
```
Migration one-time cost:  $28 + $86 + $15 = ~$130
Migration window (6 tuần): $465/tháng × 1.5 tháng = ~$700
Post-migration ongoing:   $1,150 (S3) + $465 (Polaris) = $1,615/tháng

vs Databricks Unity Catalog:
  Rough estimate: $2,000–$5,000/tháng tùy compute DBU
  ROI breakeven: tháng đầu tiên sau migration
```

---

## 6. MVP — Slice Một Tuần

**Goal:** Chứng minh một Delta table được đọc đồng thời bởi cả 4 engines qua Polaris catalog, với time-travel semantics intact.

**Không include:** Governance migration, RBAC full setup, 499 tables còn lại.

**Day 1–2:**
- Chọn 1 Gold table nhỏ nhất (~5 GB, <20 snapshots, tên: `gold.revenue_daily`)
- Enable UniForm: `ALTER TABLE gold.revenue_daily SET TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')`
- Verify Iceberg endpoint hoạt động: `SELECT * FROM iceberg.revenue_daily LIMIT 10` qua Trino

**Day 3:**
- Deploy XTable daemon (Docker container, chạy local hoặc EC2 t3.medium)
- Run initial sync: `xtable sync --sourceFormat DELTA --targetFormats ICEBERG --tableBasePath s3://bucket/gold/revenue_daily/`
- Register table vào Polaris catalog qua REST API

**Day 4:**
- Verify time travel: `SELECT COUNT(*) FROM revenue_daily FOR VERSION AS OF 5` trên cả Spark và Trino — row counts phải khớp nhau
- Verify 4 engines: Spark ✓, Trino ✓, DuckDB ✓, Snowflake (External Iceberg Table) ✓

**Day 5:**
- Deploy schema drift monitor (Lambda hoặc cron job)
- Document latency: ghi nhận XTable sync lag từ Delta commit → Polaris visible (target <30 giây)
- Write go/no-go checklist cho 499 tables còn lại

**Success criteria:**
- [x] `SELECT COUNT(*)` trả cùng số từ cả 4 engines
- [x] Time travel query về version N-5 trả đúng kết quả trên Spark và Trino
- [x] XTable sync lag < 30 giây sau Delta commit
- [x] Schema drift monitor gửi được test alert

---

## Appendix — Mapping Day 18 Concepts

| Concept | Ứng dụng trong design này |
|---|---|
| **Medallion layout** | Migration order: Gold → Silver → Bronze (criticality tiers) |
| **ACID Transactions** | UniForm đảm bảo Iceberg clients thấy atomic Delta commits; không có dirty reads trong dual-read window |
| **Time Travel** | XTable preserve toàn bộ snapshot history; Failure Mode 1 + 2 tie trực tiếp vào time-travel rollback |
| **Table Formats (Delta/Iceberg/Hudi)** | Decision 1 là core decision của document; tradeoffs từ feature matrix ảnh hưởng trực tiếp |
| **Catalogs** | Decision 4 (Polaris vs Nessie vs Glue) — catalog choice là trung tâm architecture |
| **UniForm / Apache XTable** | Decision 2 + 3 — cơ chế migration toolchain |
| **Schema Evolution** | Failure Mode 2 — type promotion conflict là failure mode thực tế, không phải hypothetical |
| **FinOps** | Section 5 — math kiểm tra được, ROI so với Unity Catalog billing |
| **Deletion Vectors** | XTable phải map Delta deletion vectors → Iceberg equality deletes (handled by XTable 0.4+) |
