# Plan: Bonus Challenge — Topic F  
## Catalog Migration: Databricks Unity Catalog → Apache Polaris (Zero-Downtime)

> Dựa trên nền tảng so sánh **Delta Lake vs Iceberg vs Hudi** (slide Day 18)  
> File submission cuối: `submission/bonus/ARCHITECTURE.md`

---

## 1. Lý do chọn Topic F

Screenshot so sánh ba table format cho thấy:

| Insight từ bảng | Ý nghĩa với Topic F |
|---|---|
| Delta: Branching = "Tag only" vs Iceberg: "✓ + Nessie" | Đây là lý do **phải migrate** — Unity Catalog khóa vào Databricks tagging, thiếu Nessie branching cho multi-engine |
| Multi-engine: Delta "via UniForm" vs Iceberg "✓ (default)" | UniForm là **bridge công nghệ** — dùng để dual-read trong migration window |
| Ecosystem: Delta = Databricks (2017), Iceberg = Netflix/Apple (2018) | Iceberg đã trở thành **vendor-neutral standard** — điểm chốt để justify migration |
| "Apache XTable convert Delta ↔ Iceberg ↔ Hudi" | Đây là **migration toolchain** chính |

**Bài toán cốt lõi:** 500 Delta tables, 20 teams, 4 query engines (Spark/Trino/DuckDB/Snowflake), 6 tuần, zero downtime.

---

## 2. Cấu trúc ARCHITECTURE.md (6 sections)

### Section 1 — Problem Statement (≤200 từ)
- Scale: 500 tables, 20 teams, 4 engines
- Constraint: zero query downtime + time-travel semantics preserved
- Why hard: Delta log format ≠ Iceberg metadata chain, phải reconcile 2 transaction logs song song
- Deadline: 6 tuần (hard deadline từ leadership)

### Section 2 — Architecture Diagram
```
ASCII diagram: dual-write migration architecture

[Source: Unity Catalog / Delta tables]
         │
         ▼
[Phase 1: UniForm layer — Delta tables expose Iceberg REST API]
         │  ← Iceberg clients read here (Trino, DuckDB, Snowflake)
         ▼
[Phase 2: Apache XTable sync job — Delta log → Iceberg metadata]
         │
         ▼
[Phase 3: Apache Polaris Catalog — Iceberg native registration]
         │
         ├── Spark (reads Polaris)
         ├── Trino (reads Polaris)
         ├── DuckDB (reads Polaris)
         └── Snowflake (reads Polaris)

[Cutover: flip DNS/catalog pointer, deprecate Unity Catalog connection]
```

**Phải show:** ingestion path, query path, dual-read window, cutover point.

### Section 3 — 5+ Key Decisions (core of the document)

**Decision 1: Table format đích — Iceberg (không phải Hudi)**
- Chọn: **Iceberg**
- Loại Hudi: MOR write model tốt cho high-frequency upserts nhưng read latency cao hơn; Hudi thiếu Hidden Partitioning → phải rewrite partition layout khi migrate — quá rủi ro
- Loại "giữ Delta": giải quyết vendor lock-in thì phải off Delta hoàn toàn; UniForm vẫn cần Delta log làm source of truth → không đạt mục tiêu

**Decision 2: Migration toolchain — Apache XTable (không phải viết tay)**
- Chọn: **Apache XTable** (open-source, Facebook/Google/Netflix backed)
- Loại manual conversion: 500 tables × avg 50 snapshots = quá nhiều manual work, error-prone
- Loại Iceberg's `add_files` procedure: chỉ migrate data files, không preserve time-travel history; vi phạm yêu cầu "giữ time-travel semantics"

**Decision 3: Dual-read strategy — UniForm trong Delta (không phải dual-write)**
- Chọn: **UniForm** (Delta 3.0+) — Delta tables expose Iceberg REST endpoint, Iceberg clients đọc được ngay
- Loại dual-write (ghi đồng thời vào cả 2 format): latency ghi tăng gấp đôi, risk consistency bug khi 1 write fail
- Loại hard cutover (cắt Delta, switch Iceberg ngay): không zero-downtime

**Decision 4: Catalog đích — Apache Polaris (không phải Project Nessie standalone)**
- Chọn: **Apache Polaris** (Snowflake donated, Apache incubating)
- Loại Nessie standalone: branching/tagging tốt nhưng thiếu enterprise auth (RBAC) và audit log; 20 teams cần governance rõ ràng
- Loại AWS Glue Catalog: vendor lock-in lại → đi ngược mục tiêu thoát lock-in

**Decision 5: Migration order — criticality-based batching (không phải alphabetical)**
- Chọn: migrate theo criticality tier: Gold tables trước (dashboard-critical), Silver sau, Bronze cuối
- Loại alphabetical: không minimize blast radius
- Loại reverse-dependency: quá phức tạp để build dependency graph trong 6 tuần với 20 teams

**Decision 6 (bonus): Time-travel preservation strategy**
- Chọn: XTable sync toàn bộ snapshot history, validate bằng checksum trước cutover
- Loại: chỉ migrate latest snapshot → mất time-travel → vi phạm yêu cầu rõ ràng

### Section 4 — Failure Modes (3 kịch bản 3AM)

**Failure 1: XTable sync lag → stale Iceberg metadata (Day 18 tie: time travel)**
- Triệu chứng: Trino query thấy data cũ hơn Spark 15 phút
- Detection: Alert khi `iceberg_snapshot_id` trên Polaris < Delta `lastCommitTimestamp - 10m`
- Rollback: Trino/DuckDB clients tạm redirect về UniForm endpoint (Delta vẫn sống), page on-call, chạy XTable force-sync

**Failure 2: Schema evolution conflict — Delta TINYINT vs Iceberg INT promotion**
- Triệu chứng: Snowflake trả lỗi type mismatch sau khi một team thêm column
- Detection: Schema validation job chạy mỗi 5 phút so sánh Delta schema vs Iceberg schema trong Polaris
- Rollback: Revert Delta schema change bằng time travel (`RESTORE TABLE TO VERSION AS OF N`), block write đến khi fix

**Failure 3: Polaris catalog downtime → tất cả Iceberg queries fail**
- Triệu chứng: 20 teams báo cáo connection refused đến Polaris REST endpoint
- Detection: Synthetic query probe mỗi 30 giây
- Rollback: Failover về UniForm endpoint (Delta vẫn serve Iceberg REST); Polaris là passive replica trong migration window nên Delta là source of truth

### Section 5 — Cost Estimate

```
Storage (không đổi — chỉ thêm Iceberg metadata):
  - Iceberg metadata overhead: ~0.1% of data size
  - 500 tables × avg 100 GB = 50 TB data
  - Metadata overhead: 50 TB × 0.1% = 50 GB → ~$1/tháng (S3)

Compute (migration jobs):
  - XTable sync: chạy 1 lần per table, ~10 min × 500 tables = 83 GPU-hours
  - EC2 c5.2xlarge ($0.34/hr) × 83 hr = ~$28 one-time
  - Ongoing XTable sync daemon: 1 c5.large ($0.085/hr) × 720 hr/tháng = ~$61/tháng
    → Tắt sau 6 tuần cutover → $0 ongoing

Polaris catalog hosting:
  - Self-hosted on EKS: 2 nodes m5.xlarge ($0.19/hr) = $274/tháng
  - (Snowflake managed Polaris: ~$500/tháng — loại vì cost)

Total migration cost: ~$28 one-time + $335/tháng trong 6 tuần = ~$530 total
Post-migration ongoing: $274/tháng (Polaris) vs $X/tháng (Unity Catalog subscription) — ROI clear nếu Unity > $274/tháng
```

### Section 6 — MVP slice (1 tuần)

**Week 1 goal:** Chứng minh một Delta table có thể được đọc bởi cả 4 engines đồng thời qua Polaris catalog, với time-travel semantics nguyên vẹn.

**Scope:**
1. Chọn 1 Gold table nhỏ (~5 GB, < 20 snapshots)
2. Enable UniForm trên table đó trong Unity Catalog
3. Chạy XTable sync → Polaris
4. Verify: `SELECT * FROM iceberg_table VERSION AS OF <old_snapshot>` trả đúng kết quả trên cả Spark + Trino
5. Đo sync lag (target: < 30 giây)

**Không làm trong tuần 1:** governance migration, auth RBAC, 499 tables còn lại.

---

## 3. PoC Notebook (optional, `submission/bonus/poc/`)

**File:** `migration_spike.py` (~100 dòng)

**Demo:** XTable sync Delta → Iceberg locally dùng delta-rs + PyIceberg:
1. Tạo Delta table với 3 commits (simulate time travel history)
2. Chạy XTable Python binding để convert metadata
3. Đọc lại qua PyIceberg REST catalog
4. Assert: snapshot IDs và row counts khớp nhau ở tất cả versions

**Đây là phần khó nhất cần prove:** time-travel history được preserve qua format conversion.

---

## 4. Checklist tự review trước khi nộp

- [ ] ≥ 5 decisions, mỗi cái có ≥ 2 alternatives bị loại với tradeoff cụ thể
- [ ] Numbers từ screenshot (Delta/Iceberg features) xuất hiện trong reasoning, không chỉ problem statement
- [ ] ≥ 4 Day 18 concepts được áp dụng: ACID, time travel, catalogs, deletion vectors, lineage, UniForm
- [ ] 3 failure modes: detection cụ thể + rollback cụ thể (không phải "chúng tôi sẽ monitor")
- [ ] Cost math kiểm tra được: show $/TB × TB
- [ ] MVP slice rõ ràng: 1 tuần, 1 table, 4 engines, time travel verified
- [ ] PoC chạy được từ clean checkout

---

## 5. Timeline viết document

| Ngày | Việc |
|---|---|
| Day 1 (2h) | Viết Section 1 + vẽ ASCII diagram Section 2 |
| Day 2 (3h) | Viết 6 decisions với alternatives (Section 3) — đây là phần quan trọng nhất |
| Day 3 (1h) | Viết failure modes + cost estimate (Section 4 + 5) |
| Day 4 (1h) | Viết MVP + tự review checklist + polish |
| Day 5 (1h) | Viết PoC notebook nếu còn thời gian |

**Total: ~8 giờ** (đúng effort target của bonus challenge)
