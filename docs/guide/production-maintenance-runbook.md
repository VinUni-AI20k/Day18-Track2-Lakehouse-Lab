# Production Lakehouse Maintenance Runbook
**Version 1.0.0** · Operations Guide · HADS Compliant

---

## AI READING INSTRUCTION

Read `[SPEC]` blocks for maintenance commands and automated scheduling.
Read `[BUG]` blocks for critical production traps (VACUUM vs. orphan drift, snapshot expiration semantics).

---

## 1. The Four Mandatory Maintenance Jobs

**[SPEC]**

| Job # | Maintenance Operation | Target Table Engine | Production Cadence | Objective |
|---|---|---|---|---|
| **Job 1** | **Bin-Packing Compaction** | Delta / Iceberg | Hourly / Post-ingest | Merge small micro-batch files (< 10MB) into 128MB–512MB chunks |
| **Job 2** | **Clustering / Z-ORDER** | Delta / Iceberg | Nightly | Sort data along multidimensional keys (`user_id`, `ts`) to maximize file skipping |
| **Job 3** | **Snapshot Expiration & Vacuum** | Delta (`VACUUM`) / Iceberg (`expire_snapshots`) | Daily (Retention: 7d) | Remove expired metadata snapshots and tombstoned physical Parquet files |
| **Job 4** | **Orphan Sweeping** | Delta / Iceberg | Weekly | Reconcile disk vs catalog to remove stranded uncommitted multipart uploads |
| **Job 5** | **Parquet Checkpoint** | Delta (`compact_logs`) | Every 10 commits | Consolidate JSON transaction commits into a single Parquet checkpoint |

---

## 2. Production Traps & Critical Failures

**[BUG] Delta VACUUM Does Not Delete Uncommitted Orphan Files**
- **Symptom:** Disk storage expands monotonically even when `VACUUM` runs daily with `retention_hours=0`.
- **Root Cause:** Delta `VACUUM` only purges files referenced in historical JSON commit logs as tombstoned (`remove` actions). Files written by failed/aborted executor tasks that never committed are invisible to the transaction log.
- **Fix:** Run an out-of-band orphan sweeper that computes the set difference between files physically on disk and active files tracked by the latest Delta table snapshot.

```python
# Orphan Sweeper Pattern
active_files = set(table.files())
all_disk_files = {p.name for p in table_path.glob("*.parquet")}
orphans = all_disk_files - active_files
for orphan in orphans:
    (table_path / orphan).unlink()
```

---

**[BUG] Iceberg `expire_snapshots` Does Not Automatically Delete Physical Data Files**
- **Symptom:** Expiring old Iceberg snapshots reduces metadata size in `catalog.db` but physical data files remain on object storage.
- **Root Cause:** Iceberg separates snapshot expiration (metadata pruning) from physical manifest unlinking. Data files shared by retained snapshots cannot be deleted.
- **Fix:** Execute Iceberg orphan cleanup routines that sweep manifest lists and manifest files unreferenced by any valid snapshot in the catalog.

---

## 3. Maintenance Execution Automation

**[SPEC]**

```python
from deltalake import DeltaTable
from pyiceberg.catalog import load_catalog

def run_delta_maintenance(table_path: str):
    dt = DeltaTable(table_path)
    # 1. Compaction
    dt.optimize.compact()
    # 2. Multidimensional Z-Order
    dt.optimize.z_order(["user_id", "ts"])
    # 3. Purge tombstoned files older than 168 hours (7 days)
    dt.vacuum(retention_hours=168, enforce_retention_duration=True)
    # 4. Checkpoint log consolidation
    dt.create_checkpoint()

def run_iceberg_maintenance(catalog_name: str, table_ident: str):
    catalog = load_catalog(catalog_name)
    table = catalog.load_table(table_ident)
    # Expire snapshots older than 7 days
    table.maintenance().expire_snapshots().expire_older_than(older_than_ms).commit()
```
