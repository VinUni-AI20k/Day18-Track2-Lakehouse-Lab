# Submission: REFLECTION.md

## Primary Risk: The Small-Files Problem (File Fragmentation)

### **Why Our Team Is at Risk**
Our ingestion architecture relies on frequent micro-batch and streaming pipelines into Delta Lake. This process continually produces thousands of tiny Parquet files. Consequently, query engines suffer severe performance degradation due to massive I/O and metadata overhead required to scan and open individual files.

---

### **Mitigation Plan**

1. **Scheduled Maintenance:** Execute nightly `OPTIMIZE` commands to compact fragmented files into optimal sizes (128 MB – 1 GB).
2. **Auto-Compaction Settings:** Enable automated write optimization directly in table configurations:
   * `delta.autoOptimize.optimizeWrite = true`
   * `delta.autoOptimize.autoCompact = true`
3. **Data Skipping:** Combine compaction with `ZORDER BY` on high-cardinality filter columns to accelerate data pruning and speed up queries.