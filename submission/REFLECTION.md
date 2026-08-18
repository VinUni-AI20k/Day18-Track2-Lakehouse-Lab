# Lakehouse Architecture Reflection

- **Họ và tên:** Nguyễn Văn Hưng
- **Mã học viên (NXX):** 2A202601284

**Target Anti-Pattern:** *Unmanaged Streaming Ingestion & Small-File Bloat (Anti-Pattern #1)*

In our real-time LLM Observability and Agent Trajectory pipelines, streaming ingestion continuously appends micro-batches (5-second trigger intervals) directly to Bronze storage. Without automated table maintenance, this creates hundreds of thousands of KB-sized Parquet files overnight.

### Risk & Impact:
1. **Query Degradation & GPU Starvation:** Analytical scans must open and decompress thousands of tiny row groups instead of reading contiguous memory blocks.
2. **FinOps Explosion:** Object stores (S3/GCS) charge per GET request. Uncompacted tables trigger millions of unnecessary GET requests per day ($4.00/day vs $0.08/day at scale), driving up storage and query engine costs non-linearly.
3. **Metadata Inflation:** Replaying 200,000 JSON commits slows cold-start planning from 200ms to over 20s.

### Mitigation Strategy:
We enforce a mandatory 4-job table maintenance schedule:
- **Daily Compaction & Z-ORDER (`user_id` / `model`):** Compacts small files to 128–512 MB and enables 90%+ file skipping.
- **Vacuum & Orphan Sweeping:** Chains snapshot expiry with orphan file cleanup (`Disk \ Log`) to reclaim uncommitted tombstoned storage.
