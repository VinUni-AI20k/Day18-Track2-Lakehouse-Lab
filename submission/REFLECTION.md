In my Day 18 lab, specifically during the **02_optimize_zorder** and **04_medallion** stages, the most common trap is creating a "Small File Syndrome" through over-partitioning.

### The Anti-Pattern: Over-Partitioning
* Definition: Dividing a Delta table into too many logical partitions (e.g., partitioning by `hour` or `minute`) when the data volume does not justify it.

### Reasons:

1. **The "Cloud-Scale" Illusion:** 
    It's usually assumed that partitioning always improves speed. In the lab, with only 200K - 1M rows, partitioning by a high-cardinality column like `timestamp` will create hundreds of folders. Each folder might contain a Parquet file of only a few Kilobytes.
    
2. **Metadata Overload:** 
    For every small file created, Delta Lake must add an entry to the `_delta_log`. When Spark tries to read the table, it spends more time performing "File Listing" and reading JSON metadata than actually processing data. This results in the **Java Gateway Exit** errors or OOM (Out of Memory) issues you've already encountered.

3. **The "Write-Amplification" in Medallion:** 
    In the `Bronze` to `Silver` transition, if you perform frequent `MERGE` operations without `OPTIMIZE`, Delta Lake keeps all historical files to support **Time Travel**. Without maintenance, your storage layer becomes a graveyard of tiny, obsolete Parquet files.

---


