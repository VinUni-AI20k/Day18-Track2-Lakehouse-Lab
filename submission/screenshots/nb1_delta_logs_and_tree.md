# Terminal: tree _lakehouse/

```text
Folder PATH listing
Volume serial number is 0000002F D0D5:77DC
C:\AI_THUCCHIEN\DAY18-TRACK2-LAKEHOUSE-LAB\_LAKEHOUSE
+---bronze
|   \---llm_calls_raw
|       \---_delta_log
+---gold
|   \---llm_daily_metrics
|       +---date=2026-04-01
|       +---date=2026-04-02
|       +---date=2026-04-03
|       +---date=2026-04-04
|       +---date=2026-04-05
|       +---date=2026-04-06
|       +---date=2026-04-07
|       +---date=2026-04-08
|       \---_delta_log
+---scratch
|   +---customers_tt
|   |   \---_delta_log
|   +---events_smallfiles
|   |   \---_delta_log
|   +---users_delta
|   |   \---_delta_log
|   \---_smoke
|       \---_delta_log
\---silver
    \---llm_calls
        +---date=2026-04-01
        +---date=2026-04-02
        +---date=2026-04-03
        +---date=2026-04-04
        +---date=2026-04-05
        +---date=2026-04-06
        +---date=2026-04-07
        +---date=2026-04-08
        \---_delta_log
```

# _delta_log/00000000000000000000.json content

**File:** `_lakehouse/scratch/users_delta/_delta_log/00000000000000000000.json`

```json
{"commitInfo":{"timestamp":1777864490813,"operation":"WRITE","operationParameters":{"mode":"Overwrite"},"engineInfo":"delta-rs:py-1.5.1","clientVersion":"delta-rs.py-1.5.1","operationMetrics":{"execution_time_ms":3,"num_added_files":1,"num_added_rows":3,"num_partitions":0,"num_removed_files":0}}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"25ecefac-ac67-459a-83ad-22f4f85c80ee","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"createdTime":1777864490809,"configuration":{}}}
{"add":{"path":"part-00000-dfb73dcd-79cb-4ccf-8ac1-488f72042383-c000.snappy.parquet","partitionValues":{},"size":1384,"modificationTime":1777864490813,"dataChange":true,"stats":"{\"numRecords\":3,\"minValues\":{\"id\":1,\"city\":\"Danang\",\"age\":25,\"name\":\"alice\"},\"maxValues\":{\"name\":\"charlie\",\"city\":\"Hanoi\",\"age\":35,\"id\":3},\"nullCount\":{\"name\":0,\"city\":0,\"id\":0,\"age\":0}}","tags":null,"baseRowId":null,"defaultRowCommitVersion":null,"clusteringProvider":null}}
```
