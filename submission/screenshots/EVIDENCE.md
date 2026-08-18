# Submission Evidence — Storage Layout & Delta Log Inspection

## 1. Directory Tree (`_lakehouse/`)

```
_lakehouse/
├── blobs/
│   ├── frame_0000.bin
│   └── ... (200 blob files)
├── bronze/
│   ├── agent_traces/
│   ├── docs_multimodal/
│   └── llm_calls_raw/
├── silver/
│   ├── agent_traces/
│   └── llm_calls/
├── gold/
│   ├── agent_cost_daily/
│   ├── llm_metrics_daily/
│   └── training_trajectories_art10/
├── iceberg/
│   ├── nb5/
│   ├── nb6/
│   └── nb8/
└── scratch/
    ├── events_smallfiles/
    └── users_delta/
        └── _delta_log/
            ├── 00000000000000000000.json
            └── 00000000000000000001.json
```

## 2. Contents of `_delta_log/00000000000000000000.json`

```json
{"commitInfo":{"timestamp":1787047679402,"operation":"WRITE","operationParameters":{"mode":"Overwrite"},"engineInfo":"delta-rs:py-1.6.2","clientVersion":"delta-rs.py-1.6.2","operationMetrics":{"execution_time_ms":4,"num_added_files":1,"num_added_rows":3,"num_partitions":0,"num_removed_files":0}}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"e6be2039-03ba-422e-be72-192e641d76b2","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"createdTime":1787047679398,"configuration":{}}}
{"add":{"path":"part-00000-f1107aa6-85eb-4d2f-929e-b10fb24fc04e-c000.snappy.parquet","partitionValues":{},"size":1384,"modificationTime":1787047679402,"dataChange":true,"stats":"{\"numRecords\":3,\"minValues\":{\"id\":1,\"name\":\"alice\",\"city\":\"Danang\",\"age\":25},\"maxValues\":{\"id\":3,\"age\":35,\"name\":\"charlie\",\"city\":\"Hanoi\"},\"nullCount\":{\"name\":0,\"id\":0,\"age\":0,\"city\":0}}","tags":null,"baseRowId":null,"defaultRowCommitVersion":null,"clusteringProvider":null}}
```

## 3. Verification Test Status

- `make smoke`: PASS (9 checks)
- `make test`: PASS (24 pytest assertions)
- `make run-all`: PASS (8/8 notebooks executed cleanly)
