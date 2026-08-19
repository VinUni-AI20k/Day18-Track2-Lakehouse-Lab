# Lakehouse Storage Verification & Delta Log Evidence

## 1. Directory Tree of `_lakehouse/`

```text
_lakehouse/
├── blobs/
│   └── 200 blob files (~12.5 MB)
├── bronze/
│   ├── agent_traces/
│   │   ├── _delta_log/
│   │   │   └── 00000000000000000000.json
│   │   └── part-*.parquet (1,578 steps / 300 sessions)
│   ├── docs_multimodal/
│   │   ├── _delta_log/
│   │   │   └── 00000000000000000000.json
│   │   └── part-*.parquet (2,000 rows, dim=256)
│   └── llm_calls_raw/
│       ├── _delta_log/
│       │   └── 00000000000000000000.json
│       └── part-*.parquet (200,000 rows)
├── silver/
│   ├── agent_trajectories/
│   │   ├── _delta_log/
│   │   ├── agent_version=policy-v2/
│   │   └── agent_version=policy-v3/
│   ├── llm_calls/
│   │   ├── _delta_log/
│   │   ├── date=2026-04-01/
│   │   ├── date=2026-04-02/
│   │   ├── date=2026-04-03/
│   │   ├── date=2026-04-04/
│   │   ├── date=2026-04-05/
│   │   ├── date=2026-04-06/
│   │   └── date=2026-04-07/
│   └── training_corpus_governed/
│       ├── _delta_log/
│       ├── provenance_bucket=licensed/
│       ├── provenance_bucket=public_domain/
│       ├── provenance_bucket=scraped_opt_in/
│       ├── provenance_bucket=synthetic/
│       └── provenance_bucket=UNCLASSIFIED/
├── gold/
│   └── llm_daily_metrics/
│       ├── _delta_log/
│       └── part-*.parquet
└── scratch/
    ├── events_smallfiles/
    ├── maint_events/
    ├── users_delta/
    │   └── _delta_log/
    │       ├── 00000000000000000000.json
    │       └── 00000000000000000001.json
    └── vector_index_external/
```

## 2. Sample Delta Log Commit: `_lakehouse/scratch/users_delta/_delta_log/00000000000000000000.json`

```json
{"commitInfo":{"timestamp":1787049036534,"operation":"WRITE","operationParameters":{"mode":"Overwrite"},"engineInfo":"delta-rs:py-1.6.2","clientVersion":"delta-rs.py-1.6.2","operationMetrics":{"execution_time_ms":199,"num_added_files":1,"num_added_rows":3,"num_partitions":0,"num_removed_files":0}}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"35266a54-15ab-4555-8740-a68fed71ac7a","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"createdTime":1787049036346,"configuration":{}}}
{"add":{"path":"part-00000-a17e6528-642b-4b14-9e10-09de7bcf15a8-c000.snappy.parquet","partitionValues":{},"size":1384,"modificationTime":1787049036531,"dataChange":true,"stats":"{\"numRecords\":3,\"minValues\":{\"city\":\"Danang\",\"age\":25,\"name\":\"alice\",\"id\":1},\"maxValues\":{\"name\":\"charlie\",\"age\":35,\"id\":3,\"city\":\"Hanoi\"},\"nullCount\":{\"city\":0,\"name\":0,\"age\":0,\"id\":0}}","tags":null,"baseRowId":null,"defaultRowCommitVersion":null,"clusteringProvider":null}}
```
