# Lakehouse Layout & Evidence

## 1. Directory Structure (`_lakehouse/`)

```
_lakehouse/
├── blobs/
│   ├── blob_000.bin ... blob_199.bin (200 binary media frames)
├── bronze/
│   ├── agent_traces/
│   │   ├── _delta_log/ (00000000000000000000.json)
│   │   └── part-*.parquet (1,578 trajectory steps)
│   ├── docs_multimodal/
│   │   ├── _delta_log/ (00000000000000000000.json)
│   │   └── part-*.parquet (2,000 docs with 256-dim embeddings)
│   └── llm_calls_raw/
│       ├── _delta_log/ (00000000000000000000.json)
│       └── part-*.parquet (200,000 raw JSON logs)
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
│       ├── provenance_bucket=scraped_optout_checked/
│       ├── provenance_bucket=synthetic/
│       └── provenance_bucket=UNCLASSIFIED/
├── gold/
│   ├── agent_performance/
│   │   ├── _delta_log/
│   │   └── part-*.parquet (policy-v2 & policy-v3 rollups)
│   └── llm_daily_metrics/
│       ├── _delta_log/
│       └── date=*/ (p50/p95 latency, tokens, cost_usd, error_rate)
├── iceberg/
│   ├── nb5/
│   │   ├── catalog.db (SQLite REST-shaped catalog)
│   │   └── warehouse/lake/llm_events/
│   │       ├── data/ (hidden partition parquet files)
│   │       └── metadata/ (*.metadata.json, snap-*.avro)
│   ├── nb6/
│   └── smoke/
└── scratch/
    ├── customers_tt/
    ├── docs_cdf/
    ├── events_smallfiles/
    ├── maint_events/
    ├── media_inline/
    ├── media_pointer/
    └── users_delta/
```

## 2. Sample Delta Transaction Log Commit (`_delta_log/00000000000000000000.json`)

```json
{"commitInfo":{"timestamp":1787021236653,"operation":"WRITE","operationParameters":{"mode":"Overwrite"},"engineInfo":"delta-rs:py-1.6.2","clientVersion":"delta-rs.py-1.6.2","operationMetrics":{"execution_time_ms":2,"num_added_files":1,"num_added_rows":3,"num_partitions":0,"num_removed_files":0}}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"07580560-c4b1-467c-b747-6593a9cd398d","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"createdTime":1787021236650,"configuration":{}}}
{"add":{"path":"part-00000-0c5e5b70-d4f5-4f7a-bef9-ef4899f7cad2-c000.snappy.parquet","partitionValues":{},"size":1384,"modificationTime":1787021236652,"dataChange":true,"stats":"{\"numRecords\":3,\"minValues\":{\"id\":1,\"name\":\"alice\",\"age\":25,\"city\":\"Danang\"},\"maxValues\":{\"name\":\"charlie\",\"city\":\"Hanoi\",\"age\":35,\"id\":3},\"nullCount\":{\"city\":0,\"id\":0,\"name\":0,\"age\":0}}","tags":null,"baseRowId":null,"defaultRowCommitVersion":null,"clusteringProvider":null}}
```
