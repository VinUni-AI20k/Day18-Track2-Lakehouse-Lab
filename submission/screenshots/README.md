# Lakehouse Lab Verification Evidence

This directory contains the storage layer artifacts required by [`rubric.md`](../../rubric.md):

## 1. Directory Tree of `_lakehouse/`
The complete folder layout showing Bronze, Silver, Gold, Scratch, and Iceberg catalog layers is preserved in [`tree_lakehouse.txt`](./tree_lakehouse.txt):
```text
_lakehouse/
  blobs/
  bronze/
    agent_traces/
      _delta_log/
    docs_multimodal/
      _delta_log/
    llm_calls_raw/
      _delta_log/
  gold/
    agent_policy_metrics/
      _delta_log/
    llm_metrics_daily/
      _delta_log/
  iceberg/
    nb5/
    nb6/
    nb8/
  scratch/
    users_delta/
      _delta_log/
        00000000000000000000.json
        00000000000000000001.json
  silver/
    agent_traces/
      _delta_log/
    llm_calls/
      _delta_log/
```

## 2. Sample Delta Transaction Log Commit JSON
Extracted from `_lakehouse/scratch/users_delta/_delta_log/00000000000000000000.json`:

```json
{"commitInfo":{"timestamp":1787021537087,"operation":"WRITE","operationParameters":{"mode":"Overwrite"},"engineInfo":"delta-rs:py-1.6.2","clientVersion":"delta-rs.py-1.6.2","operationMetrics":{"execution_time_ms":4,"num_added_files":1,"num_added_rows":3,"num_partitions":0,"num_removed_files":0}}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"b0c441b4-e51d-4833-a5e0-e6b7b17389d4","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"createdTime":1787021537083,"configuration":{}}}
{"add":{"path":"part-00000-8d124adf-1fe2-42aa-8624-69d03105cd46-c000.snappy.parquet","partitionValues":{},"size":1384,"modificationTime":1787021537087,"dataChange":true,"stats":"{\"numRecords\":3,\"minValues\":{\"name\":\"alice\",\"id\":1,\"age\":25,\"city\":\"Danang\"},\"maxValues\":{\"name\":\"charlie\",\"id\":3,\"age\":35,\"city\":\"Hanoi\"},\"nullCount\":{\"name\":0,\"age\":0,\"city\":0,\"id\":0}}","tags":null,"baseRowId":null,"defaultRowCommitVersion":null,"clusteringProvider":null}}
```
