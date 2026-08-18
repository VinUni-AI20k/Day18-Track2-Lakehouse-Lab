# Evidence — lightweight path

Rubric §Submission item 2, lightweight-path option: `tree _lakehouse/` plus the contents of one `_delta_log/*.json`.

```
Generated : 2026-08-18 04:36 UTC
Python    : 3.10.14
Platform  : Darwin 25.1.0 arm64
deltalake : 1.6.2
pyiceberg : 0.11.1
duckdb    : 1.5.5
```

## 1. `tree _lakehouse/`

Depth-limited to 3; directories with many files are collapsed to a count.

```
_lakehouse/
├── blobs/
│   ├── frame_0000.bin  (65,536 B)
│   ├── frame_0001.bin  (65,536 B)
│   └── … 198 more files (13,107,200 B total)
├── bronze/
│   ├── agent_traces/
│   │   ├── _delta_log/  (1 files)
│   │   │   └── 00000000000000000000.json  (2,167 B)
│   │   └── part-00000-e0b7ddba-520b-4c20-b009-c96c36fd5c75-c000.snappy.parquet  (29,519 B)
│   ├── docs_multimodal/
│   │   ├── _delta_log/  (1 files)
│   │   │   └── 00000000000000000000.json  (2,419 B)
│   │   └── part-00000-af5ae909-be80-4434-b1fb-4617bfbfc9f6-c000.snappy.parquet  (2,706,783 B)
│   └── llm_calls_raw/
│       ├── _delta_log/  (1 files)
│       │   └── 00000000000000000000.json  (1,639 B)
│       └── part-00000-540f83b8-181a-49f7-bfca-80e9bdf8040a-c000.snappy.parquet  (13,988,699 B)
├── gold/
│   ├── agent_performance/
│   │   ├── _delta_log/  (1 files)
│   │   │   └── 00000000000000000000.json  (1,978 B)
│   │   └── part-00000-558e1ef1-86dd-44d1-a092-2e434cd5d4e5-c000.snappy.parquet  (2,471 B)
│   └── llm_daily_metrics/
│       ├── _delta_log/  (2 files)
│       │   ├── 00000000000000000000.json  (8,859 B)
│       │   └── 00000000000000000001.json  (10,113 B)
│       ├── date=2026-04-01/  (2 files)
│       │   ├── part-00000-00436792-7983-4bf5-8fc0-4692c9f2e80f-c000.snappy.parquet  (2,688 B)
│       │   └── part-00000-845c747e-2023-477b-ad5b-c9ae5efbbe41-c000.zstd.parquet  (2,770 B)
│       ├── date=2026-04-02/  (2 files)
│       │   ├── part-00000-1db88663-56fa-4b81-aed8-a43399bb680a-c000.zstd.parquet  (2,766 B)
│       │   └── part-00000-90f09f39-168e-4dd2-924f-b29ae3d484c2-c000.snappy.parquet  (2,687 B)
│       ├── date=2026-04-03/  (2 files)
│       │   ├── part-00000-625d33eb-5457-4979-b8a7-3dbb4a829620-c000.snappy.parquet  (2,689 B)
│       │   └── part-00000-67fa5eed-dce2-42e0-9cb9-5e5d4aa2b56d-c000.zstd.parquet  (2,770 B)
│       ├── date=2026-04-04/  (2 files)
│       │   ├── part-00000-5b645094-cb4a-4919-86e7-ff3a237bc53a-c000.snappy.parquet  (2,689 B)
│       │   └── part-00000-c4a80040-fc52-460f-812c-802e674fabfe-c000.zstd.parquet  (2,770 B)
│       ├── date=2026-04-05/  (2 files)
│       │   ├── part-00000-edfd866d-601e-4495-8d8f-2fa965c4a026-c000.snappy.parquet  (2,687 B)
│       │   └── part-00000-f2ff50a0-6718-499a-877d-fab6ff19b995-c000.zstd.parquet  (2,766 B)
│       ├── date=2026-04-06/  (2 files)
│       │   ├── part-00000-580b8b01-8e9c-4579-90b0-e1546776f75e-c000.zstd.parquet  (2,770 B)
│       │   └── part-00000-cac76160-1a99-4af4-9c5e-1a47e367a016-c000.snappy.parquet  (2,688 B)
│       ├── date=2026-04-07/  (2 files)
│       │   ├── part-00000-124a9b8c-236c-429b-beab-0e926f09110e-c000.snappy.parquet  (2,687 B)
│       │   └── part-00000-c3d36590-b4fe-474d-8b79-5804f91a9dee-c000.zstd.parquet  (2,770 B)
│       └── date=2026-04-08/  (2 files)
│           ├── part-00000-126d6f1b-344e-427f-9fd0-46a726309557-c000.zstd.parquet  (2,768 B)
│           └── part-00000-f45d6baa-a593-4d60-8cff-4c76a0a632ee-c000.snappy.parquet  (2,688 B)
├── iceberg/
│   ├── nb5/
│   │   ├── warehouse/  (51 files)
│   │   │   └── lake/  (51 files)
│   │   └── catalog.db  (20,480 B)
│   ├── nb6/
│   │   ├── warehouse/  (65 files)
│   │   │   └── lake/  (65 files)
│   │   └── catalog.db  (20,480 B)
│   └── nb8/
│       ├── warehouse/  (5 files)
│       │   └── lake/  (5 files)
│       └── catalog.db  (20,480 B)
├── scratch/
│   ├── customers_tt/
│   │   ├── _delta_log/  (5 files)
│   │   │   ├── 00000000000000000000.json  (1,338 B)
│   │   │   ├── 00000000000000000001.json  (1,609 B)
│   │   │   └── … 3 more files (6,294 B total)
│   │   ├── part-00000-3bc918ef-ce43-44fa-9cdf-c742d55f6768-c000.snappy.parquet  (622,767 B)
│   │   ├── part-00000-646de5db-987e-49a1-b788-3d6ffaf59865-c000.snappy.parquet  (888,143 B)
│   │   ├── part-00000-669d16d8-c454-46bc-9654-e6a72e9b55a4-c000.snappy.parquet  (623,399 B)
│   │   └── part-00000-ac6c8e79-c2bc-430c-8eeb-a0f7d451a224-c000.snappy.parquet  (1,570 B)
│   ├── docs_cdf/
│   │   ├── _change_data/  (1 files)
│   │   │   └── part-00000-2e0e902f-3c3e-4829-a17b-b3a78c75d6eb-c000.zstd.parquet  (1,184 B)
│   │   ├── _delta_log/  (2 files)
│   │   │   ├── 00000000000000000000.json  (1,255 B)
│   │   │   └── 00000000000000000001.json  (1,181 B)
│   │   ├── part-00000-7c0d4836-e736-4454-9651-5f21fa8f4316-c000.snappy.parquet  (12,963 B)
│   │   └── part-00000-e5017099-ff55-4ec4-9d68-1a4f0d8134ef-c000.zstd.parquet  (7,440 B)
│   ├── docs_intable/
│   │   ├── _delta_log/  (2 files)
│   │   │   ├── 00000000000000000000.json  (2,419 B)
│   │   │   └── 00000000000000000001.json  (1,558 B)
│   │   ├── part-00000-41916c1a-dde0-4275-89df-ff4ad3cb8948-c000.zstd.parquet  (2,504,539 B)
│   │   └── part-00000-cfe9d720-6f49-40ce-b583-5885d610765f-c000.snappy.parquet  (2,706,767 B)
│   ├── emb_f32/
│   │   ├── _delta_log/  (1 files)
│   │   │   └── 00000000000000000000.json  (1,201 B)
│   │   └── part-00000-7d4830cd-c278-4052-8832-f43770f28db2-c000.snappy.parquet  (2,684,023 B)
│   ├── emb_int8/
│   │   ├── _delta_log/  (1 files)
│   │   │   └── 00000000000000000000.json  (1,199 B)
│   │   └── part-00000-2946e005-376c-45ce-8a0b-ed71b74ffd91-c000.snappy.parquet  (461,512 B)
│   ├── events_smallfiles/
│   │   ├── _delta_log/  (205 files)
│   │   │   ├── 00000000000000000000.json  (1,573 B)
│   │   │   ├── 00000000000000000001.json  (957 B)
│   │   │   └── … 203 more files (492,681 B total)
│   │   ├── part-00000-00f7195c-4121-45c7-8a70-761f8ee8ae1e-c000.zstd.parquet  (122,482 B)
│   │   ├── part-00000-01442221-1c30-4424-9a74-7ca8c5f10bf8-c000.snappy.parquet  (65,952 B)
│   │   └── … 320 more files (28,141,242 B total)
│   ├── maint_events/
│   │   ├── _delta_log/  (208 files)
│   │   │   ├── 00000000000000000000.json  (1,798 B)
│   │   │   ├── 00000000000000000001.json  (1,031 B)
│   │   │   └── … 206 more files (488,849 B total)
│   │   ├── part-00000-1060da4f-e356-4755-bad0-85850ab1b5ba-c000.zstd.parquet  (666,465 B)
│   │   ├── part-00001-1060da4f-e356-4755-bad0-85850ab1b5ba-c000.zstd.parquet  (665,564 B)
│   │   └── … 8 more files (6,504,120 B total)
│   ├── media_inline/
│   │   ├── _delta_log/  (1 files)
│   │   │   └── 00000000000000000000.json  (1,274 B)
│   │   └── part-00000-3cb11c75-a5bb-4835-9715-21182ab2c95c-c000.snappy.parquet  (13,111,245 B)
│   ├── media_pointer/
│   │   ├── _delta_log/  (1 files)
│   │   │   └── 00000000000000000000.json  (1,364 B)
│   │   └── part-00000-ce2a5b40-f7ad-4e4b-9c82-eb084a3eaf73-c000.snappy.parquet  (3,480 B)
│   ├── users_delta/
│   │   ├── _delta_log/  (2 files)
│   │   │   ├── 00000000000000000000.json  (1,394 B)
│   │   │   └── 00000000000000000001.json  (1,449 B)
│   │   ├── part-00000-2ea7e91a-6848-4b17-881b-43016a6e3f04-c000.snappy.parquet  (1,384 B)
│   │   └── part-00000-44e12a52-fbc3-4a83-a82f-e42db9677c6f-c000.snappy.parquet  (1,548 B)
│   └── vector_index_external/
│       ├── _delta_log/  (1 files)
│       │   └── 00000000000000000000.json  (1,201 B)
│       └── part-00000-68b4cd76-5c88-4f4f-b4f8-10b013ce87d8-c000.snappy.parquet  (2,684,023 B)
└── silver/
    ├── agent_trajectories/
    │   ├── _delta_log/  (2 files)
    │   │   ├── 00000000000000000000.json  (3,268 B)
    │   │   └── 00000000000000000001.json  (1,244 B)
    │   ├── agent_version=policy-v2/  (2 files)
    │   │   ├── part-00000-76f490ae-c176-439b-8926-b55b72d0f960-c000.snappy.parquet  (21,149 B)
    │   │   └── part-00000-dd9f15e0-6373-451d-b6da-c9b633cd477c-c000.snappy.parquet  (12,441 B)
    │   └── agent_version=policy-v3/  (1 files)
    │       └── part-00000-ded20d7e-aa25-4e47-8085-bdb707e55c24-c000.snappy.parquet  (21,202 B)
    ├── llm_calls/
    │   ├── _delta_log/  (1 files)
    │   │   └── 00000000000000000000.json  (9,084 B)
    │   ├── date=2026-04-01/  (1 files)
    │   │   └── part-00000-2f4946e7-62b2-4823-90d9-ccb1b1179a15-c000.snappy.parquet  (1,076,095 B)
    │   ├── date=2026-04-02/  (1 files)
    │   │   └── part-00000-a6254174-b7a4-45e8-b777-b40e58681105-c000.snappy.parquet  (1,493,455 B)
    │   ├── date=2026-04-03/  (1 files)
    │   │   └── part-00000-b07249c5-16db-4aaa-8063-904ca075e427-c000.snappy.parquet  (1,487,794 B)
    │   ├── date=2026-04-04/  (1 files)
    │   │   └── part-00000-85f2ae68-5b20-4807-99a3-2b5c35aeee33-c000.snappy.parquet  (1,491,825 B)
    │   ├── date=2026-04-05/  (1 files)
    │   │   └── part-00000-7bc0f990-4592-4f85-9a37-dd8e3a297bfe-c000.snappy.parquet  (1,495,119 B)
    │   ├── date=2026-04-06/  (1 files)
    │   │   └── part-00000-1296b3eb-92b7-4871-b28e-fa5310944441-c000.snappy.parquet  (1,495,553 B)
    │   ├── date=2026-04-07/  (1 files)
    │   │   └── part-00000-1fc3e3f0-4690-4be5-83f2-672ddca8266c-c000.snappy.parquet  (1,488,301 B)
    │   └── date=2026-04-08/  (1 files)
    │       └── part-00000-a248fe69-e3b8-4f75-a95e-98b1698b34c5-c000.snappy.parquet  (467,600 B)
    └── training_corpus_governed/
        ├── _delta_log/  (2 files)
        │   ├── 00000000000000000000.json  (5,819 B)
        │   └── 00000000000000000001.json  (5,014 B)
        ├── provenance_bucket=UNCLASSIFIED/  (2 files)
        │   ├── part-00000-2b75cf06-7662-46b0-aab5-8beb4277e200-c000.snappy.parquet  (8,285 B)
        │   └── part-00000-d10f65f4-e546-4c98-8c92-a929b1f51c0d-c000.zstd.parquet  (5,952 B)
        ├── provenance_bucket=licensed/  (2 files)
        │   ├── part-00000-6b4da7af-e99c-46c2-9627-474daf5b131e-c000.zstd.parquet  (9,284 B)
        │   └── part-00000-cbb11a67-1143-499a-b640-5b31ee2486d7-c000.snappy.parquet  (13,494 B)
        ├── provenance_bucket=public_domain/  (1 files)
        │   └── part-00000-aa19c812-1bf3-451b-b478-66aeacb22ff7-c000.snappy.parquet  (8,274 B)
        ├── provenance_bucket=scraped_optout_checked/  (2 files)
        │   ├── part-00000-201880e7-d879-41fe-a3e3-91b139a383f5-c000.snappy.parquet  (8,179 B)
        │   └── part-00000-d5979265-15e2-4da3-931e-903bc664005a-c000.zstd.parquet  (5,950 B)
        └── provenance_bucket=synthetic/  (2 files)
            ├── part-00000-3c9ad8da-f312-479e-a951-29ee3faf0813-c000.zstd.parquet  (6,126 B)
            └── part-00000-588d9b01-f168-438c-b672-d8157e80d402-c000.snappy.parquet  (8,352 B)
```

Bronze, Silver and Gold are all present on the storage layer (NB4 criterion), and `gold/llm_daily_metrics/` is physically partitioned by `date=` — 8 day-partitions.

## 2. One transaction log, in full

`ls _lakehouse/bronze/llm_calls_raw/_delta_log/`

```
00000000000000000000.json
```

`cat 00000000000000000000.json` — pretty-printed (the file itself is one JSON action per line):

```json
{
  "commitInfo": {
    "timestamp": 1787027577084,
    "operation": "WRITE",
    "operationParameters": {
      "mode": "Overwrite"
    },
    "engineInfo": "delta-rs:py-1.6.2",
    "clientVersion": "delta-rs.py-1.6.2",
    "operationMetrics": {
      "execution_time_ms": 80,
      "num_added_files": 1,
      "num_added_rows": 200000,
      "num_partitions": 0,
      "num_removed_files": 0
    }
  }
}
{
  "protocol": {
    "minReaderVersion": 1,
    "minWriterVersion": 2
  }
}
{
  "metaData": {
    "id": "3bc73aac-8c86-4915-a541-dfb04f38ba14",
    "name": null,
    "description": null,
    "format": {
      "provider": "parquet",
      "options": {}
    },
    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"request_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"raw_json\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
    "partitionColumns": [],
    "createdTime": 1787027577003,
    "configuration": {}
  }
}
{
  "add": {
    "path": "part-00000-540f83b8-181a-49f7-bfca-80e9bdf8040a-c000.snappy.parquet",
    "partitionValues": {},
    "size": 13988699,
    "modificationTime": 1787027577084,
    "dataChange": true,
    "stats": "{\"numRecords\":200000,\"minValues\":{\"ts\":\"2026-04-01T00:00:00Z\",\"raw_json\":\"{\\\"model\\\": \\\"claude-haiku-4-5\\\", \\\"user_id\\\": \\\"u_1\\\", \\\"usage\\\": {\\\"input\",\"request_id\":\"00001784-631e-4300-9027-071a0b6a211a\"},\"maxValues\":{\"ts\":\"2026-04-07T23:59:56Z\",\"request_id\":\"ffffc9c8-01c0-4998-9c40-3346acd1228d\",\"raw_json\":\"{\\\"model\\\": \\\"claude-sonnet-4-6\\\", \\\"user_id\\\": \\\"u_999\\\", \\\"usage\\\": {\\\"io\"},\"nullCount\":{\"ts\":0,\"request_id\":0,\"raw_json\":0}}",
    "tags": null,
    "baseRowId": null,
    "defaultRowCommitVersion": null,
    "clusteringProvider": null
  }
}
```

This is the whole mechanism: a `protocol` action (reader/writer version), a `metaData` action carrying the schema as JSON, and one `add` action per data file with `size`, `numRecords` and per-column `min`/`max` stats. Those stats are what file-skipping reads — NB2 measures a 55× file-pruning ratio off exactly this.
