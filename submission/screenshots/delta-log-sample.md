# _lakehouse/bronze/agent_traces/_delta_log/00000000000000000000.json

Transaction log cua Delta: moi dong la mot JSON doc lap (JSON Lines).
Bang = nhung gi log nay noi, khong phai nhung file dang nam trong thu muc.

--- dong 1: action = commitInfo ---
{
  "commitInfo": {
    "timestamp": 1787065941511,
    "operation": "WRITE",
    "operationParameters": {
      "mode": "Overwrite"
    },
    "engineInfo": "delta-rs:py-1.6.2",
    "operationMetrics": {
      "execution_time_ms": 56,
      "num_added_files": 1,
      "num_added_rows": 1578,
      "num_partitions": 0,
      "num_removed_files": 0
    },
    "clientVersion": "delta-rs.py-1.6.2"
  }
}

--- dong 2: action = protocol ---
{
  "protocol": {
    "minReaderVersion": 1,
    "minWriterVersion": 2
  }
}

--- dong 3: action = metaData ---
{
  "metaData": {
    "id": "05cf5f00-f446-4eeb-ab35-7aef6b71ba8c",
    "name": null,
    "description": null,
    "format": {
      "provider": "parquet",
      "options": {}
    },
    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"session_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"step\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tool\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"input_tokens\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"output_tokens\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"latency_ms\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"status\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"reward\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"subject_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
    "partitionColumns": [],
    "createdTime": 1787065941459,
    "configuration": {}
  }
}

--- dong 4: action = add ---
{
  "add": {
    "path": "part-00000-ae3a2a05-a6e3-4a08-b433-31c687370ff8-c000.snappy.parquet",
    "partitionValues": {},
    "size": 29519,
    "modificationTime": 1787065941511,
    "dataChange": true,
    "stats": {
      "numRecords": 1578,
      "minValues": {
        "step": 0,
        "output_tokens": 20,
        "session_id": "sess_0000",
        "status": "error",
        "reward": -0.0,
        "tool": "get_schema",
        "input_tokens": 205,
        "latency_ms": 150,
        "subject_id": "user_000"
      },
      "maxValues": {
        "step": 7,
        "tool": "write_report",
        "subject_id": "user_249",
        "session_id": "sess_0299",
        "output_tokens": 898,
        "input_tokens": 3995,
        "latency_ms": 5987,
        "status": "ok",
        "reward": 1.0
      },
      "nullCount": {
        "input_tokens": 0,
        "output_tokens": 0,
        "step": 0,
        "latency_ms": 0,
        "tool": 0,
        "session_id": 0,
        "subject_id": 0,
        "status": 0,
        "reward": 0
      }
    },
    "tags": null,
    "baseRowId": null,
    "defaultRowCommitVersion": null,
    "clusteringProvider": null
  }
}
