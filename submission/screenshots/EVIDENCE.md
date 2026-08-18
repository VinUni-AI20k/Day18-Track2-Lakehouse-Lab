# Lakehouse Storage Verification & Delta Log Evidence

## 1. Delta Transaction Log Structure & Commits
File: _lakehouse/scratch/users_delta/_delta_log/00000000000000000000.json

`json
{
  "commitInfo": {
    "timestamp": 1787021596024,
    "operation": "WRITE",
    "operationParameters": {
      "mode": "Overwrite"
    },
    "engineInfo": "delta-rs:py-1.6.2",
    "operationMetrics": {
      "execution_time_ms": 3,
      "num_added_files": 1,
      "num_added_rows": 3,
      "num_partitions": 0,
      "num_removed_files": 0
    },
    "clientVersion": "delta-rs.py-1.6.2"
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
    "id": "91cf6eab-874e-4df1-a366-6787afcacba7",
    "name": null,
    "description": null,
    "format": {
      "provider": "parquet",
      "options": {}
    },
    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
    "partitionColumns": [],
    "createdTime": 1787021596021,
    "configuration": {}
  }
}
{
  "add": {
    "path": "part-00000-b6fc0c54-e6dd-4ec8-83d8-57bbff0311b3-c000.snappy.parquet",
    "partitionValues": {},
    "size": 1384,
    "modificationTime": 1787021596024,
    "dataChange": true,
    "stats": "{\"numRecords\":3,\"minValues\":{\"age\":25,\"name\":\"alice\",\"city\":\"Danang\",\"id\":1},\"maxValues\":{\"id\":3,\"city\":\"Hanoi\",\"name\":\"charlie\",\"age\":35},\"nullCount\":{\"name\":0,\"id\":0,\"city\":0,\"age\":0}}",
    "tags": null,
    "baseRowId": null,
    "defaultRowCommitVersion": null,
    "clusteringProvider": null
  }
}
`

## 2. Directory Layout (_lakehouse/)
`
_lakehouse/
  ├── blobs/
    └── frame_0000.bin (65536 bytes)
    └── frame_0001.bin (65536 bytes)
    └── frame_0002.bin (65536 bytes)
    └── frame_0003.bin (65536 bytes)
    └── frame_0004.bin (65536 bytes)
    └── frame_0005.bin (65536 bytes)
    └── frame_0006.bin (65536 bytes)
    └── frame_0007.bin (65536 bytes)
    └── frame_0008.bin (65536 bytes)
    └── frame_0009.bin (65536 bytes)
    └── frame_0010.bin (65536 bytes)
    └── frame_0011.bin (65536 bytes)
    └── frame_0012.bin (65536 bytes)
    └── frame_0013.bin (65536 bytes)
    └── frame_0014.bin (65536 bytes)
    └── frame_0015.bin (65536 bytes)
    └── frame_0016.bin (65536 bytes)
    └── frame_0017.bin (65536 bytes)
    └── frame_0018.bin (65536 bytes)
    └── frame_0019.bin (65536 bytes)
    └── frame_0020.bin (65536 bytes)
    └── frame_0021.bin (65536 bytes)
    └── frame_0022.bin (65536 bytes)
    └── frame_0023.bin (65536 bytes)
    └── frame_0024.bin (65536 bytes)
    └── frame_0025.bin (65536 bytes)
    └── frame_0026.bin (65536 bytes)
    └── frame_0027.bin (65536 bytes)
    └── frame_0028.bin (65536 bytes)
    └── frame_0029.bin (65536 bytes)
    └── frame_0030.bin (65536 bytes)
    └── frame_0031.bin (65536 bytes)
    └── frame_0032.bin (65536 bytes)
    └── frame_0033.bin (65536 bytes)
    └── frame_0034.bin (65536 bytes)
    └── frame_0035.bin (65536 bytes)
    └── frame_0036.bin (65536 bytes)
    └── frame_0037.bin (65536 bytes)
    └── frame_0038.bin (65536 bytes)
    └── frame_0039.bin (65536 bytes)
    └── frame_0040.bin (65536 bytes)
    └── frame_0041.bin (65536 bytes)
    └── frame_0042.bin (65536 bytes)
    └── frame_0043.bin (65536 bytes)
    └── frame_0044.bin (65536 bytes)
    └── frame_0045.bin (65536 bytes)
    └── frame_0046.bin (65536 bytes)
    └── frame_0047.bin (65536 bytes)
    └── frame_0048.bin (65536 bytes)
    └── frame_0049.bin (65536 bytes)
    └── frame_0050.bin (65536 bytes)
    └── frame_0051.bin (65536 bytes)
    └── frame_0052.bin (65536 bytes)
    └── frame_0053.bin (65536 bytes)
    └── frame_0054.bin (65536 bytes)
    └── frame_0055.bin (65536 bytes)
    └── frame_0056.bin (65536 bytes)
    └── frame_0057.bin (65536 bytes)
    └── frame_0058.bin (65536 bytes)
    └── frame_0059.bin (65536 bytes)
    └── frame_0060.bin (65536 bytes)
    └── frame_0061.bin (65536 bytes)
    └── frame_0062.bin (65536 bytes)
    └── frame_0063.bin (65536 bytes)
    └── frame_0064.bin (65536 bytes)
    └── frame_0065.bin (65536 bytes)
    └── frame_0066.bin (65536 bytes)
    └── frame_0067.bin (65536 bytes)
    └── frame_0068.bin (65536 bytes)
    └── frame_0069.bin (65536 bytes)
    └── frame_0070.bin (65536 bytes)
    └── frame_0071.bin (65536 bytes)
    └── frame_0072.bin (65536 bytes)
    └── frame_0073.bin (65536 bytes)
    └── frame_0074.bin (65536 bytes)
    └── frame_0075.bin (65536 bytes)
    └── frame_0076.bin (65536 bytes)
    └── frame_0077.bin (65536 bytes)
    └── frame_0078.bin (65536 bytes)
    └── frame_0079.bin (65536 bytes)
    └── frame_0080.bin (65536 bytes)
    └── frame_0081.bin (65536 bytes)
    └── frame_0082.bin (65536 bytes)
    └── frame_0083.bin (65536 bytes)
    └── frame_0084.bin (65536 bytes)
    └── frame_0085.bin (65536 bytes)
    └── frame_0086.bin (65536 bytes)
    └── frame_0087.bin (65536 bytes)
    └── frame_0088.bin (65536 bytes)
    └── frame_0089.bin (65536 bytes)
    └── frame_0090.bin (65536 bytes)
    └── frame_0091.bin (65536 bytes)
    └── frame_0092.bin (65536 bytes)
    └── frame_0093.bin (65536 bytes)
    └── frame_0094.bin (65536 bytes)
    └── frame_0095.bin (65536 bytes)
    └── frame_0096.bin (65536 bytes)
    └── frame_0097.bin (65536 bytes)
... (1155 more files/directories)
`