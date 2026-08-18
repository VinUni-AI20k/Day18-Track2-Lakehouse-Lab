# Evidence — lightweight path

Rubric cho phép chọn 1 trong 2 dạng bằng chứng; đây là dạng thứ hai
(`tree _lakehouse/` + nội dung `_delta_log/*.json`).

| File | Chứng minh điều gì |
|---|---|
| `tree_lakehouse.png` | Layout Bronze/Silver/Gold trên đĩa; partition `agent_version=` và 5 rổ `provenance_bucket=` của NB8 là thư mục thật; 3 catalog Iceberg tách biệt |
| `delta_log_commit_json.png` | Transaction log Delta — cùng định dạng JSON mà Spark/Databricks ghi ra |

## Đọc `tree_lakehouse.png`

* `gold/llm_daily_metrics/date=2026-04-01…08` → 8 ngày (rubric đòi ≥ 7)
* `silver/agent_trajectories/agent_version=policy-v2|v3` → Silver partition theo `agent_version`
* `silver/training_corpus_governed/provenance_bucket=*` → đủ 4 rổ EU AI Act Art. 10
  (`licensed`, `public_domain`, `scraped_optout_checked`, `synthetic`) cộng `UNCLASSIFIED`
  tách riêng để loại khỏi tập huấn luyện
* `scratch/events_smallfiles [323 entries]` cạnh `scratch/maint_events` (10 file) →
  small-file problem trước và sau compaction

## Đọc `delta_log_commit_json.png`

Ảnh chứa hai commit của cùng một bảng. **Đọc commit `…0000` trước** (nửa dưới
ảnh), rồi tới `…0001` (nửa trên) — thứ tự trong ảnh là ngược, do thứ tự chạy lệnh:

| Commit | `metaData.schemaString` | Ý nghĩa |
|---|---|---|
| `…0000.json` | 4 field: `id, name, age, city` | bảng lúc mới tạo (`mode: Overwrite`) |
| `…0001.json` | 5 field: **thêm `tier`** | `schema_mode="merge"` mở rộng schema (`mode: Append`) |

Chênh lệch 4 → 5 field chính là schema evolution opt-in của NB1. Lần ghi
`age="thirty"` bị schema enforcement chặn nên **không sinh commit nào** — đó là
lý do log chỉ có 2 commit dù notebook thực hiện 3 lần ghi.

Hai chi tiết khác đáng chú ý:

* `protocol` chỉ có ở commit `0000` — action này chỉ ghi một lần lúc tạo bảng
* `add.stats` chứa `minValues`/`maxValues`/`nullCount` — đây là min/max stats mà
  engine đọc để **bỏ qua** file mà không cần mở, cơ chế đứng sau tỉ lệ skip 90%
  đo được ở NB6
* `engineInfo: "delta-rs:py-1.6.2"` xác nhận đường lightweight, delta-rs 1.x
