# Kết quả đo — Day 18 Lakehouse Lab

Đường chạy: **lightweight** (`deltalake` 1.6.2 + `pyiceberg` 0.11.1 + DuckDB 1.5.5 + Polars 1.43.2)
Môi trường: WSL2 Ubuntu 26.04 · Python 3.14.4 · `make run-all` → **8/8 PASS** · `make test` → **24 passed**

## Part A — Foundations (44 pts)

| NB | Tiêu chí rubric | Đích | Đo được | |
|---|---|---|---|---|
| 1 | `_delta_log/` JSON commits | có | có, `delta-rs:py-1.6.2` | ✅ |
| 1 | Schema enforcement chặn `age=str` | chặn | `Cast error: Cannot cast string 'thirty' to Int64` | ✅ |
| 1 | `schema_mode="merge"` thêm cột `tier` | có | 4 dòng, 3 null + 1 `premium` | ✅ |
| 2 | Small-file problem tái hiện | ≥ 100 file | **200 file** | ✅ |
| 2 | Speedup **hoặc** files-pruned | ≥3× / ≥10× | **10.3×** và **55.0×** (cả hai) | ✅ |
| 2 | `numFiles` giảm sau OPTIMIZE | giảm | 200 → 55 | ✅ |
| 3 | `history()` kể cả RESTORE | ≥ 5 | **5 version**, có dòng RESTORE | ✅ |
| 3 | MERGE upsert 100K | thành công | 100K (50K update + 50K insert) | ✅ |
| 3 | RESTORE xoá bad data | `score<0` = 0 | **0** | ✅ |
| 4 | Bronze/Silver/Gold trên đĩa | cả 3 | 14M / 11M / 128K | ✅ |
| 4 | Silver < Bronze (dedup) | giảm | 200,000 → **190,052** (−9,948) | ✅ |
| 4 | Gold ≥ 7 ngày × 3 model | ≥7 | **8 ngày × 3 model** | ✅ |

## Part B — Lakehouse 2026 (50 pts)

| NB | Tiêu chí rubric | Đích | Đo được | |
|---|---|---|---|---|
| 5 | Tạo bảng qua catalog, spec `day(ts)` | có | `ts_day = day(ts)` | ✅ |
| 5 | Hidden-partition pruning (lọc trên `ts`) | ≥ 5× | **10×** (10 → 1 file) | ✅ |
| 5 | Metadata:data byte ratio | báo cáo | 133.0 KB / 47.3 KB = **281.3%** | ✅ |
| 5 | Rename giữ `field_id`; ≥ 2 spec | ≥2 | `latency_millis` giữ `field_id=4`; spec `[1, 2]` | ✅ |
| 6 | **Job 1** Compaction | ≥ 10× | **18×** (200 → 11 file) | ✅ |
| 6 | **Job 2** Clustering skip | ≥ 50% | **90%** (1/10 file phải mở) | ✅ |
| 6 | **Job 3** Expiry | reclaim / 3 snap | Delta thu hồi **16.1 MB**; Iceberg 20 → **3** | ✅ |
| 6 | **Job 4** Orphans | 3 + manifest | **3 orphan** (21.2 KB) + 17 manifest list (36.9 KB) | ✅ |
| 6 | **Job 5** Checkpoint | có | `*.checkpoint.parquet` + `_last_checkpoint` | ✅ |
| 7 | Random-access amplification | ≥ 5× | **200×** (row group vs 1 blob) | ✅ |
| 7 | int8 nhỏ hơn; recall + topic fidelity | ≥3× | **5.8×** nhỏ hơn; recall@10 **0.904**; fidelity **1.000** | ✅ |
| 7 | Semantic search bằng SQL | on-topic | `array_cosine_similarity`, top-5 cùng topic | ✅ |
| 7 | **Lifecycle bug** | 0 in / >0 ex | lakehouse **0**, external index **8** ← VI PHẠM | ✅ |
| 8 | Silver theo `agent_version`; Gold 2 policy | 2 | `policy-v2`, `policy-v3` | ✅ |
| 8 | Pin version, replay khớp | khớp | replay đúng số step đã pin | ✅ |
| 8 | MCP: cache, input_required, task poll | 5→1 | **5 turn → 1 catalog read**; `input_required`; task `completed` | ✅ |
| 8 | 4 rổ Art.10 thành partition; loại UNCLASSIFIED | 4 | 4 rổ + `UNCLASSIFIED`; **1,666/2,000** dùng được, loại **334** | ✅ |

## Part C — Reproducibility (6 pts)

| Tiêu chí | Kết quả |
|---|---|
| `make test` xanh | **24 passed** |
| `make run-all` xanh từ `make setup` sạch | **8/8 passed in 41.9s** |

## Hai phát hiện đi ngược niềm tin phổ biến

1. **`VACUUM` không thấy orphan chưa từng commit.** Sau khi cắm 3 file parquet
   "do job crash để lại" (mtime 30 ngày trước), `vacuum(retention_hours=0)` dry-run
   trả về danh sách **không chứa file nào trong số đó**. `deltalake` chỉ thu hồi file
   đã bị *tombstone* trong transaction log; file chưa từng vào log thì log không biết
   nó tồn tại. Phải tự làm phép hiệu tập hợp *(file trên đĩa) − (file log tham chiếu)*
   kèm age guard.

2. **`expire_snapshots` không xoá file nào.** Snapshot 20 → 3, nhưng avro trên đĩa
   **40 → 40**, và metadata còn phình. Expiry chỉ làm file trở nên *unreferenced*;
   xoá chúng là việc của Job 4. Trên đường Python phải tự nối hai job, nếu không
   dung lượng không bao giờ giảm.

## Chi phí — vì sao các số này đáng quan tâm

| Tình huống | Chi phí |
|---|---|
| 200 file nhỏ, 50K query/ngày, chỉ tính S3 GET | **$4.00/ngày** (so với $0.08 khi 4 file) |
| Quên partition predicate kiểu Hive (512 MB/file, $5/TB, 10K query/ngày) | **$220/ngày** |
| Managed compaction 500 GB / 2M file, chạy hằng ngày | **$990/tháng**, trong đó **$240 (24%) do số lượng file** |
