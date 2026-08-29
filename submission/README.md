# Day 18 Lakehouse Lab — Submission

- **Sinh viên:** Lê Hoàng Nam
- **Mã sinh viên:** 2A202600965
- **Path thực hiện:** Lightweight (`deltalake` + DuckDB + Polars)

## Core deliverables

| Notebook | Kết quả chính | Trạng thái |
|---|---|---|
| `01_delta_basics.ipynb` | Delta log hiện diện; bad schema bị chặn; thêm cột `tier` bằng schema merge | Đạt |
| `02_optimize_zorder.ipynb` | 200 → 55 files; speedup 24.8×; files-pruned ratio 55× | Đạt |
| `03_time_travel.ipynb` | MERGE 100K: 0.21 s; RESTORE: 0.13 s; 5 versions; bad rows = 0 | Đạt |
| `04_medallion.ipynb` | Bronze 200,000 → Silver 190,052; Gold 7 ngày × 3 models = 21 rows | Đạt |

## Evidence

- `screenshots/01_lakehouse_tree.png`: cấu trúc Bronze/Silver/Gold và các bảng Delta.
- `screenshots/02_delta_log.png`: nội dung transaction log của NB1.
- `screenshots/03_results_summary.png`: tổng hợp các chỉ số rubric từ notebook output.
- Bốn notebook trong `notebooks/` đã được chạy và giữ nguyên output cells.

## Bonus

Thiết kế kiến trúc LLM observability ở quy mô 1 tỷ request/ngày nằm tại
`bonus/ARCHITECTURE.md`.
