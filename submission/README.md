# Day 18 Lakehouse Lab - Submission

**Sinh viên:** Nguyễn Huy Hưng  
**Môi trường:** Lightweight path, Python 3.12.3

## Kết quả

| Hạng mục | Kết quả |
|---|---:|
| Offline smoke test | 9/9 passed |
| Pytest | 24/24 passed |
| Headless notebook run | 8/8 passed |
| Executed notebooks | 8/8 có output, 0 error |

Tám notebook đã được thực thi và lưu output trong thư mục `notebooks/`.
Các khối kiểm tra cuối mỗi notebook đều hoàn thành mà không phát sinh lỗi.

## Minh chứng

| Notebook | Ảnh kết quả |
|---|---|
| NB1 — Delta basics | [Xem ảnh](screenshots/nb01-delta-basics-pass.png) |
| NB2 — Optimize và Z-order | [Xem ảnh](screenshots/nb02-optimize-zorder-pass.png) |
| NB3 — Time travel | [Xem ảnh](screenshots/nb03-time-travel-pass.png) |
| NB4 — Medallion | [Xem ảnh](screenshots/nb04-medallion-pass.png) |
| NB5 — Iceberg catalog | [Xem ảnh](screenshots/nb05-iceberg-catalog-pass.png) |
| NB6 — Maintenance | [Xem ảnh](screenshots/nb06-maintenance-pass.png) |
| NB7 — Vectors và multimodal | [Xem ảnh](screenshots/nb07-vectors-multimodal-pass.png) |
| NB8 — Agents và provenance | [Xem ảnh](screenshots/nb08-agents-provenance-pass.png) |

Minh chứng storage:

- [Cấu trúc thư mục Lakehouse](screenshots/lakehouse-directory-tree.png)
- [Delta transaction log](screenshots/delta-transaction-log.png)
- [Danh sách đầy đủ các file trong Lakehouse](screenshots/lakehouse_tree.txt)
- [Delta transaction log mẫu (JSON)](screenshots/delta_log_sample.json)

## Reflection

Nội dung reflection không quá 200 từ: [REFLECTION.md](REFLECTION.md).

## Checklist

- [x] `make smoke` hoàn thành.
- [x] `make test` hoàn thành 24 tests.
- [x] `make run-all` hoàn thành 8 notebooks.
- [x] Tám notebook đã lưu output.
- [x] Có ảnh kết quả của tám notebook.
- [x] Có ảnh cây `_lakehouse/` và Delta log JSON.
- [x] Có bản text/JSON của cấu trúc storage và transaction log để kiểm tra trực tiếp.
- [x] Reflection không quá 200 từ.
- [x] Không đưa `.venv/` và dữ liệu `_lakehouse/` vào bài nộp.
