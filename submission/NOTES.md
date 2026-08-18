# Ghi chú bài nộp — sai khác so với repo gốc

Ngoài 8 notebook đã chạy và thư mục `submission/`, fork này còn khác upstream
(`495ad3c`) ở 3 file. Liệt kê đầy đủ ở đây để người chấm không phải đoán.

## 1. `.gitignore` — bắt buộc, nếu không thì không nộp được bài

Upstream ignore `notebooks/*.ipynb` và `notebooks-spark/*.ipynb`. Rubric lại
yêu cầu commit *"eight executed notebooks (output cells preserved)"*, nên hai
dòng ignore đó phải bỏ. Bù lại đã thêm `notebooks/_setup.ipynb` và
`notebooks-spark/spark-warehouse/` để không commit file sinh tự động.

## 2. Hai assert block bổ sung — vá chỗ lab còn thiếu

`rubric.md` viết *"each notebook ends in an `assert` block over its own pass
criteria"*, nhưng thực tế:

* **`notebooks/04_medallion.py` dòng 159–173** — upstream không có block nào.
  Đã thêm 5 check theo đúng 4 tiêu chí NB4 của rubric: ba lớp
  bronze/silver/gold tồn tại trên đĩa, `silver < bronze` (dedup đã chạy),
  gold trải ≥ 7 ngày, gold phủ ≥ 3 model, `cost_usd` và `error_rate` khác 0.
* **`notebooks/02_optimize_zorder.py` dòng 169** — thêm
  `"small-file problem reproduced": files_before >= 100`, đúng tiêu chí 3 điểm
  của rubric mà notebook chưa kiểm.

Cả hai chỉ làm điều kiện pass **chặt hơn**, không nới lỏng tiêu chí nào.

## 3. `docker/docker-compose.yml` — đường Spark, không dùng để chấm

* Dòng 60–61: `SPARK_DRIVER_MEMORY: 6g` + `PYSPARK_SUBMIT_ARGS`. Spark
  local-mode chạy driver và executor trong cùng một JVM, heap mặc định 1 GB,
  nên OOM khi `generate_data.py` dựng 1M dòng qua 16 Parquet writer song song.
* Dòng 99: thêm `pytest` vào deps của container để `make test` chạy được bên trong.

## Kết quả kiểm tra trước khi nộp

* `make run-all` — **8/8 notebook pass**, không có assert nào fail.
* `make test` — **24/24** trên macOS/Linux. Trên Windows là 23/24:
  `test_reset_catalog_does_not_touch_siblings` fail vì `shutil.rmtree` không
  xoá được `catalog.db` đang mở (`WinError 32`). Đây là hạn chế của Windows
  (không unlink được file đang mở), không phải lỗi lab.
  Lưu ý `rubric.md` ghi 22 tests còn suite thực tế collect 24 — tôi giữ nguyên
  `rubric.md` của upstream, chỉ nêu ở đây.
* `notebooks-spark/01_delta_basics.ipynb` mới chạy một phần, kèm theo làm bằng
  chứng phụ cho đường Spark (cùng với ảnh MinIO trong `screenshots/`). Bằng
  chứng chấm điểm là đường lightweight trong `notebooks/`.
