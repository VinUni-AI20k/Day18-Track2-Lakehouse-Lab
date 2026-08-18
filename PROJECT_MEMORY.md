# Project Memory — Day 18 Lakehouse Lab

> Đây là bộ nhớ làm việc lâu dài cho repository. Đọc file này trước khi tiếp tục một phiên mới; sau mỗi thay đổi làm ảnh hưởng đến cấu trúc, runtime, notebook, dependency, test hoặc cách nộp bài thì cập nhật file này cùng thay đổi đó.
>
> File này là bản đồ và ngữ cảnh vận hành, không thay thế source code. Khi có mâu thuẫn, ưu tiên mã nguồn/test hiện tại, sau đó đến `Makefile`/`requirements.txt`, rồi cập nhật lại README/rubric và file memory cho đồng nhất.

## 0. Quy tắc duy trì memory

Mỗi phiên làm việc nên:

1. Đọc phần này và chạy `git status --short --branch` trước khi sửa.
2. Xác định file nguồn nào đã thay đổi; không suy luận trạng thái mới chỉ từ memory cũ.
3. Cập nhật các mục liên quan: tree, dependency, command, data contract, notebook contract, test, known issue và verification status.
4. Đổi `Last reviewed` và commit baseline ở phần dưới.
5. Chạy tối thiểu kiểm tra phù hợp với phạm vi thay đổi; ghi rõ lệnh nào đã chạy và lệnh nào chưa chạy.

Hiện chưa có Git hook hay automation tự cập nhật file này. Việc “cập nhật mỗi khi dự án thay đổi” hiện được thực hiện như một quy ước cộng tác: mọi agent/người sửa code phải cập nhật file trong cùng worktree/commit.

## 1. Nhận diện dự án

- Tên: **Day 18 — Lakehouse Lab (Track 2)**.
- Chương trình: `AICB-P2T2 · Ngày 18 · Data Lakehouse Architecture`.
- Mục tiêu: một lab thực hành về Delta Lake, tối ưu lưu trữ, time travel, medallion architecture, Iceberg/catalog, maintenance, vector/multimodal và agent provenance.
- Dạng dự án: repository giáo dục, nhưng có pipeline và regression tests mô phỏng các vấn đề production.
- Nguyên tắc thiết kế: đường lightweight mặc định phải chạy offline, không API key, không Docker, không JVM, không model download và không tải DuckDB extension.
- Phạm vi chính: 8 notebook lightweight; 4 notebook Spark tương ứng cho NB1–NB4; script tạo dữ liệu, smoke test, test suite và student simulator.
- Ngôn ngữ/tài liệu: mã nguồn chủ yếu Python; hướng dẫn chính bằng tiếng Việt xen kẽ tiếng Anh.

### Baseline lúc rà soát

- Ngày rà soát: `2026-08-18` (timezone `Asia/Bangkok`).
- Branch: `main`.
- HEAD lúc bắt đầu rà soát: `495ad3c` — `Add a native Apple container runner for the Spark path`.
- `main` đồng bộ với `origin/main` ở baseline trên.
- Worktree sạch trước khi tạo file này; `PROJECT_MEMORY.md` là artifact mới do yêu cầu hiện tại tạo ra và chưa được commit.
- Không có `.venv`, `_lakehouse`, `submission` hay notebook `.ipynb` được generate trong worktree lúc rà soát.
- Lịch sử gần nhất đáng chú ý:
  - `495ad3c`: thêm `scripts/apple_container.sh` cho Apple `container`.
  - `8c56635`: sửa 4 lỗi khiến Spark/Docker path không khởi động.
  - `46fb9a6`: thêm student simulator, bỏ dependency Spark chết.
  - `24a5391`: nâng lab từ 4 lên 8 notebook, delta-rs 1.x, Python 3.14.
  - `98ec440`: thêm lightweight path làm mặc định, giữ Spark/Docker là tùy chọn.

### Source of truth và các lệch đã biết

- Mã nguồn hiện tại có **24 hàm test** trong `tests/test_lab18.py`. `README.md`, `rubric.md` và một số lệnh in ra vẫn ghi “22 pytest”; đây là lệch tài liệu, không phải số test thực tế.
- `Makefile`/README hiện mô tả lightweight là đường mặc định: `make setup` tạo venv Python và cài `requirements.txt`.
- `setup.sh` vẫn là bootstrap **Docker-only** kiểu cũ: kiểm tra Docker Compose, dựng Spark/MinIO và chạy `scripts/verify.py`. Nó không tương đương với `make setup`; không dùng nhầm hai flow này.
- `notebooks-spark/` là bản minh họa/production-fidelity cho NB1–NB4, nhưng `scripts/run_all.py` chỉ chạy `notebooks/` lightweight.
- Spark notebooks không có assert cuối đầy đủ như lightweight notebooks; tiêu chí machine-checkable chính nằm ở lightweight path.

## 2. Cấu trúc repository

```text
.
├── README.md                         # Quick start, kiến trúc, deliverable, troubleshooting
├── rubric.md                         # Rubric 100 điểm, tiêu chí từng notebook
├── BONUS-CHALLENGE.md                # Architecture brief tiếng Việt
├── BONUS-CHALLENGE-EN.md             # Architecture brief tiếng Anh
├── Makefile                          # UX và command cho lightweight/Spark/Apple
├── setup.sh                          # Bootstrap Docker legacy
├── requirements.txt                  # Dependency lightweight, có major-version guards
├── pytest.ini                        # pytest testpaths/addopts
├── .gitignore                        # artifact local/generated không commit
├── docker/
│   └── docker-compose.yml            # MinIO + init buckets + Spark/Jupyter
├── notebooks/                        # 8 notebook lightweight dạng Jupytext .py
│   ├── _setup.py                     # bootstrap path tới scripts/
│   ├── 01_delta_basics.py
│   ├── 02_optimize_zorder.py
│   ├── 03_time_travel.py
│   ├── 04_medallion.py
│   ├── 05_iceberg_catalog.py
│   ├── 06_maintenance.py
│   ├── 07_vectors_multimodal.py
│   └── 08_agents_provenance.py
├── notebooks-spark/                  # 4 bản PySpark cho NB1–NB4
│   ├── 01_delta_basics.py
│   ├── 02_optimize_zorder.py
│   ├── 03_time_travel.py
│   └── 04_medallion.py
├── scripts/
│   ├── lakehouse.py                  # root/path/catalog/measurement helpers
│   ├── generate_data_lite.py         # Bronze LLM observability, 200K mặc định
│   ├── generate_data.py              # Bronze Spark/MinIO, 1M mặc định
│   ├── generate_ai_data.py           # corpus docs/blob/embedding/trajectory
│   ├── verify_lite.py                # offline smoke test
│   ├── verify.py                     # Spark + S3A + MinIO smoke test
│   ├── run_all.py                   # execute 8 lightweight notebook headless
│   ├── spark_session.py              # Spark + Delta + MinIO session factory
│   └── apple_container.sh            # native Apple container runner
├── tests/
│   ├── test_lab18.py                 # 24 fast invariant/canary tests
│   └── simulate_students.py          # 12 kịch bản sử dụng “sai” của học viên
└── PROJECT_MEMORY.md                 # file memory lâu dài này
```

`*.ipynb`, `_lakehouse/`, `.venv/`, `.pytest_cache/`, `spark-warehouse/`, `metastore_db/`, `derby.log`, `_minio-data/` và các artifact local khác bị ignore. Notebook source chính thức là các file `.py` Jupytext percent, không phải `.ipynb` generated.

## 3. Kiến trúc và data layout

### 3.1 Lightweight path — mặc định

```text
scripts/generate_data_lite.py ──┐
                                ├─> _lakehouse/bronze/llm_calls_raw
                                │       └─ NB4 parse/dedup ─> silver/llm_calls
                                │                              └─ aggregate ─> gold/llm_daily_metrics
                                │
scripts/generate_ai_data.py ────┼─> _lakehouse/bronze/docs_multimodal
                                │       └─ NB7 scratch tables / vectors / CDF
                                └─> _lakehouse/bronze/agent_traces
                                        └─ NB8 silver/gold trajectories + provenance

_lakehouse/iceberg/<catalog-name>/
    catalog.db                         # PyIceberg SqlCatalog trên SQLite
    warehouse/<namespace>/<table>/     # metadata/ + data/
```

- `scripts/lakehouse.py` đặt `ROOT` từ `LAKEHOUSE_ROOT`; mặc định là `_lakehouse` cạnh repo.
- `path(layer, table)` tạo `<ROOT>/<layer>/<table>`. Các layer quy ước: `bronze`, `silver`, `gold`, `scratch`.
- Delta table là thư mục có `_delta_log/` JSON/checkpoint và Parquet data files.
- `catalog(name)` tạo một SQLite-backed `pyiceberg.catalog.sql.SqlCatalog` riêng dưới `ROOT/iceberg/<name>`; warehouse là local file URI.
- Các notebook dùng catalog riêng: NB5=`nb5`, NB6=`nb6`, NB8=`nb8`; smoke=`smoke`. Việc cô lập theo tên ngăn notebook đang chạy bị notebook khác reset.
- DuckDB luôn nhận Arrow table từ `DeltaTable(...).to_pyarrow_table()` hoặc `to_arrow(...)`; không gọi `delta_scan()` vì extension có thể tải qua mạng.
- Polars dùng cho sinh dữ liệu/hiển thị; PyArrow là cầu nối cho Delta, DuckDB và Iceberg.

### 3.2 Spark path — tùy chọn

```text
Docker/Apple container
    MinIO buckets: lakehouse, bronze, silver, gold
       ▲ S3A
    Spark/Jupyter + delta-spark + lightweight packages
```

- Compose dùng `minio/minio:latest`, `minio/mc:latest`, `quay.io/jupyter/pyspark-notebook:spark-3.5.0`.
- Spark ghi Delta vào `s3a://lakehouse/...`, generator ghi Bronze vào `s3a://bronze/llm_calls_raw`; định dạng Delta tương thích với lightweight, nhưng physical storage là MinIO chứ không phải `_lakehouse` local.
- `scripts/spark_session.py` bật `DeltaSparkSessionExtension`, `DeltaCatalog`, S3A path-style access, access key/secret mặc định `minioadmin`, và `spark.sql.shuffle.partitions=8`.
- `MINIO_ENDPOINT` mặc định `http://minio:9000` cho Compose; Apple runner inject IP MinIO vì Apple `container` không tự resolve tên service.
- `SPARK_IVY_DIR` nếu có sẽ được đặt vào `spark.jars.ivy`; Compose dùng `/home/jovyan/.cache/ivy` để giữ quyền ghi của `jovyan`.
- Compose nối `PYTHONPATH` với `/workspace/scripts`, Spark Python và py4j; không được ghi đè mất đường dẫn Spark.

### 3.3 Important implementation caveat về S3

`lakehouse.py` có comment mô tả việc đặt `LAKEHOUSE_ROOT=s3://...`, nhưng `ROOT` thực tế luôn được tạo bằng `pathlib.Path`; `path`, `reset`, `du` và `count_files` đều dùng thao tác filesystem local (`mkdir`, `shutil.rmtree`, `Path.rglob`). Vì vậy khả năng S3 của helper chưa phải một contract end-to-end đã được chứng minh. Trước khi dùng notebook lightweight với S3 trực tiếp, cần sửa/kiểm thử helper; Spark path hiện là đường S3/MinIO được hỗ trợ rõ ràng.

## 4. Cài đặt và vận hành

### 4.1 Lightweight commands

Yêu cầu danh nghĩa: Python `3.10 <= version < 3.15` (3.10–3.14). `make setup`:

1. dùng `uv venv .venv --python '>=3.10,<3.15'` nếu có `uv`, nếu không dùng `python3 -m venv .venv`;
2. kiểm tra version Python;
3. cài `requirements.txt` bằng `uv pip` hoặc `.venv/bin/pip`;
4. convert `notebooks/*.py` sang `.ipynb` bằng Jupytext.

Các lệnh chính:

| Lệnh | Tác dụng | Artifact/điều kiện |
|---|---|---|
| `make setup` | Tạo `.venv`, cài lightweight dependencies, generate notebook | không Docker/JVM |
| `make smoke` | Chạy `scripts/verify_lite.py` | Delta, CDF, Iceberg, vector, Arrow bridge |
| `make test` | Chạy pytest suite | code thực tế hiện có 24 test functions |
| `make data` | Chạy `generate_data_lite.py` | Bronze LLM 200K rows mặc định |
| `make data-ai` | Chạy `generate_ai_data.py` | docs, blobs, agent traces |
| `make run-all` | Chạy 8 `notebooks/*.py` theo thứ tự lexical | cổng chấm machine-checkable chính |
| `make simulate` | Chạy student simulator | 12 scenarios; S10/S11 build venv chậm |
| `SIM_FAST=1 make simulate` | Bỏ S10 Python 3.10 và S11 plain pip | nhanh hơn |
| `make lab` | Mở Jupyter Lab trên `http://localhost:8888` | token rỗng, root=`notebooks` |
| `make clean` | Xóa `.venv`, `_lakehouse`, checkpoints, pytest cache | destructive local cleanup |

Flow tối thiểu chuẩn:

```bash
make setup
make smoke
make data
make data-ai
make test
make run-all
```

`run_all.py` chạy từng notebook bằng chính `sys.executable`, capture stdout/stderr, in 1500 ký tự cuối khi fail và trả exit code 1 nếu bất kỳ notebook nào fail. Nó không chạy `notebooks-spark/`.

### 4.2 Docker Compose commands

```bash
make spark-up       # dựng MinIO + minio-init + Spark/Jupyter
make spark-smoke    # verify.py bên trong container Spark
make spark-data     # generate_data.py, 1M Bronze qua Spark
make spark-down     # dừng, giữ named volumes
make spark-clean    # dừng và xóa volumes MinIO/Ivy
```

- Jupyter: `http://localhost:8888`, token `lakehouse`.
- MinIO console: `http://localhost:9001`, credentials mặc định `minioadmin/minioadmin`.
- Cổng: MinIO API 9000, console 9001, Jupyter 8888, Spark UI 4040.
- Lần đầu có thể tải image và Maven JAR/dependency; đây là ngoại lệ so với lightweight offline path.
- Compose có best-effort conversion `.py`→`.ipynb`; bind mount không writable không được phép giết container.

### 4.3 Apple `container` commands

Yêu cầu macOS 15+, Apple silicon, cài `container`, chạy kernel recommendation rồi `container system start`.

```bash
make apple-up
make apple-smoke
make apple-data
make apple-status
make apple-down       # giữ _minio-data/
make apple-clean      # xóa luôn _minio-data/
```

`scripts/apple_container.sh` dựng MinIO và Spark bằng `container run`, kiểm tra container ID chính xác bằng JSON, set Spark mặc định 4 CPU/6 GB và MinIO 2 CPU/2 GB. Có hai readiness gates: Jupyter đã lên và `import delta, pyspark` thực sự thành công. `SPARK_MEM`, `SPARK_CPUS`, `MINIO_MEM`, `MINIO_CPUS` có thể override.

### 4.4 Windows/host hiện tại

Makefile và simulator dùng cú pháp Unix (`rm`, `/bin/python`, `rsync`, shell recipe); Apple runner dùng Bash. Tại lần rà soát này PowerShell có `uv 0.12.0`, nhưng `python` không có trong PATH, `py.exe` bị `Access is denied`, và không thấy `make`/`docker`/`container` trong command lookup. Vì vậy chưa chạy được `make test`/`make run-all` trên host này; dùng WSL/Linux/macOS hoặc thiết lập Python/Make phù hợp trước khi verify.

## 5. Dependencies và version assumptions

`requirements.txt` là lightweight contract:

- `deltalake>=1.0,<2.0`: delta-rs 1.x; code cần `file_uris`, `load_cdf`, `repair`, `create_checkpoint`, `compact_logs`.
- `pyiceberg[sql-sqlite,pyarrow]>=0.9,<1.0`: Iceberg local catalog/metadata/partition/maintenance.
- `duckdb>=1.1,<2.0`: SQL và `array_cosine_similarity` core; không tải extension.
- `polars>=1.13.2,<2.0`.
- `pyarrow>=17,<26`.
- `numpy>=1.26,<3.0`.
- `jupyterlab>=4.3,<5.0`, `jupytext>=1.16,<2.0`.
- `pytest>=8.0,<10.0`.

Docker command còn cài `delta-spark==3.2.0`, `jupytext==1.16.4`, `faker==30.3.0` và các lightweight packages. `faker` không nằm trong `requirements.txt` và hiện không được generator lightweight import.

## 6. Notebook contracts — lightweight path

Mỗi notebook là Jupytext percent `.py`, tự thêm `scripts/` bằng `import _setup`, reset các output cần thiết để chạy lại được, in số đo và thường kết thúc bằng `assert`. Không phụ thuộc thứ tự notebook; NB4/NB7/NB8 tự sinh input data nếu thiếu.

### NB1 — `notebooks/01_delta_basics.py`

- Mục tiêu: Delta transaction log, schema enforcement/evolution, DuckDB offline.
- Table: `scratch/users_delta`.
- Hành trình:
  1. overwrite 3 user rows (`id`, `name`, `age`, `city`);
  2. đọc `DeltaTable`, in history và `_delta_log/*.json`;
  3. append `age="thirty"`, phải bị chặn;
  4. append thêm `tier` với `schema_mode="merge"`;
  5. đăng ký Arrow table vào DuckDB, group theo `tier`.
- Pass checks: có ít nhất 2 JSON commit; schema có `tier`; DuckDB có 2 tier groups; try/except đã chứng minh write sai bị block.
- Gotcha: DuckDB không đọc Delta bằng extension; Arrow registration là lựa chọn offline/zero-copy.

### NB2 — `notebooks/02_optimize_zorder.py`

- Mục tiêu: reproduce small-file problem, compact và Z-order; đo file pruning thay vì tin wall-clock.
- Table: `scratch/events_smallfiles`.
- Tạo 200 append batches × 5,000 rows = 1,000,000 rows; `user_id` random trong 1–100,000; target point query `user_id=4242`, `kind='purchase'`.
- Đo trước bằng `DeltaTable.to_pyarrow_table(filters=...)`, median 3 runs.
- `dt.optimize.compact(target_size=256 KiB)` rồi `dt.optimize.z_order(["user_id"], target_size=256 KiB)`; target nhỏ cố ý giữ nhiều file cho skipping.
- Đọc stats `minValues/maxValues` trong `_delta_log` để đếm số file range chứa target.
- Pass: file count giảm; wall-clock speedup ≥3× **hoặc** `files_after / hits ≥10×`; hit ranges không quá `max(2, files_after//4)`.
- API phụ thuộc delta-rs 1.x: dùng `file_uris()`, không dùng API cũ `files()`.

### NB3 — `notebooks/03_time_travel.py`

- Mục tiêu: version history, schema evolution, MERGE, RESTORE.
- Table: `scratch/customers_tt`.
- v0: 100K customers; v1: thêm `tier` bằng overwrite schema; v2: merge source 100K rows (50K updates cho IDs 50K–99,999 và 50K inserts cho IDs 100K–149,999); v3: append 50 bad rows `score=-1`.
- `DeltaTable.merge(...).when_matched_update_all().when_not_matched_insert_all().execute()` là lightweight equivalent của `MERGE INTO`.
- `restore(2)` tạo transaction mới (thường v4), không xóa history cũ; filter `score < 0` phải trả 0.
- Pass: history cuối ≥5 versions, có operation RESTORE, có MERGE, bad rows = 0.
- `versionAsOf` equivalent: `DeltaTable(path, version=N)`.

### NB4 — `notebooks/04_medallion.py`

- Mục tiêu: LLM observability Bronze→Silver→Gold.
- Input: `bronze/llm_calls_raw`; nếu thiếu thì import và gọi `generate_data_lite.main()`.
- Output: `silver/llm_calls`, partition theo `date`; `gold/llm_daily_metrics`, partition theo `date`.
- Bronze giữ `request_id`, `ts`, `raw_json`; generator tạo khoảng 5% duplicate request IDs và timestamp trong 7 UTC days.
- Silver dùng DuckDB JSON parse: model/user/usage/latency/status, `ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts)`, giữ rn=1 và model non-null.
- Gold group `(date, model)`, tính p50/p95 latency, token sums, error rate và `cost_usd` theo cost table illustrative; Z-order Gold theo `model`.
- Pass hiện tại: `silver_n < bronze_n`, Gold có ≥7 dates. Notebook in số model/rows và cost nhưng không assert riêng `n_models==3` hay cost non-zero; đó là deliverable/rubric kỳ vọng cần kiểm tra khi sửa.
- Generator lightweight dùng seed 42, default 200K; có thể truyền số rows làm argv.

### NB5 — `notebooks/05_iceberg_catalog.py`

- Mục tiêu: catalog là control plane, hidden partitioning, metadata tree, field IDs và partition evolution.
- Catalog: `ROOT/iceberg/nb5`, namespace `lake`, table `lake.llm_events`.
- Schema ban đầu: required `event_id`, required `ts`, optional `model`, `latency_ms`, `cost_usd`.
- Thêm partition spec `day(ts)` tên derived `ts_day`; user query chỉ lọc `ts`.
- Append 10 daily batches ×500 rows, tạo 10 snapshots/data files; `plan_files()` đo no-filter vs one-day và assert pruning ≥5×.
- Đi qua 3-tier metadata: metadata JSON → manifest lists → manifest files/data files; in metadata/data byte ratio.
- Add `tier`, rename `latency_ms`→`latency_millis`; assert field ID của rename vẫn là `4`, rows cũ có `tier=NULL`.
- Thêm spec thứ hai `identity(model)` tên `model_id`, append day 11 với tier; old/new partition specs cùng tồn tại, table vẫn đọc đủ 5,500 rows.
- Pass: pruning ≥5×, ≥10 snapshots, field ID stable, ≥2 spec IDs, all rows readable.
- Catalog reset chỉ xoá `nb5`, không chạm `nb6`/`nb8`.

### NB6 — `notebooks/06_maintenance.py`

- Mục tiêu: 4 maintenance jobs bắt buộc + checkpoint/log rewrite.
- Delta table: `scratch/maint_events`.
- Ingest 200 micro-batches ×500 rows =100K rows; mỗi append tạo small file, payload rộng 80 ký tự.
- Job 1: compact target 1 MiB; in file/data/log/row metrics trước–sau, yêu cầu ≥10× ít file.
- Job 2: Z-order theo `user_id=12345`; đọc add-action min/max stats, yêu cầu ≥50% file có thể skip.
- Job 3: `vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)` để demo reclaim tombstoned bytes. Đây là lab-only; production nên giữ ≥168h/7 ngày.
- Job 4 Delta: trồng 3 Parquet orphan cũ 30 ngày, chứng minh delta-rs `VACUUM` không thấy file chưa từng commit, sau đó `find_orphans()` tính `disk files - referenced live metadata` với age guard 24h và xóa đúng 3.
- Job 5: `create_checkpoint()`; phải có `*.checkpoint.parquet` và `_last_checkpoint`.
- Iceberg catalog: `nb6`, table `lake.maint`, 20 append snapshots/2,000 rows; expire snapshots giữ 3; `expire_snapshots` chỉ làm metadata và để stranded `snap-*.avro`; sweep set difference rồi kiểm tra data còn 2,000 rows.
- Pass checks bao gồm compaction, clustering, Delta vacuum bytes, 3 orphans removed/no orphans, checkpoint, Iceberg 3 snapshots, manifest sweep và data intact.
- Không xem `retention_hours=0` là production recipe.

### NB7 — `notebooks/07_vectors_multimodal.py`

- Mục tiêu: inline blob vs pointer, vector in table, quantization và lifecycle consistency.
- Nếu thiếu `bronze/docs_multimodal`, gọi `generate_ai_data.main()`.
- Corpus mặc định: 2,000 docs, 8 topics, embedding 256-dim unit vectors, 200 binary blobs ×64 KiB ở `ROOT/blobs`.
- Scratch tables: `media_inline`, `media_pointer`, `emb_f32`, `emb_int8`, `vector_index_external`, `docs_intable`, `docs_cdf`.
- Inline/pointer: đo compressed column bytes từ Parquet footer; analytical projection không cần đọc blob.
- Random read: lấy row group bytes của inline table so với một blob; amplification phải ≥5×.
- Quantization: symmetric int8 `round(emb*127)`, ghi FixedSizeList int8; disk phải ≥3× nhỏ hơn, exact recall@10 ≥0.80, topic fidelity ≥0.95.
- Semantic search: DuckDB core `array_cosine_similarity(emb::FLOAT[dim], query::FLOAT[dim])`, top-5 phải có ít nhất 3 cùng topic với query.
- Gotcha: Delta protocol đọc FixedSizeList float thành variable-length list; phải cast `FLOAT[dim]` lúc query.
- Lifecycle bug: external copy không nhận delete `user_042`; current in-table hits =0 nhưng external hits >0. Sau đó CDF trên `docs_cdf` phải phát đủ delete events.
- Vector DB nếu cần chỉ là derived/rebuildable index; lakehouse row là system of record.

### NB8 — `notebooks/08_agents_provenance.py`

- Mục tiêu: trajectories, MCP 2026-07-28-shaped boundary và EU AI Act Art. 10 provenance.
- Nếu thiếu `bronze/agent_traces` hoặc docs thì gọi `generate_ai_data.main()`.
- Trajectory Silver: `silver/agent_trajectories`, partition `agent_version`; session IDs `<150` thành `policy-v2`, còn lại `policy-v3`.
- Trajectory Gold: `gold/agent_performance`, rollup success/steps/cost/latency; phải có cả 2 policy.
- Training run lưu `run_id`, policy, table path, Delta `table_version`, count; append thêm 400 rows rồi replay đúng pinned version và count.
- Catalog riêng `ROOT/iceberg/nb8`, table `lake.trajectories`; tạo MCP class local, không phải network server.
- MCP behaviors: `tools_list()` trả `ttlMs`/`cacheScope`; `list_tables` cache 60s khiến 5 turns chỉ có 1 catalog read; destructive `delete_rows` trả `input_required` trước `_meta.confirmed`; approved call là no-op nhưng trả ok; `submit_scan` trả task ID và `tasks_get()` eventually completed; metering theo tool.
- Provenance từ docs:
  - proprietary/commercial → `licensed`;
  - cc-by-4.0 → `public_domain`;
  - user-owned + consent → `scraped_optout_checked`;
  - synthetic + generator → `synthetic`;
  - còn lại → `UNCLASSIFIED`.
- Output: `silver/training_corpus_governed`, partition `provenance_bucket`; 4 bucket trainable phải tồn tại và UNCLASSIFIED bị loại.
- Model card pin Delta version, rows used, buckets used, excluded rows; delete `user_007` trong current version và in tension giữa time travel với right-to-erasure.
- Pass: 2 agent partitions, 2 policy Gold rows, pinned replay exact, MCP cache/confirmation/task, 4 governed partitions, unclassified >0, erasure current rows=0.
- Các ngày/điều khoản pháp lý trong notebook là nội dung giáo dục của lab, không phải tư vấn pháp lý độc lập.

## 7. Notebook Spark equivalents

Các file `notebooks-spark/01`–`04` dùng `spark_session.get_spark()` và path MinIO:

- NB1: `s3a://lakehouse/users_delta`; Spark `mergeSchema=true`.
- NB2: `s3a://lakehouse/events_smallfiles`; 200 ×500 rows; Spark SQL `OPTIMIZE ... ZORDER BY (user_id)`; chỉ đo speedup, chưa có pruning fallback/assert như lightweight.
- NB3: `s3a://lakehouse/customers_tt`; Spark `DeltaTable.merge`, `restoreToVersion(2)`, `DESCRIBE HISTORY` cuối.
- NB4: `s3a://bronze/llm_calls_raw` → `s3a://silver/llm_calls` → `s3a://gold/llm_daily_metrics`; `from_json`, `dropDuplicates`, percentile_approx, cost map, Spark OPTIMIZE.

`notebooks-spark/04` cần chạy `scripts/generate_data.py` trước; không self-heal như lightweight NB4. Spark path có thể dùng làm evidence vì cùng on-disk Delta format, nhưng không phải path mà `make run-all` chấm.

## 8. Scripts và helper responsibilities

### `scripts/lakehouse.py`

- `ROOT`: repo-local `_lakehouse` mặc định; đọc env lúc import.
- `path(layer, table)`: trả string absolute/local table path và tạo parent.
- `reset(*paths)`: `shutil.rmtree(..., ignore_errors=True)` để rerun idempotent.
- `catalog(name)`: tạo `SqlCatalog` SQLite với warehouse riêng.
- `reset_catalog(name)`: xoá đúng một named catalog.
- `namespace(cat, ns)`: create namespace idempotent.
- `du`, `human`, `count_files`: đo bytes/file count local.
- `to_arrow(relation)`: normalize DuckDB `.arrow()` thành `pyarrow.Table` cho DuckDB version trả `RecordBatchReader`.

### Generators

- `generate_data_lite.py`: seed 42; default 200K; 7-day span từ `2026-04-01`; models weighted 6/3/1; statuses 95/3/2; ~5% retry duplicate; writes Delta Bronze local.
- `generate_data.py`: Spark variant default 1M; build rows ở driver, 16 Spark partitions; writes `s3a://bronze/llm_calls_raw`; requires Spark container.
- `generate_ai_data.py`: seed 42; `make_corpus`, `make_blobs`, `make_trajectories`; docs include subject/source/license/consent/generator/blob URI/embedding from start; writes two Delta Bronze tables and blobs; no network/model/API.

### Verification

- `verify_lite.py`: xóa/ghi/đọc Delta, append/history/time travel, compact/vacuum API, CDF delete, Iceberg catalog + hidden pruning, DuckDB vector core và Arrow bridge. Cuối cùng dọn scratch smoke tables, nhưng không dọn named catalogs của notebook.
- `verify.py`: Spark boot, Delta write/read/time travel/history trên MinIO; dùng trong `spark-smoke`/Apple smoke.
- `run_all.py`: source-of-truth execution gate cho 8 lightweight notebooks.
- `spark_session.py`: shared Spark builder; env overrides `MINIO_ENDPOINT`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `SPARK_IVY_DIR`.
- `apple_container.sh`: lifecycle runner `up|down|clean|smoke|data|status|logs|shell`; không dùng Docker Compose; `clean` xóa host `_minio-data` mặc định.

## 9. Tests và simulator

### Pytest

`pytest.ini` đặt `testpaths=tests`, `-q --tb=short`, bỏ DeprecationWarning. `tests/test_lab18.py` hiện có 24 test functions, gồm:

- Python version và delta-rs 1.x API canary.
- DuckDB vector core, size helpers, DuckDB Arrow normalization.
- Generator deterministic/unit-norm/topic-clustered, 4 provenance buckets, synthetic generator, multi-step trajectories.
- Delta schema enforcement/evolution, compact, time travel, min/max stats, uncommitted orphan behavior, CDF delete events.
- Iceberg hidden pruning theo source column, rename giữ field ID, snapshot expiry metadata-only.
- Catalog isolation/reset không chạm sibling catalog.

Một số test là **behavior canary** có chủ ý:

- Nếu delta-rs thay đổi để `VACUUM` tự list và xóa uncommitted orphan, phải cập nhật test + giải thích NB6.
- Nếu PyIceberg `expire_snapshots` bắt đầu xóa stranded manifest files, phải cập nhật test + cặp Job 3/4 trong NB6.
- Nếu DuckDB bỏ vector function khỏi core, offline contract bị phá và phải đổi dependency/thiết kế.

### Student simulator

`tests/simulate_students.py` clone fresh bằng `rsync` rồi chạy:

1. notebooks ngược thứ tự;
2. rerun từng notebook;
3. bỏ qua generators;
4. cwd=`notebooks/`;
5. chạy song song các notebook có catalog;
6. smoke song song NB5;
7. proxy/DNS chặn mạng hoàn toàn;
8. CPU contention cho NB2;
9. generate/execute `.ipynb` bằng nbconvert;
10. Python 3.10;
11. plain pip không uv;
12. xóa `_lakehouse` giữa chừng rồi chạy tiếp.

`SIM_FAST=1` bỏ S10 và S11 vì phải dựng venv riêng. Hai regression chính đã được mô tả trong code/test: NB4 tự sinh Bronze khi thiếu; smoke dùng catalog riêng để không phá NB5 đang chạy.

## 10. Rubric và deliverables

Rubric tổng 100 điểm, Track-2 Daily Lab weight 30%:

- Part A foundations: 44 điểm, NB1–NB4.
- Part B Lakehouse 2026: 50 điểm, NB5–NB8.
- Part C reproducibility: 6 điểm, test + run-all.

Deliverable học viên theo `README.md`/`rubric.md`:

1. 8 notebook đã execute và giữ output.
2. `submission/screenshots/`: MinIO console hoặc tree `_lakehouse` + một `_delta_log/*.json`.
3. `submission/REFLECTION.md` ≤200 từ.
4. Tùy chọn `submission/bonus/ARCHITECTURE.md` (3–6 trang) và PoC.

Các target điểm chính:

| Notebook | Bằng chứng machine-checkable |
|---|---|
| NB1 | JSON transaction log, bad schema blocked, `tier` evolution |
| NB2 | ≥100 files trước optimize, file count giảm, speedup ≥3× hoặc pruning ≥10× |
| NB3 | history ≥5 có RESTORE, MERGE 100K, score<0 sau restore =0 |
| NB4 | Bronze/Silver/Gold, Silver < Bronze, Gold ≥7 ngày ×3 model và metrics |
| NB5 | catalog-created table, day(ts) hidden pruning ≥5×, field ID stable, ≥2 specs |
| NB6 | compaction ≥10×, clustering skip ≥50%, Delta expiry/orphans/checkpoint, Iceberg expiry+sweep |
| NB7 | amplification ≥5×, int8 ≥3× nhỏ, recall/fidelity, SQL search, lifecycle bug+CDF |
| NB8 | trajectory medallion, version pin/replay, MCP cache/approval/task, 4 Art.10 buckets |

## 11. Operational pitfalls cần nhớ

1. **Dùng delta-rs 1.x.** `DeltaTable.files()` là API cũ; code dùng `file_uris()`. Không hạ major version để “sửa” lỗi.
2. **Không gọi `delta_scan()` trong môi trường offline.** Đăng ký Arrow table vào DuckDB.
3. **Không đặt catalog Iceberg chung cho nhiều notebook.** `reset_catalog()` là destructive theo tên; dùng `nb5`, `nb6`, `nb8`, `smoke` đúng convention.
4. **Small-file benchmark phải giữ nhiều file sau compact.** NB2 dùng target 256 KiB; NB6 dùng 1 MiB. Nếu đổi target quá lớn, Z-order không còn đủ file để prune.
5. **Wall-clock NB2 nhiễu.** Pass cho phép file-pruning ratio; không sửa gate thành timing-only.
6. **Delta-rs `VACUUM` không thấy file chưa commit.** Orphan removal phải là set difference giữa Parquet trên disk và live metadata, có age guard.
7. **Iceberg expiry không đồng nghĩa xóa data/manifest file.** `expire_snapshots` làm unreferenced metadata; phải sweep orphan sau đó.
8. **`retention_hours=0` chỉ để demo.** Production phải chừa time-travel window và tránh xóa file reader đang dùng.
9. **Fixed-size vector không sống nguyên kiểu qua Delta.** Cast list thành `FLOAT[dim]` trong DuckDB.
10. **Vector index external có lifecycle risk.** Delete/erase phải đi qua CDF hoặc rebuild; không chỉ upsert các rows còn tồn tại.
11. **NB8 MCP là protocol shape, không phải server thật.** `delete_rows` được wire thành no-op; không coi notebook là authorization/data-plane implementation.
12. **Không chạy generators không kiểm soát.** `generate_data_lite.py` reset toàn bộ Bronze LLM; `generate_ai_data.py` reset docs/traces và ghi đè blobs cùng tên.
13. **`make clean` có tính destructive.** Chỉ chạy khi chấp nhận mất venv, local lakehouse và generated checkpoints.
14. **Simulator cần POSIX `rsync` và Unix-style venv path.** Không coi nó là native Windows test harness.
15. **Spark Compose interpolation nhạy với `$`.** Trong `docker-compose.yml`, shell parameter expressions trong command phải dùng `$$`; cả comment trong block cũng bị Compose interpolate.
16. **Spark PYTHONPATH phải nối, không thay thế.** Image expose pyspark qua Python path; thay hoàn toàn sẽ làm `import pyspark` hỏng.
17. **Ivy path phải writable.** Dùng `/home/jovyan/.cache/ivy`, không mount root-owned `~/.ivy2` không tồn tại.
18. **Jupyter “running” chưa có nghĩa dependency đã sẵn sàng.** Apple runner chờ thêm `import delta, pyspark`; Compose path cũng cài deps trong startup command.

## 12. Known issues / follow-up candidates

Đây là ghi nhận, không tự ý sửa nếu user chưa yêu cầu:

- Đồng bộ “22 pytest” thành “24 pytest” trong README/rubric/help text.
- Quyết định `setup.sh` có nên được chuyển thành wrapper của lightweight `make setup`, hay giữ rõ vai trò legacy Docker bootstrap và đổi tên/tài liệu.
- Bổ sung assert rõ ràng cho NB4 về đúng 3 models, cost/error columns không null/non-zero nếu rubric yêu cầu machine gate chặt hơn.
- Bổ sung assert/metric fallback pruning cho Spark NB2 nếu muốn Spark path có parity với lightweight rubric.
- Thiết kế lại `LAKEHOUSE_ROOT`/measurement helpers nếu thực sự muốn support `s3://...` trong lightweight path; hiện comment và implementation chưa đồng nhất.
- Xác định chính sách platform chính thức: Makefile/simulator hiện thiên về Linux/macOS/WSL, trong khi worktree review đang ở Windows.

## 13. Checklist cập nhật sau thay đổi

### Khi đổi code/runtime

- [ ] `git status --short --branch` và `git diff` đã được xem.
- [ ] Tree/file responsibility ở mục 2 và helper/script map ở mục 8 còn đúng.
- [ ] Dependency/version assumptions và setup command đã cập nhật.
- [ ] Notebook input/output/path/threshold/assert tương ứng đã cập nhật.
- [ ] README/rubric/test count không còn lệch do thay đổi.
- [ ] Nếu đổi API delta-rs/PyIceberg/DuckDB, cập nhật behavior canary.
- [ ] Nếu đổi cleanup/concurrency, chạy hoặc cập nhật simulator regression.

### Khi verify

- [ ] `make setup`
- [ ] `make smoke`
- [ ] `make test` (ghi số test thực tế)
- [ ] `make run-all`
- [ ] `make simulate` hoặc ít nhất `SIM_FAST=1 make simulate`
- [ ] Với Spark: `make spark-up`, `make spark-smoke`, `make spark-data`, notebook Spark, rồi `make spark-down`.
- [ ] Ghi rõ lệnh đã chạy, kết quả và environment vào phần “Baseline lúc rà soát” hoặc changelog ngắn bên dưới.

## 14. Lịch sử cập nhật memory

| Ngày | Baseline | Nội dung |
|---|---|---|
| 2026-08-18 | `495ad3c` | Tạo memory sau khi đọc toàn bộ repository; ghi nhận 8 lightweight NB, 4 Spark NB, scripts/tests, 2 runtime paths, 24 test functions thực tế, lệch `setup.sh`/`make setup`, S3 helper caveat và trạng thái verify host. |

