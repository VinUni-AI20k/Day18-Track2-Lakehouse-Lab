# Lakehouse Lab - Final Report

## 1. Environment Setup
- **OS**: Windows
- **Docker**: Version 29.4.0, Compose v5.1.1
- **Python**: 3.11+ (Local and Docker)
- **MinIO**: Storage for Spark path (S3A)

## 2. Path 1: Lightweight (deltalake + DuckDB + Polars)
- **Status**: Completed Successfully.
- **Data**: 200,000 rows generated in `_lakehouse/bronze/`.
- **Key Metrics**:
    - **NB1 (Basics)**: Schema enforcement blocked bad writes; `schema_mode="merge"` successfully evolved the table.
    - **NB2 (Optimize)**: 
        - Speedup: **6.3x** (Target ≥ 3x)
        - Files-pruned ratio: **55.0x** (Target ≥ 10x)
    - **NB3 (Time Travel)**: MERGE 100K rows in **0.14s**. RESTORE confirmed working.
    - **NB4 (Medallion)**: Bronze (200K) → Silver (190K) → Gold (24 rows, 8 dates x 3 models).

## 3. Path 2: Spark (PySpark + Delta Spark + MinIO)
- **Status**: Completed Successfully (after fixing `docker-compose.yml` permissions and `PYTHONPATH`).
- **Data**: 200,000 rows generated in MinIO bucket `bronze`.
- **Key Metrics**:
    - **NB1 (Basics)**: Delta transaction log verified via `DESCRIBE HISTORY`.
    - **NB2 (Optimize)**: 
        - Speedup: **17.2x** (Target ≥ 3x) - Spark/S3 architecture shows higher benefit from Z-Ordering.
    - **NB3 (Time Travel)**: 5 versions recorded in history.
    - **NB4 (Medallion)**: Successfully aggregated metrics across 7+ days into Gold table in MinIO.

## 4. Comparison & Observations
| Feature | Lightweight | Spark (Docker) |
|---|---|---|
| **Ease of Setup** | Extremely fast (~10s) | Slower (~5 min, depends on network) |
| **Performance** | Faster on small local datasets | Scales better, visible network I/O optimization |
| **Tooling** | DuckDB integration is zero-copy | Traditional Spark/Hadoop ecosystem |
| **Use Case** | Prototyping, small-to-medium data | Production-grade, Big Data |

## 5. Conclusion
Cả hai phương pháp đều cho kết quả nhất quán về cấu trúc dữ liệu Delta Lake. Việc sử dụng Delta Lake giúp đảm bảo tính ACID và khả năng quản lý dữ liệu linh hoạt (Time Travel, Schema Evolution) bất kể công cụ tính toán là gì (Spark hay Rust-based `deltalake`).
