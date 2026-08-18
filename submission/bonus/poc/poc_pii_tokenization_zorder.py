import hashlib
import hmac
from pathlib import Path
import polars as pl
import duckdb
from deltalake import DeltaTable, write_deltalake

# PoC: Enterprise Ingestion with PII Tokenization & Z-Order Clustering
def hmac_tokenize(value: str, salt: bytes = b'lab18_secret_salt_2026') -> str:
    return hmac.new(salt, value.encode('utf-8'), hashlib.sha256).hexdigest()[:16]


def main():
    print("=== BONUS CHALLENGE PoC: LLM Telemetry Ingestion ===")
    raw_df = pl.DataFrame({
        "request_id": [f"req_{i:06d}" for i in range(1000)],
        "tenant_id": [f"tenant_{(i % 10):03d}" for i in range(1000)],
        "model": ["claude-sonnet-4-6" if i % 2 == 0 else "claude-haiku-4-5" for i in range(1000)],
        "user_email": [f"user_{i}@enterprise.com" for i in range(1000)],
        "prompt_tokens": [120 + (i % 50) for i in range(1000)],
        "completion_tokens": [80 + (i % 30) for i in range(1000)],
        "latency_ms": [250 + (i * 3) % 400 for i in range(1000)],
        "status": ["ok" if i % 20 != 0 else "error" for i in range(1000)],
    })

    # 1. PIITokenization at Bronze Landing
    silver_df = raw_df.with_columns([
        pl.col("user_email").map_elements(hmac_tokenize, return_dtype=pl.Utf8).alias("tokenized_user_id")
    ]).drop("user_email")

    silver_path = "_lakehouse/scratch/bonus_silver_telemetry"
    write_deltalake(silver_path, silver_df.to_arrow(), mode="overwrite")

    # 2. Z-Order by (tenant_id, model)
    dt = DeltaTable(silver_path)
    dt.optimize.z_order(["tenant_id", "model"])
    print(f"Silver Table: {dt.count()} rows tokenized and Z-Ordered")

    # 3. Gold Rollup (5-minute dashboard aggregations)
    con = duckdb.connect()
    con.register("silver", dt.to_pyarrow_table())
    gold_arrow = con.sql("""
        SELECT
            tenant_id,
            model,
            count(*) AS requests,
            quantile_cont(latency_ms, 0.95) AS p95_latency_ms,
            sum(prompt_tokens) AS total_prompt_tokens,
            sum(completion_tokens) AS total_completion_tokens,
            avg(case when status != 'ok' then 1.0 else 0.0 end) AS error_rate
        FROM silver
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).arrow()
    gold_path = "_lakehouse/scratch/bonus_gold_metrics"
    write_deltalake(gold_path, gold_arrow, mode="overwrite")
    print(f"Gold Mart Created: {DeltaTable(gold_path).count()} rows")
    print(pl.from_arrow(DeltaTable(gold_path).to_pyarrow_table()).head(5))


if __name__ == "__main__":
    main()
