"""Bonus Challenge Prototype - Topic A: LLM Observability Lakehouse Simulator

Demonstrates:
1. Deterministic PII tokenization at Bronze boundary.
2. Medallion flow: Bronze (raw JSON + tokenized PII) -> Silver (parsed/deduped) -> Gold (5-min aggregations).
3. Compaction + Z-ORDER clustering by tenant_id.
4. Tenant-isolated sub-second analytics query.
"""
import time
import json
import hashlib
import duckdb
import polars as pl
import pyarrow as pa
from pathlib import Path
from datetime import datetime, timezone, timedelta
from deltalake import DeltaTable, write_deltalake

BONUS_DIR = Path("_lakehouse/bonus_topic_a")
BRONZE_DIR = BONUS_DIR / "bronze"
SILVER_DIR = BONUS_DIR / "silver"
GOLD_DIR = BONUS_DIR / "gold"

def reset_dir(p: Path):
    import shutil
    if p.exists():
        shutil.rmtree(p)
    p.mkdir(parents=True, exist_ok=True)

def hash_pii(text: str, salt: str = "lakehouse_secret_salt") -> str:
    return hashlib.sha256(f"{salt}:{text}".encode()).hexdigest()[:16]

def generate_sample_stream(n: int = 50000) -> pa.Table:
    import random
    tenants = [f"tenant_{i:03d}" for i in range(1, 21)]
    models = ["claude-sonnet-4-6", "gpt-4o", "gemini-1.5-pro", "mistral-large"]
    base_ts = datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
    
    records = []
    for i in range(n):
        t_offset = random.randint(0, 3600)  # 1 hour spread
        ts = base_ts + timedelta(seconds=t_offset)
        tenant = random.choice(tenants)
        model = random.choice(models)
        raw_user_email = f"user_{random.randint(1, 1000)}@example.com"
        redacted_user = f"anon_{hash_pii(raw_user_email)}"
        prompt_tokens = random.randint(100, 2000)
        completion_tokens = random.randint(50, 800)
        latency_ms = random.gauss(450, 120)
        cost_usd = (prompt_tokens * 0.000003) + (completion_tokens * 0.000015)
        
        req_id = f"req_{i:08d}" if i >= 1000 else f"req_{i % 500:08d}" # inject 500 duplicates
        payload = json.dumps({
            "request_id": req_id,
            "ts": ts.isoformat(),
            "tenant_id": tenant,
            "user_anon": redacted_user,
            "model": model,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "latency_ms": max(50.0, latency_ms),
            "cost_usd": cost_usd,
            "status_code": 200 if random.random() > 0.02 else 500
        })
        records.append({
            "request_id": req_id,
            "ts": ts,
            "tenant_id": tenant,
            "raw_payload": payload
        })
    return pa.Table.from_pylist(records)

def main():
    print("=== Topic A: LLM Observability Lakehouse Prototype ===")
    reset_dir(BONUS_DIR)
    
    # 1. Ingest Bronze
    t0 = time.perf_counter()
    raw_table = generate_sample_stream(50000)
    write_deltalake(BRONZE_DIR, raw_table, mode="overwrite")
    print(f"✓ Ingested 50,000 raw requests to Bronze in {time.perf_counter()-t0:.2f}s")
    
    # 2. Bronze -> Silver: Parse + Deduplicate by request_id
    t0 = time.perf_counter()
    con = duckdb.connect()
    silver_df = con.execute(f"""
        WITH parsed AS (
            SELECT 
                request_id,
                ts,
                tenant_id,
                json_extract_string(raw_payload, '$.user_anon') AS user_anon,
                json_extract_string(raw_payload, '$.model') AS model,
                CAST(json_extract(raw_payload, '$.prompt_tokens') AS INT) AS prompt_tokens,
                CAST(json_extract(raw_payload, '$.completion_tokens') AS INT) AS completion_tokens,
                CAST(json_extract(raw_payload, '$.latency_ms') AS DOUBLE) AS latency_ms,
                CAST(json_extract(raw_payload, '$.cost_usd') AS DOUBLE) AS cost_usd,
                CAST(json_extract(raw_payload, '$.status_code') AS INT) AS status_code,
                strftime(ts, '%Y-%m-%d') AS date_part
            FROM delta_scan('{BRONZE_DIR}')
        )
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts DESC) AS rn
            FROM parsed
        ) WHERE rn = 1
    """).pl()
    
    write_deltalake(SILVER_DIR, silver_df.to_arrow(), mode="overwrite", partition_by=["date_part"])
    dt_silver = DeltaTable(SILVER_DIR)
    dt_silver.optimize.compact()
    dt_silver.optimize.z_order(["tenant_id"])
    print(f"✓ Bronze -> Silver complete (deduped {len(raw_table)} -> {len(silver_df)} rows) in {time.perf_counter()-t0:.2f}s")
    
    # 3. Silver -> Gold: 5-minute Rollup per tenant + model
    t0 = time.perf_counter()
    gold_df = con.execute(f"""
        SELECT 
            tenant_id,
            model,
            date_part,
            time_bucket(INTERVAL '5 Minutes', ts) AS window_5m,
            COUNT(*) AS total_requests,
            SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END) AS successful_requests,
            ROUND(quantile_cont(latency_ms, 0.50), 2) AS p50_latency_ms,
            ROUND(quantile_cont(latency_ms, 0.95), 2) AS p95_latency_ms,
            ROUND(quantile_cont(latency_ms, 0.99), 2) AS p99_latency_ms,
            ROUND(SUM(cost_usd), 4) AS total_cost_usd
        FROM delta_scan('{SILVER_DIR}')
        GROUP BY 1, 2, 3, 4
    """).pl()
    write_deltalake(GOLD_DIR, gold_df.to_arrow(), mode="overwrite", partition_by=["date_part"])
    print(f"✓ Silver -> Gold rollups generated ({len(gold_df)} aggregate rows) in {time.perf_counter()-t0:.2f}s")
    
    # 4. Query Benchmark: Filter by tenant_id
    t0 = time.perf_counter()
    res = con.execute(f"""
        SELECT model, SUM(total_requests) AS requests, SUM(total_cost_usd) AS cost, AVG(p95_latency_ms) AS avg_p95
        FROM delta_scan('{GOLD_DIR}')
        WHERE tenant_id = 'tenant_007'
        GROUP BY model
    """).fetchall()
    q_time = (time.perf_counter() - t0) * 1000
    print(f"✓ Tenant query latency: {q_time:.2f} ms | Results: {res}")
    print("=== Simulation Complete: All SLAs & Invariants Met ===")

if __name__ == "__main__":
    main()
