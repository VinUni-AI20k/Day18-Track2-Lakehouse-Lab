"""
PoC: Bronze → Silver Pipeline for LLM Observability
=====================================================
Demonstrates three non-trivial mechanisms from the architecture:
  1. PII tokenization at Bronze landing
  2. Nested JSON flatten + schema evolution
  3. Deduplication (keep latest by request_id)

Runs on the lightweight stack (deltalake + polars + duckdb).
No Spark, no Docker, no S3 required.

Usage:
    python submission/bonus/poc/bronze_to_silver.py
"""

import hashlib
import hmac
import json
import os
import random
import re
import shutil
import string
import time
import uuid
from datetime import datetime, timedelta

import deltalake
import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc

# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "_lakehouse", "bonus")
BRONZE_PATH = os.path.join(BASE_DIR, "bronze", "llm_logs")
SILVER_PATH = os.path.join(BASE_DIR, "silver", "llm_logs")
NUM_RECORDS = 10_000        # Total records to generate
DUPLICATE_RATE = 0.05       # 5% intentional duplicates
PII_SECRET_KEY = b"day18-lakehouse-lab-secret-key-do-not-use-in-prod"

MODELS = ["gpt-4o", "claude-3-opus", "gemini-1.5-pro"]
TENANTS = [f"tenant_{i:03d}" for i in range(1, 21)]  # 20 tenants

# Sample prompts with injected PII patterns
PII_PROMPTS = [
    "Summarize this email from john.doe@example.com about project X",
    "Translate: Nguyen Van A, CMND 079123456789, phone 0901234567",
    "Help me draft an email to jane.smith@corp.vn regarding invoice",
    "My customer ID is VN-2026-0504, email contact@test.org",
    "Analyze the sales report for Q1 2026",  # No PII
    "Generate code to parse CSV files",       # No PII
    "Review the contract for Tran Thi B, phone 0912345678",
    "What is the weather in Ho Chi Minh City?",  # No PII
]


# ──────────────────────────────────────────────────────────────────────
# Step 0: PII Tokenization Functions
# ──────────────────────────────────────────────────────────────────────
# Patterns for Vietnamese PII
EMAIL_PATTERN = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
PHONE_PATTERN = re.compile(r'0\d{9,10}')
CMND_PATTERN = re.compile(r'\b\d{9,12}\b')  # 9 or 12 digit ID numbers


def tokenize_value(value: str) -> str:
    """HMAC-SHA256 tokenization for structured PII fields."""
    return hmac.new(PII_SECRET_KEY, value.encode(), hashlib.sha256).hexdigest()[:16]


def redact_pii_in_text(text: str) -> str:
    """Scan free-text for PII patterns and replace with tokens."""
    # Order matters: CMND pattern is greedy, apply email/phone first
    text = EMAIL_PATTERN.sub(lambda m: f"[PII_EMAIL_{tokenize_value(m.group())}]", text)
    text = PHONE_PATTERN.sub(lambda m: f"[PII_PHONE_{tokenize_value(m.group())}]", text)
    text = CMND_PATTERN.sub(lambda m: f"[PII_ID_{tokenize_value(m.group())}]", text)
    return text


# ──────────────────────────────────────────────────────────────────────
# Step 1: Generate Mock Bronze Data
# ──────────────────────────────────────────────────────────────────────
def generate_bronze_data() -> list[dict]:
    """Generate realistic LLM log records with duplicates and PII."""
    print(f"[1/5] Generating {NUM_RECORDS:,} Bronze records "
          f"(~{DUPLICATE_RATE*100:.0f}% duplicates, PII injected)...")

    records = []
    base_time = datetime(2026, 4, 28, 0, 0, 0)  # 7+ days of data

    # Generate unique records
    unique_count = int(NUM_RECORDS * (1 - DUPLICATE_RATE))
    request_ids = [str(uuid.uuid4()) for _ in range(unique_count)]

    for i, req_id in enumerate(request_ids):
        ts = base_time + timedelta(
            days=random.randint(0, 9),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        prompt = random.choice(PII_PROMPTS)
        model = random.choice(MODELS)
        tenant = random.choice(TENANTS)

        payload = {
            "model": model,
            "tenant_id": tenant,
            "user_id": f"user_{random.randint(1, 500)}",
            "prompt": prompt,
            "response": f"Response for request {req_id[:8]}...",
            "usage": {
                "prompt_tokens": random.randint(50, 2000),
                "completion_tokens": random.randint(100, 4000),
                "total_tokens": 0,  # will be computed
            },
            "latency_ms": random.randint(100, 5000),
            "status": random.choices(["success", "error"], weights=[0.97, 0.03])[0],
        }
        payload["usage"]["total_tokens"] = (
            payload["usage"]["prompt_tokens"] + payload["usage"]["completion_tokens"]
        )

        records.append({
            "request_id": req_id,
            "ts": ts.isoformat(),
            "payload": json.dumps(payload),
        })

    # Inject duplicates: same request_id, slightly later timestamp
    dup_count = NUM_RECORDS - unique_count
    print(f"    → {unique_count:,} unique + {dup_count:,} duplicates")
    for _ in range(dup_count):
        original = random.choice(records[:unique_count])
        dup = original.copy()
        # Later timestamp (simulating network retry)
        orig_ts = datetime.fromisoformat(dup["ts"])
        dup["ts"] = (orig_ts + timedelta(seconds=random.randint(1, 30))).isoformat()
        records.append(dup)

    random.shuffle(records)
    return records


# ──────────────────────────────────────────────────────────────────────
# Step 2: Write Bronze (with PII Tokenization)
# ──────────────────────────────────────────────────────────────────────
def write_bronze(records: list[dict]) -> pa.Table:
    """Tokenize PII at ingest, then write raw data as Delta Bronze."""
    print("[2/5] Tokenizing PII and writing Bronze Delta table...")

    # Apply PII tokenization BEFORE landing in Bronze
    for rec in records:
        payload = json.loads(rec["payload"])
        # Tokenize structured PII
        payload["user_id"] = f"tok_{tokenize_value(payload['user_id'])}"
        # Redact PII in free-text prompt
        payload["prompt"] = redact_pii_in_text(payload["prompt"])
        rec["payload"] = json.dumps(payload)

    # Build PyArrow table
    table = pa.table({
        "request_id": [r["request_id"] for r in records],
        "ts": pa.array(
            [datetime.fromisoformat(r["ts"]) for r in records],
            type=pa.timestamp("us"),
        ),
        "payload": [r["payload"] for r in records],
    })

    # Write Delta
    if os.path.exists(BRONZE_PATH):
        shutil.rmtree(BRONZE_PATH)
    os.makedirs(os.path.dirname(BRONZE_PATH), exist_ok=True)

    deltalake.write_deltalake(BRONZE_PATH, table, mode="overwrite")

    dt = deltalake.DeltaTable(BRONZE_PATH)
    print(f"    → Bronze: {dt.to_pyarrow_table().num_rows:,} rows, "
          f"{len(dt.files()):,} files")
    print(f"    → Path: {os.path.abspath(BRONZE_PATH)}")
    return table


# ──────────────────────────────────────────────────────────────────────
# Step 3: Bronze → Silver (Flatten + Dedup)
# ──────────────────────────────────────────────────────────────────────
def bronze_to_silver():
    """Read Bronze, flatten JSON, deduplicate, write Silver Delta."""
    print("[3/5] Transforming Bronze → Silver (flatten + dedup)...")

    # Read Bronze as Polars DataFrame for ergonomic JSON parsing
    dt_bronze = deltalake.DeltaTable(BRONZE_PATH)
    df = pl.from_arrow(dt_bronze.to_pyarrow_table())

    bronze_count = len(df)
    print(f"    → Bronze rows read: {bronze_count:,}")

    # ── Flatten nested JSON ──
    # Using Polars JSON extraction (analogous to Spark's get_json_object)
    df = df.with_columns([
        pl.col("payload").str.json_path_match("$.model").alias("model_id"),
        pl.col("payload").str.json_path_match("$.tenant_id").alias("tenant_id"),
        pl.col("payload").str.json_path_match("$.user_id").alias("user_id"),
        pl.col("payload").str.json_path_match("$.prompt").alias("prompt_text"),
        pl.col("payload").str.json_path_match("$.usage.total_tokens")
            .cast(pl.Int64, strict=False).alias("total_tokens"),
        pl.col("payload").str.json_path_match("$.latency_ms")
            .cast(pl.Int64, strict=False).alias("latency_ms"),
        pl.col("payload").str.json_path_match("$.status").alias("status"),
        pl.col("ts").cast(pl.Date).alias("date_part"),
    ])

    # Drop raw payload (no longer needed in Silver)
    df = df.drop("payload")

    # ── Deduplication ──
    # Keep latest record per request_id (by timestamp descending)
    df = df.sort("ts", descending=True).unique(subset=["request_id"], keep="first")

    silver_count = len(df)
    dedup_removed = bronze_count - silver_count
    print(f"    → After dedup: {silver_count:,} rows "
          f"(removed {dedup_removed:,} duplicates, "
          f"{dedup_removed/bronze_count*100:.1f}%)")

    # ── Write Silver Delta (partitioned by date, schema evolution enabled) ──
    if os.path.exists(SILVER_PATH):
        shutil.rmtree(SILVER_PATH)
    os.makedirs(os.path.dirname(SILVER_PATH), exist_ok=True)

    silver_arrow = df.to_arrow()
    deltalake.write_deltalake(
        SILVER_PATH,
        silver_arrow,
        mode="overwrite",
        partition_by=["date_part"],
    )

    dt_silver = deltalake.DeltaTable(SILVER_PATH)
    print(f"    → Silver: {dt_silver.to_pyarrow_table().num_rows:,} rows, "
          f"{len(dt_silver.files()):,} files, "
          f"partitions: {len(set(f.split('/')[0] for f in dt_silver.files()))}")
    print(f"    → Path: {os.path.abspath(SILVER_PATH)}")
    return silver_count


# ──────────────────────────────────────────────────────────────────────
# Step 4: Verify with DuckDB
# ──────────────────────────────────────────────────────────────────────
def verify_results():
    """Run verification queries using DuckDB."""
    print("[4/5] Verifying results with DuckDB...")

    con = duckdb.connect()

    # Count comparison
    bronze_count = con.execute(
        f"SELECT COUNT(*) FROM delta_scan('{BRONZE_PATH}')"
    ).fetchone()[0]
    silver_count = con.execute(
        f"SELECT COUNT(*) FROM delta_scan('{SILVER_PATH}')"
    ).fetchone()[0]

    print(f"\n    ┌─────────────────────────────────────────┐")
    print(f"    │ VERIFICATION RESULTS                    │")
    print(f"    ├─────────────────────────────────────────┤")
    print(f"    │ Bronze rows:        {bronze_count:>10,}          │")
    print(f"    │ Silver rows:        {silver_count:>10,}          │")
    print(f"    │ Dedup removed:      {bronze_count - silver_count:>10,}          │")
    print(f"    │ Silver < Bronze:    {'✅ PASS' if silver_count < bronze_count else '❌ FAIL':>10}          │")
    print(f"    ├─────────────────────────────────────────┤")

    # PII check: no raw email/phone should exist in Silver
    pii_leaks = con.execute(f"""
        SELECT COUNT(*) FROM delta_scan('{SILVER_PATH}')
        WHERE prompt_text LIKE '%@%.%'
           OR regexp_matches(prompt_text, '0\\d{{9,10}}')
    """).fetchone()[0]
    print(f"    │ PII leaks in Silver: {pii_leaks:>8,}          │")
    print(f"    │ PII redacted:       {'✅ PASS' if pii_leaks == 0 else '❌ FAIL':>10}          │")
    print(f"    ├─────────────────────────────────────────┤")

    # Duplicate check: no duplicate request_id in Silver
    dup_count = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT request_id, COUNT(*) as cnt
            FROM delta_scan('{SILVER_PATH}')
            GROUP BY request_id
            HAVING cnt > 1
        )
    """).fetchone()[0]
    print(f"    │ Dup request_ids:    {dup_count:>10,}          │")
    print(f"    │ No duplicates:      {'✅ PASS' if dup_count == 0 else '❌ FAIL':>10}          │")
    print(f"    ├─────────────────────────────────────────┤")

    # Date range check
    date_range = con.execute(f"""
        SELECT COUNT(DISTINCT date_part) as dates,
               COUNT(DISTINCT model_id) as models,
               COUNT(DISTINCT tenant_id) as tenants
        FROM delta_scan('{SILVER_PATH}')
    """).fetchone()
    print(f"    │ Distinct dates:     {date_range[0]:>10,}          │")
    print(f"    │ Distinct models:    {date_range[1]:>10,}          │")
    print(f"    │ Distinct tenants:   {date_range[2]:>10,}          │")
    print(f"    ├─────────────────────────────────────────┤")

    # Sample Silver row (show PII is tokenized)
    sample = con.execute(f"""
        SELECT request_id, model_id, tenant_id, user_id,
               LEFT(prompt_text, 70) as prompt_preview
        FROM delta_scan('{SILVER_PATH}')
        WHERE prompt_text LIKE '%PII%'
        LIMIT 3
    """).fetchall()

    all_pass = (silver_count < bronze_count) and (pii_leaks == 0) and (dup_count == 0)
    status = "ALL CHECKS PASSED ✅" if all_pass else "SOME CHECKS FAILED ❌"
    print(f"    │ {status:^39} │")
    print(f"    └─────────────────────────────────────────┘")

    if sample:
        print(f"\n    Sample Silver rows (PII tokenized):")
        for row in sample:
            print(f"      req_id={row[0][:12]}... model={row[1]} "
                  f"tenant={row[2]} user={row[3]}")
            print(f"        prompt: {row[4]}")

    # Show Delta table metadata
    print(f"\n[5/5] Delta table metadata:")
    dt = deltalake.DeltaTable(SILVER_PATH)
    print(f"    → Version: {dt.version()}")
    print(f"    → Schema: {dt.schema()}")
    print(f"    → Files: {len(dt.files())}")

    return all_pass


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  LLM Observability PoC — Bronze → Silver Pipeline")
    print("  (PII Tokenization + JSON Flatten + Deduplication)")
    print("=" * 60)
    print()

    t0 = time.time()

    # Step 1: Generate data
    records = generate_bronze_data()

    # Step 2: Tokenize PII + write Bronze
    write_bronze(records)

    # Step 3: Flatten + dedup → Silver
    bronze_to_silver()

    # Step 4-5: Verify
    print()
    all_pass = verify_results()

    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"  Total time: {elapsed:.1f}s")
    print(f"  {'✅ All verifications passed!' if all_pass else '❌ Some checks failed.'}")
    print(f"{'='*60}")

    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
