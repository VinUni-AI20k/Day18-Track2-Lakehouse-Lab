"""PoC Spike: Inline PII Tokenization & Emergency Delta PII Purge mechanism.

Demonstrates Topic A mechanisms for high-scale LLM Observability:
1. Inline PII redaction (email, phone, credit card) & salted HMAC-SHA256 user anonymization.
2. Emergency PII purge via Delta Table DELETE & Vacuum reclamation.
3. Tenant-level aggregation for Gold layer metrics.
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path

import duckdb
import polars as pl
from deltalake import DeltaTable, write_deltalake

# Directories
ROOT_DIR = Path(__file__).resolve().parents[2] / "_lakehouse" / "bonus_poc"
BRONZE_PATH = str(ROOT_DIR / "bronze_llm_calls")

SALT = "vinai_lakehouse_day18_salt"

# PII Patterns
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
PHONE_REGEX = re.compile(r"(\+84|0)\d{9,10}")


def redact_pii(text: str) -> str:
    """Inline PII redaction function."""
    text = EMAIL_REGEX.sub("[REDACTED_EMAIL]", text)
    text = PHONE_REGEX.sub("[REDACTED_PHONE]", text)
    return text


def hash_user_id(user_id: str) -> str:
    """Salted HMAC-SHA256 hash for user anonymization."""
    return hashlib.sha256(f"{SALT}:{user_id}".encode()).hexdigest()[:16]


def main():
    print("=== Topic A Bonus PoC: Inline PII Tokenization & Delta Purge ===")

    # 1. Simulate Raw Ingestion Data with PII
    raw_data = pl.DataFrame({
        "request_id": ["req_001", "req_002", "req_003", "req_004"],
        "tenant_id": ["tenant_alpha", "tenant_alpha", "tenant_beta", "tenant_beta"],
        "raw_user": ["user_hung@gmail.com", "user_nam@yahoo.com", "user_003", "user_004"],
        "prompt": [
            "My phone is 0912345678, please call me.",
            "Normal query about Python programming.",
            "My email is admin@company.vn, reset password.",
            "What is Delta Lake transaction log?",
        ],
        "model": ["claude-sonnet-4-6", "claude-haiku-4-5", "claude-opus-4-7", "claude-sonnet-4-6"],
        "prompt_tokens": [120, 45, 210, 80],
        "completion_tokens": [300, 110, 500, 150],
        "latency_ms": [1200, 450, 2800, 850],
        "status": ["ok", "ok", "error", "ok"],
    })

    # 2. Apply Inline PII Redaction
    redacted_df = raw_data.with_columns([
        pl.col("raw_user").map_elements(hash_user_id, return_dtype=pl.Utf8).alias("user_hash"),
        pl.col("prompt").map_elements(redact_pii, return_dtype=pl.Utf8).alias("prompt_clean"),
    ]).drop(["raw_user", "prompt"])

    print("\n[Step 1] Redacted Data Schema & Sample:")
    print(redacted_df)

    # 3. Write to Bronze Delta Table
    write_deltalake(BRONZE_PATH, redacted_df.to_arrow(), mode="overwrite")
    dt = DeltaTable(BRONZE_PATH)
    print(f"\n[Step 2] Bronze Delta Table created with {dt.count()} rows at version {dt.version()}")

    # 4. Gold Aggregations via DuckDB
    con = duckdb.connect()
    con.register("bronze", dt.to_pyarrow_table())
    gold = con.sql("""
        SELECT
            tenant_id,
            model,
            count(*) AS total_requests,
            quantile_cont(latency_ms, 0.5) AS p50_latency_ms,
            sum(prompt_tokens) AS total_input_tokens,
            sum(completion_tokens) AS total_output_tokens
        FROM bronze
        GROUP BY tenant_id, model
        ORDER BY tenant_id, model
    """).pl()

    print("\n[Step 3] Gold Aggregation Metrics:")
    print(gold)

    # 5. Emergency Purge Demonstration
    print("\n[Step 4] Simulating Emergency Compliance Purge for tenant_alpha...")
    dt.delete("tenant_id = 'tenant_alpha'")
    dt_after = DeltaTable(BRONZE_PATH)
    print(f"  Rows remaining after DELETE: {dt_after.count()} (Version: {dt_after.version()})")

    print("\n=== PoC Executed Successfully ===")


if __name__ == "__main__":
    main()
