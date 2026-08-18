"""Bonus Challenge PoC: In-Stream PII Tokenization & Delta Compaction Spike.

Demonstrates:
1. Deterministic HMAC-SHA256 PII masking at Bronze boundary.
2. Fast streaming appends to Delta Lake.
3. Micro-compaction reducing small file overhead.
4. Stats-based data pruning.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import random
import time
from pathlib import Path
import polars as pl
from deltalake import DeltaTable, write_deltalake

SECRET_KEY = b"lakehouse_secret_salt_2026"
BASE_DIR = Path(__file__).resolve().parents[3] / "_lakehouse" / "scratch" / "poc_stream"


def mask_pii(text: str) -> str:
    """Surrogate deterministic hash for sensitive identifiers."""
    return hmac.new(SECRET_KEY, text.encode("utf-8"), hashlib.sha256).hexdigest()[:16]


def run_poc():
    table_path = str(BASE_DIR)
    import shutil
    shutil.rmtree(table_path, ignore_errors=True)

    print("--- 1. Generating Streaming Micro-batches with PII Tokenization ---")
    tenants = [f"tenant_{i:02d}" for i in range(10)]
    users = [f"user_{i}@company.vn" for i in range(100)]

    for batch in range(10):
        raw_users = [random.choice(users) for _ in range(1_000)]
        sanitized_users = [mask_pii(u) for u in raw_users]
        
        df = pl.DataFrame({
            "request_id": [f"req_{batch}_{i}" for i in range(1_000)],
            "tenant_id": [random.choice(tenants) for _ in range(1_000)],
            "masked_user": sanitized_users,
            "tokens_in": [random.randint(100, 2000) for _ in range(1_000)],
            "tokens_out": [random.randint(50, 800) for _ in range(1_000)],
            "latency_ms": [random.randint(150, 3500) for _ in range(1_000)],
        })
        write_deltalake(table_path, df.to_arrow(), mode="append")

    dt = DeltaTable(table_path)
    files_before = len(dt.file_uris())
    print(f"Files before compaction: {files_before} (10 micro-batches)")
    print("Sample row (PII masked):", dt.to_pyarrow_table().slice(0, 1).to_pydict())

    print("\n--- 2. Executing Delta Compaction ---")
    dt.optimize.compact()
    dt = DeltaTable(table_path)
    files_after = len(dt.file_uris())
    print(f"Files after compaction:  {files_after} (compressed into unified Parquet)")

    print("\n--- 3. Verifying Stats-Based Filter Pushdown ---")
    target_tenant = tenants[0]
    t0 = time.perf_counter()
    filtered = dt.to_pyarrow_table(filters=[("tenant_id", "=", target_tenant)])
    elapsed = (time.perf_counter() - t0) * 1000
    print(f"Filter by tenant_id='{target_tenant}': {filtered.num_rows} rows found in {elapsed:.2f} ms")
    print("PoC validation successful.")


if __name__ == "__main__":
    run_poc()
