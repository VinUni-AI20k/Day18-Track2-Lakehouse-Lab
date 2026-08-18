"""Bonus Challenge PoC: Ride-Hailing CDC & Decree 13 Compliance.

Demonstrates:
1. Deterministic PII Tokenization at Bronze landing (HMAC-SHA256).
2. Late-arriving CDC event resolution via MERGE (source_ts comparison).
3. Right-to-Erasure deletion and propagation via Delta Change Data Feed (CDF).

Stack: deltalake (delta-rs) + DuckDB + Polars + PyArrow (Pure Python, offline).
"""
from __future__ import annotations

import hashlib
import hmac
import os
import shutil
import time
from pathlib import Path

import duckdb
import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# Scratch path for PoC
POC_DIR = Path("_lakehouse") / "bonus_poc"
shutil.rmtree(POC_DIR, ignore_errors=True)
POC_DIR.mkdir(parents=True, exist_ok=True)

TABLE_BRONZE = str(POC_DIR / "trips_bronze")
TABLE_SILVER = str(POC_DIR / "trips_silver")

SECRET_SALT = b"vinuni_lakehouse_decree13_salt_2026"


def tokenize_pii(plaintext: str) -> str:
    """HMAC-SHA256 Pseudonymization at ingestion."""
    if plaintext is None:
        return None
    return hmac.new(SECRET_SALT, plaintext.encode("utf-8"), hashlib.sha256).hexdigest()[:16]


def main():
    print("=" * 70)
    print("PoC: Ride-Hailing CDC & Decree 13 Right-to-Erasure Pipeline")
    print("=" * 70)

    # ─────────────────────────────────────────────────────────────────
    # 1. Bronze Ingestion: Raw CDC with PII Tokenization
    # ─────────────────────────────────────────────────────────────────
    print("\n[Step 1] Ingesting raw CDC events with PII tokenization at Bronze...")
    raw_events = [
        {"trip_id": "T001", "driver_id": "D10", "user_phone": "0912345678", "user_cccd": "001099012345", "status": "REQUESTED", "source_ts": 1000},
        {"trip_id": "T002", "driver_id": "D20", "user_phone": "0987654321", "user_cccd": "001099098765", "status": "REQUESTED", "source_ts": 1000},
        {"trip_id": "T003", "driver_id": "D30", "user_phone": "0912345678", "user_cccd": "001099012345", "status": "REQUESTED", "source_ts": 1000},
    ]

    bronze_data = []
    for ev in raw_events:
        bronze_data.append({
            "trip_id": ev["trip_id"],
            "driver_id": ev["driver_id"],
            "user_token": tokenize_pii(ev["user_phone"]),  # Pseudonymized!
            "status": ev["status"],
            "source_ts": ev["source_ts"],
            "ingest_ts": int(time.time()),
        })

    df_bronze = pl.DataFrame(bronze_data)
    write_deltalake(TABLE_BRONZE, df_bronze.to_arrow(), mode="overwrite",
                    configuration={"delta.enableChangeDataFeed": "true"})
    print("  Bronze table written:")
    print(df_bronze)

    # ─────────────────────────────────────────────────────────────────
    # 2. Silver Medallion: Initial Load & Late-Data Resolution
    # ─────────────────────────────────────────────────────────────────
    print("\n[Step 2] Promoting to Silver & testing Late-Data MERGE...")
    write_deltalake(TABLE_SILVER, df_bronze.to_arrow(), mode="overwrite",
                    configuration={"delta.enableChangeDataFeed": "true"})

    # Simulate late-arriving event: T001 COMPLETED at source_ts=1200 arrives AFTER T001 CANCELLED at source_ts=1100
    updates = pl.DataFrame([
        {"trip_id": "T001", "driver_id": "D10", "user_token": tokenize_pii("0912345678"), "status": "CANCELLED", "source_ts": 1100, "ingest_ts": int(time.time())},
    ])
    (DeltaTable(TABLE_SILVER).merge(
        source=updates.to_arrow(),
        predicate="t.trip_id = s.trip_id",
        source_alias="s", target_alias="t")
     .when_matched_update_all("s.source_ts > t.source_ts")
     .when_not_matched_insert_all()
     .execute())

    # Late arrival with newer timestamp
    newer_late_update = pl.DataFrame([
        {"trip_id": "T001", "driver_id": "D10", "user_token": tokenize_pii("0912345678"), "status": "COMPLETED", "source_ts": 1200, "ingest_ts": int(time.time())},
        # Outdated event that should be IGNORED
        {"trip_id": "T002", "driver_id": "D20", "user_token": tokenize_pii("0987654321"), "status": "STALE_STATUS", "source_ts": 900, "ingest_ts": int(time.time())},
    ])
    (DeltaTable(TABLE_SILVER).merge(
        source=newer_late_update.to_arrow(),
        predicate="t.trip_id = s.trip_id",
        source_alias="s", target_alias="t")
     .when_matched_update_all("s.source_ts > t.source_ts")
     .when_not_matched_insert_all()
     .execute())

    dt_silver = DeltaTable(TABLE_SILVER)
    con = duckdb.connect()
    con.register("silver", dt_silver.to_pyarrow_table())
    res = con.sql("SELECT trip_id, user_token, status, source_ts FROM silver ORDER BY trip_id").fetchall()
    print("  Silver status after late-data MERGE:")
    for r in res:
        print(f"    trip={r[0]} token={r[1]} status={r[2]} source_ts={r[3]}")
    assert res[0][2] == "COMPLETED", "T001 should resolve to latest COMPLETED"
    assert res[1][2] == "REQUESTED", "T002 should NOT be overwritten by stale status"

    # ─────────────────────────────────────────────────────────────────
    # 3. Decree 13 / Law 60: Right-to-Erasure & CDF Propagation
    # ─────────────────────────────────────────────────────────────────
    target_token = tokenize_pii("0912345678")
    print(f"\n[Step 3] Executing Right-to-Erasure for token: {target_token} (trips T001, T003)...")
    dt_silver.delete(f"user_token = '{target_token}'")

    dt_after_del = DeltaTable(TABLE_SILVER)
    con.register("silver_after", dt_after_del.to_pyarrow_table())
    remaining = con.sql(f"SELECT count(*) FROM silver_after WHERE user_token = '{target_token}'").fetchone()[0]
    print(f"  Rows remaining for user in Silver: {remaining} (Expected 0)")
    assert remaining == 0, "Right-to-erasure failed"

    # Read Change Data Feed (CDF)
    print("\n[Step 4] Reading Delta Change Data Feed (CDF) for downstream erasure...")
    cdf = dt_after_del.load_cdf(starting_version=1).read_all()
    cdf_df = pl.from_arrow(cdf)
    deletes = cdf_df.filter(pl.col("_change_type") == "delete")
    print(f"  CDF Delete events captured: {deletes.height}")
    print(deletes.select(["trip_id", "user_token", "_change_type", "_commit_version"]))

    assert deletes.height == 2, f"Expected 2 deleted trips in CDF, got {deletes.height}"
    print("\n✓ Bonus PoC Pipeline successfully executed with 100% compliance checks.")


if __name__ == "__main__":
    main()
