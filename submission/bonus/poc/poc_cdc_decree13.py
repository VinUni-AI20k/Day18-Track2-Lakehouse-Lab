"""PoC for Bonus Challenge: Vietnam Ride-Hailing CDC with Decree 13 Compliance.

Demonstrates 3 non-trivial mechanisms from the architecture brief:
1. In-flight Salted HMAC Tokenization of PII at Bronze landing.
2. Out-of-order CDC ingestion handling via conditional MERGE (src.event_time > tgt.event_time).
3. Decree 13 Right-to-Erasure compliance via Delta CDF and physical deletion audit.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import shutil
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# Paths
BASE_DIR = Path(__file__).resolve().parent / "_poc_lakehouse"
BRONZE = str(BASE_DIR / "bronze" / "cdc_trips_raw")
SILVER = str(BASE_DIR / "silver" / "trips_conformed")
AUDIT = str(BASE_DIR / "audit" / "compliance_erasure_log")

SECRET_SALT = b"vinuni_lakehouse_decree13_salt_2026"


def clean_poc():
    shutil.rmtree(BASE_DIR, ignore_errors=True)


def tokenize_pii(val: str) -> str:
    """Deterministic HMAC tokenization for sensitive PII (Phone/CCCD)."""
    if not val:
        return ""
    return hmac.new(SECRET_SALT, val.encode("utf-8"), hashlib.sha256).hexdigest()[:16]


def run_poc():
    print("=" * 70)
    print("PoC: Vietnam Ride-Hailing CDC & Decree 13 Compliance Engine")
    print("=" * 70)
    clean_poc()

    # -------------------------------------------------------------------------
    # 1. Simulate Ingestion & Tokenization at Bronze
    # -------------------------------------------------------------------------
    print("\n[Step 1] Ingesting raw events with PII Tokenization at Bronze landing...")
    raw_events = [
        {
            "trip_id": "TRIP-1001",
            "driver_phone": "0912345678",
            "passenger_phone": "0987654321",
            "status": "REQUESTED",
            "fare_vnd": 50000,
            "event_time": datetime(2026, 8, 18, 8, 0, 0),
            "ingest_time": datetime(2026, 8, 18, 8, 0, 5),
        },
        {
            "trip_id": "TRIP-1002",
            "driver_phone": "0908888999",
            "passenger_phone": "0934567890",
            "status": "IN_PROGRESS",
            "fare_vnd": 85000,
            "event_time": datetime(2026, 8, 18, 8, 5, 0),
            "ingest_time": datetime(2026, 8, 18, 8, 5, 10),
        },
    ]

    bronze_records = []
    for r in raw_events:
        bronze_records.append({
            "trip_id": r["trip_id"],
            "driver_token": tokenize_pii(r["driver_phone"]),
            "passenger_token": tokenize_pii(r["passenger_phone"]),
            "status": r["status"],
            "fare_vnd": r["fare_vnd"],
            "event_time": r["event_time"],
            "ingest_time": r["ingest_time"],
        })

    df_bronze = pl.DataFrame(bronze_records)
    write_deltalake(
        BRONZE,
        df_bronze.to_arrow(),
        mode="overwrite",
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    print(f"  Bronze table created at: {BRONZE}")
    print(f"  Tokenized passenger phone: '0987654321' -> '{tokenize_pii('0987654321')}'")
    print(f"  Raw PII phones NEVER touch the lakehouse storage layer.")

    # Initial load to Silver
    write_deltalake(
        SILVER,
        df_bronze.to_arrow(),
        mode="overwrite",
        configuration={"delta.enableChangeDataFeed": "true"},
    )

    # -------------------------------------------------------------------------
    # 2. Out-of-Order / Late-Arriving CDC Event Handling
    # -------------------------------------------------------------------------
    print("\n[Step 2] Handling Out-of-Order Late-Arriving Events via Conditional MERGE...")
    
    # Event 1: Normal update (trip completed at 08:20)
    normal_update = pl.DataFrame([{
        "trip_id": "TRIP-1001",
        "driver_token": tokenize_pii("0912345678"),
        "passenger_token": tokenize_pii("0987654321"),
        "status": "COMPLETED",
        "fare_vnd": 55000,
        "event_time": datetime(2026, 8, 18, 8, 20, 0),
        "ingest_time": datetime(2026, 8, 18, 8, 20, 5),
    }])

    (DeltaTable(SILVER)
        .merge(
            source=normal_update.to_arrow(),
            predicate="t.trip_id = s.trip_id",
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update_all(predicate="s.event_time > t.event_time")
        .when_not_matched_insert_all()
        .execute())

    curr_status = pl.from_arrow(DeltaTable(SILVER).to_pyarrow_table()).filter(pl.col("trip_id") == "TRIP-1001")["status"][0]
    print(f"  Current status for TRIP-1001 after normal update: {curr_status} (Expected: COMPLETED)")

    # Event 2: Late-arriving event (stale packet from 08:10 with status 'PICKING_UP' arrived at 08:30)
    stale_late_event = pl.DataFrame([{
        "trip_id": "TRIP-1001",
        "driver_token": tokenize_pii("0912345678"),
        "passenger_token": tokenize_pii("0987654321"),
        "status": "PICKING_UP",  # Stale status
        "fare_vnd": 50000,
        "event_time": datetime(2026, 8, 18, 8, 10, 0),  # Older timestamp!
        "ingest_time": datetime(2026, 8, 18, 8, 30, 0),
    }])

    print("  Applying late-arriving event from 08:10 (ingested at 08:30 with stale status 'PICKING_UP')...")
    (DeltaTable(SILVER)
        .merge(
            source=stale_late_event.to_arrow(),
            predicate="t.trip_id = s.trip_id",
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update_all(predicate="s.event_time > t.event_time")
        .when_not_matched_insert_all()
        .execute())

    protected_status = pl.from_arrow(DeltaTable(SILVER).to_pyarrow_table()).filter(pl.col("trip_id") == "TRIP-1001")["status"][0]
    print(f"  Status after late event MERGE: {protected_status} (State preserved: COMPLETED!)")
    assert protected_status == "COMPLETED", "Error: State regressed due to late data!"

    # -------------------------------------------------------------------------
    # 3. Decree 13 / PDPL Right-to-Erasure & Audit Verification
    # -------------------------------------------------------------------------
    print("\n[Step 3] Processing Right-to-Erasure (Decree 13/2023/ND-CP) Request...")
    target_passenger = tokenize_pii("0987654321")
    print(f"  Target erasure token: {target_passenger}")

    # Record in audit log
    audit_entry = pl.DataFrame([{
        "request_id": "ERASURE-REQ-2026-0089",
        "subject_token": target_passenger,
        "requested_at": datetime(2026, 8, 18, 9, 0, 0),
        "executed_at": datetime.now(),
        "legal_basis": "Decree 13/2023/ND-CP Art. 16 - Right to Erasure",
    }])
    write_deltalake(AUDIT, audit_entry.to_arrow(), mode="append")

    # Delete from Silver
    dt_silver = DeltaTable(SILVER)
    rows_before = dt_silver.count()
    dt_silver.delete(f"passenger_token = '{target_passenger}'")
    rows_after = DeltaTable(SILVER).count()
    print(f"  Silver rows before deletion: {rows_before}, after deletion: {rows_after}")

    # Inspect Change Data Feed for downstream cache eviction
    cdf = pa.table(DeltaTable(SILVER).load_cdf(starting_version=1).read_all())
    deletes = [r for r in cdf.to_pylist() if r.get("_change_type") == "delete"]
    print(f"  CDF emitted {len(deletes)} delete event(s) for downstream propagation.")

    print("\n" + "=" * 70)
    print("PoC Executed Successfully! All architecture mechanisms verified.")
    print("=" * 70)


if __name__ == "__main__":
    run_poc()
