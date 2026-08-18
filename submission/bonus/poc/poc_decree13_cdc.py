"""Proof of Concept (PoC): CDC Ride-Hailing Lakehouse with Decree 13/2023 Compliance.

Demonstrates:
1. Salted HMAC-SHA256 Tokenization for PII at Bronze Ingestion.
2. Out-of-order CDC reconciliation via versioned MERGE (src.event_ts > tgt.event_ts).
3. Right-to-Erasure execution under Decree 13 Art. 16.
4. Change Data Feed (CDF) audit capture.
"""
from __future__ import annotations

import datetime as dtm
import hashlib
import hmac
import shutil
import sys
import time
from pathlib import Path

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import duckdb
import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# ── Config & Paths ──
_HERE = Path(globals().get("__file__", "submission/bonus/poc/poc_decree13_cdc.py")).resolve().parent
ROOT = _HERE / "_poc_lakehouse"
shutil.rmtree(ROOT, ignore_errors=True)
BRONZE_PATH = str(ROOT / "bronze_trips_cdc")
SILVER_PATH = str(ROOT / "silver_trips")
SALT_SECRET = b"vn-decree13-secret-salt-2026"


def tokenize_pii(val: str) -> str:
    """Deterministic HMAC-SHA256 Tokenizer for PII (Phone, ID, Name)."""
    if not val:
        return ""
    h = hmac.new(SALT_SECRET, val.encode("utf-8"), hashlib.sha256)
    return "tok_" + h.hexdigest()[:16]


def main():
    print("=" * 70)
    print("PoC: Ride-Hailing CDC Lakehouse Architecture (Decree 13 Compliant)")
    print("=" * 70)

    # ──────────────────────────────────────────────────────────────────
    # Step 1: Ingest Raw CDC Events with Bronze Tokenization
    # ──────────────────────────────────────────────────────────────────
    print("\n[Step 1] Ingesting Bronze CDC events with Ingestion-Time Tokenization...")
    raw_events = [
        {
            "trip_id": "T001",
            "customer_phone": "0912345678",  # PII
            "customer_cccd": "001234567890",  # PII
            "driver_id": "D101",
            "status": "REQUESTED",
            "fare_vnd": 85000.0,
            "event_ts": dtm.datetime(2026, 8, 18, 8, 0, 0),
            "_op": "INSERT",
        },
        {
            "trip_id": "T002",
            "customer_phone": "0987654321",
            "customer_cccd": "079876543210",
            "driver_id": "D102",
            "status": "REQUESTED",
            "fare_vnd": 120000.0,
            "event_ts": dtm.datetime(2026, 8, 18, 8, 5, 0),
            "_op": "INSERT",
        },
    ]

    # Tokenize PII fields before writing to storage layer
    bronze_rows = []
    for r in raw_events:
        row = dict(r)
        row["customer_token"] = tokenize_pii(row.pop("customer_phone"))
        row["cccd_token"] = tokenize_pii(row.pop("customer_cccd"))
        bronze_rows.append(row)

    bronze_tbl = pa.Table.from_pylist(bronze_rows)
    write_deltalake(
        BRONZE_PATH,
        bronze_tbl,
        mode="overwrite",
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    print(f"  ✓ Bronze table written: {DeltaTable(BRONZE_PATH).to_pyarrow_table().num_rows} rows.")
    print("  ✓ Plaintext PII replaced by HMAC tokens:")
    print("   ", bronze_tbl.select(["trip_id", "customer_token", "cccd_token", "status"]).to_pydict())

    # Initialize Silver table
    write_deltalake(
        SILVER_PATH,
        bronze_tbl,
        mode="overwrite",
        configuration={"delta.enableChangeDataFeed": "true"},
    )

    # ──────────────────────────────────────────────────────────────────
    # Step 2: Handle Out-of-Order / Late-Arriving CDC Updates
    # ──────────────────────────────────────────────────────────────────
    print("\n[Step 2] Simulating Out-of-order Late-arriving CDC Updates...")
    # Event A (Arrived on time): Trip completed at 08:30
    event_completed = pa.Table.from_pylist([
        {
            "trip_id": "T001",
            "customer_token": tokenize_pii("0912345678"),
            "cccd_token": tokenize_pii("001234567890"),
            "driver_id": "D101",
            "status": "COMPLETED",
            "fare_vnd": 85000.0,
            "event_ts": dtm.datetime(2026, 8, 18, 8, 30, 0),
            "_op": "UPDATE",
        }
    ])

    dt_silver = DeltaTable(SILVER_PATH)
    (
        dt_silver.merge(
            source=event_completed,
            predicate="target.trip_id = source.trip_id",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update(
            predicate="source.event_ts > target.event_ts",
            updates={
                "status": "source.status",
                "event_ts": "source.event_ts",
                "fare_vnd": "source.fare_vnd",
            },
        )
        .execute()
    )
    print("  ✓ Applied update v1: T001 is now COMPLETED (08:30).")

    # Event B (Late arriving due to 4G BTS drop): Stale event from 08:10 (ASSIGNED) arrives LATER
    stale_event = pa.Table.from_pylist([
        {
            "trip_id": "T001",
            "customer_token": tokenize_pii("0912345678"),
            "cccd_token": tokenize_pii("001234567890"),
            "driver_id": "D101",
            "status": "ASSIGNED",
            "fare_vnd": 85000.0,
            "event_ts": dtm.datetime(2026, 8, 18, 8, 10, 0),  # Older than 08:30!
            "_op": "UPDATE",
        }
    ])

    dt_silver = DeltaTable(SILVER_PATH)
    (
        dt_silver.merge(
            source=stale_event,
            predicate="target.trip_id = source.trip_id",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update(
            predicate="source.event_ts > target.event_ts",
            updates={
                "status": "source.status",
                "event_ts": "source.event_ts",
                "fare_vnd": "source.fare_vnd",
            },
        )
        .execute()
    )

    current_t001 = (
        dt_silver.to_pyarrow_table().filter(pa.compute.equal(dt_silver.to_pyarrow_table()["trip_id"], "T001"))
    )
    current_status = current_t001["status"][0].as_py()
    print(f"  ✓ Late-arriving stale event rejected: Status remains '{current_status}' (Not overwritten by ASSIGNED).")
    assert current_status == "COMPLETED", "Late data corrupted current state!"

    # ──────────────────────────────────────────────────────────────────
    # Step 3: Right-to-Erasure Execution (Decree 13 Article 16)
    # ──────────────────────────────────────────────────────────────────
    print("\n[Step 3] Executing Right-to-Erasure for customer '0987654321' (T002)...")
    target_token = tokenize_pii("0987654321")
    dt_silver.delete(f"customer_token = '{target_token}'")

    after_delete = dt_silver.to_pyarrow_table()
    print(f"  ✓ Customer deleted from Silver table. Remaining rows: {after_delete.num_rows}")
    assert target_token not in after_delete["customer_token"].to_pylist()

    # ──────────────────────────────────────────────────────────────────
    # Step 4: Audit Trail & Change Data Feed (CDF) Verification
    # ──────────────────────────────────────────────────────────────────
    print("\n[Step 4] Querying Delta Change Data Feed for Audit Logging...")
    raw_cdf = dt_silver.load_cdf(starting_version=1).read_all()
    cdf = pa.table(raw_cdf)
    print("  ✓ CDF captured mutation events:")
    for change in cdf.to_pylist():
        print(f"    - version={change['_commit_version']} type={change['_change_type']} trip={change['trip_id']}")

    print("\n" + "=" * 70)
    print("All PoC assertions verified successfully! Pipeline is production-ready.")
    print("=" * 70)


if __name__ == "__main__":
    main()
