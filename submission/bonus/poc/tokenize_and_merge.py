"""PoC for the bonus architecture (Topic C — CDC ride-hailing + Nghi dinh 13).

Demonstrates the two non-trivial mechanisms from ARCHITECTURE.md §3-4:

  1. Deterministic tokenization of a phone number at Bronze landing time
     (HMAC-based, same input -> same token, so joins across days still work
     without ever storing the raw PII column past ingestion).
  2. A late-arriving CDC event handled correctly by a conditional MERGE:
     `WHEN MATCHED AND src.updated_at > tgt.updated_at THEN UPDATE`.
     An out-of-order event (older updated_at) must NOT clobber a newer row.

Run: python submission/bonus/poc/tokenize_and_merge.py
Standalone — only needs the same deps the lab already installs
(deltalake, polars). No Oracle/Kafka/Debezium; those are simulated as
plain dicts to keep the spike small and inspectable.
"""
from __future__ import annotations

import hashlib
import hmac
import shutil
from pathlib import Path

import polars as pl
from deltalake import DeltaTable, write_deltalake

TABLE_PATH = str(Path(__file__).resolve().parents[3] / "_lakehouse" / "bonus" / "trips_silver")
TOKEN_KEY = b"demo-kms-key-epoch-1"  # production: pulled from KMS, rotated periodically


def tokenize_phone(raw_phone: str) -> str:
    """Deterministic, one-way pseudonymization: same phone -> same token.

    Deterministic (not random-salted) on purpose: analysts need to group
    "how many trips did this rider take" without ever seeing the raw phone
    number. HMAC means the token cannot be reversed to the phone without
    the KMS key, satisfying Decree 13's data-minimization requirement while
    keeping join/group-by semantics intact.
    """
    return hmac.new(TOKEN_KEY, raw_phone.encode(), hashlib.sha256).hexdigest()[:16]


def landing_transform(cdc_event: dict) -> dict:
    """What happens at Bronze landing, before anything is committed."""
    return {
        "trip_id": cdc_event["trip_id"],
        "rider_token": tokenize_phone(cdc_event["rider_phone"]),
        "status": cdc_event["status"],
        "updated_at": cdc_event["updated_at"],
    }


def merge_cdc_event(table_path: str, event: dict) -> None:
    """Apply one CDC event with the late-arrival-safe MERGE condition.

    This is the crux of the design: a naive `MERGE ... WHEN MATCHED THEN
    UPDATE` would let an out-of-order (late) event silently overwrite a
    newer row. The `AND src.updated_at > tgt.updated_at` clause makes the
    merge a no-op for stale events instead of corrupting Silver.
    """
    row = landing_transform(event)
    src = pl.DataFrame([row]).to_arrow()

    if not Path(table_path).exists():
        write_deltalake(table_path, src, mode="overwrite")
        return

    dt = DeltaTable(table_path)
    (
        dt.merge(
            source=src,
            predicate="target.trip_id = source.trip_id",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all(predicate="source.updated_at > target.updated_at")
        .when_not_matched_insert_all()
        .execute()
    )


def current_state(table_path: str) -> pl.DataFrame:
    return pl.from_arrow(DeltaTable(table_path).to_pyarrow_table()).sort("trip_id")


def main() -> None:
    shutil.rmtree(TABLE_PATH, ignore_errors=True)

    # t0: trip created
    merge_cdc_event(TABLE_PATH, {
        "trip_id": "T-1001", "rider_phone": "0912345678",
        "status": "created", "updated_at": "2026-04-01T08:00:00",
    })
    # t1: driver completes the trip (arrives on time)
    merge_cdc_event(TABLE_PATH, {
        "trip_id": "T-1001", "rider_phone": "0912345678",
        "status": "completed", "updated_at": "2026-04-01T08:20:00",
    })

    after_normal = current_state(TABLE_PATH)
    print("--- After two in-order events ---")
    print(after_normal.to_dicts())
    assert after_normal.row(0, named=True)["status"] == "completed"

    # A LATE event arrives 6h later from a low-connectivity rural driver phone:
    # it's the "assigned" event that should have landed BEFORE "completed",
    # but the network dropped it. Its updated_at (08:05) is older than what's
    # already in Silver (08:20) -> must be a no-op, not an overwrite.
    merge_cdc_event(TABLE_PATH, {
        "trip_id": "T-1001", "rider_phone": "0912345678",
        "status": "assigned", "updated_at": "2026-04-01T08:05:00",
    })

    after_late = current_state(TABLE_PATH)
    print("\n--- After a late out-of-order event (updated_at older) ---")
    print(after_late.to_dicts())
    assert after_late.row(0, named=True)["status"] == "completed", (
        "Late event clobbered a newer row -- the whole point of the "
        "conditional MERGE was to prevent exactly this."
    )

    # Verify tokenization is deterministic (join/group-by safety) and
    # never reversible to the raw phone number without the key.
    token_a = tokenize_phone("0912345678")
    token_b = tokenize_phone("0912345678")
    token_c = tokenize_phone("0987654321")
    assert token_a == token_b, "same phone must tokenize identically (join safety)"
    assert token_a != token_c, "different phones must not collide"
    assert "0912345678" not in after_late.to_dicts().__str__(), (
        "raw phone leaked into Silver -- tokenization at landing failed"
    )

    print(f"\ntoken('0912345678') = {token_a}  (deterministic, one-way)")
    print("\n[PASS] deterministic tokenization + late-arrival-safe MERGE both hold.")
    print("PoC complete.")


if __name__ == "__main__":
    main()
