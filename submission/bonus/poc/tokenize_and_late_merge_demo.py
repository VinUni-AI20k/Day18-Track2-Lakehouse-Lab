"""PoC for ARCHITECTURE.md §3.5 (PII tokenization at landing) and §3.6
(late-data-safe MERGE). Simulates a few Debezium CDC events without needing
a real Oracle/Kafka — run with the lab's venv:

    .venv/bin/python submission/bonus/poc/tokenize_and_late_merge_demo.py
"""

import hashlib
import hmac

import polars as pl
from deltalake import DeltaTable, write_deltalake

VAULT_SECRET = b"replace-with-a-real-secret-from-a-kms"  # noqa: S105 (demo only)


def tokenize(raw: str) -> str:
    """Deterministic HMAC token — same input always maps to the same token,
    so joins/dedup still work downstream without ever storing the raw value.
    """
    return hmac.new(VAULT_SECRET, raw.encode(), hashlib.sha256).hexdigest()[:16]


# ── 1. Tokenize PII at landing (before anything touches Bronze) ──────────
raw_events = pl.DataFrame({
    "trip_id": [1, 2, 3],
    "driver_phone": ["0901234567", "0912345678", "0987654321"],
    "rider_cmnd": ["012345678901", "023456789012", "034567890123"],
})
landed = raw_events.with_columns(
    pl.col("driver_phone").map_elements(tokenize, return_dtype=pl.Utf8).alias("driver_phone_token"),
    pl.col("rider_cmnd").map_elements(tokenize, return_dtype=pl.Utf8).alias("rider_cmnd_token"),
).drop("driver_phone", "rider_cmnd")

print("Bronze row (PII already tokenized, raw values never written to disk):")
print(landed)

# ── 2. Late-data-safe MERGE: `WHEN MATCHED AND src.ts > tgt.ts` ──────────
table_path = "/tmp/poc_silver_trips"

current = pl.DataFrame({
    "trip_id": [1, 2],
    "status": ["ongoing", "ongoing"],
    "ts": [100, 100],
})
write_deltalake(table_path, current.to_arrow(), mode="overwrite")

# A "late" event for trip 1 (ts=90, OLDER than what's already in Silver) and
# a fresh event for trip 2 (ts=150, newer) arrive in the same micro-batch —
# this is exactly what happens when a province regains connectivity and a
# backlog of stale + fresh events lands together.
incoming = pl.DataFrame({
    "trip_id": [1, 2, 3],
    "status": ["ongoing_STALE_DO_NOT_APPLY", "completed", "ongoing"],
    "ts": [90, 150, 200],
})

dt = DeltaTable(table_path)
(
    dt.merge(
        source=incoming.to_arrow(),
        predicate="target.trip_id = source.trip_id",
        source_alias="source",
        target_alias="target",
    )
    .when_matched_update(
        updates={"status": "source.status", "ts": "source.ts"},
        predicate="source.ts > target.ts",  # <-- the late-data guard
    )
    .when_not_matched_insert_all()
    .execute()
)

result = pl.from_arrow(DeltaTable(table_path).to_pyarrow_table()).sort("trip_id")
print("\nSilver after MERGE (trip 1 must stay 'ongoing' — the stale update was rejected):")
print(result)

assert result.filter(pl.col("trip_id") == 1)["status"][0] == "ongoing", (
    "Late-data guard failed: a stale event overwrote current state"
)
assert result.filter(pl.col("trip_id") == 2)["status"][0] == "completed"
assert result.filter(pl.col("trip_id") == 3)["status"][0] == "ongoing"
print("\nPoC checks passed: PII tokenized at landing, late/out-of-order event rejected.")
