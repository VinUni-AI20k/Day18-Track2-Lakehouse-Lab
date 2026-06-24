# PoC: Handle Late-Arriving CDC Events with Delta Lake MERGE
import polars as pl
import duckdb
from deltalake import DeltaTable, write_deltalake
import os

# 1. Setup Bronze & Silver paths
BRONZE = "_poc_lakehouse/bronze_cdc"
SILVER = "_poc_lakehouse/silver_trips"

os.makedirs(BRONZE, exist_ok=True)
os.makedirs(SILVER, exist_ok=True)

# 2. Simulate Initial State (Silver table already has an event)
initial_silver = pl.DataFrame({
    "trip_id": ["TRIP_123"],
    "driver_mapped_id": ["HASH_ABC999"], # Tokenized PII
    "status": ["IN_PROGRESS"],
    "updated_at": ["2026-05-04 10:00:00"]
})
write_deltalake(SILVER, initial_silver.to_arrow(), mode="overwrite")
print("Silver initial state:")
print(pl.from_arrow(DeltaTable(SILVER).to_pyarrow_table()))

# 3. Simulate Late-Arriving Event in Bronze
# - Event 1: The trip completed (arrives late from driver's poor network)
# - Event 2: A newer event regarding a different trip
bronze_batch = pl.DataFrame({
    "trip_id": ["TRIP_123", "TRIP_123", "TRIP_456"],
    "driver_mapped_id": ["HASH_ABC999", "HASH_ABC999", "HASH_XYZ111"],
    "status": ["COMPLETED", "PICKED_UP", "IN_PROGRESS"],
    "updated_at": ["2026-05-04 10:15:00", "2026-05-04 10:05:00", "2026-05-04 10:20:00"]
})

# Note: TRIP_123 completed at 10:15 but we also process an older "PICKED_UP" at 10:05.
# This simulates messages arriving out of order.

# 4. DuckDB MERGE for SCD Type 1 & Late Data handling
dt = DeltaTable(SILVER)

dedup_source_arrow = duckdb.sql(f"""
    WITH deduplicated_source AS (
        SELECT trip_id, driver_mapped_id, status, updated_at
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY updated_at DESC) as rn
            FROM bronze_batch
        ) 
        WHERE rn = 1
    )
    SELECT * FROM deduplicated_source
""").arrow()

# In DuckDB + deltalake python we simulate the merge via DeltaTable python API
(
    dt.merge(
        source=dedup_source_arrow,
        predicate="s.trip_id = t.trip_id",
        source_alias="s",
        target_alias="t"
    )
    .when_matched_update_all("s.updated_at > t.updated_at")
    .when_not_matched_insert_all()
    .execute()
)

print("\nSilver table after MERGE (SCD Type 1 with late-data handling):")
print(pl.from_arrow(DeltaTable(SILVER).to_pyarrow_table()))

# Notice that TRIP_123 becomes COMPLETED (10:15:00) 
# and ignores the PICKED_UP (10:05:00) because of `s.updated_at > t.updated_at` logic.