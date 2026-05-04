# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # NB1 — Delta Lake Basics (lightweight path)
#
# **Stack:** `deltalake` (delta-rs) + Polars + DuckDB. No Spark, no JVM.
# Maps to slide §2 (Delta Lake) + deliverable bullet 1.
#
# > Spark equivalent: `spark.read.format("delta").load(path)` ↔ `DeltaTable(path).to_pyarrow_table()`.
# > Same on-disk format, different binding.

# %%
import _setup  # noqa: F401  -- adds scripts/ to sys.path (file-relative)
import polars as pl
from pathlib import Path
from deltalake import DeltaTable, write_deltalake
from lakehouse import path, reset

pl.Config.set_tbl_formatting("ASCII_MARKDOWN")

table_path = path("scratch", "users_delta")
reset(table_path)  # idempotent rerun

# %% [markdown]
# ## 1. Write a Delta table

# %%
df = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["alice", "bob", "charlie"],
    "age": [30, 25, 35],
    "city": ["Hanoi", "HCMC", "Danang"],
})
write_deltalake(table_path, df.to_arrow(), mode="overwrite")

# %% [markdown]
# ## 2. Read it back + inspect transaction log
#
# Look at `_lakehouse/scratch/users_delta/_delta_log/00000000000000000000.json` —
# that's the transaction log. Same JSON format Spark/Databricks would write.

# %%
dt = DeltaTable(table_path)
print(pl.from_arrow(dt.to_pyarrow_table()))
log_jsons = sorted((Path(table_path) / "_delta_log").glob("*.json"))
print("\nDelta transaction log JSON files:")
for log_file in log_jsons:
    print(f"  - {log_file.name}")
assert log_jsons, "Expected Delta transaction log JSON files under _delta_log/"

print("\nHistory:")
for h in dt.history():
    print(f"  v{h['version']}  {h['operation']}  {h.get('operationMetrics', {})}")

# %% [markdown]
# ## 3. Schema enforcement — try to write a wrong schema

# %%
bad = pl.DataFrame({"id": [4], "name": ["dan"], "age": ["thirty"], "city": ["Hue"]})
bad_write_blocked = False
try:
    write_deltalake(table_path, bad.to_arrow(), mode="append")
    print("UNEXPECTED: bad write succeeded - schema enforcement broken")
except Exception as e:
    bad_write_blocked = True
    msg = str(e).splitlines()[0][:120]
    print(f"BLOCKED by schema enforcement (expected): {type(e).__name__}: {msg}")
assert bad_write_blocked, "Schema enforcement should block age=str append"

# %% [markdown]
# ## 4. Schema evolution (opt-in)

# %%
new = pl.DataFrame({
    "id": [4], "name": ["dan"], "age": [28], "city": ["Hue"], "tier": ["premium"],
})
write_deltalake(table_path, new.to_arrow(), mode="append", schema_mode="merge")
dt = DeltaTable(table_path)
# Sort by id so the printout is stable across reruns — Delta does not
# preserve write-order across appends.
print(pl.from_arrow(dt.to_pyarrow_table()).sort("id"))
schema_cols = dt.schema().to_pyarrow().names
assert "tier" in schema_cols, "schema_mode='merge' should add the tier column"

# %% [markdown]
# ## 5. Bonus — query with DuckDB (zero copy)

# %%
import duckdb
tier_counts = duckdb.sql(f"""
    SELECT COALESCE(tier, '(null)') AS tier, count(*) AS n
    FROM delta_scan('{table_path}')
    GROUP BY 1
    ORDER BY 1
""").fetchall()
print("Tier counts:", tier_counts)
assert len(tier_counts) == 2, "Expected two tier groups: null legacy rows and premium"
assert sum(n for _, n in tier_counts) == 4, "Expected 4 total rows after schema evolution"

# %% [markdown]
# ## ✅ Deliverable check
# - [x] `_delta_log/` contains JSON files
# - [x] Schema enforcement blocked the bad write
# - [x] schema_mode="merge" added the `tier` column
# - [x] DuckDB query returned 2 tier groups
