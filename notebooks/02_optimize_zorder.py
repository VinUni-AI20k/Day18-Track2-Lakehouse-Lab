# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # NB2 - Small-File Problem & OPTIMIZE + Z-order (lightweight)
#
# Maps to slide section 5 + deliverable bullet 2.
#
# > Spark equivalent: `OPTIMIZE delta.\`path\` ZORDER BY (user_id)`
# > delta-rs:        `dt.optimize.compact()` + `dt.optimize.z_order(["user_id"])`
#
# Key idea: Z-order helps through file-skipping. Delta stores min/max stats
# per file; queries can skip files whose ranges cannot contain the predicate.

# %%
import _setup  # noqa: F401  -- adds scripts/ to sys.path
import json
import os
import random
import time

import duckdb
import polars as pl
from deltalake import DeltaTable, write_deltalake
from lakehouse import path, reset

table_path = path("scratch", "events_smallfiles")
reset(table_path)  # idempotent

# %% [markdown]
# ## 1. Manufacture the small-file problem
#
# The original 1M-row profile can crash low-memory Windows kernels inside the
# native `z_order()` implementation. This lighter profile preserves the same
# lesson shape:
# - >100 files before OPTIMIZE
# - multiple files after compaction
# - enough spread for visible pruning on `user_id`

# %%
random.seed(42)
TARGET_USER = 4242
BATCHES = 120
ROWS_PER_BATCH = 1_500
PAYLOAD_BYTES = 120
PAYLOAD_VARIANTS = 32

PAYLOADS = [("p" * PAYLOAD_BYTES) + str(i) for i in range(PAYLOAD_VARIANTS)]

for batch in range(BATCHES):
    rows = pl.DataFrame({
        "event_id": list(range(batch * ROWS_PER_BATCH, (batch + 1) * ROWS_PER_BATCH)),
        "kind": [random.choice(["click", "view", "scroll", "purchase"]) for _ in range(ROWS_PER_BATCH)],
        # 100K distinct users -> before z-order, the target user is scattered.
        "user_id": [random.randint(1, 100_000) for _ in range(ROWS_PER_BATCH)],
        "payload": [random.choice(PAYLOADS) for _ in range(ROWS_PER_BATCH)],
    })
    mode = "overwrite" if batch == 0 else "append"
    write_deltalake(table_path, rows.to_arrow(), mode=mode)

dt = DeltaTable(table_path)
files_before = len(dt.files())
print(f"Files before OPTIMIZE: {files_before}")
assert files_before >= 100, "Expected >= 100 files before OPTIMIZE for the lab deliverable."

# %% [markdown]
# ## 2. Benchmark BEFORE optimize
#
# Use Delta's native filter pushdown so the benchmark reflects file pruning.

# %%
def bench(label: str, runs: int = 3) -> float:
    times = []
    n = 0
    for _ in range(runs):
        dt_local = DeltaTable(table_path)
        t0 = time.perf_counter()
        tbl = dt_local.to_pyarrow_table(
            filters=[("user_id", "=", TARGET_USER), ("kind", "=", "purchase")]
        )
        n = tbl.num_rows
        times.append(time.perf_counter() - t0)
    times.sort()
    median = times[len(times) // 2]
    print(f"{label:25s}  count={n}  median={median*1000:6.1f} ms  (n={runs})")
    return median


before = bench("BEFORE OPTIMIZE")

# %% [markdown]
# ## 3. OPTIMIZE (compact small files) + Z-ORDER (co-locate by user_id)
#
# Keep target files small enough that multiple files remain after compaction.

# %%
TARGET_SIZE = 256 * 1024  # 256 KB

dt = DeltaTable(table_path)
dt.optimize.compact(target_size=TARGET_SIZE)
dt.optimize.z_order(["user_id"], target_size=TARGET_SIZE)

dt = DeltaTable(table_path)
files_after = len(dt.files())
print(f"Files after OPTIMIZE+ZORDER: {files_after}  (was {files_before})")

# %% [markdown]
# ## 4. Benchmark AFTER

# %%
after = bench("AFTER OPTIMIZE+ZORDER")
print(f"\nSpeedup: {before/max(after, 1e-6):.1f}x  (target >= 3x)")
print(f"File reduction: {files_before} -> {files_after}  ({files_before/max(files_after,1):.0f}x fewer)")

# %% [markdown]
# ## 5. Why this works - inspect file-level stats

# %%
log_dir = os.path.join(table_path, "_delta_log")
last_log = sorted(f for f in os.listdir(log_dir) if f.endswith(".json"))[-1]
print(f"Inspecting {last_log}:")
hits = 0
ranges = []
with open(os.path.join(log_dir, last_log), encoding="utf-8") as fh:
    for line in fh:
        e = json.loads(line)
        if "add" in e and "stats" in e["add"]:
            stats = json.loads(e["add"]["stats"])
            mn = stats.get("minValues", {}).get("user_id")
            mx = stats.get("maxValues", {}).get("user_id")
            if mn is not None:
                ranges.append((mn, mx))
                if mn <= TARGET_USER <= mx:
                    hits += 1
for mn, mx in sorted(ranges):
    marker = " <- contains target" if mn <= TARGET_USER <= mx else ""
    print(f"  file user_id range: [{mn:>6}, {mx:>6}]{marker}")

pruned_ratio = files_after / max(hits, 1)
print(
    f"\n---- Z-order deliverable metrics ----\n"
    f"  Speedup (wall-clock):   {before/max(after, 1e-6):>5.1f}x   (target >= 3x)\n"
    f"  Files-pruned ratio:     {pruned_ratio:>5.1f}x   (target >= 10x)   "
    f"[{hits} of {files_after} files cover user_id={TARGET_USER}]"
)

# %% [markdown]
# ## Deliverable check
# - [ ] Speedup >= 3x or files-pruned ratio >= 10x
# - [ ] File count dropped substantially after compact()
# - [ ] Stats inspection shows about one file covers `user_id=4242`
# - [ ] Screenshot the printed numbers
