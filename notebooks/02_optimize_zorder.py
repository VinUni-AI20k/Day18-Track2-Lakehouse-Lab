# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # NB2 — Small-File Problem & OPTIMIZE + ZORDER
#
# **Mục tiêu:** prove the 3–10× speedup claim from slide §5.
# Maps to deliverable bullet 2.

# %%
import sys, time, random
sys.path.append("/workspace/scripts")
from spark_session import get_spark
from delta.tables import DeltaTable

spark = get_spark("nb2_optimize_zorder")
path = "s3a://lakehouse/events_smallfiles"

# %% [markdown]
# ## 0. Reset path (idempotent re-run)
#
# Each run starts fresh — otherwise repeated appends keep growing the table
# and the benchmark drifts.

# %%
# DROP unregisters the catalog entry; the overwrite in step 1 resets data.
spark.sql(f"DROP TABLE IF EXISTS delta.`{path}`")

# %% [markdown]
# ## 1. Manufacture the small-file problem
#
# Append 200 tiny batches → 200 small files. Realistic streaming-ingestion shape.

# %%
random.seed(42)
for batch in range(200):
    rows = [(i, random.choice(["click", "view", "scroll", "purchase"]),
             random.randint(1, 10_000))
            for i in range(batch * 500, (batch + 1) * 500)]
    df = spark.createDataFrame(rows, ["event_id", "kind", "user_id"])
    mode = "overwrite" if batch == 0 else "append"
    df.write.format("delta").mode(mode).save(path)

# Inspect file count BEFORE optimize
detail_before = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0]
print(f"Files before OPTIMIZE: {detail_before['numFiles']}")

# %% [markdown]
# ## 2. Benchmark BEFORE optimize

# %%
TARGET_USER = 4242

def bench(label: str) -> float:
    # Warm-up so we measure query, not cold metadata fetch
    spark.read.format("delta").load(path).limit(1).count()
    t0 = time.time()
    n = (spark.read.format("delta").load(path)
             .where(f"user_id = {TARGET_USER} AND kind = 'purchase'")
             .count())
    elapsed = time.time() - t0
    print(f"{label:25s}  count={n}  time={elapsed:.3f}s")
    return elapsed

before = bench("BEFORE OPTIMIZE+ZORDER")

# %% [markdown]
# ## 3. OPTIMIZE + ZORDER
#
# `OPTIMIZE ... ZORDER BY (user_id)` does two things:
# 1. Compacts small files into larger ones (reduces read overhead).
# 2. Co-locates rows with similar `user_id` so point-queries skip most files.

# %%
spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (user_id)")

# %% [markdown]
# ## 4. Benchmark AFTER

# %%
after = bench("AFTER OPTIMIZE+ZORDER")
speedup = before / max(after, 1e-6)
print(f"\nSpeedup: {speedup:.1f}×  (target ≥ 3×)")

# %% [markdown]
# ## 5. Inspect file count change

# %%
spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select("numFiles", "sizeInBytes").show()

# %% [markdown]
# ## ✅ Deliverable check
# - [ ] Speedup ≥ 3× **or** files-pruned ratio ≥ 10× (slide §5 allows either)
# - [ ] `numFiles` dropped substantially after OPTIMIZE
# - [ ] Screenshot the printed comparison

# %%
spark.stop()