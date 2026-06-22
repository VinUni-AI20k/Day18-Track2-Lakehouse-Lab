# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # NB2 — Small-File Problem & OPTIMIZE + ZORDER
#
# **Mục tiêu:** prove the 3–10× speedup claim from slide §6 (Storage Optimization).
# Maps to deliverable bullet 2.

# %%
import sys, time, random
sys.path.append("/workspace/scripts")
from spark_session import get_spark, reset_path
from delta.tables import DeltaTable

spark = get_spark("nb2_optimize_zorder")
path = "s3a://lakehouse/events_smallfiles"

# %% [markdown]
# ## 0. Reset path (idempotent re-run)
#
# Each run starts fresh — otherwise repeated appends keep growing the table
# and the benchmark drifts.

# %%
reset_path(spark, path)

# %% [markdown]
# ## 1. Manufacture the small-file problem
#
# Append 200 tiny batches → 200 small files. Realistic streaming-ingestion shape.

# %%
for batch in range(200):
    rows = [(i, random.choice(["click", "view", "scroll", "purchase"]),
             random.randint(1, 10000))
            for i in range(batch * 500, (batch + 1) * 500)]
    df = spark.createDataFrame(rows, ["event_id", "kind", "user_id"])
    mode = "overwrite" if batch == 0 else "append"
    df.write.format("delta").mode(mode).save(path)

# %% [markdown]
# ## 2. Benchmark BEFORE optimize

# %%
def bench(label):
    # Warm-up read so we measure query, not cold metadata fetch
    spark.read.format("delta").load(path).limit(1).count()
    t0 = time.time()
    n = (spark.read.format("delta").load(path)
            .where("user_id = 4242 AND kind = 'purchase'").count())
    dt = time.time() - t0
    print(f"{label:25s}  count={n}  time={dt:.2f}s")
    return dt

before_detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select("numFiles", "sizeInBytes").collect()[0]
print(f"Files before OPTIMIZE: {before_detail['numFiles']}  size={before_detail['sizeInBytes']} bytes")
before = bench("BEFORE OPTIMIZE+ZORDER")

# %% [markdown]
# ## 3. OPTIMIZE + ZORDER

# %%
spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (user_id)")

# %% [markdown]
# ## 4. Benchmark AFTER

# %%
after = bench("AFTER OPTIMIZE+ZORDER")
after_detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select("numFiles", "sizeInBytes").collect()[0]
speedup = before / max(after, 1e-6)
file_reduction = before_detail["numFiles"] / max(after_detail["numFiles"], 1)
print(f"\nSpeedup: {speedup:.1f}×  (target ≥ 3×)")
print(f"Files after OPTIMIZE: {after_detail['numFiles']}  size={after_detail['sizeInBytes']} bytes")
print(f"File-count reduction: {file_reduction:.1f}×")

# %% [markdown]
# ## 5. Inspect file count change

# %%
spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select(
    "numFiles", "sizeInBytes"
).show()

# %% [markdown]
# ## ✅ Deliverable check
# - [ ] Speedup ≥ 3×
# - [ ] `numFiles` dropped substantially after OPTIMIZE
# - [ ] Screenshot the printed comparison

# %%
spark.stop()
