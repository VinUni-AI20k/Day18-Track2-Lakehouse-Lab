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
# # NB4 — Medallion Pipeline (Bronze → Silver → Gold)
#
# **Use case:** LLM observability — exact schema from slide §6 medallion frame.
# Maps to deliverable bullet 4.
#
# **Pre-req:** ran `make spark-data` (runs `python /workspace/scripts/generate_data.py`
# inside the container — writes 1M rows to `s3a://bronze/llm_calls_raw`).

# %%
import sys
sys.path.append("/workspace/scripts")
from spark_session import get_spark
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

spark = get_spark("nb4_medallion")

BRONZE = "s3a://bronze/llm_calls_raw"
SILVER = "s3a://silver/llm_calls"
GOLD   = "s3a://gold/llm_daily_metrics"

# %% [markdown]
# ## Bronze — verify raw data is loaded

# %%
bronze = spark.read.format("delta").load(BRONZE)
print(f"Bronze rows: {bronze.count():,}")
bronze.printSchema()
bronze.show(2, truncate=80)

# %% [markdown]
# ## Silver — parse JSON, validate, deduplicate
#
# Rules:
# 1. Parse `raw_json` into typed columns using a strict schema.
# 2. Drop rows where JSON is malformed (null parse result).
# 3. Deduplicate on `request_id` — keeps the earliest occurrence (retry pattern).

# %%
parsed_schema = T.StructType([
    T.StructField("model", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("usage", T.StructType([
        T.StructField("input",  T.IntegerType()),
        T.StructField("output", T.IntegerType()),
    ])),
    T.StructField("latency_ms", T.IntegerType()),
    T.StructField("status", T.StringType()),
])

silver_df = (
    bronze
    .withColumn("p", F.from_json("raw_json", parsed_schema))
    .where(F.col("p").isNotNull() & F.col("p.model").isNotNull())
    .select(
        "request_id",
        "ts",
        F.col("p.model").alias("model"),
        F.col("p.user_id").alias("user_id"),
        F.col("p.usage.input").alias("prompt_tokens"),
        F.col("p.usage.output").alias("completion_tokens"),
        F.col("p.latency_ms").alias("latency_ms"),
        F.col("p.status").alias("status"),
        F.to_date("ts").alias("date"),
    )
    .dropDuplicates(["request_id"])   # dedup — this is the observable metric
)

(silver_df.write.format("delta").mode("overwrite")
    .partitionBy("date")
    .save(SILVER))

bronze_n = bronze.count()
silver_n = spark.read.format("delta").load(SILVER).count()
print(f"Silver rows: {silver_n:,}  (Bronze {bronze_n:,} → dedup dropped {bronze_n - silver_n:,})")
assert silver_n < bronze_n, (
    "Silver == Bronze — dedup did not drop any rows. "
    "Re-run `make spark-data` with the latest generator (which seeds ~5% retries)."
)

# %% [markdown]
# ## Gold — aggregate to (date, model) daily metrics
#
# Illustrative cost model (NOT canonical pricing):
#
# | Model | Input ($/M tok) | Output ($/M tok) |
# |---|---|---|
# | claude-haiku-4-5  | 0.80  |  4.00 |
# | claude-sonnet-4-6 | 3.00  | 15.00 |
# | claude-opus-4-7   | 15.00 | 75.00 |

# %%
silver = spark.read.format("delta").load(SILVER)

# Build cost lookup maps — used in withColumn after groupBy
COST = {
    "claude-haiku-4-5":   (0.80,  4.00),
    "claude-sonnet-4-6":  (3.00, 15.00),
    "claude-opus-4-7":    (15.00, 75.00),
}
cost_in  = F.create_map(*[x for k, v in COST.items() for x in (F.lit(k), F.lit(v[0]))])
cost_out = F.create_map(*[x for k, v in COST.items() for x in (F.lit(k), F.lit(v[1]))])

gold_df = (
    silver
    .groupBy("date", "model")
    .agg(
        F.percentile_approx("latency_ms", 0.50).alias("p50_latency_ms"),
        F.percentile_approx("latency_ms", 0.95).alias("p95_latency_ms"),
        F.sum("prompt_tokens").alias("total_prompt_tokens"),
        F.sum("completion_tokens").alias("total_completion_tokens"),
        (F.sum(F.when(F.col("status") != "ok", 1).otherwise(0))
            / F.count("*")).alias("error_rate"),
    )
    .withColumn(
        "cost_usd",
        (F.col("total_prompt_tokens")     * cost_in[F.col("model")]  / F.lit(1_000_000)) +
        (F.col("total_completion_tokens") * cost_out[F.col("model")] / F.lit(1_000_000))
    )
)

(gold_df.write.format("delta").mode("overwrite")
    .partitionBy("date")
    .save(GOLD))

# Z-ORDER by model for fast filter-by-model dashboard queries
spark.sql(f"OPTIMIZE delta.`{GOLD}` ZORDER BY (model)")

# %% [markdown]
# ## Verify Gold deliverable

# %%
gold = spark.read.format("delta").load(GOLD)
gold.orderBy("date", "model").show(25, truncate=False)

n_dates  = gold.select("date").distinct().count()
n_models = gold.select("model").distinct().count()
print(
    f"\n──── Gold deliverable metrics ────\n"
    f"  Distinct dates:   {n_dates:>3}   (target ≥ 7)\n"
    f"  Distinct models:  {n_models:>3}\n"
    f"  Total Gold rows:  {gold.count():>3}   (= dates × models)"
)
assert n_dates >= 7, (
    f"Gold has only {n_dates} date(s) — slide deliverable requires ≥ 7. "
    "Re-run `make spark-data`."
)

# %% [markdown]
# ## ✅ Deliverable check
# - [ ] MinIO console shows `bronze/`, `silver/`, `gold/` all with `_delta_log/`
# - [ ] Silver has fewer rows than Bronze (dedup observable)
# - [ ] Gold spans ≥ 7 dates × 3 models with p50/p95/cost_usd/error_rate populated

# %%
spark.stop()