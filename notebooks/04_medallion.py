# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # NB4 — Medallion Pipeline (Bronze → Silver → Gold), lightweight
#
# **Use case:** LLM observability — exact schema from slide §6 medallion frame.
# Maps to deliverable bullet 4 (the Milestone-1 Lakehouse artifact).
#
# Pre-req: ran `make data` (or `python scripts/generate_data_lite.py`).

# %%
import _setup  # noqa: F401  -- adds scripts/ to sys.path
import polars as pl
import duckdb
from pathlib import Path
from deltalake import DeltaTable, write_deltalake
from lakehouse import path, reset

pl.Config.set_tbl_formatting("ASCII_MARKDOWN")

BRONZE = path("bronze", "llm_calls_raw")
SILVER = path("silver", "llm_calls")
GOLD   = path("gold",   "llm_daily_metrics")

# %% [markdown]
# ## Bronze — verify raw is loaded

# %%
bronze_n = DeltaTable(BRONZE).to_pyarrow_table().num_rows
print(f"Bronze rows: {bronze_n:,}")
bronze_preview = pl.from_arrow(DeltaTable(BRONZE).to_pyarrow_table().slice(0, 2))
print(bronze_preview.write_csv().strip())
assert (Path(BRONZE) / "_delta_log").exists(), "Bronze Delta table must exist on disk"

# %% [markdown]
# ## Silver — parse, validate, dedup
#
# Rules: drop malformed JSON, dedupe by `request_id`, project typed columns.

# %%
reset(SILVER)

# DuckDB does the JSON parse + dedup in one query — Polars also works,
# DuckDB just has nicer JSON syntax for this case.
bronze_quality = duckdb.sql(f"""
    SELECT
      count(*) AS bronze_rows,
      count(DISTINCT request_id) AS unique_request_ids,
      count(*) - count(DISTINCT request_id) AS duplicate_rows,
      sum(CASE WHEN json_valid(raw_json) THEN 0 ELSE 1 END) AS malformed_json_rows
    FROM delta_scan('{BRONZE}')
""").fetchone()
print(
    "Bronze quality gate: "
    f"rows={bronze_quality[0]:,}, unique_request_ids={bronze_quality[1]:,}, "
    f"duplicates={bronze_quality[2]:,}, malformed_json={bronze_quality[3]:,}"
)

silver_arrow = duckdb.sql(f"""
    WITH parsed AS (
      SELECT
        request_id,
        ts,
        CAST(ts AS DATE)                            AS date,
        json_extract_string(raw_json, '$.model')          AS model,
        json_extract_string(raw_json, '$.user_id')        AS user_id,
        CAST(json_extract(raw_json, '$.usage.input')  AS INTEGER) AS prompt_tokens,
        CAST(json_extract(raw_json, '$.usage.output') AS INTEGER) AS completion_tokens,
        CAST(json_extract(raw_json, '$.latency_ms')   AS INTEGER) AS latency_ms,
        json_extract_string(raw_json, '$.status')         AS status,
        ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts) AS rn
      FROM delta_scan('{BRONZE}')
      WHERE json_valid(raw_json)
    )
    SELECT request_id, ts, date, model, user_id,
           prompt_tokens, completion_tokens, latency_ms, status
    FROM parsed
    WHERE rn = 1
      AND model IS NOT NULL
      AND user_id IS NOT NULL
      AND prompt_tokens IS NOT NULL
      AND completion_tokens IS NOT NULL
      AND latency_ms IS NOT NULL
      AND latency_ms > 0
      AND status IS NOT NULL
""").arrow()

write_deltalake(SILVER, silver_arrow, mode="overwrite", partition_by=["date"])

silver_n = DeltaTable(SILVER).to_pyarrow_table().num_rows
print(f"Silver rows: {silver_n:,}  (Bronze {bronze_n:,} -> dedup/quality dropped {bronze_n - silver_n:,})")
assert silver_n < bronze_n, (
    "Silver has the same row count as Bronze - dedup did not run. "
    "Did you regenerate Bronze with the latest generator (which injects retries)?"
)
assert (Path(SILVER) / "_delta_log").exists(), "Silver Delta table must exist on disk"

silver_profile = duckdb.sql(f"""
    SELECT
      count(*) AS silver_rows,
      min(date) AS first_date,
      max(date) AS last_date,
      count(DISTINCT date) AS n_dates,
      count(DISTINCT model) AS n_models,
      count(DISTINCT request_id) AS unique_request_ids
    FROM delta_scan('{SILVER}')
""").fetchone()
print(
    "Silver profile: "
    f"rows={silver_profile[0]:,}, dates={silver_profile[3]}, models={silver_profile[4]}, "
    f"range={silver_profile[1]}..{silver_profile[2]}"
)
assert silver_profile[0] == silver_profile[5], "Silver should contain one row per request_id"

# %% [markdown]
# ## Gold — aggregate to (date, model) metrics

# %%
reset(GOLD)

# Illustrative cost model — NOT canonical pricing.
# (input USD / 1M tokens, output USD / 1M tokens)
COST_TABLE = """
  VALUES
    ('claude-haiku-4-5',  0.80,  4.00),
    ('claude-sonnet-4-6', 3.00, 15.00),
    ('claude-opus-4-7', 15.00, 75.00)
"""

gold_arrow = duckdb.sql(f"""
    WITH cost(model, c_in, c_out) AS ({COST_TABLE})
    SELECT
      s.date,
      s.model,
      COUNT(*) AS n_requests,
      QUANTILE_CONT(s.latency_ms, 0.50) AS p50_latency_ms,
      QUANTILE_CONT(s.latency_ms, 0.95) AS p95_latency_ms,
      SUM(s.prompt_tokens)              AS total_prompt_tokens,
      SUM(s.completion_tokens)          AS total_completion_tokens,
      AVG(CASE WHEN s.status <> 'ok' THEN 1.0 ELSE 0.0 END) AS error_rate,
      (SUM(s.prompt_tokens)     * c.c_in  / 1e6) +
      (SUM(s.completion_tokens) * c.c_out / 1e6) AS cost_usd
    FROM delta_scan('{SILVER}') s
    JOIN cost c USING (model)
    GROUP BY s.date, s.model, c.c_in, c.c_out
    ORDER BY s.date, s.model
""").arrow()

write_deltalake(GOLD, gold_arrow, mode="overwrite", partition_by=["date"])

# Z-order for fast filter-by-model dashboards
DeltaTable(GOLD).optimize.z_order(["model"])
assert (Path(GOLD) / "_delta_log").exists(), "Gold Delta table must exist on disk"

# %% [markdown]
# ## Verify Gold

# %%
gold_df = pl.from_arrow(DeltaTable(GOLD).to_pyarrow_table())
print(gold_df.sort(["date", "model"]).write_csv().strip())

# Slide-5 deliverable: "Gold p50/p95/cost qua ≥ 7 ngày". Make that explicit.
n_dates = gold_df.select("date").n_unique()
n_models = gold_df.select("model").n_unique()
print(
    f"\n---- Gold deliverable metrics ----\n"
    f"  Distinct dates:   {n_dates:>3}   (target >= 7)\n"
    f"  Distinct models:  {n_models:>3}\n"
    f"  Total Gold rows:  {gold_df.height:>3}   (= dates x models)"
)
assert n_dates >= 7, (
    f"Gold has only {n_dates} dates - slide deliverable requires >= 7. "
    "Re-run `make data` (the generator spreads across 7 UTC days)."
)
assert n_models == 3, f"Gold should include exactly 3 LLM models, got {n_models}"
assert gold_df.height >= 21, "Gold should contain at least 7 dates x 3 models"
assert gold_df.filter(pl.col("p50_latency_ms").is_null() | pl.col("p95_latency_ms").is_null()).height == 0
assert gold_df.filter(pl.col("p95_latency_ms") < pl.col("p50_latency_ms")).height == 0
assert gold_df.filter(pl.col("cost_usd").is_null() | (pl.col("cost_usd") <= 0)).height == 0
assert gold_df.filter((pl.col("error_rate") < 0) | (pl.col("error_rate") > 1)).height == 0
print("Gold quality checks passed: p50/p95 populated, p95>=p50, cost_usd>0, 0<=error_rate<=1")

# %% [markdown]
# ## ✅ Deliverable check
# - [x] All three tables exist under `_lakehouse/{bronze,silver,gold}/`
# - [x] Silver has fewer rows than Bronze (dedup worked)
# - [x] Gold spans ≥ 7 dates × 3 models (slide §6 medallion contract)
# - [x] Cost & error_rate columns populated and non-zero
