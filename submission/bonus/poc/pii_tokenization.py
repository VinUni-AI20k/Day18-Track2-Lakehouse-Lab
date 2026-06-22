# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # PoC: PII Tokenization + MERGE Dedup cho LLM Observability
#
# Demo cơ chế khó nhất trong architecture: tokenization PII mapping + dedup
# với Delta MERGE. Chạy được từ clean checkout với lightweight stack.

# %%
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
import hashlib, os, json
import polars as pl
from deltalake import DeltaTable, write_deltalake
from lakehouse import path, reset

PII_MAP = path("bonus", "pii_mapping")
RAW_LOG = path("bonus", "raw_llm_sample")
reset(PII_MAP)
reset(RAW_LOG)

# %% [markdown]
# ## 1. Generate sample data với PII

# %%
sample = pl.DataFrame({
    "request_id": ["r001", "r002", "r003"],
    "user_phone": ["0901234567", "0912345678", "0901234567"],  # r003 duplicate
    "user_cmnd": ["123456789", "987654321", "123456789"],
    "prompt":     ["How to file tax?", "What is AI?", "How to file tax?"],
    "latency_ms": [1200, 800, 1500],
    "status":     ["ok", "ok", "error"],
})
print("Sample raw:")
print(sample)

# %% [markdown]
# ## 2. Tokenization function
# Map PII → hash, lưu mapping table riêng biệt (encrypted trong production).

# %%
def tokenize(df: pl.DataFrame) -> pl.DataFrame:
    """Thay PII columns bằng hash, lưu mapping vào Delta table."""
    pii_cols = ["user_phone", "user_cmnd"]
    mappings = []

    for col in pii_cols:
        unique_vals = df.select(col).unique().to_series().to_list()
        for val in unique_vals:
            if val is None:
                continue
            token = hashlib.sha256(val.encode()).hexdigest()[:16]
            mappings.append({"pii_type": col, "original": val, "token": token})

    mapping_df = pl.DataFrame(mappings)

    # Append mapping table
    if os.path.isdir(PII_MAP) and DeltaTable(PII_MAP).to_pyarrow_table().num_rows > 0:
        existing = pl.from_arrow(DeltaTable(PII_MAP).to_pyarrow_table())
        mapping_df = pl.concat([existing, mapping_df]).unique(subset=["pii_type", "original"])
        (DeltaTable(PII_MAP)
         .merge(source=mapping_df.to_arrow(),
                predicate="t.pii_type = s.pii_type AND t.original = s.original",
                source_alias="s", target_alias="t")
         .when_not_matched_insert_all()
         .execute())
    else:
        write_deltalake(PII_MAP, mapping_df.to_arrow(), mode="overwrite")

    # Replace PII columns with tokens
    result = df.clone()
    for col in pii_cols:
        tokens = {}
        for row in mapping_df.filter(pl.col("pii_type") == col).iter_rows(named=True):
            tokens[row["original"]] = row["token"]
        result = result.with_columns(
            pl.col(col).replace_strict(tokens).alias(col + "_token")
        ).drop(col)

    return result

tokenized = tokenize(sample)
print("\nTokenized (PII removed):")
print(tokenized)

# %% [markdown]
# ## 3. Ghi raw log + verify mapping table

# %%
write_deltalake(RAW_LOG, tokenized.to_arrow(), mode="overwrite")

print("\nPII Mapping table:")
print(pl.from_arrow(DeltaTable(PII_MAP).to_pyarrow_table()))

# %% [markdown]
# ## 4. Dedup bằng MERGE (giống NB3)

# %%
# Simulate batch 2 với 1 record mới + 1 duplicate
batch2 = pl.DataFrame({
    "request_id": ["r003", "r004"],
    "user_phone_token": ["abc", "def"],
    "user_cmnd_token":  ["ghi", "jkl"],
    "prompt":     ["How to file tax?", "New query"],
    "latency_ms": [1500, 600],
    "status":     ["error", "ok"],
})

(DeltaTable(RAW_LOG)
 .merge(source=batch2.to_arrow(),
        predicate="t.request_id = s.request_id",
        source_alias="s", target_alias="t")
 .when_matched_update_all()
 .when_not_matched_insert_all()
 .execute())

print("\nAfter MERGE (r003 updated, r004 inserted):")
print(pl.from_arrow(DeltaTable(RAW_LOG).to_pyarrow_table()).sort("request_id"))
