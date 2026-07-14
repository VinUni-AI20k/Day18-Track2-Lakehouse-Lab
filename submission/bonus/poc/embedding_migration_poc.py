import os
import shutil
import polars as pl
from deltalake import DeltaTable, write_deltalake

# PoC: Embedding Migration using Delta Lake Schema Evolution
# This script demonstrates how to add a new embedding version (v2) to an existing
# table without breaking the original embedding version (v1), ensuring reproducibility.

POC_PATH = "scratch/embedding_poc"

def reset_poc():
    if os.path.exists(POC_PATH):
        shutil.rmtree(POC_PATH)

def generate_mock_embeddings(dim=3, prefix="v1"):
    # Mocking embeddings for the PoC
    return [[float(i) for i in range(dim)] for _ in range(5)]

def main():
    print("--- 1. Initialize Table with Embedding v1 ---")
    reset_poc()
    
    # Initial data with v1 embeddings
    v1_data = pl.DataFrame({
        "chunk_id": [1, 2, 3, 4, 5],
        "text": ["legal doc A", "legal doc B", "legal doc C", "legal doc D", "legal doc E"],
        "embedding_v1": generate_mock_embeddings(dim=3, prefix="v1")
    })
    
    write_deltalake(POC_PATH, v1_data.to_arrow(), mode="overwrite")
    
    dt = DeltaTable(POC_PATH)
    print("Schema v1:")
    print(dt.schema().to_pyarrow().names)
    print(pl.from_arrow(dt.to_pyarrow_table()))

    print("\n--- 2. Model Upgrade: Generate Embedding v2 ---")
    # Instead of overwriting embedding_v1, we append a new column.
    # In production, we'd process this in batches and MERGE into the Delta table.
    # For this PoC, we simulate reading the table, generating v2, and updating via schema evolution.
    
    current_data = pl.from_arrow(dt.to_pyarrow_table())
    
    # Generate new v2 embeddings
    v2_embeddings = [[float(i*2) for i in range(3)] for _ in range(5)]
    
    # Add new column
    updated_data = current_data.with_columns(
        pl.Series("embedding_v2", v2_embeddings)
    )
    
    # Write back with schema evolution
    write_deltalake(POC_PATH, updated_data.to_arrow(), mode="overwrite", schema_mode="overwrite")
    
    dt = DeltaTable(POC_PATH)
    print("Schema v2 (Notice embedding_v1 is still intact):")
    print(dt.schema().to_pyarrow().names)
    print(pl.from_arrow(dt.to_pyarrow_table()))

    print("\n--- 3. Time Travel: Reproducing 5-Year-Old Query ---")
    # A query citing version 0 (when only v1 existed)
    dt_v0 = DeltaTable(POC_PATH, version=0)
    print("Schema of Table Version 0:")
    print(dt_v0.schema().to_pyarrow().names)
    
    print("\nConclusion: By adding columns and using Delta's Time Travel, we achieve 100% reproducibility.")

if __name__ == "__main__":
    main()
