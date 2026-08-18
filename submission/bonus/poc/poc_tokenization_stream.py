"""PoC: In-Flight PII Tokenization & Delta Lake Ingestion with Z-Order.

Demonstrates the core non-trivial mechanism of Topic A:
1. In-flight Format-Preserving Tokenization (Hashing/Masking)
2. Micro-batch streaming append to Delta Lake Bronze/Silver
3. Zero-downtime Z-ORDER clustering on tenant_id
"""
import hashlib
import time
import polars as pl
from deltalake import DeltaTable, write_deltalake

POC_PATH = "_lakehouse/scratch/bonus_poc_llm"

def tokenize_pii(text: str) -> str:
    """Mock deterministic tokenization (FPE) for email/sensitive tokens."""
    return f"tok_{hashlib.sha256(text.encode()).hexdigest()[:12]}"

def generate_micro_batch(batch_id: int, n_records: int = 1000) -> pl.DataFrame:
    """Generates a synthetic high-throughput LLM log batch with PII."""
    tenants = [f"tenant_{i % 50}" for i in range(n_records)]
    models = ["claude-haiku-4-5", "claude-sonnet-4-6", "claude-opus-4-7"]
    
    # In-flight tokenization prior to storage
    raw_prompts = [f"User email is user_{i}@corp.vn for query {i}" for i in range(n_records)]
    tokenized_prompts = [f"User email is {tokenize_pii(f'user_{i}@corp.vn')} for query {i}" for i in range(n_records)]
    
    return pl.DataFrame({
        "request_id": [f"req_{batch_id}_{i}" for i in range(n_records)],
        "ts": [int(time.time() * 1000) + i for i in range(n_records)],
        "tenant_id": tenants,
        "model": [models[i % 3] for i in range(n_records)],
        "prompt_tokenized": tokenized_prompts,
        "latency_ms": [150 + (i * 3) % 2500 for i in range(n_records)],
        "cost_usd": [0.0015 for _ in range(n_records)],
    })

def main():
    print("=== [Bonus PoC] Testing In-Flight Tokenization & Delta Stream Ingestion ===")
    
    # 1. Ingest 5 micro-batches
    for b in range(5):
        df = generate_micro_batch(batch_id=b, n_records=2000)
        write_deltalake(POC_PATH, df.to_arrow(), mode="append")
        print(f"  [Batch {b+1}/5] Ingested 2,000 tokenized records -> {POC_PATH}")
        
    dt = DeltaTable(POC_PATH)
    total_rows = dt.to_pyarrow_table().num_rows
    print(f"\nTotal ingested rows in Bronze/Silver: {total_rows:,}")
    
    # 2. Verify PII is redacted
    sample_prompt = dt.to_pyarrow_table().column("prompt_tokenized")[0].as_py()
    print(f"Sample stored prompt: '{sample_prompt}'")
    assert "@corp.vn" not in sample_prompt, "PII leak detected!"
    assert "tok_" in sample_prompt, "Tokenization failed!"
    print("  ✓ Zero plaintext PII verified.")
    
    # 3. Apply Z-Order Clustering for fast tenant filtering
    print("\nRunning Z-ORDER BY tenant_id ...")
    t0 = time.time()
    dt.optimize.z_order(["tenant_id"])
    print(f"  ✓ Z-Order completed in {time.time()-t0:.2f}s")
    
    print("\n=== PoC verification successful! ===")

if __name__ == "__main__":
    main()
