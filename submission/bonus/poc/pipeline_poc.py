import os
import shutil
import hashlib
import json
import time
import random
import polars as pl
import duckdb
from deltalake import DeltaTable, write_deltalake

# Set up paths for the PoC
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
POC_DATA_DIR = os.path.join(BASE_DIR, "_lakehouse_poc")
BRONZE_PATH = os.path.join(POC_DATA_DIR, "bronze_telemetry")
SILVER_PATH = os.path.join(POC_DATA_DIR, "silver_telemetry")

def cleanup():
    """Wipe any existing POC data to ensure a clean run."""
    if os.path.exists(POC_DATA_DIR):
        print(f"Cleaning up existing PoC directory: {POC_DATA_DIR}")
        shutil.rmtree(POC_DATA_DIR)
    os.makedirs(POC_DATA_DIR, exist_ok=True)

def generate_mock_bronze_data(num_robots=10, records_per_robot=500):
    """Simulate raw streaming telemetry landing in Bronze."""
    print(f"\n[1/5] Generating raw telemetry logs for {num_robots} robots...")
    
    raw_records = []
    users = ["Alice Johnson", "Bob Smith", "Charlie Brown", "Diana Prince", "Evan Wright"]
    commands = ["move_forward 5m", "turn_left 90deg", "lift_arm 30cm", "dock_charger", "stop"]
    
    t_start = time.time() - 3600  # Start 1 hour ago
    
    for r_idx in range(num_robots):
        robot_id = f"robot_{r_idx:03d}"
        owner = random.choice(users)
        
        for record_idx in range(records_per_robot):
            ts = t_start + (record_idx * 2)  # Update every 2 seconds
            dt_str = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts))
            date_str = dt_str.split(' ')[0]
            
            # Nested raw telemetry & voice prompt payload
            payload = {
                "owner_name": owner,
                "gps": {
                    "lat": 21.0285 + random.uniform(-0.01, 0.01),
                    "lon": 105.8542 + random.uniform(-0.01, 0.01)
                },
                "telemetry": {
                    "battery": max(10, 100 - (record_idx // 10)),
                    "joint_angles": [random.randint(0, 180) for _ in range(6)],
                    "error_code": 0 if random.random() > 0.02 else random.choice([101, 102, 303])
                },
                "interaction": {
                    "voice_command": random.choice(commands),
                    "llm_response_latency_ms": random.randint(150, 2000),
                    "confidence_score": round(random.uniform(0.7, 0.99), 2)
                }
            }
            
            raw_records.append({
                "robot_id": robot_id,
                "ts": dt_str,
                "date": date_str,
                "raw_json": json.dumps(payload)
            })
            
    # Write to Bronze Delta Table
    df = pl.DataFrame(raw_records)
    write_deltalake(BRONZE_PATH, df.to_arrow(), mode="overwrite")
    print(f"  ✓ Bronze table created at {BRONZE_PATH} with {len(raw_records):,} raw telemetry entries.")

def tokenize_and_clean_silver():
    """Parse JSON, redact PII (hash usernames, fuzz GPS), and save to Silver."""
    print("\n[2/5] Parsing raw Bronze JSON and tokenizing PII into Silver layer...")
    
    # We define a SQL-UDF for SHA-256 hashing to redact usernames in DuckDB
    # and fuzzed GPS coordinates (rounded to 3 decimals to protect location privacy)
    duckdb.sql(f"""
        CREATE OR REPLACE TEMP TABLE parsed AS 
        SELECT
            robot_id,
            ts,
            CAST(ts AS TIMESTAMP) as timestamp,
            CAST(date AS DATE) as date,
            -- Redact PII: Hash Owner Name
            sha256(json_extract_string(raw_json, '$.owner_name')) AS anonymized_owner_hash,
            -- Redact PII: Fuzz GPS values to 3 decimal places (~100m error)
            round(CAST(json_extract(raw_json, '$.gps.lat') AS DOUBLE), 3) AS fuzzed_lat,
            round(CAST(json_extract(raw_json, '$.gps.lon') AS DOUBLE), 3) AS fuzzed_lon,
            -- Extract Telemetry metrics
            CAST(json_extract(raw_json, '$.telemetry.battery') AS INTEGER) AS battery_percent,
            CAST(json_extract(raw_json, '$.telemetry.error_code') AS INTEGER) AS error_code,
            -- Extract LLM interaction metrics
            json_extract_string(raw_json, '$.interaction.voice_command') AS command,
            CAST(json_extract(raw_json, '$.interaction.llm_response_latency_ms') AS INTEGER) AS latency_ms
        FROM delta_scan('{BRONZE_PATH}')
    """)
    
    # Check if a parsed sample row is indeed redacted
    sample = duckdb.sql("SELECT anonymized_owner_hash, fuzzed_lat, command FROM parsed LIMIT 1").fetchone()
    print(f"  [Security Audit] Redacted Owner Name hash: {sample[0][:15]}... (originally a clear text name)")
    print(f"  [Security Audit] Fuzzed Latitude: {sample[1]} (truncated to 3 decimal places)")
    
    # Write to Silver Delta Table partitioned by Date
    silver_arrow = duckdb.sql("SELECT * FROM parsed").arrow()
    write_deltalake(SILVER_PATH, silver_arrow, mode="overwrite", partition_by=["date"])
    print(f"  ✓ Silver table written at {SILVER_PATH}")

def optimize_and_zorder():
    """Compact files and apply Z-ordering by robot_id on the Silver table."""
    print("\n[3/5] Performing OPTIMIZE & Z-ORDER on Silver table...")
    
    dt = DeltaTable(SILVER_PATH)
    files_before = len(dt.files())
    print(f"  Files before optimization: {files_before}")
    
    # Run optimize compaction and Z-order
    dt.optimize.compact()
    dt.optimize.z_order(["robot_id"])
    
    dt.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
    
    dt_after = DeltaTable(SILVER_PATH)
    files_after = len(dt_after.files())
    print(f"  Files after optimization & vacuum: {files_after}")
    print(f"  ✓ Compaction factor: {files_before / max(files_after, 1):.1f}x reduction in files.")

def benchmark_point_queries(target_robot="robot_005"):
    """Measure Z-Order query speedup using DuckDB point scan."""
    print(f"\n[4/5] Running benchmark query for point lookup of robot: {target_robot}")
    
    t0 = time.time()
    res = pl.from_arrow(duckdb.sql(f"""
        SELECT 
            robot_id,
            avg(latency_ms) as avg_latency_ms,
            min(battery_percent) as min_battery_percent,
            count(CASE WHEN error_code > 0 THEN 1 END) as total_errors
        FROM delta_scan('{SILVER_PATH}')
        WHERE robot_id = '{target_robot}'
        GROUP BY 1
    """).arrow())
    t_delta = time.time() - t0
    
    print(res)
    print(f"  ✓ Query executed in {t_delta * 1000:.2f} ms")

def show_gold_aggregate():
    """Materialize daily fleet status and success rate (Gold metrics)."""
    print("\n[5/5] Extracting fleet-level Gold metrics...")
    
    gold_df = pl.from_arrow(duckdb.sql(f"""
        SELECT 
            date,
            count(distinct robot_id) as active_robots,
            count(*) as total_commands_executed,
            round(avg(latency_ms), 1) as avg_llm_latency_ms,
            round(100.0 * count(CASE WHEN error_code = 0 THEN 1 END) / count(*), 2) as command_success_rate
        FROM delta_scan('{SILVER_PATH}')
        GROUP BY 1
        ORDER BY 1
    """).arrow())
    
    print(gold_df)
    print("\n✓ MVP Pipeline PoC run completed successfully!")

if __name__ == "__main__":
    cleanup()
    generate_mock_bronze_data(num_robots=10, records_per_robot=1000)
    tokenize_and_clean_silver()
    optimize_and_zorder()
    benchmark_point_queries()
    show_gold_aggregate()
