# Day 18 Lakehouse Lab — Execution Evidence

## 01_delta_basics.ipynb
BLOCKED by schema enforcement (expected): Exception: Cast error: Cannot cast string 'thirty' to value of Int64 type

## 02_optimize_zorder.ipynb
Files before OPTIMIZE: 200
Files after OPTIMIZE+ZORDER: 55  (was 200)
Speedup: 13.0×  (target ≥ 3×)
  Speedup (wall-clock):    13.0×   (target ≥ 3×)
  Files-pruned ratio:      55.0×   (target ≥ 10×)   [1 of 55 files cover user_id=4242]

## 03_time_travel.ipynb
MERGE 100K rows: 0.05s
RESTORE → v2: 0.00s   (target < 30s)
Rows with score<0 after restore: 0  (expected 0)
Total versions: 5  (target ≥ 5)

## 04_medallion.ipynb
Bronze rows: 200,000
Silver rows: 190,052  (Bronze 200,000 → dedup dropped 9,948)
  Distinct dates:     8   (target ≥ 7)
  Distinct models:    3
  Total Gold rows:   24   (= dates × models)

## Delta log: _lakehouse/scratch/users_delta/_delta_log
_lakehouse/scratch/users_delta/_delta_log/00000000000000000000.json
_lakehouse/scratch/users_delta/_delta_log/00000000000000000001.json

## Delta log: _lakehouse/bronze/llm_calls_raw/_delta_log
_lakehouse/bronze/llm_calls_raw/_delta_log/00000000000000000000.json

## Delta log: _lakehouse/silver/llm_calls/_delta_log
_lakehouse/silver/llm_calls/_delta_log/00000000000000000000.json

## Delta log: _lakehouse/gold/llm_daily_metrics/_delta_log
_lakehouse/gold/llm_daily_metrics/_delta_log/00000000000000000000.json
_lakehouse/gold/llm_daily_metrics/_delta_log/00000000000000000001.json

