#!/usr/bin/env bash
# Tạo các khung output "vừa một màn hình" để chụp ảnh thật.
#
#   ./submission/screenshots/capture.sh gates    # smoke + test + run-all   (~32 dòng)
#   ./submission/screenshots/capture.sh tree     # tree _lakehouse/         (~34 dòng)
#   ./submission/screenshots/capture.sh log      # nội dung 1 _delta_log/*.json (~30 dòng)
#   ./submission/screenshots/capture.sh nb       # bảng Gold + 4 job maintenance (~24 dòng)
#
# Mỗi khung tự in tiêu đề để ảnh chụp có ngữ cảnh. Chạy từ repo root.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1
PY=.venv/bin/python
[ -x "$PY" ] || { echo "Chưa có .venv — chạy 'make setup' trước."; exit 1; }

hr() { printf '─%.0s' $(seq 1 78); echo; }
title() { echo; hr; echo "  $1"; hr; }

case "${1:-}" in

gates)
  title "Day18 Lakehouse Lab — cổng chấm điểm (lightweight, offline)"
  python3 --version
  echo
  echo "\$ make smoke";   make smoke   2>&1 | grep -v '^$'
  echo
  echo "\$ make test";    $PY -m pytest tests -p no:cacheprovider 2>&1 | tail -2
  echo
  echo "\$ make run-all"; make run-all 2>&1 | grep -v '^$'
  echo
  ;;

tree)
  title "\$ tree -L 2 _lakehouse/   (bronze / silver / gold + iceberg catalog)"
  if command -v tree >/dev/null 2>&1; then
    tree -L 2 -d --noreport _lakehouse
  else
    find _lakehouse -maxdepth 2 -type d | sort | sed 's|[^/]*/|   |g'
    echo "   (chỉ hiện thư mục; 'brew install tree' để có bản đầy đủ)"
  fi
  echo
  echo "── partition thật trên đĩa (hidden partition, medallion, provenance) ──"
  ls -d _lakehouse/silver/llm_calls/date=2026-04-0[1-3] \
        _lakehouse/silver/agent_trajectories/agent_version=* \
        _lakehouse/silver/training_corpus_governed/provenance_bucket=* 2>/dev/null \
    | sed 's|_lakehouse/|  |'
  echo
  echo "── tổng dung lượng ──"; du -sh _lakehouse
  echo
  ;;

log)
  title "\$ cat _lakehouse/scratch/users_delta/_delta_log/*.json   (NB1, rút gọn)"
  $PY - <<'PYEOF'
import json, pathlib
log = pathlib.Path("_lakehouse/scratch/users_delta/_delta_log")
for f in sorted(log.glob("*.json")):
    print(f"\n### {f.name}")
    for line in f.read_text().splitlines():
        if not line.strip():
            continue
        e = json.loads(line)
        k = next(iter(e))
        if k == "commitInfo":
            c = e[k]
            print(f'  commitInfo   operation={c["operation"]} '
                  f'{c["operationParameters"]}  metrics={c.get("operationMetrics", {})}')
        elif k == "metaData":
            cols = [fld["name"] for fld in json.loads(e[k]["schemaString"])["fields"]]
            print(f'  metaData     schema = {cols}   partitionColumns={e[k]["partitionColumns"]}')
        elif k == "add":
            a = e[k]
            print(f'  add          {a["path"][:46]}…  size={a["size"]} B')
            print(f'               stats = {json.dumps(json.loads(a["stats"]))[:300]}')
        elif k == "protocol":
            print(f'  protocol     {e[k]}')
print("\n  → v1 thêm đúng một field ('tier') vào schema mà KHÔNG rewrite parquet của v0:")
print("    đó là schema evolution metadata-only. stats min/max chính là dữ liệu")
print("    engine dùng để prune file — không có nó thì Z-ORDER vô nghĩa.")
PYEOF
  echo
  ;;

nb)
  title "NB4 Gold (medallion) + NB6 bốn job maintenance — số đo thật"
  $PY - <<'PYEOF'
import polars as pl
from deltalake import DeltaTable
b = DeltaTable("_lakehouse/bronze/llm_calls_raw").count()
s = DeltaTable("_lakehouse/silver/llm_calls").count()
g = pl.from_arrow(DeltaTable("_lakehouse/gold/llm_daily_metrics").to_pyarrow_table())
print(f"Bronze {b:,} → Silver {s:,}  (dedup bỏ {b-s:,} = {(b-s)/b*100:.2f}%)")
print(f"Gold {g.height} dòng = {g['date'].n_unique()} date × {g['model'].n_unique()} model\n")
print(g.select("date", "model", "p50_latency_ms", "p95_latency_ms", "error_rate", "cost_usd")
       .sort("date", "model").head(6))
m = DeltaTable("_lakehouse/scratch/maint_events")
print(f"\nNB6 maint_events: {len(m.file_uris())} data file sau compaction+vacuum "
      f"(baseline 200), {m.count():,} dòng")
PYEOF
  ls _lakehouse/scratch/maint_events/_delta_log/ | grep -E 'checkpoint|_last' | sed 's/^/  /'
  echo
  ;;

*)
  sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
  ;;
esac
