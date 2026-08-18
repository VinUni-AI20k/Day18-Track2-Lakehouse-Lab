"""PoC — the hard part of the Topic C design: a CDC MERGE that survives a
six-hour-late replay from a driver's handset.

Runs offline against local Delta tables (same stack as the lab):
    .venv/bin/python submission/bonus/poc/late_arrival_merge.py

Demonstrates, in order:
  D3  deterministic HMAC tokenization at Bronze landing (joins survive, PII does not)
  D4  blind MERGE vs guarded MERGE on the SAME late batch — the divergence is measured
  D4  SCD2 history so the superseded state stays auditable
  D6  crypto-shredding: destroy the key version, and the token stops resolving
"""
from __future__ import annotations

import hashlib
import hmac
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
from deltalake import DeltaTable, write_deltalake

ROOT = Path(__file__).resolve().parents[3] / "_lakehouse" / "scratch" / "poc_decree13"
T0 = datetime(2026, 4, 1, 20, 0, tzinfo=timezone.utc)

# --- D3: tokenization -------------------------------------------------------
# One key per column per version. In production these live in KMS/HSM and this
# process never sees them; destroying a version is the erasure primitive (D6).
KEYRING: dict[tuple[str, int], bytes] = {("phone", 1): b"demo-key-v1-not-a-real-secret"}
KEY_VERSION = 1


def tokenize(col: str, value: str, version: int = KEY_VERSION) -> str:
    """Deterministic, irreversible. Same input → same token, so joins still work."""
    key = KEYRING.get((col, version))
    if key is None:
        return "<unresolvable: key version destroyed>"
    norm = value.strip().replace(" ", "").lstrip("+").removeprefix("84").lstrip("0")
    return hmac.new(key, f"{col}:{norm}".encode(), hashlib.sha256).hexdigest()[:32]


def trips(rows: list[tuple[str, str, str, int, str]]) -> pl.DataFrame:
    """(trip_id, status, phone, minutes_after_T0, fare) → a CDC batch."""
    return pl.DataFrame(
        [
            {
                "trip_id": t, "status": s,
                "phone_token": tokenize("phone", p),
                "pii_key_version": KEY_VERSION,
                "event_ts": T0 + timedelta(minutes=m),
                "fare_vnd": f,
            }
            for t, s, p, m, f in rows
        ],
        schema_overrides={"fare_vnd": pl.Int64},
    )


def merge(table: str, batch: pl.DataFrame, guard: str | None) -> None:
    """Upsert a CDC batch. `guard` is the late-arrival predicate — or None."""
    (
        DeltaTable(str(ROOT / table))
        .merge(batch.to_arrow(), "src.trip_id = tgt.trip_id",
               source_alias="src", target_alias="tgt")
        .when_matched_update_all(predicate=guard)
        .when_not_matched_insert_all()
        .execute()
    )


def state(table: str) -> pl.DataFrame:
    return pl.from_arrow(DeltaTable(str(ROOT / table)).to_pyarrow_table()).sort("trip_id")


# --- Setup: the state Oracle already streamed to us -------------------------
shutil.rmtree(ROOT, ignore_errors=True)
live = trips([
    ("T-001", "completed",   "+84 901 234 567", 55, 185_000),
    ("T-002", "completed",   "0912 345 678",    58, 240_000),
    ("T-003", "in_progress", "0987 654 321",    59,       0),
])
for tbl in ("blind", "guarded"):
    write_deltalake(str(ROOT / tbl), live.to_arrow(), mode="overwrite")

print("D3 — tokenization at Bronze landing")
print(f"  '+84 901 234 567' → {tokenize('phone', '+84 901 234 567')}")
print(f"  '0901234567'      → {tokenize('phone', '0901234567')}   ← same subject, same token")
print(f"  '0912345678'      → {tokenize('phone', '0912345678')}   ← different subject")
print("  no plaintext phone column exists downstream of this point\n")

# --- D4: the 03:00 replay ---------------------------------------------------
# A handset buffered offline for 6h reconnects. Its events are OLDER than what
# we already hold for T-001/T-002, and NEWER for T-003. A correct pipeline
# applies only the third.
late = trips([
    ("T-001", "in_progress", "+84 901 234 567", 10,       0),   # stale by 45 min
    ("T-002", "in_progress", "0912 345 678",    12,       0),   # stale by 46 min
    ("T-003", "completed",   "0987 654 321",    75, 310_000),   # legitimately newer
])
print("D4 — the same late batch, applied two ways")
merge("blind",   late, guard=None)
merge("guarded", late, guard="src.event_ts > tgt.event_ts")

for name in ("blind", "guarded"):
    df = state(name)
    reverted = df.filter((pl.col("trip_id").is_in(["T-001", "T-002"])) & (pl.col("status") == "in_progress"))
    revenue = df["fare_vnd"].sum()
    print(f"  {name:<8} reverted completed trips: {reverted.height}   "
          f"revenue on the books: {revenue:>9,} VND")

blind_loss = state("guarded")["fare_vnd"].sum() - state("blind")["fare_vnd"].sum()
n_reverted = 2
# Extrapolate on the *affected* population, not the whole fleet: a stale replay
# only destroys revenue when it lands on an already-completed trip. REPLAY_RATE
# is the assumption to argue with — it is the share of daily trips whose handset
# buffers through completion and replays afterwards.
REPLAY_RATE, TRIPS_PER_DAY = 0.005, 274_000
per_trip = blind_loss / n_reverted
daily = per_trip * TRIPS_PER_DAY * REPLAY_RATE
print(f"\n  Blind MERGE silently reverted {n_reverted} completed trips "
      f"({blind_loss:,} VND, avg {per_trip:,.0f}/trip).")
print(f"  At {TRIPS_PER_DAY:,} trips/day and a {REPLAY_RATE:.1%} rural-replay rate "
      f"→ ~{TRIPS_PER_DAY * REPLAY_RATE:,.0f} trips/day")
print(f"  ≈ {daily / 1e6:,.0f}M VND/day of revenue written off (~${daily / 26_000:,.0f}/day).")
print("  Nothing errored. No alert fired. The guard is one predicate:")
print("    .when_matched_update_all(predicate='src.event_ts > tgt.event_ts')\n")

# --- D4: SCD2 — the superseded state stays auditable ------------------------
hist = pl.concat([live.with_columns(is_current=pl.lit(False)),
                  state("guarded").with_columns(is_current=pl.lit(True))])
write_deltalake(str(ROOT / "history"), hist.to_arrow(), mode="overwrite")
t3 = pl.from_arrow(DeltaTable(str(ROOT / "history")).to_pyarrow_table()).filter(pl.col("trip_id") == "T-003")
print("D4 — SCD2 history for T-003 (the row that legitimately changed)")
for r in t3.sort("event_ts").iter_rows(named=True):
    print(f"  {r['event_ts']:%H:%M}  {r['status']:<12} {r['fare_vnd']:>7,} VND  current={r['is_current']}")

# --- D6: erasure by crypto-shredding ---------------------------------------
subject = tokenize("phone", "0912345678")
print(f"\nD6 — erasure request from the subject holding token {subject[:16]}…")
print(f"  rows for this subject: {state('guarded').filter(pl.col('phone_token') == subject).height}")
del KEYRING[("phone", 1)]
print(f"  after destroying key ('phone', v1): {tokenize('phone', '0912345678')}")
print("  every row written under v1 is now unresolvable — including rows sitting")
print("  in older table versions that time travel can still reach (NB8's conflict).")
print(f"\nTables written under {ROOT}")
