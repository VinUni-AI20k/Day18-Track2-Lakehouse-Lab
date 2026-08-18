"""PoC — the hard part of Topic C: tokenize-on-landing + a late-arriving MERGE.

Proves three things a design review would actually question:

  1. Raw PII never reaches disk. Bronze is written from an already-tokenized
     record; a byte-level grep over the parquet finds no phone number.
  2. A 6-hour-late `started` event replayed over an already-`paid` trip does
     NOT reopen the settled trip, because the MERGE carries `src.ts > tgt.ts`.
     A genuinely newer correction still applies.
  3. Erasure is bounded and provable: crypto-shred the vault entry, DELETE the
     rows, and the token can no longer be resolved to a subject.

Run (from the repo root, using the lab venv):

    .venv/bin/python submission/bonus/poc/late_arriving_merge.py
"""
from __future__ import annotations

import gc
import hashlib
import hmac
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_poc_out")
TRIPS = os.path.join(ROOT, "silver_trips")

# --- 1. Tokenization -------------------------------------------------------
# Deterministic so joins work; keyed so the ~1e9 Vietnamese mobile space is not
# brute-forceable; versioned so a rotation is detectable (failure mode F3).
TOKEN_KEY_V = "kv2"
_KEY = os.environ.get("POC_TOKEN_KEY", "demo-key-lives-in-KMS-not-in-git").encode()
_VAULT: dict[str, str] = {}          # token -> ciphertext; crypto-shred target


def tokenize(pii: str) -> str:
    token = "tok_" + hmac.new(_KEY, pii.encode(), hashlib.sha256).hexdigest()[:24]
    _VAULT[token] = "enc(" + pii + ")"   # stands in for envelope encryption
    return token


def ts(hours_ago: float) -> datetime:
    return datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc) - timedelta(hours=hours_ago)


def row(trip_id, status, phone, src_ts, fare):
    return {
        "trip_id": trip_id,
        "status": status,
        "rider_token": tokenize(phone),
        "token_key_v": TOKEN_KEY_V,
        "src_ts": src_ts,
        "fare_vnd": fare,
    }


SCHEMA = pa.schema([
    ("trip_id", pa.int64()),
    ("status", pa.string()),
    ("rider_token", pa.string()),
    ("token_key_v", pa.string()),
    ("src_ts", pa.timestamp("us", tz="UTC")),
    ("fare_vnd", pa.int64()),
])


def tbl(rows) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=SCHEMA)


def merge(src: pa.Table) -> None:
    """The one predicate that prevents a class of financial incident."""
    (
        DeltaTable(TRIPS)
        .merge(src, predicate="t.trip_id = s.trip_id", source_alias="s", target_alias="t")
        .when_matched_update_all(predicate="s.src_ts > t.src_ts")
        .when_not_matched_insert_all()
        .execute()
    )


def state(trip_id: int) -> dict:
    t = DeltaTable(TRIPS).to_pyarrow_table().to_pylist()
    return next(r for r in t if r["trip_id"] == trip_id)


def main() -> int:
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)

    # --- settled state: trip 1001 is paid, as of 1 hour ago ---------------
    settled = tbl([row(1001, "paid", "0912345678", ts(1), 84_000)])
    write_deltalake(TRIPS, settled, mode="overwrite")
    print(f"v0  trip 1001 = {state(1001)['status']}  fare={state(1001)['fare_vnd']:,}")

    # --- 1. no raw PII on disk -------------------------------------------
    blob = b"".join(
        open(p.replace("file://", ""), "rb").read() for p in DeltaTable(TRIPS).file_uris()
    )
    log = open(os.path.join(TRIPS, "_delta_log", "00000000000000000000.json"), "rb").read()
    leaked = b"0912345678" in blob or b"0912345678" in log
    print(f"    raw phone found in parquet or log: {leaked}   (must be False)")

    # --- 2a. the 3 AM case: a 6-hour-late `started` replays --------------
    late = tbl([row(1001, "started", "0912345678", ts(6), 0)])
    merge(late)
    after_late = state(1001)
    print(f"v1  after 6h-late 'started' → {after_late['status']}  "
          f"fare={after_late['fare_vnd']:,}   (settled state preserved)")

    # --- 2b. a genuinely newer correction still applies -------------------
    correction = tbl([row(1001, "fare_adjusted", "0912345678", ts(0.5), 76_000)])
    merge(correction)
    corrected = state(1001)
    print(f"v2  after newer correction   → {corrected['status']}  "
          f"fare={corrected['fare_vnd']:,}")

    # --- 2c. an unseen trip inserts, it does not vanish -------------------
    merge(tbl([row(1002, "requested", "0987654321", ts(0.2), 0)]))
    n = DeltaTable(TRIPS).to_pyarrow_table().num_rows
    print(f"v3  unmatched trip 1002 inserted → {n} rows")

    # --- 3. erasure: crypto-shred + DELETE, bounded and provable ---------
    victim = tokenize("0987654321")
    dt = DeltaTable(TRIPS)
    dt.delete(f"rider_token = '{victim}'")
    _VAULT.pop(victim, None)                       # crypto-shred: token is now inert
    remaining = [r for r in dt.to_pyarrow_table().to_pylist() if r["rider_token"] == victim]
    print(f"    erasure: rows for subject = {len(remaining)}, "
          f"token resolvable = {victim in _VAULT}")
    print(f"    NOTE version {dt.version() - 1} still holds them — "
          f"only VACUUM past retention makes the delete real (NB6).")

    assert not leaked, "raw PII reached disk"
    assert after_late["status"] == "paid" and after_late["fare_vnd"] == 84_000, \
        "late event clobbered settled state — the ts guard is not working"
    assert corrected["fare_vnd"] == 76_000, "newer correction failed to apply"
    assert n == 2 and not remaining and victim not in _VAULT
    print("\nPoC OK — late events cannot reopen settled trips; PII never landed raw.")
    del dt
    gc.collect()          # drop delta-rs handles before interpreter teardown
    return 0


if __name__ == "__main__":
    rc = main()
    sys.stdout.flush()
    # delta-rs 1.x occasionally aborts in its tokio runtime during Py_Finalize.
    # The lab's own notebooks avoid it by never exiting mid-runtime; a script has
    # to, so exit hard once the work is done and flushed.
    os._exit(rc)
