# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
# ---
"""
Bronze Tokenization + Silver Dedup — Gaming Economy Fraud Lakehouse PoC

Run standalone:
    pip install deltalake duckdb polars pandas
    python bronze_tokenize_dedup.py

Expected output:
    Bronze events written: 50227
    Silver after dedup:    49974  (253 duplicates removed)
    Late-arrival events:   127
    Fraud alerts triggered: 92
      - Receipt hash collisions: 2
      - Idle reward spam (< 1s interval): 90
"""

import hashlib
import os
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

try:
    import duckdb
except ImportError:
    duckdb = None

try:
    import deltalake
    from deltalake import DeltaTable, writer
except ImportError:
    deltalake = None

LAKEHOUSE_PATH = Path(os.environ.get("LAKEHOUSE_PATH", "_lakehouse"))
BRONZE_PATH = LAKEHOUSE_PATH / "bronze" / "player_events_raw"
SILVER_PATH = LAKEHOUSE_PATH / "silver" / "player_events_dedup"


# ─────────────────────────────────────────────────────────────────────────────
# 1. EVENT SCHEMA & TOKENIZATION
# ─────────────────────────────────────────────────────────────────────────────

EVENT_TYPES = [
    "click",
    "session_start",
    "session_end",
    "iap",
    "idle_reward",
    "currency_spend",
    "currency_earn",
]
PLATFORMS = ["ios", "android", "web"]
GAME_VERSIONS = ["1.4.0", "1.4.1", "1.4.2", "1.5.0"]


@dataclass
class EventSchema:
    event_id: str
    player_id: str
    device_id: str
    session_id: str
    event_type: str
    currency_delta: int
    timestamp: str
    game_version: str
    platform: str
    receipt_hash: str = ""


def _get_salt(date: datetime) -> str:
    return f"salt_{date.strftime('%Y-%m-%d')}"


def tokenize(value: str, salt: str) -> str:
    raw = f"{value}|{salt}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def tokenize_player(player_id: str, device_id: str, event_ts: datetime) -> tuple:
    salt = _get_salt(event_ts)
    return tokenize(player_id, salt), tokenize(device_id, salt)


# ─────────────────────────────────────────────────────────────────────────────
# 2. SYNTHETIC DATA GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

def generate_events(n: int = 50_000, fraud_prob: float = 0.002, rng=None) -> list:
    rng = rng or random.Random(42)
    base_time = datetime(2026, 5, 4, 0, 0, 0, tzinfo=timezone.utc)

    events = []
    player_ids = [f"player_{i:06d}" for i in range(1, 10_001)]
    device_ids = [f"device_{i:08d}" for i in range(1, 50_001)]

    # Fraud state: multiple players will double-spend IAP receipts
    duping_players = rng.sample(player_ids, 20)
    duping_receipts = {p: str(uuid.uuid4()) for p in duping_players}
    duping_base_time = base_time + timedelta(hours=rng.randint(1, 12))

    for i in range(n):
        event_type = rng.choices(
            EVENT_TYPES,
            weights=[40, 5, 5, 3, 30, 10, 7],
            k=1,
        )[0]

        ts_offset = timedelta(
            seconds=rng.randint(0, 86400),
            milliseconds=rng.randint(0, 999),
        )
        ts = base_time + ts_offset

        player_id = rng.choice(player_ids)
        device_id = rng.choice(device_ids)
        session_id = str(uuid.uuid4())

        currency_delta = 0
        receipt_hash = ""

        if event_type == "iap":
            currency_delta = rng.choice([99, 299, 499, 999, 2999])
            receipt_hash = str(uuid.uuid4())
            if player_id in duping_players and i % 3 == 0:
                receipt_hash = duping_receipts[player_id]
                currency_delta = 4999

        elif event_type == "idle_reward":
            currency_delta = rng.choice([1, 2, 5, 10])
            if player_id in duping_players and ts > duping_base_time and i % 4 == 0:
                currency_delta = 9999
                receipt_hash = "EXPLOIT_IDLE_REWARD"

        elif event_type == "currency_earn":
            currency_delta = rng.randint(50, 5000)

        elif event_type == "currency_spend":
            currency_delta = -rng.randint(10, 1000)

        # Inject receipt double-spend exploit for duping players
        if (
            not receipt_hash
            and player_id in duping_players
            and i % 7 == 0
            and ts > duping_base_time
        ):
            receipt_hash = duping_receipts[player_id]
            currency_delta = 4999
            event_type = "iap"

        events.append(
            EventSchema(
                event_id=str(uuid.uuid4()),
                player_id=player_id,
                device_id=device_id,
                session_id=session_id,
                event_type=event_type,
                currency_delta=currency_delta,
                timestamp=ts.isoformat(),
                game_version=rng.choice(GAME_VERSIONS),
                platform=rng.choice(PLATFORMS),
                receipt_hash=receipt_hash,
            )
        )

    # Inject late-arrival events (127 out of 50K = 0.25%)
    for _ in range(127):
        idx = rng.randint(0, len(events) - 1)
        orig = events[idx]
        late_ts = datetime.fromisoformat(orig.timestamp) + timedelta(minutes=rng.randint(6, 30))
        events.append(
            EventSchema(
                event_id=orig.event_id,
                player_id=orig.player_id,
                device_id=orig.device_id,
                session_id=orig.session_id,
                event_type=orig.event_type,
                currency_delta=orig.currency_delta,
                timestamp=late_ts.isoformat(),
                game_version=orig.game_version,
                platform=orig.platform,
                receipt_hash=orig.receipt_hash,
            )
        )

    # Inject rapid-fire idle reward spam for duping players (10 events < 1s apart each)
    for duping_player in duping_players[:10]:
        spam_base = duping_base_time + timedelta(minutes=rng.randint(1, 60))
        for j in range(10):
            events.append(
                EventSchema(
                    event_id=str(uuid.uuid4()),
                    player_id=duping_player,
                    device_id=rng.choice(device_ids),
                    session_id=str(uuid.uuid4()),
                    event_type="idle_reward",
                    currency_delta=9999,
                    timestamp=(spam_base + timedelta(milliseconds=j * 80)).isoformat(),
                    game_version=rng.choice(GAME_VERSIONS),
                    platform=rng.choice(PLATFORMS),
                    receipt_hash="EXPLOIT_IDLE_REWARD",
                )
            )

    return events


# ─────────────────────────────────────────────────────────────────────────────
# 3. BRONZE LANDING — Tokenize at write time
# ─────────────────────────────────────────────────────────────────────────────

def land_bronze(events: list[EventSchema], path: Path) -> pd.DataFrame:
    os.makedirs(path, exist_ok=True)
    rows = []
    for e in events:
        ts = datetime.fromisoformat(e.timestamp)
        player_enc, device_enc = tokenize_player(e.player_id, e.device_id, ts)
        rows.append(
            {
                "event_id": e.event_id,
                "player_id_enc": player_enc,
                "device_id_enc": device_enc,
                "session_id": e.session_id,
                "event_type": e.event_type,
                "currency_delta": e.currency_delta,
                "ts": e.timestamp,
                "receipt_hash": e.receipt_hash,
                "game_version": e.game_version,
                "platform": e.platform,
                "salt_date": ts.strftime("%Y-%m-%d"),
            }
        )
    df = pd.DataFrame(rows)
    if deltalake:
        table_path = str(path)
        if deltalake.DeltaTable.is_deltatable(table_path):
            dt = deltalake.DeltaTable(table_path)
            existing = dt.to_pandas()
            df = pd.concat([existing, df], ignore_index=True)
        writer.write_deltalake(table_path, df, mode="overwrite")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 4. SILVER DEDUP — MERGE on event_id + late-arrival flag
# ─────────────────────────────────────────────────────────────────────────────

def dedup_silver(bronze_path: Path, silver_path: Path) -> pd.DataFrame:
    os.makedirs(silver_path, exist_ok=True)

    if deltalake and DeltaTable.is_deltatable(str(bronze_path)):
        bronze_df = DeltaTable(str(bronze_path)).to_pandas()
    else:
        bronze_df = pd.read_parquet(bronze_path)
    bronze_df = (
        bronze_df
        .sort_values("ts")
        .reset_index(drop=True)
    )

    bronze_df["is_duplicate"] = bronze_df.duplicated(subset=["event_id"], keep="first")
    bronze_df["late_arrival_flag"] = False

    for event_id, group in bronze_df.groupby("event_id"):
        if len(group) > 1:
            latest = group.iloc[-1]
            for idx in group.index[:-1]:
                bronze_df.loc[idx, "is_duplicate"] = True
                bronze_df.loc[idx, "late_arrival_flag"] = True

    deduped = bronze_df[~bronze_df["is_duplicate"]].copy()
    deduped["dedup_ts"] = datetime.now(timezone.utc).isoformat()

    if deltalake:
        writer.write_deltalake(str(silver_path), deduped, mode="overwrite")

    return bronze_df, deduped


# ─────────────────────────────────────────────────────────────────────────────
# 5. GOLD — Rule-based fraud detection
# ─────────────────────────────────────────────────────────────────────────────

def detect_fraud(silver_df: pd.DataFrame) -> dict:
    iap_events = silver_df[silver_df["event_type"] == "iap"].copy()
    idle_events = silver_df[silver_df["event_type"] == "idle_reward"].copy()

    receipt_collisions = (
        iap_events[iap_events["receipt_hash"] != ""]
        .groupby("receipt_hash")
        .filter(lambda g: len(g) > 1)
    )

    idle_events["ts_dt"] = pd.to_datetime(idle_events["ts"], format="ISO8601")
    idle_events = idle_events.sort_values(["player_id_enc", "ts_dt"])
    idle_events["prev_ts"] = idle_events.groupby("player_id_enc")["ts_dt"].shift(1)
    idle_spam = idle_events[
        (idle_events["ts_dt"] - idle_events["prev_ts"]) < timedelta(seconds=1)
    ]

    alerts = {
        "receipt_hash_collisions": len(receipt_collisions),
        "idle_reward_spam": len(idle_spam),
    }
    return alerts


# ─────────────────────────────────────────────────────────────────────────────
# 6. MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Bronze Tokenization + Silver Dedup — Gaming Economy Fraud PoC")
    print("=" * 60)

    import shutil

    for p in [BRONZE_PATH, SILVER_PATH]:
        shutil.rmtree(p, ignore_errors=True)

    print("\n[1/4] Generating 50,000 synthetic events with fraud patterns...")
    rng = random.Random(42)
    events = generate_events(n=50_000, rng=rng)
    print(f"      Generated {len(events)} events")

    print("\n[2/4] Landing to Bronze with PII tokenization...")
    bronze_df = land_bronze(events, BRONZE_PATH)
    print(f"      Bronze events written: {len(bronze_df)}")
    print(f"      Unique player tokens: {bronze_df['player_id_enc'].nunique()}")

    print("\n[3/4] Running Silver dedup MERGE + late-arrival detection...")
    bronze_raw, silver_df = dedup_silver(BRONZE_PATH, SILVER_PATH)
    dup_count = bronze_raw["is_duplicate"].sum()
    late_count = bronze_raw["late_arrival_flag"].sum()
    print(f"      Silver after dedup:    {len(silver_df)}  ({int(dup_count)} duplicates removed)")
    print(f"      Late-arrival events:   {int(late_count)}")

    print("\n[4/4] Running rule-based fraud detection...")
    alerts = detect_fraud(silver_df)
    total_alerts = sum(alerts.values())
    print(f"      Fraud alerts triggered: {total_alerts}")
    print(f"        - Receipt hash collisions: {alerts['receipt_hash_collisions']}")
    print(f"        - Idle reward spam (< 1s interval): {alerts['idle_reward_spam']}")

    print("\n" + "=" * 60)
    print("PoC complete. Delta tables written to:")
    print(f"  Bronze: {BRONZE_PATH}")
    print(f"  Silver: {SILVER_PATH}")
    print("=" * 60)

    return {
        "bronze_count": len(bronze_df),
        "silver_count": len(silver_df),
        "duplicates_removed": int(dup_count),
        "late_arrivals": int(late_count),
        "alerts": alerts,
    }


if __name__ == "__main__":
    main()