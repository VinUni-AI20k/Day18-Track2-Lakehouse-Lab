# %%
from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from pprint import pprint


TOKEN_SECRET_V1 = b"demo-only-secret-rotate-in-kms"


def normalize_phone(phone: str) -> str:
    digits = "".join(ch for ch in phone if ch.isdigit())
    if digits.startswith("84"):
        return "+" + digits
    if digits.startswith("0"):
        return "+84" + digits[1:]
    return "+" + digits


def hmac_token(value: str, namespace: str, version: int = 1) -> str:
    normalized = value.strip().lower()
    message = f"{namespace}:{version}:{normalized}".encode("utf-8")
    digest = hmac.new(TOKEN_SECRET_V1, message, hashlib.sha256).hexdigest()
    return f"tok_v{version}_{digest[:32]}"


def coarse_gps(lat: float, lon: float) -> str:
    return f"{round(lat, 3)}:{round(lon, 3)}"


@dataclass(frozen=True)
class CdcEvent:
    table: str
    pk: str
    op: str
    source_lsn: int
    source_ts: str
    ingest_ts: str
    payload: dict


def tokenize_for_bronze(event: CdcEvent) -> dict:
    payload = dict(event.payload)
    if "rider_phone" in payload:
        payload["rider_phone_token"] = hmac_token(
            normalize_phone(payload.pop("rider_phone")), "rider_phone"
        )
    if "driver_phone" in payload:
        payload["driver_phone_token"] = hmac_token(
            normalize_phone(payload.pop("driver_phone")), "driver_phone"
        )
    if "national_id" in payload:
        payload["national_id_token"] = hmac_token(
            payload.pop("national_id"), "national_id"
        )
    if "pickup_lat" in payload and "pickup_lon" in payload:
        payload["pickup_grid"] = coarse_gps(
            payload.pop("pickup_lat"), payload.pop("pickup_lon")
        )

    return {
        "table": event.table,
        "pk": event.pk,
        "op": event.op,
        "source_lsn": event.source_lsn,
        "source_ts": event.source_ts,
        "ingest_ts": event.ingest_ts,
        "payload": payload,
        "token_version": 1,
    }


def merge_current_state(silver: dict[str, dict], bronze_event: dict) -> str:
    pk = bronze_event["pk"]
    incoming_ts = bronze_event["source_ts"]
    current = silver.get(pk)

    if current and incoming_ts < current["source_ts"]:
        return "ignored_late_event"
    if current and incoming_ts == current["source_ts"]:
        if bronze_event["source_lsn"] <= current["source_lsn"]:
            return "ignored_duplicate_or_retry"

    if bronze_event["op"] == "d":
        silver[pk] = {
            "pk": pk,
            "deleted": True,
            "source_ts": incoming_ts,
            "source_lsn": bronze_event["source_lsn"],
        }
        return "soft_deleted"

    row = {
        "pk": pk,
        "deleted": False,
        "source_ts": incoming_ts,
        "source_lsn": bronze_event["source_lsn"],
    }
    row.update(bronze_event["payload"])
    silver[pk] = row
    return "upserted"


def audit_pii_read(user: str, purpose: str, token: str) -> dict:
    if not purpose:
        raise ValueError("PII reads fail closed without a purpose code")
    return {
        "user": user,
        "purpose": purpose,
        "token_prefix": token[:16],
        "decision": "approved_for_demo",
    }


# %%
events = [
    CdcEvent(
        table="trips", pk="trip_001", op="c", source_lsn=100,
        source_ts="2026-05-04T09:00:00Z", ingest_ts="2026-05-04T09:00:03Z",
        payload={
            "status": "requested",
            "city_id": "HCMC",
            "rider_phone": "0901 222 333",
            "driver_phone": "+84 902 444 555",
            "pickup_lat": 10.7758,
            "pickup_lon": 106.7010,
        },
    ),
    CdcEvent(
        table="trips", pk="trip_001", op="u", source_lsn=102,
        source_ts="2026-05-04T09:02:00Z", ingest_ts="2026-05-04T09:02:04Z",
        payload={"status": "completed", "city_id": "HCMC"},
    ),
    CdcEvent(
        table="trips", pk="trip_001", op="u", source_lsn=101,
        source_ts="2026-05-04T09:01:00Z", ingest_ts="2026-05-04T09:03:10Z",
        payload={"status": "driver_arrived", "city_id": "HCMC"},
    ),
]

# %%
bronze = [tokenize_for_bronze(event) for event in events]
silver_current: dict[str, dict] = {}
merge_results = [merge_current_state(silver_current, event) for event in bronze]

print("Bronze payloads contain tokens, not raw PII:")
pprint(bronze[0])
print("\nMerge results:")
pprint(merge_results)
print("\nSilver current state:")
pprint(silver_current)


# %%
assert "rider_phone" not in bronze[0]["payload"]
assert "pickup_lat" not in bronze[0]["payload"]
assert merge_results == ["upserted", "upserted", "ignored_late_event"]
assert silver_current["trip_001"]["status"] == "completed"

audit_row = audit_pii_read(
    user="privacy-oncall@company.vn",
    purpose="INCIDENT-2026-05-04",
    token=bronze[0]["payload"]["rider_phone_token"],
)
print("\nPII audit row:")
pprint(audit_row)
