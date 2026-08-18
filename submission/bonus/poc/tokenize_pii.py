"""Day 18 Bonus PoC — tokenization function for Topic C.

Demonstrates the *hard part* of design D4 (tokenize PII at the wire):
  - stable, collision-resistant hash (HMAC-SHA256 with rotating key)
  - deterministic across retries but not reversible
  - separate HMAC keys per PII column class so a breach of one
    does not expose the other
  - subject_id tokenization preserves join-ability across trip/driver
    tables via the SAME (key_version, subject_class) pair.

This is a *spike* — proves the mechanism is feasible. Production
implementation would store `key_version` in Vault and rotate per
Decree 13 §4 (quarterly).

Run from repo root:  python submission/bonus/poc/tokenize_pii.py
"""
from __future__ import annotations

import datetime as dtm
import hashlib
import hmac
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

# ── Configuration ─────────────────────────────────────────────────────────
# In prod: keys are loaded from Vault per `key_version`. Two keys are
# kept live simultaneously for 30 days during rotation.

KEY_VAULT: dict[str, dict[str, bytes]] = {
    "v2026Q3": {
        "rider":    b"\x9c" * 32,
        "driver":   b"\xa1" * 32,
        "phone":    b"\xb7" * 32,
    },
    "v2026Q4": {                                     # rotated key
        "rider":    b"\xc4" * 32,
        "driver":   b"\xd2" * 32,
        "phone":    b"\xe5" * 32,
    },
}


def tokenize(value: str, key_version: str, klass: str) -> str:
    """Deterministic, key-versioned, irreversible token.

    Same (value, key_version, klass) → same token, always.
    Different (value, key_version, klass) → independent (HMAC).
    """
    key = KEY_VAULT[key_version][klass]
    return hmac.new(key, value.encode("utf-8"), hashlib.sha256).hexdigest()[:16]


# ── Demonstrate the four properties that make this design work ────────────

def demo_properties() -> None:
    import random
    print("── Tokenization property tests ─────────────────────────────────")
    # (a) Deterministic: same input → same token (joins work across tables)
    a = tokenize("0912345678", "v2026Q3", "phone")
    b = tokenize("0912345678", "v2026Q3", "phone")
    assert a == b, "non-deterministic"
    print(f"  deterministic:    {a == b}  ({a})")

    # (b) Collision-resistant: 1 K random phones → 1 K unique tokens
    phones = [f"09{random.randint(10_000_000, 99_999_999):08d}" for _ in range(1000)]
    toks = [tokenize(p, "v2026Q3", "phone") for p in phones]
    assert len(set(toks)) == 1000, f"collision: {len(set(toks))} unique of 1000"
    print(f"  no collisions:    1000 unique phones → 1000 unique tokens")

    # (c) Key-versioned: rotation changes the token (audit mirror uses both)
    old = tokenize("0912345678", "v2026Q3", "phone")
    new = tokenize("0912345678", "v2026Q4", "phone")
    assert old != new
    print(f"  key rotation:     v2026Q3={old} != v2026Q4={new}")

    # (d) Class-isolated: phone token differs from rider token of same string
    p = tokenize("alice@example", "v2026Q3", "phone")
    r = tokenize("alice@example", "v2026Q3", "rider")
    assert p != r, "class isolation broken"
    print(f"  class isolation:  phone!=rider  ({p[:8]}... vs {r[:8]}...)")


# ── CDC late-arriving scenario ────────────────────────────────────────────

def demo_late_arriving() -> None:
    """Bronze lands an event with `cdc.ts` earlier than the row already in Silver.

    The `MERGE WHEN MATCHED AND src._cdc.ts > tgt.valid_from` predicate
    keeps the LATER truth. We show that tokenization is stable across
    the late event so the join still resolves to the same subject.
    """
    print("\n── Late-arriving event (F1 from ARCHITECTURE §4) ────────────────")
    KEY = "v2026Q3"
    # Initial: trip closed at 14:00 ICT, CDC ts = 14:00:05
    initial_ts = dtm.datetime(2026, 8, 18, 14, 0, 5)
    # Late: same trip, but with a corrected dropoff location, CDC ts = 13:55:00
    late_ts = dtm.datetime(2026, 8, 18, 13, 55, 0)
    print(f"  initial CDC ts:    {initial_ts}")
    print(f"  late   CDC ts:     {late_ts}  (earlier — should NOT overwrite)")
    # The MERGE predicate says: keep initial because src.cdc_ts < tgt.valid_from.
    # Token equality ensures the rider is still recognised as the same subject.
    rider_initial = tokenize("0912345678", KEY, "phone")
    rider_late = tokenize("0912345678", KEY, "phone")
    assert rider_initial == rider_late
    print(f"  rider token match: True   (joins still resolve to same subject)")
    print(f"  token:             {rider_initial}")


def main() -> None:
    print("Bonus PoC — PII tokenization (ARCHITECTURE.md §3 D4)\n")
    demo_properties()
    demo_late_arriving()
    print("\nMechanism is feasible. Rotate via Vault; audit mirror joins")
    print("on (token, key_version) so old rows still resolve for 30 days.")


if __name__ == "__main__":
    main()