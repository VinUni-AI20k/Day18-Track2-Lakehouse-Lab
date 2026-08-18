"""PoC — tokenize PII in the SAME batch that lands Bronze (Architecture §3.2).

Non-trivial mechanism this proves: a human can query Silver the instant it
exists and never see raw PII, because Bronze -> Silver happens as one pass,
not "land raw, redact later." An append-only audit table records exactly
what was redacted, and a regex scan proves Silver is clean.

Not the full pipeline (no Kafka, no multi-tenant scale) — a spike that
proves the redact-at-landing + verify + audit mechanism is feasible with
the exact stack this lab already uses (deltalake + polars).

Run: python submission/bonus/poc/tokenize_pii.py
"""
from __future__ import annotations

import hashlib
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import polars as pl
from deltalake import DeltaTable, write_deltalake

from lakehouse import path, reset

EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
PHONE_RE = re.compile(r"\+?\d[\d\-\s]{7,}\d")
# Single alternation so email/phone spans are found in ONE pass over the
# ORIGINAL text — scanning phone patterns over already-substituted text
# would risk matching digit runs inside a previously inserted hash token.
PII_RE = re.compile(rf"(?P<email>{EMAIL_RE.pattern})|(?P<phone>{PHONE_RE.pattern})")
TOKEN_RE = re.compile(r"<(?:EMAIL|PHONE):[0-9a-f]{12}>")


def synthetic_batch(n: int = 50) -> pl.DataFrame:
    """Simulate one Bronze micro-batch with PII embedded in free text,
    the way a real prompt/response payload would carry it."""
    now = datetime.now(timezone.utc)
    rows = []
    for i in range(n):
        rows.append(
            {
                "request_id": f"req-{i:04d}",
                "tenant_id": f"tenant-{i % 5}",
                "ts": now,
                "raw_text": (
                    f"user reachable at user{i}@example.com or "
                    f"+1-555-{100 + i:04d} regarding order #{i}"
                ),
            }
        )
    return pl.DataFrame(rows)


def tokenize(value: str) -> tuple[str, int]:
    """Replace PII spans with a stable, non-reversible token, in one pass
    over the original text.

    Stable (same input -> same token) so joins on a hashed identifier still
    work downstream; non-reversible (sha256, no salt lookup stored) so the
    token itself carries no PII even if Silver leaks. One pass matters: a
    second regex run over already-tokenized text could match a digit run
    inside a *previous* hash token and double-redact garbage.
    """
    hits = 0

    def repl(m: re.Match) -> str:
        nonlocal hits
        hits += 1
        tag = "EMAIL" if m.group("email") else "PHONE"
        digest = hashlib.sha256(m.group(0).encode()).hexdigest()[:12]
        return f"<{tag}:{digest}>"

    return PII_RE.sub(repl, value), hits


def main() -> int:
    bronze_path = path("bonus", "bronze_raw_pii")
    silver_path = path("bonus", "silver_redacted")
    audit_path = path("bonus", "pii_audit_log")
    for p in (bronze_path, silver_path, audit_path):
        reset(p)

    # 1. Land Bronze exactly as it arrives — raw PII intact, restricted layer.
    bronze = synthetic_batch()
    write_deltalake(bronze_path, bronze.to_arrow(), mode="overwrite")

    # 2. SAME pass: tokenize -> Silver. No second job, no raw-readable window.
    redacted_texts, hit_counts = [], []
    for text in bronze["raw_text"]:
        clean, hits = tokenize(text)
        redacted_texts.append(clean)
        hit_counts.append(hits)

    silver = bronze.with_columns(
        pl.Series("raw_text", redacted_texts),
        pl.Series("pii_tokens_replaced", hit_counts),
    ).drop("raw_text")
    silver = silver.with_columns(pl.Series("text", redacted_texts))
    write_deltalake(silver_path, silver.to_arrow(), mode="overwrite")

    # 3. Append-only audit record — who/what/when, not the PII itself.
    audit = pl.DataFrame(
        [
            {
                "batch_ts": datetime.now(timezone.utc),
                "rows_in": bronze.height,
                "pii_tokens_replaced": int(sum(hit_counts)),
                "source_table": bronze_path,
                "dest_table": silver_path,
            }
        ]
    )
    write_deltalake(audit_path, audit.to_arrow(), mode="append")

    # 4. Verify: Silver must contain ZERO raw PII patterns. This is the
    # check that would run continuously in prod (Architecture §4a).
    silver_read = pl.from_arrow(DeltaTable(silver_path).to_pyarrow_table())
    # Strip already-tokenized spans first — a hash digest is 12 hex chars
    # and can coincidentally contain a digit run that LOOKS like a phone
    # number to the naive scanner. A real verifier must be token-aware, or
    # it flags its own redaction tokens as "leaks."
    leaked = sum(
        1
        for t in silver_read["text"]
        if EMAIL_RE.search(TOKEN_RE.sub("", t)) or PHONE_RE.search(TOKEN_RE.sub("", t))
    )

    print(f"Bronze rows (raw PII):      {bronze.height}")
    print(f"Silver rows (tokenized):    {silver_read.height}")
    print(f"PII tokens replaced:        {sum(hit_counts)}")
    print(f"Sample Bronze text:         {bronze['raw_text'][0]}")
    print(f"Sample Silver text:         {silver_read['text'][0]}")
    print(f"Leaked PII patterns found in Silver: {leaked}")

    assert sum(hit_counts) == bronze.height * 2, "expected 1 email + 1 phone per row"
    assert leaked == 0, "PII leaked into Silver — redact-at-landing failed"
    print("\n[PASS] Silver is PII-clean; audit log recorded the batch.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
