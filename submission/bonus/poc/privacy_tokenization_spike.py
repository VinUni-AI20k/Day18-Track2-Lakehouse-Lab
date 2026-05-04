"""PoC: privacy-first tokenization for LLM observability Bronze -> Silver.

This is intentionally small and dependency-free. It demonstrates the mechanism
from ARCHITECTURE.md, not a production security library:

1. Detect obvious PII in prompt/response text.
2. Replace PII with deterministic HMAC tokens before Silver.
3. Store plaintext in a scoped "vault" table keyed by token.
4. Require an incident ticket to rehydrate plaintext.
5. Append an audit row for every rehydrate attempt.

Run:
    python submission/bonus/poc/privacy_tokenization_spike.py
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Iterable


SECRET = b"demo-key-rotate-daily-in-kms"

PII_PATTERNS = {
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    "id_hint": re.compile(r"\b(?:CMND|CCCD|passport)\s*[:#]?\s*[A-Z0-9-]{6,16}\b", re.I),
    "phone_vn": re.compile(r"(?<!\w)(?:\+84|0)(?:\d[\s.-]?){8,10}(?!\w)"),
}


@dataclass(frozen=True)
class RawCall:
    request_id: str
    tenant_id: str
    model: str
    prompt: str
    response: str
    latency_ms: int
    input_tokens: int
    output_tokens: int
    status: str


@dataclass(frozen=True)
class SilverCall:
    request_id: str
    tenant_id: str
    model: str
    prompt_redacted: str
    response_redacted: str
    latency_ms: int
    input_tokens: int
    output_tokens: int
    status: str
    pii_tokens: list[str]
    redaction_version: str


def token_for(kind: str, plaintext: str) -> str:
    digest = hmac.new(SECRET, f"{kind}:{plaintext}".encode(), hashlib.sha256).hexdigest()
    return f"pii_{kind}_{digest[:16]}"


def tokenize_text(text: str, vault: dict[str, dict[str, str]]) -> tuple[str, list[str]]:
    tokens: list[str] = []
    redacted = text

    for kind, pattern in PII_PATTERNS.items():
        for match in list(pattern.finditer(redacted)):
            plaintext = match.group(0)
            token = token_for(kind, plaintext)
            vault[token] = {"kind": kind, "plaintext": plaintext}
            tokens.append(token)
            redacted = redacted.replace(plaintext, f"<{token}>")

    return redacted, tokens


def bronze_to_silver(rows: Iterable[RawCall]) -> tuple[list[SilverCall], dict[str, dict[str, str]]]:
    vault: dict[str, dict[str, str]] = {}
    silver: list[SilverCall] = []
    seen: set[str] = set()

    for row in rows:
        if row.request_id in seen:
            continue
        seen.add(row.request_id)

        prompt_redacted, prompt_tokens = tokenize_text(row.prompt, vault)
        response_redacted, response_tokens = tokenize_text(row.response, vault)
        silver.append(
            SilverCall(
                request_id=row.request_id,
                tenant_id=row.tenant_id,
                model=row.model,
                prompt_redacted=prompt_redacted,
                response_redacted=response_redacted,
                latency_ms=row.latency_ms,
                input_tokens=row.input_tokens,
                output_tokens=row.output_tokens,
                status=row.status,
                pii_tokens=prompt_tokens + response_tokens,
                redaction_version="pii-rules-2026-05-04",
            )
        )

    return silver, vault


def rehydrate(
    redacted_text: str,
    vault: dict[str, dict[str, str]],
    audit_log: list[dict[str, str]],
    *,
    principal: str,
    ticket_id: str | None,
) -> str:
    allowed = bool(ticket_id and ticket_id.startswith("INC-"))
    audit_log.append(
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "principal": principal,
            "ticket_id": ticket_id or "",
            "allowed": str(allowed),
            "tokens_requested": ",".join(sorted(set(re.findall(r"pii_[a-z0-9_]+", redacted_text)))),
        }
    )
    if not allowed:
        raise PermissionError("rehydrate requires incident ticket INC-*")

    output = redacted_text
    for token in sorted(set(re.findall(r"pii_[a-z0-9_]+", redacted_text))):
        output = output.replace(f"<{token}>", vault[token]["plaintext"])
    return output


def main() -> None:
    bronze = [
        RawCall(
            request_id="r-001",
            tenant_id="tenant-a",
            model="claude-sonnet-4-6",
            prompt="User Nguyen Van A, email an@example.com, phone +84901234567 asks about invoice.",
            response="I found account CMND: 012345678 for an@example.com.",
            latency_ms=1240,
            input_tokens=120,
            output_tokens=80,
            status="ok",
        ),
        RawCall(
            request_id="r-001",
            tenant_id="tenant-a",
            model="claude-sonnet-4-6",
            prompt="retry duplicate",
            response="retry duplicate",
            latency_ms=1300,
            input_tokens=1,
            output_tokens=1,
            status="ok",
        ),
        RawCall(
            request_id="r-002",
            tenant_id="tenant-b",
            model="claude-haiku-4-5",
            prompt="No PII here, just latency test.",
            response="Done.",
            latency_ms=420,
            input_tokens=20,
            output_tokens=5,
            status="ok",
        ),
    ]

    silver, vault = bronze_to_silver(bronze)
    audit_log: list[dict[str, str]] = []

    print("silver_rows", len(silver), "bronze_rows", len(bronze), "dedup_dropped", len(bronze) - len(silver))
    print("vault_tokens", len(vault))
    print(json.dumps([asdict(row) for row in silver], indent=2))

    try:
        rehydrate(silver[0].prompt_redacted, vault, audit_log, principal="analyst@example.com", ticket_id=None)
    except PermissionError as exc:
        print("blocked_rehydrate", str(exc))

    restored = rehydrate(
        silver[0].prompt_redacted,
        vault,
        audit_log,
        principal="oncall@example.com",
        ticket_id="INC-2026-0504-001",
    )
    print("rehydrated_prompt", restored)
    print("audit_log")
    print(json.dumps(audit_log, indent=2))


if __name__ == "__main__":
    main()
