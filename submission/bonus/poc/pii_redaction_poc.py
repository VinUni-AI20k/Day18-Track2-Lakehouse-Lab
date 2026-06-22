import csv
import hashlib
import hmac
import json
import re
from pathlib import Path

SECRET = b"demo-secret-not-for-production"

EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
PHONE_RE = re.compile(r"(?<!\w)(?:\+?84|0)(?:[\s.-]?\d){8,10}(?!\w)")


def token_for(kind: str, value: str) -> str:
    normalized = value.strip().lower()
    digest = hmac.new(SECRET, f"{kind}:{normalized}".encode(), hashlib.sha256).hexdigest()
    return f"{kind}_tok_{digest[:16]}"


def redact_text(text: str):
    pii_tokens = []

    def replace_email(match):
        value = match.group(0)
        token = token_for("email", value)
        pii_tokens.append({"kind": "email", "token": token})
        return f"[EMAIL:{token}]"

    def replace_phone(match):
        value = match.group(0)
        compact = re.sub(r"\D", "", value)
        token = token_for("phone", compact)
        pii_tokens.append({"kind": "phone", "token": token})
        return f"[PHONE:{token}]"

    redacted = EMAIL_RE.sub(replace_email, text)
    redacted = PHONE_RE.sub(replace_phone, redacted)
    return redacted, pii_tokens


raw_events = [
    {
        "request_id": "req-001",
        "tenant_id": "tenant-a",
        "model": "claude-sonnet-4-6",
        "latency_ms": 1420,
        "prompt_tokens": 820,
        "completion_tokens": 210,
        "prompt": "Customer email is linh.nguyen@example.com and phone is 0912345678. Please summarize.",
        "response": "I cannot expose linh.nguyen@example.com, but I can summarize the safe part.",
    },
    {
        "request_id": "req-002",
        "tenant_id": "tenant-a",
        "model": "claude-sonnet-4-6",
        "latency_ms": 980,
        "prompt_tokens": 510,
        "completion_tokens": 180,
        "prompt": "Call +84 912 345 678 if the delivery fails.",
        "response": "Noted. The phone number should be treated as sensitive.",
    },
    {
        "request_id": "req-003",
        "tenant_id": "tenant-b",
        "model": "claude-haiku-4-5",
        "latency_ms": 430,
        "prompt_tokens": 200,
        "completion_tokens": 90,
        "prompt": "No PII here. Just classify this support ticket.",
        "response": "The ticket is about billing.",
    },
]

out_dir = Path("submission/bonus/poc/output")
out_dir.mkdir(parents=True, exist_ok=True)

silver_rows = []
audit_rows = []

for event in raw_events:
    redacted_prompt, prompt_tokens = redact_text(event["prompt"])
    redacted_response, response_tokens = redact_text(event["response"])
    pii_tokens = prompt_tokens + response_tokens

    silver_rows.append(
        {
            "request_id": event["request_id"],
            "tenant_id": event["tenant_id"],
            "model": event["model"],
            "latency_ms": event["latency_ms"],
            "prompt_tokens": event["prompt_tokens"],
            "completion_tokens": event["completion_tokens"],
            "redacted_prompt": redacted_prompt,
            "redacted_response": redacted_response,
            "pii_token_count": len(pii_tokens),
            "pii_tokens_json": json.dumps(pii_tokens, sort_keys=True),
        }
    )

    for token in pii_tokens:
        audit_rows.append(
            {
                "request_id": event["request_id"],
                "tenant_id": event["tenant_id"],
                "kind": token["kind"],
                "token": token["token"],
                "action": "tokenized_before_silver",
            }
        )

with (out_dir / "silver_redacted.csv").open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=silver_rows[0].keys())
    writer.writeheader()
    writer.writerows(silver_rows)

with (out_dir / "pii_token_audit.csv").open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=audit_rows[0].keys())
    writer.writeheader()
    writer.writerows(audit_rows)

# Verification: no raw emails or phones should remain in analyst-readable Silver text.
for row in silver_rows:
    combined = row["redacted_prompt"] + " " + row["redacted_response"]
    assert not EMAIL_RE.search(combined), f"Email leaked in {row['request_id']}"
    assert not PHONE_RE.search(combined), f"Phone leaked in {row['request_id']}"

# Verification: same email appears in prompt and response, so deterministic token should repeat.
email_tokens = [r["token"] for r in audit_rows if r["kind"] == "email"]
assert len(set(email_tokens)) == 1, "Expected same email to map to same deterministic token"

phone_tokens = [r["token"] for r in audit_rows if r["kind"] == "phone"]
assert len(phone_tokens) == 2, f"Expected 2 phone tokenization events, got {len(phone_tokens)}"

print("PII REDACTION POC PASS")
print(f"silver_rows={len(silver_rows)}")
print(f"audit_rows={len(audit_rows)}")
print(f"outputs={out_dir}")
print()
print("--- SILVER SAMPLE ---")
for row in silver_rows:
    print(json.dumps(row, ensure_ascii=False, indent=2))
