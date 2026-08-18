"""
LLM Observability Bonus — PoC: PII Tokenization for Bronze Layer

This notebook demonstrates the PII tokenization mechanism from ARCHITECTURE.md.
Topic: A. LLM observability at 1B requests/day

The key mechanism: tokenize PII at Bronze, so downstream Silver/Gold
are automatically compliant without re-processing.

Run: python pii_tokenization.py
"""

import re
import json
import duckdb
from deltalake import DeltaTable, write_deltalake
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# PII Tokenization Engine
# ─────────────────────────────────────────────────────────────────────────────

class PIIRedactor:
    """Tokenizes PII patterns in text.

    Creates deterministic tokens from raw values, enabling:
    - Re-identification (with token mapping table)
    - Audit (tokenized form preserves structure)
    - Compliance (original PII never appears in lakehouse)
    """

    PATTERNS = {
        "EMAIL":      (r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", "EMAIL_{hash}"),
        "PHONE":      (r"\b\d{10,11}\b", "PHONE_{hash}"),
        "CC":         (r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b", "CC_{hash}"),
        "SSN":        (r"\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b", "SSN_{hash}"),
        "IP_ADDR":    (r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", "IP_{hash}"),
    }

    def __init__(self):
        self._mapping: dict[str, tuple[str, str]] = {}  # token → (type, original)

    def _hash(self, value: str) -> str:
        """Deterministic hash so same value → same token (for dedup)."""
        import hashlib
        return hashlib.md5(value.encode()).hexdigest()[:8]

    def redact(self, text: str) -> tuple[str, list[tuple[str, str, str]]]:
        """Redact all PII from text.

        Returns:
            (redacted_text, [(token, type, original)]) for audit trail
        """
        results: list[tuple[str, str, str]] = []
        redacted = text

        for pii_type, (pattern, template) in self.PATTERNS.items():
            for match in re.finditer(pattern, redacted):
                original = match.group()
                token = template.format(hash=self._hash(original))
                self._mapping[token] = (pii_type, original)
                results.append((token, pii_type, original))
                redacted = redacted[:match.start()] + token + redacted[match.end():]

        return redacted, results


# ─────────────────────────────────────────────────────────────────────────────
# Demo: Bronze → Silver transformation with tokenization
# ─────────────────────────────────────────────────────────────────────────────

def demo():
    print("=" * 60)
    print("LLM Observability — PoC: PII Tokenization")
    print("=" * 60)

    # Sample raw LLM call data (simulating Kafka message)
    raw_calls = [
        {
            "request_id": "req_001",
            "tenant_id": "acme-corp",
            "timestamp": "2026-08-18T10:30:00Z",
            "user_prompt": "Hello alice.smith@acme.com, what's my account balance?",
            "assistant_response": "Your balance is $1,234.56 for account 1234567890.",
            "cost_usd": 0.0023,
            "tokens_used": 150,
        },
        {
            "request_id": "req_002",
            "tenant_id": "beta-inc",
            "timestamp": "2026-08-18T10:30:05Z",
            "user_prompt": "Call John at 0912345678 about meeting",
            "assistant_response": "I've noted to call John (0912345678) regarding your 2pm meeting.",
            "cost_usd": 0.0018,
            "tokens_used": 120,
        },
    ]

    print("\n1. RAW DATA (contains PII)")
    print("-" * 40)
    for call in raw_calls:
        print(f"  {call['request_id']}: {call['user_prompt'][:50]}...")

    # Tokenize Bronze data
    redactor = PIIRedactor()
    tokenized_calls = []

    print("\n2. TOKENIZATION (Bronze layer)")
    print("-" * 40)
    for call in raw_calls:
        redacted_prompt, pii_found = redactor.redact(call["user_prompt"])
        redacted_response, _ = redactor.redact(call["assistant_response"])

        tokenized = {
            "request_id": call["request_id"],
            "tenant_id": call["tenant_id"],
            "timestamp": call["timestamp"],
            "user_prompt_tokenized": redacted_prompt,
            "assistant_response_tokenized": redacted_response,
            "cost_usd": call["cost_usd"],
            "tokens_used": call["tokens_used"],
        }
        tokenized_calls.append(tokenized)

        for token, pii_type, original in pii_found:
            print(f"  [{pii_type}] {original} → {token}")

    # Show token mapping (separate, encrypted table in production)
    print("\n3. TOKEN MAPPING (separate table, encrypted)")
    print("-" * 40)
    print("  (In production: stored in separate encrypted namespace)")
    for token, (pii_type, original) in sorted(redactor._mapping.items()):
        print(f"  {token} → {pii_type}: {original}")

    # Demonstrate that Silver/Gold never see PII
    print("\n4. SILVER LAYER (downstream, PII-free)")
    print("-" * 40)
    for call in tokenized_calls:
        # Extract non-PII fields for aggregation
        print(f"  {call['request_id']}: tenant={call['tenant_id']}, "
              f"cost=${call['cost_usd']:.4f}, tokens={call['tokens_used']}")

    print("\n5. COMPLIANCE CHECK")
    print("-" * 40)

    # Check for any remaining PII patterns
    for call in tokenized_calls:
        combined_text = call["user_prompt_tokenized"] + call["assistant_response_tokenized"]
        remaining_pii = []
        for pii_type, (pattern, _) in PIIRedactor.PATTERNS.items():
            if re.search(pattern, combined_text):
                remaining_pii.append(pii_type)

        status = "❌ PII FOUND" if remaining_pii else "✅ CLEAN"
        print(f"  {call['request_id']}: {status}")

    print("\n" + "=" * 60)
    print("✓ PII Tokenization PoC complete")
    print("=" * 60)

    return tokenized_calls, redactor._mapping


if __name__ == "__main__":
    demo()
