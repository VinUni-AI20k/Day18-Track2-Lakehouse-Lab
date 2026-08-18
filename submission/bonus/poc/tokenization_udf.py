"""
POC: PII Tokenization UDF for LLM Observability Lakehouse

Demonstrates: PII redaction at Bronze landing using deterministic tokenization.
Non-trivial mechanism: replaces PII before it hits raw storage, enabling
downstream analysts to query Gold without PII exposure.

Run: python tokenization_udf.py
"""
import re
import hashlib
import json
from typing import Optional
from dataclasses import dataclass


@dataclass
class PIIRedactionConfig:
    """Config for tokenization - in production, store in Secrets Manager."""
    salt: str  # Deterministic salt for consistent hashing
    patterns: dict[str, str]  # regex -> placeholder


class PIIRedactor:
    """
    Deterministic PII tokenization.
    Same input always produces same token - enables join with token map.
    """

    def __init__(self, salt: str = "contentforge-llm-2024"):
        self.salt = salt
        self.config = PIIRedactionConfig(
            salt=salt,
            patterns={
                # Email: john@example.com -> EMAIL_[hash]
                r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b':
                    'EMAIL_TOKEN',
                # Phone: 0xxx... or +84xxx... -> PHONE_TOKEN
                r'\+?\d{9,15}':
                    'PHONE_TOKEN',
                # IP: 192.168.x.x -> IP_TOKEN
                r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b':
                    'IP_TOKEN',
                # Credit card (for completeness)
                r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b':
                    'CC_TOKEN',
            }
        )
        self._token_map: dict[str, str] = {}  # token -> original (for authorized access)

    def _hash_token(self, value: str) -> str:
        """Deterministic hash for consistent tokenization."""
        return hashlib.sha256(f"{self.salt}:{value}".encode()).hexdigest()[:16]

    def _get_or_create_token(self, original: str) -> str:
        """Get existing token or create new one."""
        if original not in self._token_map:
            self._token_map[original] = f"TOKEN_{self._hash_token(original)}"
        return self._token_map[original]

    def redact(self, text: str) -> tuple[str, list[dict]]:
        """
        Redact PII from text, return redacted text + token map for authorized access.

        Returns:
            (redacted_text, token_replacements) where token_replacements
            contains list of {original, token, type} for audit.
        """
        replacements = []
        redacted = text

        for pattern, placeholder in self.config.patterns.items():
            for match in re.finditer(pattern, text):
                original = match.group()
                token = self._get_or_create_token(original)

                # Determine type from placeholder
                pii_type = placeholder.replace('_TOKEN', '').lower()
                replacements.append({
                    "type": pii_type,
                    "original_hash": self._hash_token(original),
                    "token": token
                })

                # Replace in text
                redacted = redacted.replace(original, token, 1)

        return redacted, replacements


def process_llm_event(raw_event: dict, redactor: PIIRedactor) -> dict:
    """
    Process a single LLM event: redact PII, preserve metrics.

    In production: this runs as Spark UDF on Bronze layer.
    """
    result = {
        "request_id": raw_event.get("request_id"),
        "tenant_id": raw_event.get("tenant_id"),
        "model": raw_event.get("model"),
        "prompt_tokens": raw_event.get("prompt_tokens", 0),
        "completion_tokens": raw_event.get("completion_tokens", 0),
        "latency_ms": raw_event.get("latency_ms", 0),
        "cost_usd": raw_event.get("cost_usd", 0.0),
        "status": raw_event.get("status", "success"),
    }

    # Redact user_message and assistant_message
    redacted_messages = []
    for msg in raw_event.get("messages", []):
        redacted_content, replacements = redactor.redact(msg.get("content", ""))
        redacted_messages.append({
            "role": msg.get("role"),
            "content": redacted_content,
            "pii_tokens": len(replacements)
        })

    result["messages"] = redacted_messages
    result["total_pii_tokens"] = sum(m["pii_tokens"] for m in redacted_messages)

    return result


def demo():
    """Demonstrate PII redaction on sample LLM event."""
    print("=" * 60)
    print("PII Tokenization POC - LLM Observability")
    print("=" * 60)

    redactor = PIIRedactor(salt="contentforge-2024")

    sample_event = {
        "request_id": "req-12345",
        "tenant_id": "tenant-acme",
        "model": "gpt-4o-mini",
        "prompt_tokens": 150,
        "completion_tokens": 200,
        "latency_ms": 850,
        "cost_usd": 0.0025,
        "messages": [
            {"role": "user", "content": "Write ad copy for john.doe@acme.com, contact 0912345678"},
            {"role": "assistant", "content": "Draft created for john.doe@acme.com"},
        ]
    }

    print("\n[RAW EVENT]")
    print(json.dumps(sample_event, indent=2, ensure_ascii=False))

    redacted = process_llm_event(sample_event, redactor)

    print("\n[REDACTED EVENT - Safe for Bronze Layer]")
    print(json.dumps(redacted, indent=2, ensure_ascii=False))

    print(f"\n[TOKEN MAP - Store securely, access for authorized re-hydration]")
    for original, token in list(redactor._token_map.items())[:3]:
        print(f"  {token} -> {original}")


if __name__ == "__main__":
    demo()
