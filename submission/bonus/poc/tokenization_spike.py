import re
import json
from typing import Dict

PII_PATTERNS = {
    'phone': re.compile(r"(\+?84|0)\d{9,10}"),
    'email': re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"),
    'id_number': re.compile(r"\b\d{9,12}\b")
}

KEY = 'static-demo-key'  # In production, use HSM/KMS and deterministic crypto


def deterministic_token(value: str, key: str = KEY) -> str:
    """Simple deterministic tokenization (demo only)."""
    # WARNING: demo-only; use HMAC/KMS-based deterministic tokenization in prod
    import hashlib
    h = hashlib.sha256((key + '|' + value).encode('utf-8')).hexdigest()
    return 'TKN-' + h[:16]


def redact_text(text: str, patterns=PII_PATTERNS) -> str:
    """Replace PII matches with deterministic tokens."""
    out = text
    for name, pat in patterns.items():
        def repl(m):
            token = deterministic_token(m.group(0))
            return token
        out = pat.sub(repl, out)
    return out


def redact_event(event: Dict) -> Dict:
    """Recursively redact strings in a JSON-like dict."""
    if isinstance(event, dict):
        return {k: redact_event(v) for k, v in event.items()}
    if isinstance(event, list):
        return [redact_event(x) for x in event]
    if isinstance(event, str):
        return redact_text(event)
    return event


if __name__ == '__main__':
    sample = {
        'tenant': 'acme',
        'request_id': 'req-123',
        'prompt': 'Hi, contact me at +84912345678 or alice@example.com. my id 012345678',
        'response': 'Hello. charge 0.02 USD'
    }
    print('RAW:\n', json.dumps(sample, ensure_ascii=False, indent=2))
    redacted = redact_event(sample)
    print('\nREDACTED:\n', json.dumps(redacted, ensure_ascii=False, indent=2))
