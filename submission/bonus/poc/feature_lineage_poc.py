"""PoC: tac dong lineage theo cap cot cho feature store.

Muc tieu: neu mot cot nguon bi xoa, tim cac feature va model bi anh huong.
Day la demo nho, khong can thu vien ngoai.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass


@dataclass(frozen=True)
class ModelInfo:
    name: str
    monthly_value_usd: int
    features: list[str]


# Cot goc tu bang Silver
BASE_COLS = {
    "customer.address_country",
    "customer.age",
    "customer.income",
    "txn.amount",
    "txn.country",
    "txn.merchant_id",
    "device.is_rooted",
}

# Dinh nghia feature: feature -> phu thuoc (cot goc hoac feature khac)
FEATURE_DEPS: dict[str, list[str]] = {
    "f_age_bucket": ["customer.age"],
    "f_country_risk": ["customer.address_country"],
    "f_txn_avg_7d": ["txn.amount"],
    "f_txn_country_mismatch": ["customer.address_country", "txn.country"],
    "f_device_risk": ["device.is_rooted"],
    "f_spend_score": ["f_txn_avg_7d", "customer.income"],
    "f_fraud_score": ["f_country_risk", "f_txn_country_mismatch", "f_device_risk"],
}

# Model va cac feature phu thuoc
MODELS = [
    ModelInfo("fraud_v3", 800000, ["f_fraud_score", "f_spend_score"]),
    ModelInfo("credit_v2", 500000, ["f_spend_score", "f_age_bucket"]),
    ModelInfo("kyc_watch", 200000, ["f_country_risk"]),
]


def build_reverse_index(feature_deps: dict[str, list[str]]) -> dict[str, list[str]]:
    """Tao do thi nguoc: phu thuoc -> feature phu thuoc vao no."""
    rev: dict[str, list[str]] = defaultdict(list)
    for feat, deps in feature_deps.items():
        for dep in deps:
            rev[dep].append(feat)
    return rev


def impacted_features(dropped_col: str) -> set[str]:
    """Tra ve tat ca feature bi anh huong khi mot cot goc bi xoa."""
    if dropped_col not in BASE_COLS:
        return set()

    rev = build_reverse_index(FEATURE_DEPS)
    impacted: set[str] = set()
    q: deque[str] = deque(rev.get(dropped_col, []))

    while q:
        feat = q.popleft()
        if feat in impacted:
            continue
        impacted.add(feat)
        for next_feat in rev.get(feat, []):
            q.append(next_feat)

    return impacted


def impacted_models(impacted_feats: set[str]) -> list[ModelInfo]:
    """Tra ve cac model bi anh huong (giao voi tap feature bi tac dong)."""
    out: list[ModelInfo] = []
    for m in MODELS:
        if any(f in impacted_feats for f in m.features):
            out.append(m)
    return out


def main() -> None:
    dropped = "customer.address_country"
    feats = impacted_features(dropped)
    models = impacted_models(feats)
    total_risk = sum(m.monthly_value_usd for m in models)

    print("Cot bi xoa:", dropped)
    print("Feature bi anh huong:", sorted(feats))
    print("Model bi anh huong:", [m.name for m in models])
    print("Uoc tinh gia tri rui ro hang thang (USD):", total_risk)


if __name__ == "__main__":
    main()
