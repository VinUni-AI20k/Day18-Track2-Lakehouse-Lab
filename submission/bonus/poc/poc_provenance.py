"""Bonus PoC: provenance-aware multimodal lakehouse on the lab's own data.

This script is offline and self-contained:
- reads the repo's synthetic docs_multimodal and agent_traces data
- writes provenance-normalized Silver tables
- pins a training version and replays it
- executes a right-to-erasure delete and checks CDF output
"""
from __future__ import annotations

import hashlib
import hmac
import json
import shutil
import sys
from pathlib import Path

import duckdb
import polars as pl
from deltalake import DeltaTable, write_deltalake

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

import generate_ai_data as gen  # noqa: E402
from lakehouse import path, reset  # noqa: E402

SALT = b"bonus_provenance_salt_2026"

DOCS = path("bronze", "docs_multimodal")
TRACES = path("bronze", "agent_traces")

OUT_ROOT = ROOT / "_lakehouse" / "bonus_provenance"
DOCS_SILVER = str(OUT_ROOT / "docs_silver")
TRACES_SILVER = str(OUT_ROOT / "traces_silver")


def tokenise(value: str | None) -> str | None:
    if value is None:
        return None
    return hmac.new(SALT, value.encode("utf-8"), hashlib.sha256).hexdigest()[:16]


def ensure_seed_data() -> None:
    if not Path(DOCS).exists() or not Path(TRACES).exists():
        gen.main()


def bucket_provenance(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        pl.when(pl.col("license").is_in(["proprietary", "commercial"]))
        .then(pl.lit("licensed"))
        .when(pl.col("license") == "cc-by-4.0")
        .then(pl.lit("public_domain"))
        .when(pl.col("license") == "user-owned")
        .then(pl.lit("scraped_optout_checked"))
        .when(pl.col("license") == "synthetic")
        .then(pl.lit("synthetic"))
        .otherwise(pl.lit("unclassified"))
        .alias("provenance_bucket"),
        pl.col("subject_id")
        .map_elements(tokenise, return_dtype=pl.Utf8)
        .alias("subject_token"),
    )


def build_docs_silver() -> pl.DataFrame:
    docs = pl.from_arrow(DeltaTable(DOCS).to_pyarrow_table())
    silver = bucket_provenance(docs)
    silver = silver.select(
        "doc_id",
        "title",
        "topic",
        "subject_token",
        "source",
        "license",
        "consent_train",
        "generator",
        "blob_uri",
        "provenance_bucket",
    )
    reset(DOCS_SILVER)
    write_deltalake(
        DOCS_SILVER,
        silver.to_arrow(),
        mode="overwrite",
        partition_by=["provenance_bucket"],
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    return silver


def build_traces_silver() -> pl.DataFrame:
    traces = pl.from_arrow(DeltaTable(TRACES).to_pyarrow_table())
    traces = traces.with_columns(
        pl.when(pl.col("session_id").str.slice(5).cast(pl.Int64) < 150)
        .then(pl.lit("policy-v2"))
        .otherwise(pl.lit("policy-v3"))
        .alias("agent_version"),
    )
    traces = traces.select(
        "session_id",
        "step",
        "tool",
        "status",
        "reward",
        "subject_id",
        "latency_ms",
        "agent_version",
    )
    reset(TRACES_SILVER)
    write_deltalake(
        TRACES_SILVER,
        traces.to_arrow(),
        mode="overwrite",
        partition_by=["agent_version"],
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    return traces


def main() -> None:
    ensure_seed_data()
    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    docs_silver = build_docs_silver()
    traces_silver = build_traces_silver()

    con = duckdb.connect()
    con.register("docs", DeltaTable(DOCS_SILVER).to_pyarrow_table())
    con.register("traces", DeltaTable(TRACES_SILVER).to_pyarrow_table())

    docs_summary = con.sql("""
        SELECT provenance_bucket, count(*) AS n_docs, count(distinct topic) AS n_topics
        FROM docs
        GROUP BY 1
        ORDER BY 1
    """).fetchall()
    traces_summary = con.sql("""
        SELECT agent_version, count(*) AS n_steps, round(avg(latency_ms), 1) AS avg_latency_ms
        FROM traces
        GROUP BY 1
        ORDER BY 1
    """).fetchall()

    training_run = {
        "run_id": "bonus-provenance-001",
        "table": DOCS_SILVER,
        "table_version": DeltaTable(DOCS_SILVER).version(),
        "rows_seen": DeltaTable(DOCS_SILVER).count(),
    }

    # New data lands after the run was pinned.
    write_deltalake(
        DOCS_SILVER,
        docs_silver.head(1).to_arrow(),
        mode="append",
        partition_by=["provenance_bucket"],
        configuration={"delta.enableChangeDataFeed": "true"},
    )

    replay = DeltaTable(DOCS_SILVER, version=training_run["table_version"])

    target_token = tokenise("user_007")
    before_delete = DeltaTable(DOCS_SILVER).version()
    current = DeltaTable(DOCS_SILVER)
    current.delete(f"subject_token = '{target_token}'")
    after_delete = DeltaTable(DOCS_SILVER)
    con.register("docs_after", after_delete.to_pyarrow_table())
    remaining = con.sql(
        f"SELECT count(*) FROM docs_after WHERE subject_token = '{target_token}'"
    ).fetchone()[0]
    cdf = after_delete.load_cdf(starting_version=before_delete + 1).read_all()
    cdf_df = pl.DataFrame(cdf)
    delete_events = cdf_df.filter(pl.col("_change_type") == "delete").height

    print("=" * 72)
    print("Bonus PoC: provenance-aware multimodal lakehouse")
    print("=" * 72)
    print("docs summary:")
    for row in docs_summary:
        print(" ", row)
    print("traces summary:")
    for row in traces_summary:
        print(" ", row)
    print("\ntraining run:")
    print(json.dumps(training_run, indent=2))
    print(
        f"pinned replay rows: {replay.count()} (matches original: "
        f"{replay.count() == training_run['rows_seen']})"
    )
    print(f"remaining rows for user_007 after delete: {remaining}")
    print(f"CDF delete events captured: {delete_events}")
    assert replay.count() == training_run["rows_seen"]
    assert remaining == 0
    assert delete_events > 0
    print("\nBonus PoC complete.")


if __name__ == "__main__":
    main()
