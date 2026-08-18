# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # PoC — Rollback một deploy redaction lỗi (FM2 của ARCHITECTURE.md)
#
# Không demo phần dễ (ghi Delta, vẽ dashboard). Demo phần khó: **cái gì thực sự xảy ra
# khi bạn ship một regex tokenize thiếu sót, và làm sao rollback cho tới khi PII biến mất
# *vật lý*.** Bốn mechanism, mỗi cái là một bước của FM2:
#
# | Bước | Mechanism | Vì sao khó |
# |---|---|---|
# | 1 | cột `redaction_version` | không có nó, "dòng nào bị ảnh hưởng" là câu hỏi không đáp án |
# | 2 | `DELETE` theo version + replay | replay phải lấy từ Kafka, **không** từ Bronze (Bronze đã bị nhiễm) |
# | 3 | time travel + `VACUUM` | delete **chưa** làm PII biến mất — version cũ vẫn đọc được |
# | 4 | Change Data Feed | derived consumer chỉ evict đúng nếu nó *subscribe delete*, không đoán |
#
# Chạy: `.venv/bin/python submission/bonus/poc/redaction_rollback.py` — offline, ~2s.

# %%
import base64
import hashlib
import re
import sys
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# Tìm repo root bằng cách đi lên tới khi thấy scripts/lakehouse.py — chạy được cả khi
# `python file.py` (có __file__) và khi thực thi dưới dạng .ipynb (không có __file__).
_start = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd().resolve()
ROOT = next(p for p in (_start, *_start.parents) if (p / "scripts" / "lakehouse.py").exists())
sys.path.insert(0, str(ROOT / "scripts"))
from lakehouse import count_files, human, du, path, reset  # noqa: E402

BRONZE, SILVER = path("scratch", "poc_bronze_events"), path("scratch", "poc_silver_events")
reset(BRONZE, SILVER)
CFG = {"delta.enableChangeDataFeed": "true"}          # bắt buộc để có bước 4
SECRET, N = b"poc-key-not-real", 5_000

# %% [markdown]
# ## 0. Kafka là nguồn replay duy nhất của text CHƯA tokenize
#
# Trong design, tokenize xảy ra **trước lần ghi đầu tiên** — nên Bronze cũng đã tokenize.
# Hệ quả quan trọng: khi chính tokenizer là thứ bị lỗi, Bronze **không** cứu được bạn
# (nó chứa đúng phần rò rỉ đó). Cửa sổ hồi phục là **retention 24h của Kafka**, không phải
# 48h của Bronze. Đó là lý do canary ở bước 2 phải chạy inline và fail fast.

# %%
KAFKA = [                                             # 50% dùng định dạng +84
    {"event_id": i, "tenant_id": f"tenant-{i % 7}",
     "payload": (f"khach yeu cau goi lai 0912{i:06d} sang mai" if i % 2 == 0
                 else f"khach yeu cau goi lai +84912{i:06d} sang mai")}
    for i in range(N)
]

RE_V1    = re.compile(r"0\d{9}")                # ← BUG: bỏ sót hoàn toàn định dạng +84…
RE_V2    = re.compile(r"(?:\+84|0)\d{9}")       # ← bản sửa
RE_AUDIT = re.compile(r"(?:\+?84|0)\d{9}")      # canary: chặt hơn/chậm hơn, chạy trên sample


def tokenize(m: re.Match) -> str:
    """Deterministic token. Base32 nên KHÔNG chứa '0'/'8'/'9' → canary không tự báo động vì token."""
    digest = hashlib.sha256(SECRET + m.group(0).encode()).digest()
    return "TOK_" + base64.b32encode(digest).decode()[:12]


def ingest(version: str, rx: re.Pattern) -> pa.Table:
    return pa.table({
        "event_id":          pa.array([e["event_id"] for e in KAFKA], pa.int64()),
        "tenant_id":         [e["tenant_id"] for e in KAFKA],
        "payload":           [rx.sub(tokenize, e["payload"]) for e in KAFKA],
        "redaction_version": [version] * len(KAFKA),
    })


def leaks(table_path: str, version: int | None = None) -> int:
    t = DeltaTable(table_path, version=version).to_pyarrow_table()
    return sum(1 for p in t.column("payload").to_pylist() if RE_AUDIT.search(p))


# %% [markdown]
# ## 1. Ship bản v1 (lỗi) — Bronze rồi Silver

# %%
write_deltalake(BRONZE, ingest("v1", RE_V1), mode="overwrite", configuration=CFG)
write_deltalake(SILVER, DeltaTable(BRONZE).to_pyarrow_table(), mode="overwrite", configuration=CFG)

leak_v1 = leaks(SILVER)
print(f"Silver sau deploy v1: {DeltaTable(SILVER).count():,} dòng, "
      f"**{leak_v1:,} dòng còn PII thô** ({leak_v1 / N * 100:.0f}%)")
print(f"  ví dụ: {DeltaTable(SILVER).to_pyarrow_table().column('payload')[1].as_py()}")
print(f"  đã tokenize đúng: {DeltaTable(SILVER).to_pyarrow_table().column('payload')[0].as_py()}")

# %% [markdown]
# ## 2. Canary bắt được — rồi rollback theo `redaction_version`
#
# `DELETE` là metadata op (deletion vectors / tombstone), không rewrite. Replay lấy từ **Kafka**.

# %%
assert leak_v1 > 0, "canary phải đỏ — nếu không, cơ chế detect của FM2 vô dụng"
print(f"CANARY ĐỎ: {leak_v1:,} hit trên detector chặt → dừng writer, rollback\n")

for tbl in (SILVER, BRONZE):                    # cả hai tầng đều bị nhiễm
    DeltaTable(tbl).delete("redaction_version = 'v1'")
write_deltalake(BRONZE, ingest("v2", RE_V2), mode="append")
write_deltalake(SILVER, DeltaTable(BRONZE).to_pyarrow_table(), mode="append")

leak_v2 = leaks(SILVER)
print(f"Silver sau replay v2: {DeltaTable(SILVER).count():,} dòng, PII thô = **{leak_v2}**")
print(f"  ví dụ (+84 giờ đã tokenize): "
      f"{DeltaTable(SILVER).to_pyarrow_table().column('payload')[1].as_py()}")

# %% [markdown]
# ## 3. CDF: derived consumer evict đúng cái gì
#
# Nếu buộc phải có index dẫn xuất, nó **subscribe delete** chứ không sync lại cả bảng —
# đúng bug mà NB7 tái hiện (lakehouse 0 hit, index cũ vẫn 8 hit).

# %%
cdf = DeltaTable(SILVER).load_cdf(starting_version=1).read_all()
types = cdf.column("_change_type").to_pylist()
n_del = types.count("delete")
evict = [i for i, t in zip(cdf.column("event_id").to_pylist(), types) if t == "delete"]
print(f"CDF từ v1: {len(types):,} dòng thay đổi — delete={n_del:,}, insert={types.count('insert'):,}")
print(f"event_id cần evict khỏi index: {sorted(evict)[:5]} ... (tổng {len(evict):,})")

# %% [markdown]
# ## 4. Phần người ta bỏ qua: DELETE **chưa** làm PII biến mất
#
# Time travel vẫn đọc được version trước khi xoá. Rollback chỉ *hoàn tất* sau `VACUUM`.

# %%
leak_tt = leaks(SILVER, version=0)
files_before = count_files(SILVER)
print(f"Đọc Silver tại version=0 (trước khi xoá): PII thô = **{leak_tt:,}** ← VẪN RÒ RỈ")
print(f"  → 'đã DELETE' và 'đã xoá' là hai chuyện khác nhau.\n")

DeltaTable(SILVER).vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
try:
    leaks(SILVER, version=0)
    tt_gone, err = False, "version=0 VẪN đọc được — rollback CHƯA hoàn tất"
except Exception as e:
    tt_gone, err = True, f"{type(e).__name__}"
print(f"Sau VACUUM retention=0: {count_files(SILVER)} file (trước {files_before}), "
      f"{human(du(SILVER))}")
print(f"Đọc lại version=0 → {err}")
print(f"PII trong version hiện tại: {leaks(SILVER)}")
print("\nĐánh đổi vừa trả: time travel của Silver mất. Đó chính là lý do ARCHITECTURE.md")
print("chọn VACUUM retention 24h và để Kafka/Bronze làm đường replay — chứ không dựa vào")
print("time travel để rollback, vì time travel là thứ giữ lại đúng cái ta vừa cam kết xoá.")

# %% [markdown]
# ## ✅ Pass criteria

# %%
checks = {
    "v1 rò rỉ PII (bug tái hiện được)":        leak_v1 == N // 2,
    "canary phát hiện được trước khi sửa":     leak_v1 > 0,
    "v2 sạch sau replay từ Kafka":             leak_v2 == 0,
    "số dòng giữ nguyên sau rollback":         DeltaTable(SILVER).count() == N,
    "CDF phát ra đủ delete event để evict":    n_del == N and len(evict) == N,
    "time travel VẪN rò rỉ trước VACUUM":      leak_tt == N // 2,
    "VACUUM làm version cũ biến mất vật lý":   tt_gone,
}
for k, v in checks.items():
    print(f"  [{'PASS' if v else 'FAIL'}] {k}")
assert all(checks.values()), "PoC incomplete — xem dòng FAIL"
print("\nPoC complete — FM2 rollback đã chứng minh end-to-end.")
