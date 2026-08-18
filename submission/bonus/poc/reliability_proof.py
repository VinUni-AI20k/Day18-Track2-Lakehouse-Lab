"""PoC 2 — Chứng minh độ tin cậy của đường MERGE bằng thí nghiệm, không bằng lời.

Tài liệu kiến trúc tuyên bố đường ingest "chịu được sự kiện đến sai thứ tự,
chịu được retry, chịu được ghi đồng thời". Ba câu đó là giả thuyết. File này
biến chúng thành mệnh đề kiểm chứng được và đo.

  E1 ORDER-INDEPENDENCE  Xáo trộn thứ tự đến của cùng một tập sự kiện 200 lần.
                         Trạng thái current cuối cùng phải GIỐNG HỆT nhau cả
                         200 lần. Đây là tính chất cốt lõi: nếu nó đúng thì
                         "sự kiện đến muộn" không còn là ca đặc biệt nữa.
  E2 IDEMPOTENCY         Phát lại y nguyên một batch 5 lần (Kafka at-least-once,
                         connector restart). Sau lần đầu, trạng thái không đổi
                         và không sinh dòng trùng.
  E3 CONCURRENT WRITERS  8 writer MERGE đồng thời vào cùng một bảng. Không được
                         mất update, không được hỏng bảng.
  E4 HISTORY COMPLETE    Mọi sự kiện — kể cả bản ghi bị guard TỪ CHỐI khỏi bảng
                         current — vẫn phải có mặt đủ trong bảng history. Từ
                         chối ghi đè KHÔNG được phép đồng nghĩa với mất dữ liệu.

E1 là thí nghiệm quan trọng nhất. Nó đối chứng bằng cách chạy CẢ phương án SAI
(MERGE không guard) trên cùng dữ liệu, để con số "tin cậy" có cái để so.

Chạy: .venv/bin/python submission/bonus/poc/reliability_proof.py
"""
from __future__ import annotations

import hashlib
import itertools
import json
import random
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError

SCHEMA = pa.schema([
    pa.field("driver_id", pa.int64(), nullable=False),
    pa.field("status", pa.string()),
    pa.field("src_ts", pa.int64()),
])

# 12 sự kiện CDC cho 4 tài xế. Mỗi tài xế đổi trạng thái nhiều lần.
# Trạng thái ĐÚNG cuối cùng = sự kiện có src_ts lớn nhất của mỗi tài xế.
EVENTS = [
    (1, "online",  1000), (1, "busy",    3000), (1, "offline", 2000),
    (2, "offline", 1500), (2, "online",  2500), (2, "busy",    1200),
    (3, "busy",    4000), (3, "online",  1100), (3, "offline", 3500),
    (4, "online",  2200), (4, "offline", 2100), (4, "busy",    2300),
]
TRUTH = {d: max((e for e in EVENTS if e[0] == d), key=lambda e: e[2])[1]
         for d in {e[0] for e in EVENTS}}


def tbl(rows) -> pa.Table:
    return pa.table({"driver_id": [r[0] for r in rows],
                     "status":    [r[1] for r in rows],
                     "src_ts":    [r[2] for r in rows]}, schema=SCHEMA)


def state_hash(path: str) -> str:
    """Vân tay tất định của trạng thái current — so sánh được giữa các lần chạy."""
    rows = sorted(DeltaTable(path).to_pyarrow_table().to_pylist(),
                  key=lambda r: r["driver_id"])
    return hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()[:12]


def state_dict(path: str) -> dict:
    return {r["driver_id"]: r["status"]
            for r in DeltaTable(path).to_pyarrow_table().to_pylist()}


def new_table() -> str:
    p = tempfile.mkdtemp(prefix="rel_"); shutil.rmtree(p)
    write_deltalake(p, tbl([]), mode="overwrite",
                    configuration={"delta.enableChangeDataFeed": "true"})
    return p


def reduce_batch(rows):
    """Gom mỗi khoá về ĐÚNG MỘT dòng (src_ts lớn nhất) trước khi MERGE.

    Bắt buộc, không phải tối ưu. Delta từ chối MERGE khi một dòng đích khớp
    nhiều dòng nguồn cùng thoả mệnh đề WHEN MATCHED:

        DeltaError: MERGE matched a target row with multiple source rows
        that satisfy duplicate relevant WHEN MATCHED clauses

    Một micro-batch 30 giây trong thực tế gần như LUÔN chứa nhiều sự kiện cho
    cùng một tài xế, nên bước gom này nằm trên đường chạy chính, không phải
    trên nhánh ngoại lệ. Tương đương `ROW_NUMBER() OVER (PARTITION BY key
    ORDER BY src_ts DESC) = 1` — đúng phép dedup mà NB4 dùng ở tầng Silver.
    """
    best: dict[int, tuple] = {}
    for r in rows:
        if r[0] not in best or r[2] > best[r[0]][2]:
            best[r[0]] = r
    return list(best.values())


def apply_batch(path: str, rows, guarded: bool = True) -> None:
    """Một micro-batch CDC. `guarded=False` là phương án sai, để đối chứng."""
    rows = reduce_batch(rows)
    m = DeltaTable(path).merge(source=tbl(rows), predicate="t.driver_id = s.driver_id",
                               source_alias="s", target_alias="t")
    upd = {c: f"s.{c}" for c in SCHEMA.names}
    m = (m.when_matched_update(updates=upd, predicate="s.src_ts > t.src_ts")
         if guarded else m.when_matched_update_all())
    m.when_not_matched_insert_all().execute()


print(__doc__.splitlines()[0])
print("=" * 72)
print(f"\nTrạng thái ĐÚNG (theo src_ts lớn nhất mỗi tài xế): {TRUTH}\n")

# ── E1 ─────────────────────────────────────────────────────────────────
print("─" * 72)
print("E1  ORDER-INDEPENDENCE — 200 thứ tự đến ngẫu nhiên, mỗi sự kiện 1 batch")
print("─" * 72)

TRIALS = 200
rng = random.Random(42)


def trial(guarded: bool) -> tuple[set, int]:
    hashes, wrong = set(), 0
    for _ in range(TRIALS):
        order = EVENTS[:]; rng.shuffle(order)
        p = new_table()
        for ev in order:
            apply_batch(p, [ev], guarded=guarded)
        hashes.add(state_hash(p))
        if state_dict(p) != TRUTH:
            wrong += 1
        shutil.rmtree(p, ignore_errors=True)
    return hashes, wrong


g_hashes, g_wrong = trial(guarded=True)
n_hashes, n_wrong = trial(guarded=False)

print(f"  CÓ guard    : {len(g_hashes):>3} trạng thái cuối khác nhau / {TRIALS} lần  "
      f"→ sai {g_wrong}/{TRIALS} lần")
print(f"  KHÔNG guard : {len(n_hashes):>3} trạng thái cuối khác nhau / {TRIALS} lần  "
      f"→ sai {n_wrong}/{TRIALS} lần")
print(f"\n  → Có guard: đúng 1 vân tay duy nhất ({list(g_hashes)[0]}) bất kể thứ tự đến.")
print(f"  → Không guard: kết quả phụ thuộc thứ tự gói tin đến — tức là phụ thuộc")
print("    chất lượng sóng ở tỉnh xa. Đó không phải một hệ thống, đó là xổ số.")

# Bổ sung: quét VÉT CẠN mọi hoán vị của một tập con, không chỉ mẫu ngẫu nhiên.
SUB = EVENTS[:6]
sub_truth = {d: max((e for e in SUB if e[0] == d), key=lambda e: e[2])[1]
             for d in {e[0] for e in SUB}}
perm_hashes = set()
for order in itertools.permutations(SUB):
    p = new_table()
    for ev in order:
        apply_batch(p, [ev], guarded=True)
    perm_hashes.add(state_hash(p))
    shutil.rmtree(p, ignore_errors=True)
print(f"\n  Vét cạn {len(list(itertools.permutations(SUB)))} hoán vị của 6 sự kiện đầu: "
      f"{len(perm_hashes)} trạng thái cuối khác nhau")
print("  → Không phải lấy mẫu may mắn. Mọi thứ tự có thể đều cho cùng một kết quả.")

# ── E2 ─────────────────────────────────────────────────────────────────
print("\n" + "─" * 72)
print("E2  IDEMPOTENCY — phát lại y nguyên cùng một batch 5 lần")
print("─" * 72)

# Đối chứng, trên bảng RIÊNG đã có sẵn 4 dòng — phải có dòng đích thì mệnh đề
# WHEN MATCHED mới kích hoạt được. (Trên bảng rỗng, mọi dòng đi vào nhánh
# INSERT và xung đột không bao giờ lộ ra — một cái bẫy khi tự kiểm.)
ctl = new_table()
apply_batch(ctl, [(d, "init", 1) for d in TRUTH], guarded=True)
raw_rejected = False
try:
    DeltaTable(ctl).merge(source=tbl(EVENTS), predicate="t.driver_id = s.driver_id",
                          source_alias="s", target_alias="t") \
        .when_matched_update(updates={c: f"s.{c}" for c in SCHEMA.names},
                             predicate="s.src_ts > t.src_ts") \
        .when_not_matched_insert_all().execute()
except DeltaError as e:
    raw_rejected = "multiple source rows" in str(e)
shutil.rmtree(ctl, ignore_errors=True)
print("  Đối chứng — MERGE batch thô (12 sự kiện, 4 khoá trùng lặp) vào bảng đã có dòng:")
print(f"    Delta TỪ CHỐI, không ghi mù: {raw_rejected}")
print("    → ràng buộc thật của format, phải gom về 1 dòng/khoá trước khi MERGE.")
print(f"  Sau gom: {len(EVENTS)} sự kiện → {len(reduce_batch(EVENTS))} dòng nguồn\n")

p = new_table()
apply_batch(p, EVENTS, guarded=True)
first_hash, first_rows = state_hash(p), DeltaTable(p).to_pyarrow_table().num_rows
replays = []
for i in range(5):
    apply_batch(p, EVENTS, guarded=True)
    replays.append((state_hash(p), DeltaTable(p).to_pyarrow_table().num_rows))
stable = all(h == first_hash and n == first_rows for h, n in replays)
print(f"  Sau lần nạp đầu : hash={first_hash}  rows={first_rows}")
for i, (h, n) in enumerate(replays, 1):
    print(f"  Phát lại lần {i}   : hash={h}  rows={n}")
print(f"\n  → Trạng thái bất biến qua mọi lần phát lại: {stable}")
print("  → Kafka at-least-once và connector restart không tạo dòng trùng, vì guard")
print("    `s.src_ts > t.src_ts` cũng từ chối luôn chính bản ghi bằng nhau.")
idempotent = stable
shutil.rmtree(p, ignore_errors=True)

# ── E3 ─────────────────────────────────────────────────────────────────
print("\n" + "─" * 72)
print("E3  CONCURRENT WRITERS — 8 writer MERGE đồng thời vào cùng một bảng")
print("─" * 72)

p = new_table()
apply_batch(p, [(d, "init", 1) for d in TRUTH], guarded=True)


def writer(ev):
    """Mỗi writer commit độc lập; xung đột optimistic-concurrency thì thử lại."""
    for attempt in range(12):
        try:
            apply_batch(p, [ev], guarded=True)
            return True
        except DeltaError:
            continue
    return False


with ThreadPoolExecutor(max_workers=8) as ex:
    results = list(ex.map(writer, EVENTS))

final = state_dict(p)
versions = len(DeltaTable(p).history())
readable = DeltaTable(p).to_pyarrow_table().num_rows
print(f"  Writer commit thành công : {sum(results)}/{len(EVENTS)}")
print(f"  Version trong log        : {versions}")
print(f"  Dòng đọc lại được        : {readable}  (kỳ vọng {len(TRUTH)} — không nhân bản)")
print(f"  Trạng thái cuối          : {final}")
concurrent_ok = final == TRUTH and readable == len(TRUTH) and all(results)
print(f"\n  → Không mất update, không hỏng bảng, kết quả vẫn = trạng thái đúng: {concurrent_ok}")
print("  → Đây là optimistic concurrency của Delta làm việc: writer thua cuộc đọc lại")
print("    snapshot mới rồi commit lại, chứ không ghi đè mù.")
shutil.rmtree(p, ignore_errors=True)

# ── E4 ─────────────────────────────────────────────────────────────────
print("\n" + "─" * 72)
print("E4  HISTORY COMPLETE — bản ghi bị từ chối khỏi current có mất không?")
print("─" * 72)

cur, hist = new_table(), tempfile.mkdtemp(prefix="hist_")
shutil.rmtree(hist)
write_deltalake(hist, tbl([]), mode="overwrite")
order = EVENTS[:]; rng.shuffle(order)
for ev in order:
    apply_batch(cur, [ev], guarded=True)      # current: có guard
    write_deltalake(hist, tbl([ev]), mode="append")  # history: append-only, nhận tất

hist_rows = DeltaTable(hist).to_pyarrow_table().num_rows
cur_rows = DeltaTable(cur).to_pyarrow_table().num_rows
rejected = len(EVENTS) - len({e[0] for e in EVENTS})
print(f"  Sự kiện đưa vào          : {len(EVENTS)}")
print(f"  Dòng ở bảng current      : {cur_rows}  (1 dòng/tài xế — đúng thiết kế)")
print(f"  Dòng ở bảng history      : {hist_rows}  (kỳ vọng {len(EVENTS)} — không mất gì)")
print(f"  Bản ghi bị guard từ chối : {rejected}  → vẫn truy vết được ở history")
history_ok = hist_rows == len(EVENTS)
print(f"\n  → Từ chối ghi đè KHÔNG đồng nghĩa mất dữ liệu: {history_ok}")
print("  → Đây là điều kiện để audit theo NĐ13 vẫn khả thi: mọi thay đổi tài xế")
print("    từng khai báo đều còn nguyên, kể cả cái đến muộn và bị từ chối.")
shutil.rmtree(cur, ignore_errors=True); shutil.rmtree(hist, ignore_errors=True)

# ── Tổng kết ───────────────────────────────────────────────────────────
print("\n" + "=" * 72)
checks = {
    f"E1 có guard: 1 trạng thái duy nhất qua {TRIALS} thứ tự":  len(g_hashes) == 1 and g_wrong == 0,
    "E1 đối chứng: không guard thì kết quả phân kỳ":            len(n_hashes) > 1,
    "E1 vét cạn 720 hoán vị: vẫn 1 trạng thái":                 len(perm_hashes) == 1,
    "E2 batch thô bị Delta từ chối (không ghi mù)":              raw_rejected,
    "E2 idempotent qua 5 lần phát lại":                         idempotent,
    "E3 8 writer đồng thời: không mất update, không hỏng":      concurrent_ok,
    "E4 history giữ đủ mọi sự kiện kể cả bị từ chối":           history_ok,
}
for k, v in checks.items():
    print(f"  [{'PASS' if v else 'FAIL'}] {k}")
assert all(checks.values()), "Reliability proof failed — xem dòng FAIL ở trên"
print("\nReliability proof complete.")
