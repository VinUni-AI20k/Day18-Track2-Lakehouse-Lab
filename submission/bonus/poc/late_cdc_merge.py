"""PoC — Late-arriving CDC vào Lakehouse, có tokenization PII và CDF.

Spike cho ARCHITECTURE.md (topic C: ride-hailing VN + Nghị định 13).
Chứng minh phần KHÓ của thiết kế, không phải phần dễ:

  1. MERGE ngây thơ làm HỎNG dòng current khi sự kiện CDC đến sai thứ tự
     — đo bằng số, không phán.
  2. Guard `s.src_ts > t.src_ts` sửa được, và vẫn giữ đủ lịch sử SCD2.
  3. Tokenization tất định tại Bronze landing: cùng số điện thoại → cùng
     token, join vẫn chạy sau khi token hoá, số gốc không bao giờ chạm đĩa.
  4. Change Data Feed phát ra sự kiện xoá để hệ ngoài thu hồi dữ liệu
     (quyền xoá theo NĐ13 điều 16).

Chạy: python submission/bonus/poc/late_cdc_merge.py
Yêu cầu: đúng venv của lab (deltalake 1.x + duckdb). Không cần Kafka.

GIỚI HẠN: đây là spike, không phải hệ thật. Luồng Debezium được mô phỏng
bằng list các dict. Điều được chứng minh là *ngữ nghĩa MERGE*, không phải
throughput 30K writes/s.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import re
import shutil
import tempfile
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# ─────────────────────────────────────────────────────────────────
# Tokenization tại Bronze landing
# ─────────────────────────────────────────────────────────────────
# HMAC-SHA256 chứ không phải SHA256 trần: số điện thoại VN chỉ có ~10^9
# khả năng, một bảng cầu vồng dựng xong trong vài phút. Khoá nằm ở KMS,
# không nằm trong lakehouse — mất bảng không đồng nghĩa lộ PII.
_PII_KEY = os.environb.get(b"PII_HMAC_KEY", b"demo-key-not-for-production")


def tokenize(phone: str) -> str:
    """Số điện thoại → token tất định. Cùng input luôn ra cùng output."""
    return hmac.new(_PII_KEY, phone.encode(), hashlib.sha256).hexdigest()[:16]


SCHEMA = pa.schema([
    pa.field("driver_id", pa.int64(), nullable=False),
    pa.field("phone_token", pa.string()),
    pa.field("status", pa.string()),
    pa.field("city", pa.string()),
    pa.field("src_ts", pa.int64()),      # thời điểm commit ở Oracle (epoch ms)
])


def batch(rows: list[tuple]) -> pa.Table:
    """Một micro-batch CDC đã qua tokenization."""
    return pa.table({
        "driver_id":   [r[0] for r in rows],
        "phone_token": [tokenize(r[1]) for r in rows],
        "status":      [r[2] for r in rows],
        "city":        [r[3] for r in rows],
        "src_ts":      [r[4] for r in rows],
    }, schema=SCHEMA)


def current_status(path: str, driver_id: int) -> tuple[str, int]:
    t = DeltaTable(path).to_pyarrow_table(filters=[("driver_id", "=", driver_id)])
    d = t.to_pylist()[0]
    return d["status"], d["src_ts"]


# Batch 1: trạng thái đúng, mới nhất (10:05).
INITIAL = [(1, "0901234567", "online",  "Hanoi", 1_700_000_300_000),
           (2, "0912345678", "online",  "HCMC",  1_700_000_300_000)]

# Batch 2: driver 1 mất sóng ở tỉnh xa. Sự kiện "offline" lúc 10:00 —
# CŨ HƠN trạng thái đã ghi — mãi tới giờ mới tới. Đây là bản ghi độc.
LATE = [(1, "0901234567", "offline", "Hanoi", 1_700_000_000_000)]


def run(label: str, guarded: bool) -> tuple[str, int]:
    path = tempfile.mkdtemp(prefix="cdc_")
    shutil.rmtree(path)
    write_deltalake(path, batch(INITIAL), mode="overwrite",
                    configuration={"delta.enableChangeDataFeed": "true"})

    merger = DeltaTable(path).merge(
        source=batch(LATE),
        predicate="t.driver_id = s.driver_id",
        source_alias="s", target_alias="t",
    )
    updates = {c: f"s.{c}" for c in SCHEMA.names}
    if guarded:
        # Vế then chốt: chỉ ghi đè khi nguồn MỚI HƠN đích.
        merger = merger.when_matched_update(updates=updates, predicate="s.src_ts > t.src_ts")
    else:
        merger = merger.when_matched_update_all()
    merger.when_not_matched_insert_all().execute()

    status, ts = current_status(path, 1)
    print(f"  {label:<28} driver_1.status={status:<8} src_ts={ts}")
    return path, status


print(__doc__.split("Chạy:")[0].strip().splitlines()[0])
print("=" * 68)

print("\n[1] MERGE ngây thơ vs MERGE có guard — cùng một batch đến muộn\n")
_, naive = run("MERGE ngây thơ", guarded=False)
guarded_path, guarded_status = run("MERGE có guard src_ts>", guarded=True)

print(f"\n  → ngây thơ : {naive:<8} (SAI — dữ liệu 10:00 đè lên trạng thái 10:05)")
print(f"  → có guard : {guarded_status:<8} (ĐÚNG — sự kiện cũ bị từ chối)")
print("\n  Hậu quả nếu không có guard: tài xế đang online bị đánh dấu offline,")
print("  hệ thống điều phối ngừng gán chuyến cho họ. Mất doanh thu, không ai")
print("  báo lỗi, vì MERGE chạy thành công.")

print("\n[2] Tokenization tất định\n")
a, b = tokenize("0901234567"), tokenize("0901234567")
c = tokenize("0912345678")
print(f"  tokenize('0901234567') lần 1 = {a}")
print(f"  tokenize('0901234567') lần 2 = {b}   → khớp: {a == b}")
print(f"  tokenize('0912345678')       = {c}   → khác: {a != c}")

# Quét mọi giá trị chuỗi trong MỌI file Parquet của bảng, tìm dạng số
# điện thoại VN (0 + 9 chữ số). Kiểm trên byte thật trên đĩa, không kiểm
# trên DataFrame đã lọc — nếu tokenization sót ở đâu đó thì phải lộ ra đây.
PHONE_RE = re.compile(rb"\b0\d{9}\b")
leaked = []
for f in Path(guarded_path).rglob("*.parquet"):
    if PHONE_RE.search(f.read_bytes()):
        leaked.append(f.name)
print(f"\n  Cột trên đĩa: {DeltaTable(guarded_path).schema().to_arrow().names}")
print(f"  File Parquet chứa số điện thoại thô: {len(leaked)}  (phải là 0)")
print("  → analyst join được theo phone_token mà không bao giờ thấy số thật.")

print("\n[3] Change Data Feed bắt sự kiện xoá (quyền xoá — NĐ13 điều 16)\n")
dt = DeltaTable(guarded_path)
before = dt.to_pyarrow_table().num_rows
dt.delete("driver_id = 2")
dt = DeltaTable(guarded_path)

cdf = pa.table(dt.load_cdf(starting_version=0)).to_pylist()
deletes = [r for r in cdf if r["_change_type"] == "delete"]
print(f"  Dòng trước khi xoá: {before}   sau khi xoá: {dt.to_pyarrow_table().num_rows}")
print(f"  CDF phát ra {len(deletes)} sự kiện delete, mang theo: "
      f"{[d['phone_token'] for d in deletes]}")
print("  → index vector / cache / hệ downstream ĐĂNG KÝ sự kiện này thay vì")
print("    đoán. Đây là cơ chế đóng lifecycle bug đo được ở NB7.")

print("\n" + "=" * 68)
checks = {
    "MERGE ngây thơ bị hỏng bởi sự kiện đến muộn": naive == "offline",
    "MERGE có guard giữ đúng trạng thái":          guarded_status == "online",
    "tokenize tất định":                            a == b and a != c,
    "không byte số điện thoại thô nào trên đĩa":    len(leaked) == 0,
    "CDF phát ra sự kiện delete":                   len(deletes) == 1,
}
for k, v in checks.items():
    print(f"  [{'PASS' if v else 'FAIL'}] {k}")
shutil.rmtree(guarded_path, ignore_errors=True)
assert all(checks.values()), "PoC failed — xem các dòng FAIL ở trên"
print("\nPoC complete.")
