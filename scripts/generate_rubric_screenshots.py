import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAKEHOUSE = ROOT / "_lakehouse"
SCREENSHOTS = ROOT / "submission" / "screenshots"

SCREENSHOTS.mkdir(parents=True, exist_ok=True)

# 1. lakehouse_tree.txt
tree_lines = []
for root, dirs, files in os.walk(LAKEHOUSE):
    for f in sorted(files):
        rel_root = os.path.relpath(root, ROOT)
        tree_lines.append(f"{rel_root}/{f}".replace("\\", "/"))

with open(SCREENSHOTS / "lakehouse_tree.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(sorted(tree_lines)) + "\n")

# 2. delta_log_sample.json
src_json = LAKEHOUSE / "scratch" / "users_delta" / "_delta_log" / "00000000000000000000.json"
if src_json.exists():
    with open(src_json, "r", encoding="utf-8") as f_in, open(SCREENSHOTS / "delta_log_sample.json", "w", encoding="utf-8") as f_out:
        f_out.write(f_in.read())

print("Successfully generated:")
print(" -", SCREENSHOTS / "lakehouse_tree.txt")
print(" -", SCREENSHOTS / "delta_log_sample.json")
