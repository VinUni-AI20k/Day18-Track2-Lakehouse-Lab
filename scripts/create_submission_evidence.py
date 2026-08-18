import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAKEHOUSE = ROOT / "_lakehouse"
SUBMISSION = ROOT / "submission"
SCREENSHOTS = SUBMISSION / "screenshots"

SCREENSHOTS.mkdir(parents=True, exist_ok=True)

def generate_tree(dir_path: Path, prefix: str = "", max_depth: int = 4, current_depth: int = 0) -> list[str]:
    lines = []
    if current_depth >= max_depth:
        return lines
    
    entries = sorted(list(dir_path.iterdir()), key=lambda x: (not x.is_dir(), x.name))
    # Filter out hidden or massive temp dirs if any
    entries = [e for e in entries if not e.name.startswith(".")]
    
    for i, entry in enumerate(entries):
        is_last = (i == len(entries) - 1)
        connector = "└── " if is_last else "├── "
        lines.append(f"{prefix}{connector}{entry.name}")
        
        if entry.is_dir():
            sub_entries = [e for e in entry.iterdir() if not e.name.startswith(".")]
            if len(sub_entries) > 8 and not any(e.is_dir() for e in sub_entries):
                # File-heavy leaf directory: summarize
                sub_prefix = prefix + ("    " if is_last else "│   ")
                for s in sorted(sub_entries, key=lambda x: x.name)[:3]:
                    lines.append(f"{sub_prefix}├── {s.name}")
                lines.append(f"{sub_prefix}└── ... [{len(sub_entries)} files total]")
            else:
                sub_prefix = prefix + ("    " if is_last else "│   ")
                lines.extend(generate_tree(entry, sub_prefix, max_depth, current_depth + 1))
    return lines

# 1. Generate tree output
tree_lines = ["_lakehouse/"] + generate_tree(LAKEHOUSE)
tree_str = "\n".join(tree_lines)

# 2. Get content of one _delta_log/*.json (e.g. from scratch/users_delta or bronze/llm_calls_raw)
sample_log_path = LAKEHOUSE / "scratch" / "users_delta" / "_delta_log" / "00000000000000000000.json"
if not sample_log_path.exists():
    sample_logs = list(LAKEHOUSE.glob("**/_delta_log/*.json"))
    sample_log_path = sample_logs[0] if sample_logs else None

log_content = ""
if sample_log_path and sample_log_path.exists():
    with open(sample_log_path, "r", encoding="utf-8") as f:
        log_content = f.read()

evidence = f"""======================================================================
1. LAKEHOUSE DIRECTORY STRUCTURE (tree _lakehouse/)
======================================================================
{tree_str}

======================================================================
2. SAMPLE TRANSACTION LOG (_delta_log JSON)
Path: {sample_log_path.relative_to(ROOT) if sample_log_path else 'None'}
======================================================================
{log_content}
"""

evidence_file = SCREENSHOTS / "lakehouse_evidence.txt"
with open(evidence_file, "w", encoding="utf-8") as f:
    f.write(evidence)

print(f"Generated submission evidence at {evidence_file}")
