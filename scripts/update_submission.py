"""Refresh submission evidence from executed notebooks and lakehouse files.

Run this after each completed checkpoint:

    .venv/Scripts/python scripts/update_submission.py   # Windows
    .venv/bin/python scripts/update_submission.py       # macOS/Linux

The generated files are intentionally plain text so they can be inspected by
the instructor without Jupyter or the lab dependencies.
"""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_DIR = ROOT / "notebooks"
LAKEHOUSE_DIR = ROOT / "_lakehouse"
SUBMISSION_DIR = ROOT / "submission"
SCREENSHOT_DIR = SUBMISSION_DIR / "screenshots"
CHECKPOINT_DIR = SUBMISSION_DIR / "checkpoints"


def output_text(notebook: dict) -> str:
    """Return readable text from stream and display outputs in cell order."""
    chunks: list[str] = []
    for cell in notebook.get("cells", []):
        for output in cell.get("outputs", []):
            text = output.get("text")
            if text:
                chunks.append("".join(text) if isinstance(text, list) else text)
            plain = output.get("data", {}).get("text/plain")
            if plain:
                chunks.append("".join(plain) if isinstance(plain, list) else plain)
    return "\n".join(chunk.rstrip() for chunk in chunks if chunk).strip() + "\n"


def refresh_tree() -> int:
    files = sorted(
        path.relative_to(ROOT).as_posix()
        for path in LAKEHOUSE_DIR.rglob("*")
        if path.is_file()
        and not any(part.startswith("pytest-tmp") for part in path.parts)
        and path.relative_to(LAKEHOUSE_DIR).parts[:2] != ("iceberg", "inspect")
    )
    (SCREENSHOT_DIR / "lakehouse_tree.txt").write_text(
        "\n".join(files) + ("\n" if files else ""), encoding="utf-8"
    )
    return len(files)


def refresh_delta_log_sample() -> Path:
    source = (
        LAKEHOUSE_DIR
        / "scratch"
        / "users_delta"
        / "_delta_log"
        / "00000000000000000000.json"
    )
    if not source.exists():
        raise FileNotFoundError(
            "CP1 Delta log is missing. Execute notebooks/01_delta_basics.ipynb first."
        )
    destination = SCREENSHOT_DIR / "delta_log_sample.json"
    destination.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    return destination


def refresh_checkpoint_logs() -> list[tuple[int, str, str]]:
    rows: list[tuple[int, str, str]] = []
    for checkpoint in range(1, 9):
        matches = sorted(NOTEBOOK_DIR.glob(f"{checkpoint:02d}_*.ipynb"))
        if not matches:
            rows.append((checkpoint, "MISSING", "—"))
            continue

        notebook_path = matches[0]
        notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
        text = output_text(notebook)
        marker = f"NB{checkpoint} complete."
        executed = any(
            cell.get("cell_type") == "code" and cell.get("execution_count") is not None
            for cell in notebook.get("cells", [])
        )

        if marker in text:
            log_path = CHECKPOINT_DIR / f"CP{checkpoint:02d}_execution.log"
            log_path.write_text(
                f"Notebook: notebooks/{notebook_path.name}\n"
                f"Completion marker: {marker}\n\n{text}",
                encoding="utf-8",
            )
            rows.append(
                (checkpoint, "PASS", f"checkpoints/{log_path.name}")
            )
        elif executed:
            rows.append((checkpoint, "INCOMPLETE", "—"))
        else:
            rows.append((checkpoint, "NOT RUN", "—"))
    return rows


def refresh_status(rows: list[tuple[int, str, str]], tree_files: int) -> None:
    table_rows = [
        f"| CP{checkpoint} | {status} | {log} |"
        for checkpoint, status, log in rows
    ]
    reflection = SUBMISSION_DIR / "REFLECTION.md"
    final_log = CHECKPOINT_DIR / "CP09_test.log"
    final_text = final_log.read_text(encoding="utf-8") if final_log.exists() else ""
    tests_pass = "24/24 unit tests: PASS" in final_text
    notebooks_pass = "8/8 notebooks: PASS" in final_text
    complete_count = sum(status == "PASS" for _, status, _ in rows)
    content = f"""# Trạng thái bài nộp Lab 18

File này được sinh bởi `scripts/update_submission.py` sau mỗi checkpoint.

| Checkpoint | Trạng thái | Bằng chứng output |
|---|---|---|
{chr(10).join(table_rows)}

## Artefact bắt buộc

- Executed notebooks đạt completion marker: **{complete_count}/8**.
- `screenshots/lakehouse_tree.txt`: **{tree_files} files**.
- `screenshots/delta_log_sample.json`: **đã tạo**.
- `REFLECTION.md` (không quá 200 từ): **{'đã có' if reflection.exists() else 'chưa tạo'}**.
- `make test` / Windows equivalent: **{'24/24 PASS' if tests_pass else 'chưa đạt'}**.
- `make run-all` / Windows equivalent: **{'8/8 PASS' if notebooks_pass else 'chưa đạt'}**.
- Log nghiệm thu CP9: **{'checkpoints/CP09_test.log' if tests_pass and notebooks_pass else 'chưa hoàn tất'}**.
"""
    (SUBMISSION_DIR / "CHECKPOINT_STATUS.md").write_text(content, encoding="utf-8")


def main() -> None:
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    tree_files = refresh_tree()
    sample = refresh_delta_log_sample()
    rows = refresh_checkpoint_logs()
    refresh_status(rows, tree_files)

    passed = [f"CP{n}" for n, status, _ in rows if status == "PASS"]
    print(f"Updated submission evidence: {', '.join(passed) or 'no completed checkpoints'}")
    print(f"Lakehouse tree: {tree_files} files")
    print(f"Delta log sample: {sample.relative_to(ROOT).as_posix()}")


if __name__ == "__main__":
    main()
