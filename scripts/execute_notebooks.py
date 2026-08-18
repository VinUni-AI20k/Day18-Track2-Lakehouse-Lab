import sys
from pathlib import Path

if sys.platform == "win32":
    try:
        if sys.stdout and hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if sys.stderr and hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import nbformat
from nbclient import NotebookClient

ROOT = Path(__file__).resolve().parents[1]
NB_DIR = ROOT / "notebooks"

def main():
    notebooks = sorted(NB_DIR.glob("[0-9]*.ipynb"))
    if not notebooks:
        print("No .ipynb notebooks found.")
        return 1

    for nb_path in notebooks:
        print(f"Executing {nb_path.name}...")
        nb = nbformat.read(nb_path, as_version=4)
        client = NotebookClient(
            nb,
            timeout=600,
            kernel_name="day18-lakehouse",
            resources={"metadata": {"path": str(NB_DIR)}}
        )
        client.execute()
        nbformat.write(nb, nb_path)
        print(f"  ✓ {nb_path.name} executed and saved successfully.")

    print("\nAll notebooks executed with output cells saved!")
    return 0

if __name__ == "__main__":
    sys.exit(main())
