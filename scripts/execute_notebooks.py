"""Execute all notebooks in-place with output cells preserved."""
import sys
import time
from pathlib import Path
import nbformat
from nbclient import NotebookClient

ROOT = Path(__file__).resolve().parents[1]
NB_DIR = ROOT / "notebooks"


def main():
    notebooks = sorted(p for p in NB_DIR.glob("0*.ipynb"))
    if not notebooks:
        print("No .ipynb notebooks found in notebooks/")
        return 1

    print(f"Executing {len(notebooks)} notebooks with kernel 'lakehouse_venv'...\n")
    for nb_path in notebooks:
        t0 = time.perf_counter()
        print(f"Executing {nb_path.name} ...", end="", flush=True)
        with open(nb_path, "r", encoding="utf-8") as f:
            nb = nbformat.read(f, as_version=4)

        client = NotebookClient(
            nb,
            timeout=600,
            kernel_name="lakehouse_venv",
            resources={"metadata": {"path": str(NB_DIR)}},
        )
        client.execute()

        with open(nb_path, "w", encoding="utf-8") as f:
            nbformat.write(nb, f)

        dt = time.perf_counter() - t0
        print(f" DONE ({dt:.1f}s)")

    print("\nAll 8 notebooks executed and outputs preserved successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
