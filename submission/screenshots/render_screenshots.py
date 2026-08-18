"""Render two PNG screenshots to satisfy rubric §Submission §2.

  01_lakehouse_tree.png              — `tree _lakehouse/`
  02_delta_log_first_commit.png      — contents of one `_delta_log/*.json`

Pillow's default font is monospaced; output is light-on-dark to read like a
terminal screenshot. Each image gets a header banner naming it.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from lakehouse import ROOT as LH  # noqa: E402

OUT = ROOT / "submission" / "screenshots"

# ── Font ─────────────────────────────────────────────────────────────────
FONT_PATH_CANDIDATES = [
    r"C:\Windows\Fonts\consola.ttf",      # Consolas (Win)
    r"C:\Windows\Fonts\cour.ttf",         # Courier New
    "/System/Library/Fonts/Menlo.ttc",    # macOS
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
]
FONT: ImageFont.FreeTypeFont | ImageFont.ImageFont = ImageFont.load_default()
for fp in FONT_PATH_CANDIDATES:
    if Path(fp).exists():
        try:
            FONT = ImageFont.truetype(fp, 18)
            break
        except OSError:
            continue
TITLE_FONT: ImageFont.FreeTypeFont | ImageFont.ImageFont = FONT
for fp in FONT_PATH_CANDIDATES:
    if Path(fp).exists():
        try:
            TITLE_FONT = ImageFont.truetype(fp, 22)
            break
        except OSError:
            continue

BG       = (24, 24, 30)      # near-black background
FG       = (220, 220, 220)   # light text
BANNER   = (86, 156, 214)    # blue title
COMMENT  = (106, 153, 85)    # green for json keys
ACCENT   = (206, 145, 120)   # warm for json strings
NUM      = (181, 206, 168)   # numbers
PADDING  = 28
LINE_GAP = 6


def measure(lines: list[str], font) -> tuple[int, int]:
    draw = ImageDraw.Draw(Image.new("RGB", (1, 1)))
    widths = [draw.textbbox((0, 0), ln, font=font)[2] for ln in lines]
    line_h = draw.textbbox((0, 0), "Ag", font=font)[3] + LINE_GAP
    return max(widths), line_h * len(lines)


def render_png(path: Path, title: str, body_lines: list[str], color_for) -> None:
    title_lines = [title, ""]
    body_w, body_h = measure(body_lines, FONT)
    title_w, title_h = measure(title_lines, TITLE_FONT)
    width = max(title_w, body_w) + PADDING * 2
    height = title_h + body_h + PADDING * 2

    img = Image.new("RGB", (width, height), BG)
    draw = ImageDraw.Draw(img)

    y = PADDING
    for ln in title_lines:
        color = BANNER if not ln.startswith(" ") else FG
        draw.text((PADDING, y), ln, fill=color, font=TITLE_FONT)
        y += draw.textbbox((0, 0), "Ag", font=TITLE_FONT)[3] + LINE_GAP

    for ln in body_lines:
        draw.text((PADDING, y), ln, fill=color_for(ln), font=FONT)
        y += draw.textbbox((0, 0), "Ag", font=FONT)[3] + LINE_GAP

    img.save(path, format="PNG", optimize=True)
    print(f"  wrote  {path.relative_to(ROOT)}  ({path.stat().st_size:,} B, {width}x{height})")


# ── 01 — tree _lakehouse/ ─────────────────────────────────────────────────

def build_tree_lines() -> list[str]:
    """Mirror of submission/screenshots/make_evidence.py — recursive ASCII tree."""
    def walk(p: Path, prefix: str = "", depth: int = 0, max_depth: int = 2,
          max_files_per_dir: int = 4) -> list[str]:
        out: list[str] = []
        entries = sorted(p.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
        shown = 0
        for i, e in enumerate(entries):
            last = i == len(entries) - 1
            c = "└── " if last else "├── "
            if e.is_dir():
                n = sum(1 for _ in e.rglob("*") if _.is_file())
                if depth >= max_depth:
                    out.append(f"{prefix}{c}{e.name}/  ({n} files, truncated)")
                else:
                    out.append(f"{prefix}{c}{e.name}/  ({n} files)")
                    ext = "    " if last else "│   "
                    out.extend(walk(e, prefix + ext, depth + 1, max_depth, max_files_per_dir))
            else:
                if shown >= max_files_per_dir:
                    continue
                out.append(f"{prefix}{c}{e.name}  ({e.stat().st_size:,} B)")
                shown += 1
        if shown == max_files_per_dir and any(True for _ in entries if _.is_file()) > max_files_per_dir:
            out.append(f"{prefix}└── … (showing first {shown}, total {sum(1 for e in entries if e.is_file())} files)")
        return out

    total = sum(1 for _ in LH.rglob("*") if _.is_file())
    header = f"_lakehouse/  ({total} files)"
    return [header, *walk(LH)]


def tree_color(_: str) -> tuple[int, int, int]:
    return FG


# ── 02 — _delta_log/*.json ───────────────────────────────────────────────

def build_delta_log_lines() -> list[str]:
    log = LH / "scratch" / "users_delta" / "_delta_log"
    files = sorted(log.glob("*.json"))
    if not files:
        return ["(no _delta_log/*.json found — run NB1 first)"]
    first = files[0]
    pretty: list[str] = []
    pretty.append(f"# _delta_log/{first.name}")
    pretty.append(f"# bytes={first.stat().st_size:,}")
    pretty.append("")
    for raw in first.read_text(encoding="utf-8").splitlines():
        obj = json.loads(raw)
        pretty.append(json.dumps(obj, indent=2))
        pretty.append("")
    return pretty


def delta_color(ln: str) -> tuple[int, int, int]:
    s = ln.lstrip()
    if s.startswith("#"):
        return COMMENT
    if s.endswith(":") and '"' in s:
        return COMMENT
    if s.startswith('"') and s.endswith(','):
        return ACCENT
    if s.startswith('"') and s.endswith('"'):
        return ACCENT
    if any(c.isdigit() for c in s) and not any(c in s for c in "{}[]"):
        return NUM
    return FG


# ── main ─────────────────────────────────────────────────────────────────

def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)

    tree_lines = build_tree_lines()
    render_png(
        OUT / "01_lakehouse_tree.png",
        title="$ tree _lakehouse/   (lightweight path)",
        body_lines=tree_lines,
        color_for=tree_color,
    )

    json_lines = build_delta_log_lines()
    render_png(
        OUT / "02_delta_log_first_commit.png",
        title="$ cat _delta_log/00000000000000000000.json",
        body_lines=json_lines,
        color_for=delta_color,
    )


if __name__ == "__main__":
    main()