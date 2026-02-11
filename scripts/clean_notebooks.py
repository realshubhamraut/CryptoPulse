"""
Fix notebook cell issues:
1. Convert Cell 0 from code (with # MAGIC %md) to proper markdown
2. Find any remaining code cells with only # MAGIC %md content and convert
3. Remove emojis, downgrade headings
4. Clean print-statement emoji/symbol decorators in code cells
"""

import json
import re
from pathlib import Path


EMOJI_PATTERN = re.compile(
    "["
    "\U0001F300-\U0001F9FF"
    "\U0001FA00-\U0001FAFF"
    "\U00002600-\U000027BF"
    "\U0000FE0F"
    "\U0000200D"
    "\U00002B50"
    "\U00002728"
    "\U0000203C"
    "\U00002049"
    "\U000025AA-\U000025FE"
    "\U000025B6"
    "\U000025C0"
    "\U00003030"
    "\U00003297"
    "\U00003299"
    "]+",
    flags=re.UNICODE,
)

# Box-drawing and decorative unicode used in print() calls
DECORATIVE_CHARS = str.maketrans("", "", "═━─✓✗⚠⚡▶◀●○■□▪▫★☆♦♠♣♥")


def strip_emojis(text: str) -> str:
    text = EMOJI_PATTERN.sub("", text)
    text = text.translate(DECORATIVE_CHARS)
    text = re.sub(r"  +", " ", text)
    return text


def is_magic_md_block(source_lines: list[str]) -> bool:
    """Check if a code cell is actually a Databricks markdown cell."""
    text = "".join(source_lines).strip()
    lines = text.split("\n")
    return all(
        l.strip().startswith("# MAGIC") or l.strip() == ""
        for l in lines
        if l.strip()
    )


def magic_to_markdown(source_lines: list[str]) -> list[str]:
    """Convert # MAGIC %md lines to clean markdown."""
    text = "".join(source_lines)
    lines = text.split("\n")
    md_lines = []
    for l in lines:
        stripped = l.strip()
        if stripped.startswith("# MAGIC %md"):
            remainder = stripped[len("# MAGIC %md"):].strip()
            if remainder:
                md_lines.append(remainder)
        elif stripped.startswith("# MAGIC "):
            md_lines.append(stripped[len("# MAGIC "):])
        elif stripped.startswith("# MAGIC"):
            md_lines.append("")
        elif stripped == "":
            md_lines.append("")
    return md_lines


def downgrade_headings(lines: list[str]) -> list[str]:
    """Replace # and ## with ###."""
    result = []
    for line in lines:
        s = line.lstrip()
        if s.startswith("# ") and not s.startswith("## "):
            line = "### " + s[2:]
        elif s.startswith("## ") and not s.startswith("### "):
            line = "### " + s[3:]
        result.append(line)
    return result


def clean_code_prints(lines: list[str]) -> list[str]:
    """Replace decorative print() patterns with clean ones."""
    result = []
    for line in lines:
        # Replace print("=" * N) and similar decorative dividers
        line = re.sub(r'print\(f?"\\n"\)', 'print()', line)
        result.append(line)
    return result


def fix_notebook(filepath: Path) -> int:
    with open(filepath, "r", encoding="utf-8") as f:
        nb = json.load(f)

    changes = 0
    new_cells = []

    for cell in nb.get("cells", []):
        source = cell.get("source", [])
        if not source:
            continue

        # Check if code cell is actually a MAGIC markdown cell
        if cell["cell_type"] == "code" and is_magic_md_block(source):
            md_lines = magic_to_markdown(source)
            md_lines = [strip_emojis(l) for l in md_lines]
            md_lines = downgrade_headings(md_lines)

            # Remove leading/trailing empty lines
            while md_lines and md_lines[0].strip() == "":
                md_lines.pop(0)
            while md_lines and md_lines[-1].strip() == "":
                md_lines.pop()

            if not md_lines:
                continue

            new_source = []
            for i, line in enumerate(md_lines):
                if i < len(md_lines) - 1:
                    new_source.append(line + "\n")
                else:
                    new_source.append(line)

            new_cells.append({
                "cell_type": "markdown",
                "source": new_source,
                "metadata": {},
            })
            changes += 1
        elif cell["cell_type"] == "markdown":
            text = "".join(source)
            text = strip_emojis(text)
            lines = text.split("\n")
            lines = downgrade_headings(lines)

            while lines and lines[0].strip() == "":
                lines.pop(0)
            while lines and lines[-1].strip() == "":
                lines.pop()

            new_source = []
            for i, line in enumerate(lines):
                if i < len(lines) - 1:
                    new_source.append(line + "\n")
                else:
                    new_source.append(line)

            cell["source"] = new_source
            new_cells.append(cell)
            changes += 1
        elif cell["cell_type"] == "code":
            text = "".join(source)
            text = strip_emojis(text)
            lines = text.split("\n")

            while lines and lines[-1].strip() == "":
                lines.pop()

            new_source = []
            for i, line in enumerate(lines):
                if i < len(lines) - 1:
                    new_source.append(line + "\n")
                else:
                    new_source.append(line)

            cell["source"] = new_source
            new_cells.append(cell)
            changes += 1
        else:
            new_cells.append(cell)

    nb["cells"] = new_cells

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1, ensure_ascii=False)

    return changes


def main():
    notebooks_dir = Path("/Users/proxim/projects/CryptoPulse/notebooks")
    ipynb_files = sorted(notebooks_dir.glob("*.ipynb"))

    print(f"Fixing {len(ipynb_files)} notebooks...\n")

    for nb_path in ipynb_files:
        changes = fix_notebook(nb_path)
        size_kb = nb_path.stat().st_size / 1024
        print(f"  {nb_path.name:<35} {changes:>3} cells fixed  ({size_kb:.1f} KB)")

    print(f"\nDone.")


if __name__ == "__main__":
    main()
