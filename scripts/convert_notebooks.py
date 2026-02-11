"""
Convert Databricks .py notebooks to Jupyter .ipynb format.

Handles:
- # MAGIC %md  →  markdown cells
- # MAGIC %pip  →  code cells with !pip
- # COMMAND ----------  →  cell separators
- # Databricks notebook source  →  skipped
- Regular Python code  →  code cells
"""

import json
import re
import sys
from pathlib import Path


def parse_databricks_py(filepath: Path) -> list[dict]:
    """Parse a Databricks .py notebook into a list of cell dicts."""
    text = filepath.read_text(encoding="utf-8")
    lines = text.split("\n")

    # Split into raw blocks by COMMAND separator
    blocks: list[list[str]] = []
    current_block: list[str] = []

    for line in lines:
        if line.strip() == "# Databricks notebook source":
            continue
        if line.strip() == "# COMMAND ----------":
            if current_block:
                blocks.append(current_block)
                current_block = []
            continue
        current_block.append(line)

    if current_block:
        blocks.append(current_block)

    # Classify each block
    cells = []
    for block in blocks:
        # Strip leading/trailing blank lines
        while block and block[0].strip() == "":
            block.pop(0)
        while block and block[-1].strip() == "":
            block.pop()

        if not block:
            continue

        # Check if it's a markdown block
        is_markdown = all(
            l.startswith("# MAGIC %md") or l.startswith("# MAGIC ") or l.strip() == ""
            for l in block
        )

        # Check if it's a %pip block
        is_pip = any(l.strip().startswith("# MAGIC %pip") for l in block)

        if is_pip:
            # Convert %pip to !pip
            sources = []
            for l in block:
                if l.strip().startswith("# MAGIC %pip"):
                    cmd = l.strip().replace("# MAGIC %pip", "!pip", 1)
                    sources.append(cmd)
            cells.append({
                "cell_type": "code",
                "source": sources,
                "metadata": {},
                "execution_count": None,
                "outputs": [],
            })
        elif is_markdown:
            sources = []
            for l in block:
                stripped = l.strip()
                if stripped.startswith("# MAGIC %md"):
                    # First line — remove the %md prefix
                    remainder = stripped[len("# MAGIC %md"):].strip()
                    if remainder:
                        sources.append(remainder)
                elif stripped.startswith("# MAGIC"):
                    # Subsequent lines — remove # MAGIC prefix
                    remainder = stripped[len("# MAGIC"):] if len(stripped) > len("# MAGIC") else ""
                    # Preserve leading space after # MAGIC
                    if l.strip().startswith("# MAGIC "):
                        remainder = l.strip()[len("# MAGIC "):]
                    sources.append(remainder)
                elif stripped == "":
                    sources.append("")
            cells.append({
                "cell_type": "markdown",
                "source": sources,
                "metadata": {},
            })
        else:
            # Code cell
            sources = []
            for l in block:
                sources.append(l)
            cells.append({
                "cell_type": "code",
                "source": sources,
                "metadata": {},
                "execution_count": None,
                "outputs": [],
            })

    return cells


def cells_to_ipynb(cells: list[dict]) -> dict:
    """Convert cells to a complete .ipynb notebook structure."""
    # Add newlines to all but last source line
    for cell in cells:
        source = cell["source"]
        if source:
            cell["source"] = [
                line + "\n" if i < len(source) - 1 else line
                for i, line in enumerate(source)
            ]

    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
                "version": "3.11.0",
                "mimetype": "text/x-python",
                "file_extension": ".py",
                "codemirror_mode": {"name": "ipython", "version": 3},
                "pygments_lexer": "ipython3",
                "nbconvert_exporter": "python",
            },
        },
        "cells": cells,
    }


def convert_notebook(py_path: Path, ipynb_path: Path) -> None:
    """Convert a single Databricks .py notebook to .ipynb."""
    cells = parse_databricks_py(py_path)
    notebook = cells_to_ipynb(cells)

    with open(ipynb_path, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=1, ensure_ascii=False)

    print(f"  ✓ {py_path.name} → {ipynb_path.name}  ({len(cells)} cells)")


def main():
    notebooks_dir = Path("/Users/proxim/projects/CryptoPulse/notebooks")

    py_files = sorted(notebooks_dir.glob("*.py"))
    if not py_files:
        print("No .py notebooks found!")
        sys.exit(1)

    print(f"═══ Converting {len(py_files)} Databricks notebooks to Jupyter .ipynb ═══\n")

    for py_file in py_files:
        ipynb_file = notebooks_dir / (py_file.stem + ".ipynb")
        convert_notebook(py_file, ipynb_file)

    print(f"\n✓ All {len(py_files)} notebooks converted successfully!")

    # List the output
    ipynb_files = sorted(notebooks_dir.glob("*.ipynb"))
    print(f"\nGenerated files:")
    for f in ipynb_files:
        size_kb = f.stat().st_size / 1024
        print(f"  {f.name}  ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
