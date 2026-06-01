#!/usr/bin/env python3
"""
Split large PDFs in a directory into sub-100-page parts for processing.

For each PDF found in INPUT_DIR:
  - Creates a same-name subfolder
  - Splits the PDF into CHUNK_SIZE-page parts named <stem>_part001.pdf, etc.

Usage:
    python3 scripts/split_pdfs.py [INPUT_DIR] [CHUNK_SIZE]

Defaults:
    INPUT_DIR  = finalization/
    CHUNK_SIZE = 50
"""

import sys
from pathlib import Path

import fitz


def split_pdf(pdf_path: Path, out_dir: Path, chunk_size: int) -> list[tuple[Path, int, int]]:
    """
    Split pdf_path into chunk_size-page parts written to out_dir.
    Returns list of (part_path, start_page_0indexed, end_page_exclusive).
    """
    doc = fitz.open(str(pdf_path))
    n_pages = len(doc)
    parts = []

    starts = list(range(0, n_pages, chunk_size))
    pad = len(str(len(starts)))

    for i, start in enumerate(starts, 1):
        end = min(start + chunk_size, n_pages)
        part_path = out_dir / f"part{i:0{pad}d}.pdf"

        part_doc = fitz.open()
        part_doc.insert_pdf(doc, from_page=start, to_page=end - 1)
        part_doc.save(str(part_path))
        part_doc.close()

        parts.append((part_path, start, end))
        print(f"  {part_path.name}  (pages {start + 1}–{end} of {n_pages})")

    doc.close()
    return parts


def main():
    input_dir  = Path(sys.argv[1]) if len(sys.argv) > 1 else INPUT_DIR
    chunk_size = int(sys.argv[2]) if len(sys.argv) > 2 else CHUNK_SIZE

    if not input_dir.is_dir():
        print(f"ERROR: {input_dir} not found")
        sys.exit(1)

    pdfs = sorted(p for p in input_dir.iterdir() if p.suffix == ".pdf" and p.is_file())
    if not pdfs:
        print("No PDFs found.")
        sys.exit(0)

    print(f"Splitting {len(pdfs)} PDF(s) into {chunk_size}-page parts\n")

    for pdf_path in pdfs:
        out_dir = input_dir / pdf_path.stem
        out_dir.mkdir(exist_ok=True)
        print(f"{pdf_path.name}")
        parts = split_pdf(pdf_path, out_dir, chunk_size)
        print(f"  → {len(parts)} part(s)\n")


REPO_ROOT  = Path(__file__).resolve().parent.parent
INPUT_DIR  = REPO_ROOT / "finalization"
CHUNK_SIZE = 50

if __name__ == "__main__":
    main()
