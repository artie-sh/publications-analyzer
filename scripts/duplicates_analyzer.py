#!/usr/bin/env python3
"""
Scan a folder recursively for PDF files and report duplicates by MD5 hash.
"""

import csv
import hashlib
from collections import defaultdict
from pathlib import Path

from tqdm import tqdm

# ---------------------------------------------------------------------------
ROOT = Path("/home/artie-sh/repos/spiders/exp")
CSV_OUT = Path("/home/artie-sh/repos/spiders/duplicates_exp.csv")
CHUNK_SIZE = 1024 * 1024  # 1 MB
# ---------------------------------------------------------------------------


def md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    # --- Collect ---
    pdfs = sorted(ROOT.rglob("*.pdf"))
    print(f"Found {len(pdfs)} PDF file(s) under '{ROOT}'.\n")

    # --- Hash ---
    hashes: dict[str, list[str]] = defaultdict(list)
    for path in tqdm(pdfs, desc="Hashing", unit="file"):
        try:
            hashes[md5(path)].append(str(path))
        except OSError as e:
            print(f"\nSkipped {path}: {e}")

    # --- Full dictionary ---
    print("\nFull hash -> paths dictionary:")
    for digest, paths in hashes.items():
        print(f"  {digest}:")
        for p in paths:
            print(f"    {p}")

    # --- Duplicate report ---
    duplicates = {d: ps for d, ps in hashes.items() if len(ps) > 1}
    print(f"\nDuplicate report ({len(duplicates)} group(s)):")
    if duplicates:
        for digest, paths in duplicates.items():
            print(f"  {digest}:")
            for p in paths:
                print(f"    {p}")
    else:
        print("  No duplicates found.")

    # --- CSV output ---
    with open(CSV_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["hash", "path"])
        for digest, paths in hashes.items():
            for p in paths:
                writer.writerow([digest, p])
    print(f"\nCSV written to '{CSV_OUT}'.")

    # --- Summary ---
    total = len(pdfs)
    distinct_hashes = len(hashes)
    no_duplicate = sum(1 for ps in hashes.values() if len(ps) == 1)
    has_duplicate = total - no_duplicate
    print(f"\nSummary: {total} PDFs scanned | {distinct_hashes} distinct hashes | "
          f"{no_duplicate} files with no duplicate | {has_duplicate} files with at least one duplicate")


if __name__ == "__main__":
    main()
