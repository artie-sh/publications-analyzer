#!/usr/bin/env python3
"""
Mirror the symlink structure from an input PDF library into a processed output tree.

After downloading processed results from S3 (which doesn't preserve symlinks),
run this script locally to recreate the symlinks in the output tree.

For every symlink in INPUT_DIR that points to a real PDF, the script creates a
corresponding symlink in OUTPUT_DIR pointing to the canonical output folder.

Usage:
    python3 scripts/mirror_symlinks.py <input_dir> <output_dir>

Example:
    python3 scripts/mirror_symlinks.py pardosa/ pardosa_processed/
"""

import os
import sys
from pathlib import Path


def out_dir_for(pdf_path: Path, input_dir: Path, output_base: Path) -> Path:
    """Compute the output folder for a PDF, mirroring the input structure."""
    if pdf_path.parent == input_dir:
        return output_base / pdf_path.stem
    return output_base / pdf_path.parent.name / pdf_path.stem


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    input_dir   = Path(sys.argv[1]).resolve()
    output_base = Path(sys.argv[2]).resolve()

    if not input_dir.is_dir():
        print(f"ERROR: input directory not found: {input_dir}")
        sys.exit(1)

    symlinks = [p for p in input_dir.rglob("*.pdf") if p.is_symlink()]

    if not symlinks:
        print(f"No symlinks found under {input_dir}")
        sys.exit(0)

    print(f"Found {len(symlinks)} symlink(s) under {input_dir}")
    print(f"Output base: {output_base}\n")

    ok = skip = fail = 0

    for link_path in sorted(symlinks):
        try:
            canonical_pdf = link_path.resolve()
            canonical_out = out_dir_for(canonical_pdf, input_dir, output_base)

            if not canonical_out.exists():
                print(f"  SKIP  {link_path.parent.name}/{link_path.name}"
                      f" — canonical output not found: {canonical_out.name}")
                skip += 1
                continue

            link_out = out_dir_for(link_path, input_dir, output_base)

            if link_out.exists() or link_out.is_symlink():
                print(f"  SKIP  {link_out.parent.name}/{link_out.name} — already exists")
                skip += 1
                continue

            link_out.parent.mkdir(parents=True, exist_ok=True)
            rel_target = os.path.relpath(canonical_out, link_out.parent)
            link_out.symlink_to(rel_target)
            print(f"  LINK  {link_out.parent.name}/{link_out.name} → {rel_target}")
            ok += 1

        except Exception as exc:
            print(f"  FAIL  {link_path.name}: {exc}")
            fail += 1

    print(f"\nDone:  {ok} created,  {skip} skipped,  {fail} failed")


if __name__ == "__main__":
    main()
