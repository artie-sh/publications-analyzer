#!/usr/bin/env python3
"""
Quick pre-scan of all PDFs to determine which need force_full_page_ocr.

Usage:
    python3 scripts/scan_garbled.py <input_dir> [threshold]

Example:
    python3 scripts/scan_garbled.py /home/ubuntu/spiders/pardosa 0.3

Outputs a summary and writes two files next to the script:
    scan_native.txt   — PDFs that can be processed without force OCR
    scan_ocr.txt      — PDFs that need force_full_page_ocr=True
"""

import sys
from pathlib import Path

import fitz

MIN_ALPHA_RATIO = 0.4

def looks_garbled(text: str) -> bool:
    non_ws = [c for c in text if not c.isspace()]
    if not non_ws:
        return False
    non_ascii_alpha = sum(1 for c in non_ws if c.isalpha() and ord(c) > 127)
    if non_ascii_alpha / len(non_ws) >= 0.15:
        return False
    alpha_count = sum(1 for c in non_ws if c.isalpha())
    if (alpha_count / len(non_ws)) < MIN_ALPHA_RATIO:
        return True
    digits_in_word = sum(
        1 for i in range(1, len(text) - 1)
        if text[i].isdigit() and text[i - 1].isalpha() and text[i + 1].isalpha()
    )
    return digits_in_word / len(non_ws) > 0.005


def garbled_ratio(pdf_path: Path) -> tuple[float, int]:
    doc = fitz.open(str(pdf_path))
    n_pages = len(doc)
    garbled, total = 0, 0
    for page in doc:
        text = page.get_text()
        nws = sum(1 for c in text if not c.isspace())
        if nws >= 50:
            total += 1
            if looks_garbled(text):
                garbled += 1
    doc.close()
    return (garbled / total if total else 0.0), n_pages


def main():
    input_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
    threshold = float(sys.argv[2]) if len(sys.argv) > 2 else 0.3

    pdf_files = sorted(
        p for p in input_dir.rglob("*.pdf") if not p.is_symlink()
    )

    if not pdf_files:
        print(f"No PDFs found under {input_dir}")
        sys.exit(1)

    print(f"Scanning {len(pdf_files)} PDFs under {input_dir}...")
    print(f"Force-OCR threshold: garbled_ratio > {threshold}\n")

    native = []
    needs_ocr = []

    for i, pdf_path in enumerate(pdf_files, 1):
        try:
            ratio, n_pages = garbled_ratio(pdf_path)
            entry = (pdf_path, ratio, n_pages)
            if ratio > threshold:
                needs_ocr.append(entry)
            else:
                native.append(entry)
            status = "OCR" if ratio > threshold else "ok "
            print(f"  [{i:3}/{len(pdf_files)}] {status}  {ratio:.2f}  {pdf_path.parent.name}/{pdf_path.name}")
        except Exception as e:
            print(f"  [{i:3}/{len(pdf_files)}] ERR  {pdf_path.name}: {e}")

    # Write output files
    out_dir = Path(__file__).parent
    native_file = out_dir / "scan_native.txt"
    ocr_file    = out_dir / "scan_ocr.txt"

    native_file.write_text(
        "\n".join(f"{r:.3f}\t{n}\t{p}" for p, r, n in native),
        encoding="utf-8"
    )
    ocr_file.write_text(
        "\n".join(f"{r:.3f}\t{n}\t{p}" for p, r, n in needs_ocr),
        encoding="utf-8"
    )

    total_pages_native  = sum(n for _, _, n in native)
    total_pages_ocr     = sum(n for _, _, n in needs_ocr)

    print(f"\n{'═' * 60}")
    print(f"Native (no force OCR) : {len(native):4} PDFs  {total_pages_native:6} pages")
    print(f"Needs force OCR       : {len(needs_ocr):4} PDFs  {total_pages_ocr:6} pages")
    print(f"{'═' * 60}")
    print(f"Results written to:")
    print(f"  {native_file}")
    print(f"  {ocr_file}")


if __name__ == "__main__":
    main()
