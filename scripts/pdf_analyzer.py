#!/usr/bin/env python3
"""
PDF extraction script for arachnology papers.

Input:  downloads/<Species_name>/Author_Year_Paper_Title.pdf
Output: output/<Species_name>/Author_Year_Paper_Title/
            text.txt
            metadata.json
            images/page<N>_img<N>.png

Dependencies:
    pip install pymupdf pytesseract pillow tqdm langdetect
    system: tesseract-ocr tesseract-ocr-rus  (apt install tesseract-ocr tesseract-ocr-rus)
"""

import csv
import io
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import fitz  # pymupdf
import pytesseract
from PIL import Image
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DOWNLOADS_DIR = Path("/home/artie-sh/repos/spiders/exp_clean")
OUTPUT_DIR = Path("/home/artie-sh/repos/spiders/exp_clean_processed")
LOG_FILE = OUTPUT_DIR / "extraction_log.csv"

MIN_NONWS_CHARS = 50   # chars of non-whitespace per page before falling back to OCR
MIN_ALPHA_RATIO = 0.4  # minimum fraction of non-ws chars that should be letters;
                       # below this the page is likely a font-encoding artifact
MIN_IMG_WIDTH = 50     # pixels
MIN_IMG_HEIGHT = 50    # pixels
OCR_DPI = 200          # render resolution for scanned-page OCR

LOG_FIELDS = [
    "file_path",
    "species",
    "page_count",
    "text_length",
    "image_count",
    "ocr_used",
    "error",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_filename(stem: str) -> dict[str, str]:
    """
    Parse author / year / title from a PDF stem.

    Naming conventions:
      - Single-author initials use underscores:   Izmailova_M_V
      - Multiple authors separated by __:         Logunov_D_V__Marusik_Y_M
      - Year is 4 digits + optional letter:       1980, 1955c
      - Title follows immediately after the year token

    Strategy: find the first underscore-delimited token that looks like a year
    (^\\d{4}[a-z]?$).  Everything to its left is the author string; everything
    to its right is the title (underscores replaced with spaces).
    """
    # Match the first year-like token.
    # (?:^|_)  — start of string or a literal underscore (consumed as delimiter)
    # (\d{4}[a-z]?)  — 4 digits + optional lowercase suffix, captured
    # (?=_|$)  — must be followed by underscore or end of string (lookahead, not consumed)
    m = re.search(r'(?:^|_)(\d{4}[a-z]?)(?=_|$)', stem)
    if m:
        author = stem[:m.start()]                           # before the leading '_'
        year   = m.group(1)
        title  = stem[m.end():].lstrip('_').replace('_', ' ').strip()
        return {"author": author, "year": year, "title": title}

    # Fallback for filenames that don't contain a recognisable year token
    parts = stem.split("_", 2)
    return {
        "author": parts[0] if len(parts) > 0 else "",
        "year":   parts[1] if len(parts) > 1 else "",
        "title":  parts[2].replace("_", " ") if len(parts) > 2 else "",
    }


def nonws_len(text: str) -> int:
    """Count non-whitespace characters in a string."""
    return sum(1 for c in text if not c.isspace())


def looks_garbled(text: str) -> bool:
    """
    Return True if text looks like a font-encoding artifact rather than real content.

    Catches two known artifact patterns:

    1. Low alpha ratio — PDFs with missing/broken ToUnicode maps produce raw glyph
       codes: many symbols and digits, very few letters.

    2. Digits embedded inside letter sequences — some older PDFs (typically Soviet-era
       Russian publications) use a custom glyph encoding where Cyrillic characters are
       stored as visually-similar Latin codepoints, while Cyrillic digits (З→'3', б→'6',
       etc.) land inside words: e.g. "H3yqaJIHCb" (Изучались), "pa6oTa" (работа).
       In authentic text this pattern is negligible; a rate above 0.5 % flags corruption.

    Exception: if a meaningful share of non-whitespace characters are non-ASCII
    letters (Cyrillic, Greek, etc.), the page is real multilingual text and must
    not be flagged — even if its overall alpha ratio is below the threshold
    (e.g. a Russian page heavy with numerical tables).
    """
    non_ws = [c for c in text if not c.isspace()]
    if not non_ws:
        return False
    non_ascii_alpha = sum(1 for c in non_ws if c.isalpha() and ord(c) > 127)
    if non_ascii_alpha / len(non_ws) >= 0.15:
        return False  # substantial non-Latin script content → real text
    alpha_count = sum(1 for c in non_ws if c.isalpha())
    if (alpha_count / len(non_ws)) < MIN_ALPHA_RATIO:
        return True
    # Detect garbled Cyrillic-as-Latin: count digits that sit between two letters.
    digits_in_word = sum(
        1 for i in range(1, len(text) - 1)
        if text[i].isdigit() and text[i - 1].isalpha() and text[i + 1].isalpha()
    )
    return digits_in_word / len(non_ws) > 0.005


def page_to_pil(page: fitz.Page, dpi: int = OCR_DPI) -> Image.Image:
    """Render a PDF page to a PIL Image (for OCR)."""
    pix = page.get_pixmap(dpi=dpi)
    return Image.open(io.BytesIO(pix.tobytes("png")))


# ---------------------------------------------------------------------------
# Core extraction
# ---------------------------------------------------------------------------

def extract_pdf(pdf_path: Path, out_dir: Path) -> dict:
    """
    Extract text, images, and metadata from one PDF.
    Returns a stats dict suitable for the CSV log row.
    """
    species = pdf_path.parent.name
    parsed = parse_filename(pdf_path.stem)

    out_dir.mkdir(parents=True, exist_ok=True)
    images_dir = out_dir / "images"

    text_parts: list[str] = []
    ocr_used = False
    image_count = 0
    images_dir_created = False

    doc = fitz.open(pdf_path)
    try:
        page_count = len(doc)

        for page_num in range(page_count):
            page = doc[page_num]
            label = page_num + 1  # 1-based for filenames / readability

            # ------------------------------------------------------------------
            # Text extraction (with OCR fallback for scanned pages)
            # ------------------------------------------------------------------
            raw_text = page.get_text()

            if nonws_len(raw_text) < MIN_NONWS_CHARS or looks_garbled(raw_text):
                try:
                    pil_img = page_to_pil(page)
                    ocr_text = pytesseract.image_to_string(pil_img, lang="eng+rus")
                    text_parts.append(f"[Page {label} — OCR]\n{ocr_text}")
                    ocr_used = True
                except Exception as ocr_err:
                    text_parts.append(
                        f"[Page {label} — OCR failed: {ocr_err}]"
                    )
            else:
                text_parts.append(f"[Page {label}]\n{raw_text}")

            # Number of non-whitespace characters extracted for this page.
            # Used below to decide whether a full-page scan is a text page
            # (skip) or a figure plate (keep).
            page_text_nws = nonws_len(text_parts[-1])

            # ------------------------------------------------------------------
            # Image extraction
            # ------------------------------------------------------------------
            seen_xrefs: set[int] = set()
            img_index = 0

            for img_info in page.get_images(full=True):
                xref = img_info[0]
                if xref in seen_xrefs:
                    continue  # skip duplicate references on the same page
                seen_xrefs.add(xref)

                try:
                    # Use fitz.Pixmap so MuPDF applies the PDF Decode array
                    # (fixes inverted colours) and all colour-space transforms.
                    pix = fitz.Pixmap(doc, xref)

                    if pix.width < MIN_IMG_WIDTH or pix.height < MIN_IMG_HEIGHT:
                        continue  # likely an icon or artifact

                    # Skip full-page background scans, but only when the page
                    # also has substantial text.  Pages with little text (≤ 300
                    # non-ws chars) are figure plates and must be kept even when
                    # the image fills the whole page.
                    rects = page.get_image_rects(xref)
                    if rects:
                        r = rects[0]
                        if (r.width  >= page.rect.width  * 0.85 and
                                r.height >= page.rect.height * 0.85 and
                                page_text_nws > 300):
                            continue

                    # Normalise to a saveable image.
                    if pix.colorspace is None:
                        # 1-bit /ImageMask: MuPDF exposes it as an alpha-only
                        # Pixmap (255 = ink/opaque, 0 = paper/transparent).
                        # Invert to produce a white-background grayscale image.
                        pil_img = Image.frombytes(
                            "L", (pix.width, pix.height), bytes(pix.samples)
                        ).point(lambda x: 255 - x)
                    else:
                        # Convert all non-RGB colourspaces (DeviceGray, CMYK,
                        # Separation, ICCBased, etc.) to sRGB.  This correctly
                        # handles Separation(Black) where raw values are ink
                        # amounts (0 = no ink = white), not grayscale luminance.
                        if pix.colorspace != fitz.csRGB:
                            pix = fitz.Pixmap(fitz.csRGB, pix)
                        if pix.alpha:
                            pix = fitz.Pixmap(pix, 0)
                        pil_img = None

                    img_index += 1
                    if not images_dir_created:
                        images_dir.mkdir(exist_ok=True)
                        images_dir_created = True

                    out_path = images_dir / f"page{label}_img{img_index}.png"
                    if pil_img is not None:
                        pil_img.save(out_path, "PNG")
                    else:
                        pix.save(out_path)
                    image_count += 1

                except Exception:
                    # Unreadable image — skip silently
                    pass

    finally:
        doc.close()

    # --------------------------------------------------------------------------
    # Write text.txt
    # --------------------------------------------------------------------------
    full_text = "\n\n".join(text_parts)
    (out_dir / "text.txt").write_text(full_text, encoding="utf-8")

    # --------------------------------------------------------------------------
    # Write metadata.json
    # --------------------------------------------------------------------------
    metadata = {
        "original_pdf_path": str(pdf_path),
        "species": species,
        "author": parsed["author"],
        "year": parsed["year"],
        "title": parsed["title"],
        "page_count": page_count,
        "text_length_chars": len(full_text),
        "image_count": image_count,
        "ocr_used": ocr_used,
        "extraction_datetime": datetime.now().isoformat(),
    }
    (out_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    return {
        "file_path": str(pdf_path),
        "species": species,
        "page_count": page_count,
        "text_length": len(full_text),
        "image_count": image_count,
        "ocr_used": ocr_used,
        "error": "",
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def collect_pdfs(root: Path) -> list[Path]:
    """Return all PDFs under root, sorted for deterministic ordering."""
    return sorted(root.rglob("*.pdf"))


def main() -> None:
    if not DOWNLOADS_DIR.exists():
        print(f"Error: downloads directory not found at '{DOWNLOADS_DIR}'.")
        sys.exit(1)

    pdf_files = collect_pdfs(DOWNLOADS_DIR)
    if not pdf_files:
        print(f"No PDF files found under '{DOWNLOADS_DIR}/'.")
        sys.exit(0)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Found {len(pdf_files)} PDF(s). Starting extraction...\n")

    with open(LOG_FILE, "w", newline="", encoding="utf-8") as log_fh:
        writer = csv.DictWriter(log_fh, fieldnames=LOG_FIELDS)
        writer.writeheader()

        for pdf_path in tqdm(pdf_files, desc="PDFs", unit="pdf"):
            species = pdf_path.parent.name
            out_paper_dir = OUTPUT_DIR / species / pdf_path.stem

            try:
                row = extract_pdf(pdf_path, out_paper_dir)
            except Exception as exc:
                row = {
                    "file_path": str(pdf_path),
                    "species": species,
                    "page_count": "",
                    "text_length": "",
                    "image_count": "",
                    "ocr_used": "",
                    "error": str(exc),
                }

            writer.writerow(row)
            log_fh.flush()  # keep the log current if the run is interrupted

    print(f"\nExtraction complete. Log: {LOG_FILE}")


if __name__ == "__main__":
    main()
