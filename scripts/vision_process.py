#!/usr/bin/env python3
"""
Process PDFs that couldn't be handled by the main pipeline (garbled/scanned text,
exotic scripts) using Claude Vision for text extraction.

Finds all PDFs in INPUT_DIR that have no corresponding chunks.json in PROCESSED_DIR,
renders each page as an image, transcribes via Claude Vision, extracts embedded
images with fitz, and writes chunks.json + doc_stats.json in the same format as
pdf_processor.py.

Usage:
    ANTHROPIC_API_KEY=sk-ant-... python3 scripts/vision_process.py

Resume-safe: already-completed output dirs (with chunks.json) are skipped.
"""

import base64
import json
import os
import re
import sys
from pathlib import Path

import anthropic
import fitz
import numpy as np
from scipy.ndimage import convolve

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REPO_ROOT     = Path(__file__).resolve().parent.parent
INPUT_DIR     = REPO_ROOT / "pardosa"
PROCESSED_DIR = REPO_ROOT / "pardosa_processed"
MODEL         = "claude-haiku-4-5-20251001"
PAGE_DPI      = 150          # render resolution for Vision
MAX_TOKENS    = 4096         # per page transcription
CHUNK_TOKENS  = 400          # target tokens per RAG chunk
MIN_IMG_SIZE  = 50           # minimum image dimension to save (px)

TRANSCRIPTION_PROMPT = """\
You are transcribing a page from a scientific spider taxonomy paper.
Extract all text content accurately, preserving reading order.

Rules:
- Transcribe ALL text: headings, body text, captions, footnotes
- For tables, wrap each one like this:
  TABLE_START
  col1 | col2 | col3
  val1 | val2 | val3
  TABLE_END
- Preserve paragraph breaks with a blank line
- If the page is blank or contains only images with no text, output: [NO TEXT]
- Output ONLY the transcribed content — no commentary
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def out_dir_for(pdf_path: Path) -> Path:
    if pdf_path.parent == INPUT_DIR:
        return PROCESSED_DIR / pdf_path.stem
    return PROCESSED_DIR / pdf_path.parent.name / pdf_path.stem


def find_pending(input_dir: Path, processed_dir: Path) -> list[Path]:
    pending = []
    for pdf_path in sorted(input_dir.rglob("*.pdf")):
        if pdf_path.is_symlink():
            continue
        out_dir = out_dir_for(pdf_path)
        if not (out_dir / "chunks.json").exists():
            pending.append(pdf_path)
    return pending


def render_page_b64(page, dpi: int = PAGE_DPI) -> str:
    mat = fitz.Matrix(dpi / 72, dpi / 72)
    pix = page.get_pixmap(matrix=mat)
    return base64.standard_b64encode(pix.tobytes("png")).decode()


def transcribe_page(client: anthropic.Anthropic, page_b64: str) -> str:
    response = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": page_b64,
                    },
                },
                {"type": "text", "text": TRANSCRIPTION_PROMPT},
            ],
        }],
    )
    return response.content[0].text.strip()


def parse_tables(page_text: str) -> tuple[str, list[str]]:
    """
    Extract TABLE_START...TABLE_END blocks from page text.
    Returns (clean_text_without_tables, list_of_table_csv_strings).
    """
    tables_csv = []
    def replace_table(m):
        block = m.group(1).strip()
        rows = [line.strip() for line in block.splitlines() if line.strip()]
        tables_csv.append("\n".join(rows))
        return ""
    clean = re.sub(r"TABLE_START(.*?)TABLE_END", replace_table, page_text, flags=re.DOTALL)
    return clean.strip(), tables_csv


def assess_image_quality(img_path: Path) -> dict:
    try:
        from PIL import Image as _Image
        img = _Image.open(img_path)
        gray = np.array(img.convert("L")).astype(np.float32)
        kernel = np.array([[0, 1, 0], [1, -4, 1], [0, 1, 0]], dtype=np.float32)
        sharpness = float(np.var(convolve(gray, kernel)))
        return {
            "width_px": img.width, "height_px": img.height,
            "megapixels": round((img.width * img.height) / 1e6, 3),
            "sharpness": round(sharpness, 2),
            "contrast": round(float(gray.std()), 2),
        }
    except Exception as exc:
        return {"error": str(exc)}


def extract_images_fitz(
    fitz_doc, out_dir: Path, source_file: str, species: str
) -> dict:
    """Extract embedded images from PDF using fitz, same naming as pdf_processor."""
    images_dir = out_dir / "images"
    images_dir.mkdir(exist_ok=True)
    ok = fail = skipped = 0
    page_counters: dict[int, int] = {}

    for page_no, page in enumerate(fitz_doc, 1):
        for img_info in page.get_images(full=True):
            xref = img_info[0]
            try:
                base_img = fitz_doc.extract_image(xref)
                w, h = base_img["width"], base_img["height"]
                if w < MIN_IMG_SIZE or h < MIN_IMG_SIZE:
                    skipped += 1
                    continue
                page_counters[page_no] = page_counters.get(page_no, 0) + 1
                slug = f"page_{page_no:03d}_img_{page_counters[page_no]}"
                png_path = images_dir / f"{slug}.png"
                png_path.write_bytes(base_img["image"])
                quality = assess_image_quality(png_path)
                meta = {
                    "caption": "",
                    "page_number": page_no,
                    "picture_ref": slug,
                    "source_file": source_file,
                    "species": species,
                    "image_quality": quality,
                }
                (images_dir / f"{slug}_meta.json").write_text(
                    json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8"
                )
                ok += 1
            except Exception as exc:
                print(f"    image error (xref {xref}): {exc}")
                fail += 1

    return {"total": ok + fail + skipped, "ok": ok, "fail": fail, "skipped": skipped}


def simple_tokenize(text: str) -> int:
    """Rough token count: ~4 chars per token."""
    return max(1, len(text) // 4)


def build_chunks(
    pages: list[tuple[int, str, list[str]]],  # (page_no, text, tables_csv)
    out_dir: Path,
    source_file: str,
    species: str,
) -> tuple[list[dict], dict]:
    """
    Build RAG chunks from per-page transcribed text.
    Splits pages into paragraph-based chunks of ~CHUNK_TOKENS tokens.
    Tables on the same page are referenced from chunks on that page.
    """
    # Save tables first, collect slugs per page
    tables_dir = out_dir / "tables"
    tables_dir.mkdir(exist_ok=True)
    page_table_slugs: dict[int, list[str]] = {}
    table_total = table_ok = 0
    page_tbl_counters: dict[int, int] = {}

    for page_no, _, tables_csv in pages:
        for csv_str in tables_csv:
            table_total += 1
            page_tbl_counters[page_no] = page_tbl_counters.get(page_no, 0) + 1
            slug = f"page_{page_no:03d}_tbl_{page_tbl_counters[page_no]}"
            csv_path = tables_dir / f"{slug}.csv"
            try:
                csv_path.write_text(csv_str, encoding="utf-8")
                meta = {
                    "caption": "",
                    "page_no": page_no,
                    "source_file": source_file,
                    "species": species,
                }
                (tables_dir / f"{slug}_meta.json").write_text(
                    json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8"
                )
                page_table_slugs.setdefault(page_no, []).append(f"tables/{slug}.csv")
                table_ok += 1
            except Exception as exc:
                print(f"    table save error: {exc}")

    # Build chunks from paragraph blocks
    chunks: list[dict] = []
    current_text = ""
    current_pages: list[int] = []
    current_tables: list[dict] = []

    def flush():
        nonlocal current_text, current_pages, current_tables
        t = current_text.strip()
        if t:
            chunks.append({
                "text": t,
                "headings": [],
                "item_types": ["TextItem"],
                "source_file": source_file,
                "species": species,
                "page_numbers": sorted(set(current_pages)),
                "figures": [],
                "tables": [{"caption": "", "csv_file": f} for f in current_tables],
            })
        current_text = ""
        current_pages = []
        current_tables = []

    for page_no, text, _ in pages:
        if text == "[NO TEXT]" or not text.strip():
            continue
        paragraphs = re.split(r"\n{2,}", text)
        tbl_slugs = page_table_slugs.get(page_no, [])

        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
            if simple_tokenize(current_text + " " + para) > CHUNK_TOKENS and current_text:
                flush()
            current_text = (current_text + "\n\n" + para).strip() if current_text else para
            current_pages.append(page_no)
            for s in tbl_slugs:
                if s not in current_tables:
                    current_tables.append(s)
            if simple_tokenize(current_text) >= CHUNK_TOKENS:
                flush()

    flush()

    (out_dir / "chunks.json").write_text(
        json.dumps(chunks, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    with_tables = sum(1 for c in chunks if c["tables"])
    stats = {
        "total": len(chunks),
        "with_figures": 0,
        "with_tables": with_tables,
    }
    table_stats = {"total": table_total, "ok": table_ok, "fail": table_total - table_ok}
    return chunks, stats, table_stats


def save_doc_stats(
    out_dir: Path,
    source_file: str,
    species: str,
    n_pages: int,
    chunk_stats: dict,
    table_stats: dict,
    picture_stats: dict,
) -> None:
    image_captions: list[str] = []
    for meta_path in sorted((out_dir / "images").glob("*_meta.json")):
        try:
            m = json.loads(meta_path.read_text(encoding="utf-8"))
            cap = (m.get("caption") or "").strip()
            if cap:
                image_captions.append(cap)
        except Exception:
            pass

    table_captions: list[str] = []
    for meta_path in sorted((out_dir / "tables").glob("*_meta.json")):
        try:
            m = json.loads(meta_path.read_text(encoding="utf-8"))
            cap = (m.get("caption") or "").strip()
            if cap:
                table_captions.append(cap)
        except Exception:
            pass

    stats = {
        "source_file":   source_file,
        "species":       species,
        "page_count":    n_pages,
        "garbled_ratio": 1.0,   # these docs were flagged as garbled/scanned
        "force_ocr":     True,
        "vision_processed": True,
        "chunks": {
            "total":        chunk_stats["total"],
            "with_figures": chunk_stats["with_figures"],
            "with_tables":  chunk_stats["with_tables"],
        },
        "tables": {
            "total":    table_stats["total"],
            "ok":       table_stats["ok"],
            "fail":     table_stats["fail"],
            "captions": table_captions,
        },
        "images": {
            "total":   picture_stats["total"],
            "saved":   picture_stats["ok"],
            "skipped": picture_stats["skipped"],
            "fail":    picture_stats["fail"],
            "captions": image_captions,
        },
    }
    (out_dir / "doc_stats.json").write_text(
        json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def process_pdf(pdf_path: Path, client: anthropic.Anthropic) -> bool:
    out_dir = out_dir_for(pdf_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    source_file = pdf_path.name
    species = pdf_path.parent.name

    print(f"  {species}/{pdf_path.stem}")

    try:
        fitz_doc = fitz.open(str(pdf_path))
        n_pages = len(fitz_doc)
    except Exception as exc:
        print(f"    ERROR opening PDF: {exc}")
        return False

    # Extract embedded images (no Claude needed)
    picture_stats = extract_images_fitz(fitz_doc, out_dir, source_file, species)

    # Transcribe each page with Claude Vision
    pages: list[tuple[int, str, list[str]]] = []
    for page_no, page in enumerate(fitz_doc, 1):
        print(f"    page {page_no}/{n_pages} ...", end=" ", flush=True)
        try:
            page_b64 = render_page_b64(page)
            raw_text = transcribe_page(client, page_b64)
            clean_text, tables_csv = parse_tables(raw_text)
            pages.append((page_no, clean_text, tables_csv))
            print(f"{len(clean_text)} chars, {len(tables_csv)} table(s)")
        except (anthropic.AuthenticationError, anthropic.PermissionDeniedError) as exc:
            fitz_doc.close()
            raise  # propagate fatal errors to main
        except Exception as exc:
            print(f"ERROR: {exc}")
            pages.append((page_no, "", []))

    fitz_doc.close()

    # Build chunks and save everything
    chunks, chunk_stats, table_stats = build_chunks(pages, out_dir, source_file, species)
    save_doc_stats(out_dir, source_file, species, n_pages, chunk_stats, table_stats, picture_stats)

    print(f"    → {chunk_stats['total']} chunks, {table_stats['total']} tables, "
          f"{picture_stats['ok']} images")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set.", file=sys.stderr)
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)
    pending = find_pending(INPUT_DIR, PROCESSED_DIR)

    if not pending:
        print("Nothing to process.")
        sys.exit(0)

    print(f"Found {len(pending)} PDF(s) to process with Claude Vision ({MODEL})\n")

    ok = fail = 0
    for pdf_path in pending:
        try:
            if process_pdf(pdf_path, client):
                ok += 1
            else:
                fail += 1
        except (anthropic.AuthenticationError, anthropic.PermissionDeniedError) as exc:
            print(f"\nFATAL: {exc}")
            print("Check your API key and account balance.")
            sys.exit(1)
        except Exception as exc:
            print(f"    UNEXPECTED ERROR: {exc}")
            fail += 1

    print(f"\nDone: {ok} succeeded, {fail} failed.")


if __name__ == "__main__":
    main()
