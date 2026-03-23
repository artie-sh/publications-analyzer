#!/usr/bin/env python3
"""
Docling experiment: extract everything useful from a single PDF for RAG.

Outputs written to out_dir/:
    document.json            full docling JSON (structure + provenance)
    <slug>.md / .csv         each table
    <slug>_meta.json         table metadata
    images/<slug>.png        each figure
    images/<slug>_meta.json  figure metadata
    chunks.json              RAG-ready chunks with rich provenance
"""

import json
import logging
import os
import re
import sys
import time
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm

import numpy as np
from scipy.ndimage import convolve

# Suppress noisy but harmless library warnings
warnings.filterwarnings("ignore", message=".*pin_memory.*")          # torch, no GPU
warnings.filterwarnings("ignore", message=".*Token indices sequence.*")  # HybridChunker > 512 tokens
logging.getLogger("huggingface_hub").setLevel(logging.ERROR)          # HF connectivity retries
logging.getLogger("transformers").setLevel(logging.ERROR)
os.environ.setdefault("HF_HUB_OFFLINE", "1")                         # skip HF update checks; use local cache


class Tee:
    """Write to multiple streams simultaneously (e.g. stdout + log file)."""

    def __init__(self, *streams):
        self.streams = streams

    def write(self, data: str) -> None:
        for s in self.streams:
            s.write(data)

    def flush(self) -> None:
        for s in self.streams:
            s.flush()


try:
    from rapidfuzz import fuzz as _fuzz
except ImportError:
    _fuzz = None
    print("Warning: rapidfuzz not installed — fuzzy figure injection will be skipped.")

import fitz
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    EasyOcrOptions,
    PdfPipelineOptions,
    TableFormerMode,
    TesseractCliOcrOptions,
)
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling_core.transforms.chunker import HybridChunker
from docling_core.types.doc import PictureItem, TableItem

# ---------------------------------------------------------------------------
# Garbled-text detection (formerly in pdf_analyzer.py)
# ---------------------------------------------------------------------------

MIN_ALPHA_RATIO = 0.4  # minimum fraction of non-ws chars that should be letters


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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_slug(text: str, max_len: int = 50) -> str:
    """Filesystem-safe slug from a caption string."""
    slug = re.sub(r"[^\w\s-]", "", text)
    slug = re.sub(r"\s+", "_", slug.strip())
    slug = slug[:max_len].rstrip("_-")
    return slug or text[:max_len]


def get_page_numbers(item) -> list[int]:
    """Collect unique 1-based page numbers from a DocItem's provenance."""
    pages = []
    if hasattr(item, "prov") and item.prov:
        for prov in item.prov:
            if hasattr(prov, "page_no") and prov.page_no is not None:
                pages.append(prov.page_no)
    return sorted(set(pages))


def page_count(pdf_path: Path) -> int:
    """Return the number of pages in a PDF without reading any text (fast)."""
    doc = fitz.open(str(pdf_path))
    n = len(doc)
    doc.close()
    return n


def assess_image_quality(image_path: str, bbox_pts: dict | None = None) -> dict:
    """
    Compute basic quality metrics for a saved PNG.

    bbox_pts — optional dict with keys l, t, r, b in PDF points (from provenance).
               When provided, DPI is derived from the ratio of pixel dimensions to
               the bounding-box size; otherwise dpi is None.

    Returns a dict with width_px, height_px, megapixels, aspect_ratio, mode,
    sharpness (variance of Laplacian), contrast (std-dev of luminance), dpi,
    and dpi_reliable.  On any failure returns {"error": <message>}.
    """
    try:
        from PIL import Image as _Image
        img = _Image.open(image_path)
        gray = np.array(img.convert("L")).astype(np.float32)

        kernel = np.array([[0,  1, 0],
                           [1, -4, 1],
                           [0,  1, 0]], dtype=np.float32)
        laplacian = convolve(gray, kernel)
        sharpness = float(np.var(laplacian))

        width_px  = img.width
        height_px = img.height

        if bbox_pts:
            bbox_w_pts = bbox_pts["r"] - bbox_pts["l"]
            bbox_h_pts = bbox_pts["b"] - bbox_pts["t"]
            dpi_x = round(width_px  / (bbox_w_pts / 72)) if bbox_w_pts > 0 else None
            dpi_y = round(height_px / (bbox_h_pts / 72)) if bbox_h_pts > 0 else None
        else:
            dpi_x, dpi_y = None, None

        aspect_ratio = round(
            max(width_px / height_px, height_px / width_px), 2
        ) if height_px > 0 and width_px > 0 else None

        return {
            "width_px":     width_px,
            "height_px":    height_px,
            "megapixels":   round((width_px * height_px) / 1e6, 3),
            "aspect_ratio": aspect_ratio,
            "mode":         img.mode,
            "sharpness":    round(sharpness, 2),
            "contrast":     round(float(gray.std()), 2),
            "dpi":          dpi_x,
            "dpi_reliable": dpi_x is not None,
        }
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Step 0 — pre-check + convert
# ---------------------------------------------------------------------------

def garbled_ratio(pdf_path: Path) -> tuple[float, int]:
    """
    Return (ratio, page_count) where ratio is the fraction of text-bearing
    pages whose native text looks garbled.
    """
    doc = fitz.open(str(pdf_path))
    page_count = len(doc)
    garbled, total = 0, 0
    for page in doc:
        text = page.get_text()
        nws = sum(1 for c in text if not c.isspace())
        if nws >= 50:
            total += 1
            if looks_garbled(text):
                garbled += 1
    doc.close()
    return (garbled / total if total else 0.0), page_count


def convert_pdf(
    source: Path,
    *,
    garbled_threshold: float = 0.3,
    images_scale: float = 4.0,
    table_mode: TableFormerMode = TableFormerMode.ACCURATE,
    ocr_langs: list[str] | None = None,
):
    """
    Pre-check the PDF for garbled text, then run the Docling pipeline.

    Returns (DoclingDocument, page_count, garbled_ratio, force_ocr).
    """
    if ocr_langs is None:
        ocr_langs = ["eng", "rus", "ukr", "deu", "spa", "fra", "ita", "lat"]

    ratio, page_count = garbled_ratio(source)
    force_ocr = ratio > garbled_threshold
    print(
        f"Pre-check: {ratio:.0%} of pages have garbled native text"
        f"  →  force_full_page_ocr={force_ocr}"
    )

    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = True
    pipeline_options.ocr_options = TesseractCliOcrOptions(lang=ocr_langs)  # selective OCR on blank/scanned pages
    pipeline_options.do_table_structure = True
    pipeline_options.table_structure_options.mode = table_mode
    pipeline_options.generate_picture_images = True
    pipeline_options.generate_page_images = True   # required for table.get_image()
    pipeline_options.images_scale = images_scale

    if force_ocr:
        pipeline_options.ocr_options = TesseractCliOcrOptions(
            lang=ocr_langs,
            force_full_page_ocr=True,
        )

    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
    )

    print(f"Converting: {source.name}")
    result = converter.convert(str(source))
    return result.document, page_count, ratio, force_ocr


# ---------------------------------------------------------------------------
# Step 1 — tables
# ---------------------------------------------------------------------------

def extract_tables(
    doc,
    out_dir: Path,
    source_file: str,
    species: str,
    tables_subdir: str = "tables",
) -> tuple[dict[str, str], dict]:
    """
    Export every TableItem to CSV, Markdown, and a metadata sidecar.

    Files are written to out_dir/<tables_subdir>/.

    Returns:
        table_slugs  — dict mapping table.self_ref → relative path from out_dir
                       (e.g. "tables/Table_1_Measurements.csv"), passed to
                       build_chunks so chunk table entries point to the right file
        stats        — {"total", "ok", "fail"}
    """
    tables = [item for item, _ in doc.iterate_items() if isinstance(item, TableItem)]
    print(f"\n=== Tables ({len(tables)}) ===")

    tables_dir = out_dir / tables_subdir
    tables_dir.mkdir(exist_ok=True)

    table_slugs: dict[str, str] = {}
    ok = fail = 0
    page_counters: dict[int, int] = {}  # page_no → count of tables on that page

    for i, table in enumerate(tables, 1):
        caption = table.caption_text(doc) or ""
        pages = get_page_numbers(table)
        page_no = pages[0] if pages else 0
        page_counters[page_no] = page_counters.get(page_no, 0) + 1
        slug = f"page_{page_no:03d}_tbl_{page_counters[page_no]}"
        csv_name = f"{slug}.csv"
        table_slugs[table.self_ref] = f"{tables_subdir}/{csv_name}"

        try:
            df = table.export_to_dataframe(doc)
            df.to_csv(tables_dir / csv_name, index=False)

            img = table.get_image(doc)
            if img:
                img.save(tables_dir / f"{slug}.png")

            meta = {
                "caption": caption,
                "page_number": pages[0] if pages else None,
                "rows": df.shape[0],
                "cols": df.shape[1],
                "image_file": f"tables/{slug}.png" if img else None,
                "source_file": source_file,
                "species": species,
            }
            (tables_dir / f"{slug}_meta.json").write_text(
                json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            img_note = "  +img" if img else ""
            print(
                f"  Table {i}: {caption[:80] or '(no caption)'}"
                f"  [{df.shape[0]}×{df.shape[1]}]  p.{pages}{img_note}  ✓"
            )
            ok += 1
        except Exception as e:
            print(f"  Table {i}: FAILED — {e}")
            fail += 1

    return table_slugs, {"total": len(tables), "ok": ok, "fail": fail}


# ---------------------------------------------------------------------------
# Step 2 — pictures
# ---------------------------------------------------------------------------

def extract_pictures(
    doc,
    out_dir: Path,
    source_file: str,
    species: str,
    images_subdir: str = "images",
) -> dict:
    """
    Save every PictureItem as a PNG with a metadata sidecar.

    Returns stats — {"total", "ok", "fail"}
    """
    pictures = [item for item, _ in doc.iterate_items() if isinstance(item, PictureItem)]
    print(f"\n=== Pictures ({len(pictures)}) ===")

    images_dir = out_dir / images_subdir
    images_dir.mkdir(exist_ok=True)
    ok = fail = skipped = 0
    MAX_ASPECT_RATIO = 5.0
    page_counters: dict[int, int] = {}  # page_no → count of images saved on that page

    for i, pic in enumerate(pictures, 1):
        caption = pic.caption_text(doc) or ""
        pages = get_page_numbers(pic)
        page_no = pages[0] if pages else 0
        page_counters[page_no] = page_counters.get(page_no, 0) + 1
        slug = f"page_{page_no:03d}_img_{page_counters[page_no]}"

        print(f"  Picture {i}: {caption[:120] or '(no caption)'}  p.{pages}")
        try:
            png_path = images_dir / f"{slug}.png"
            img = pic.get_image(doc)
            if img:
                w, h = img.size
                if h > 0 and w > 0:
                    aspect = max(w / h, h / w)
                    if aspect > MAX_ASPECT_RATIO:
                        print(f"    Skipping — aspect ratio {aspect:.1f}:1 (likely decorative)")
                        skipped += 1
                        continue
                img.save(png_path)
            else:
                print("    (no image data)")

            bbox_pts = None
            if pic.prov:
                b = pic.prov[0].bbox
                if b is not None:
                    bbox_pts = {"l": b.l, "t": b.t, "r": b.r, "b": b.b}

            if png_path.exists():
                image_quality = assess_image_quality(str(png_path), bbox_pts=bbox_pts)
            else:
                print(f"    Warning: {png_path.name} not found — image_quality set to null")
                image_quality = None

            meta = {
                "caption": caption,
                "page_number": pages[0] if pages else None,
                "picture_ref": pic.self_ref,
                "source_file": source_file,
                "species": species,
                "image_quality": image_quality,
            }
            (images_dir / f"{slug}_meta.json").write_text(
                json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            ok += 1
        except Exception as e:
            print(f"    image error: {e}")
            fail += 1

    return {"total": len(pictures), "ok": ok, "fail": fail, "skipped": skipped}


# ---------------------------------------------------------------------------
# Step 3 — full document JSON
# ---------------------------------------------------------------------------

def save_document_json(doc, out_dir: Path, filename: str = "document.json") -> None:
    """Dump the complete Docling document model (provenance, bboxes, …)."""
    (out_dir / filename).write_text(
        json.dumps(doc.export_to_dict(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"\nJSON → {filename}")


# ---------------------------------------------------------------------------
# Step 4 — RAG chunks
# ---------------------------------------------------------------------------

def build_chunks(
    doc,
    out_dir: Path,
    source_file: str,
    species: str,
    table_slugs: dict[str, str],
    filename: str = "chunks.json",
) -> tuple[list[dict], dict]:
    """
    Chunk the document with HybridChunker and annotate each chunk with
    page provenance, figure references, and table references.

    Returns:
        chunks_data  — list of chunk dicts ready for chunks.json
        stats        — {"total", "with_figures", "with_tables"}
    """
    print(f"\n=== RAG Chunks ===")
    pictures = [item for item, _ in doc.iterate_items() if isinstance(item, PictureItem)]

    # caption text → picture self_ref, for resolving standalone caption items
    caption_to_pic_ref: dict[str, str] = {
        cap: pic.self_ref
        for pic in pictures
        if (cap := pic.caption_text(doc) or "")
    }

    print(f"Caption lookup: {len(caption_to_pic_ref)} entries")
    for cap, ref in caption_to_pic_ref.items():
        print(f"  {ref}: {cap[:80]}")

    chunker = HybridChunker()
    chunks = list(chunker.chunk(doc))
    print(f"Total chunks: {len(chunks)}")

    chunks_data: list[dict] = []

    for chunk in chunks:
        entry: dict = {
            "text": chunk.text,
            "headings": [],
            "item_types": [],
            "source_file": source_file,
            "species": species,
            "page_numbers": [],
            "figures": [],
            "tables": [],
        }

        if not hasattr(chunk, "meta"):
            chunks_data.append(entry)
            continue

        meta = chunk.meta
        entry["headings"] = getattr(meta, "headings", None) or []
        doc_items = getattr(meta, "doc_items", None) or []
        entry["item_types"] = [type(it).__name__ for it in doc_items]

        all_pages: list[int] = []
        for it in doc_items:
            all_pages.extend(get_page_numbers(it))
        entry["page_numbers"] = sorted(set(all_pages))

        seen_pic_refs: set[str] = set()
        seen_table_refs: set[str] = set()

        for it in doc_items:
            if isinstance(it, PictureItem):
                if it.self_ref not in seen_pic_refs:
                    seen_pic_refs.add(it.self_ref)
                    entry["figures"].append({
                        "caption": it.caption_text(doc) or "",
                        "picture_ref": it.self_ref,
                    })
            elif isinstance(it, TableItem):
                if it.self_ref not in seen_table_refs:
                    seen_table_refs.add(it.self_ref)
                    entry["tables"].append({
                        "caption": it.caption_text(doc) or "",
                        "csv_file": table_slugs.get(it.self_ref, ""),
                    })
            else:
                label_str = str(getattr(it, "label", "")).lower()
                if "caption" in label_str:
                    cap_text = getattr(it, "text", "") or ""
                    pic_ref = caption_to_pic_ref.get(cap_text)
                    if pic_ref and pic_ref not in seen_pic_refs:
                        seen_pic_refs.add(pic_ref)
                        entry["figures"].append({
                            "caption": cap_text,
                            "picture_ref": pic_ref,
                        })

        chunks_data.append(entry)

    (out_dir / filename).write_text(
        json.dumps(chunks_data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Saved → {filename}")

    with_figures = sum(1 for c in chunks_data if c["figures"])
    with_tables  = sum(1 for c in chunks_data if c["tables"])
    return chunks_data, {"total": len(chunks_data), "with_figures": with_figures, "with_tables": with_tables}


# ---------------------------------------------------------------------------
# Step 5 — post-processing: inject figure refs, resolve image files, dedup
# ---------------------------------------------------------------------------

def fix_figure_refs(
    chunks: list[dict],
    doc,
    fuzzy_threshold: int = 85,
) -> int:
    """
    Fuzzy-match picture captions against chunk text to inject figure refs
    that HybridChunker lost during splitting.

    Returns count of figure refs injected.
    """
    if _fuzz is None:
        print("  Skipping fuzzy figure injection (rapidfuzz not installed).")
        return 0

    pictures = [item for item, _ in doc.iterate_items() if isinstance(item, PictureItem)]
    caption_to_pic_ref: dict[str, str] = {
        cap: pic.self_ref
        for pic in pictures
        if (cap := pic.caption_text(doc) or "")
    }
    print(f"  Caption lookup: {len(caption_to_pic_ref)} captioned picture(s)")

    injected = 0
    for chunk in chunks:
        chunk_text = chunk.get("text") or ""
        if not chunk_text:
            continue
        existing_refs = {
            fig["picture_ref"] for fig in chunk.get("figures", []) if fig.get("picture_ref")
        }
        for cap_text, pic_ref in caption_to_pic_ref.items():
            if pic_ref in existing_refs:
                continue
            if _fuzz.partial_ratio(cap_text, chunk_text) >= fuzzy_threshold:
                chunk.setdefault("figures", []).append({
                    "caption": cap_text,
                    "picture_ref": pic_ref,
                })
                existing_refs.add(pic_ref)
                injected += 1

    return injected


def resolve_image_files(chunks: list[dict], images_dir: Path) -> int:
    """
    Add image_file field to figure entries by reading _meta.json sidecars.

    Returns count of image_file fields added.
    """
    pic_ref_to_image: dict[str, str] = {}
    for meta_path in sorted(images_dir.glob("*_meta.json")):
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            ref = meta.get("picture_ref", "")
            if ref:
                img_name = meta_path.name[: -len("_meta.json")] + ".png"
                pic_ref_to_image[ref] = f"images/{img_name}"
        except Exception as e:
            print(f"  Warning: could not read {meta_path.name}: {e}")

    resolved = 0
    for chunk in chunks:
        for fig in chunk.get("figures", []):
            if fig.get("image_file"):
                continue
            img_path = pic_ref_to_image.get(fig.get("picture_ref", ""))
            if img_path:
                fig["image_file"] = img_path
                resolved += 1
    return resolved


def dedup_per_chunk(chunks: list[dict]) -> None:
    """Deduplicate figures (by picture_ref) and tables (by csv_file) within each chunk."""
    for chunk in chunks:
        seen: set[str] = set()
        deduped = []
        for fig in chunk.get("figures", []):
            key = fig.get("picture_ref", "")
            if key not in seen:
                seen.add(key)
                deduped.append(fig)
        chunk["figures"] = deduped

        seen = set()
        deduped = []
        for tbl in chunk.get("tables", []):
            key = tbl.get("csv_file", "")
            if key not in seen:
                seen.add(key)
                deduped.append(tbl)
        chunk["tables"] = deduped


def dedup_cross_chunk(chunks: list[dict]) -> int:
    """
    Remove figure entries whose picture_ref already appears in an earlier chunk.
    First occurrence wins. Returns count removed.
    """
    seen: set[str] = set()
    removed = 0
    for chunk_idx, chunk in enumerate(chunks):
        kept = []
        for fig in chunk.get("figures", []):
            ref = fig.get("picture_ref", "")
            if ref and ref in seen:
                print(f"  WARNING: duplicate cross-chunk figure {ref} removed from chunk {chunk_idx}")
                removed += 1
            else:
                if ref:
                    seen.add(ref)
                kept.append(fig)
        chunk["figures"] = kept
    return removed


def split_oversized_chunks(
    chunks: list[dict],
    max_tokens: int = 400,
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
) -> tuple[list[dict], int]:
    """
    Split any chunk whose text exceeds max_tokens into smaller sub-chunks,
    splitting at sentence boundaries where possible.

    Uses the same tokenizer as HybridChunker so token counts are consistent.
    Headings and page_numbers are propagated to all sub-chunks.
    Figures and tables are attached to the first sub-chunk only (they belong
    to the beginning of the original chunk's context).

    Returns (new_chunks_list, count_of_chunks_that_were_split).
    """
    try:
        from transformers import AutoTokenizer
        tokenizer = AutoTokenizer.from_pretrained(model_name)
    except Exception as e:
        print(f"  Warning: could not load tokenizer ({e}) — skipping oversized chunk split.")
        return chunks, 0

    def count_tokens(text: str) -> int:
        return len(tokenizer.encode(text, add_special_tokens=True))

    # Split on sentence-ending punctuation followed by whitespace
    sent_re = re.compile(r'(?<=[.!?])\s+')

    split_count = 0
    result: list[dict] = []

    for chunk in chunks:
        text = chunk.get("text", "")
        if count_tokens(text) <= max_tokens:
            result.append(chunk)
            continue

        sentences = sent_re.split(text)
        sub_texts: list[str] = []
        current = ""

        for sent in sentences:
            candidate = (current + " " + sent).strip() if current else sent
            if current and count_tokens(candidate) > max_tokens:
                sub_texts.append(current)
                current = sent
            else:
                current = candidate

        if current:
            sub_texts.append(current)

        if len(sub_texts) <= 1:
            # Single sentence already > max_tokens — keep as-is; nothing to split
            result.append(chunk)
            continue

        split_count += 1
        for j, sub_text in enumerate(sub_texts):
            sub: dict = {
                "text":         sub_text,
                "headings":     chunk.get("headings", []),
                "item_types":   chunk.get("item_types", []),
                "source_file":  chunk.get("source_file", ""),
                "species":      chunk.get("species", ""),
                "page_numbers": chunk.get("page_numbers", []),
                # Figures/tables belong to the first sub-chunk only
                "figures":      chunk.get("figures", []) if j == 0 else [],
                "tables":       chunk.get("tables", []) if j == 0 else [],
            }
            result.append(sub)

    return result, split_count


def inject_fallback_figures(chunks: list[dict], images_dir: Path) -> int:
    """
    Inject figures with empty captions (not fuzzy-matchable) by matching
    their page_number to the first chunk that covers that page.

    Returns count of figures injected.
    """
    assigned_refs: set[str] = {
        fig["picture_ref"]
        for chunk in chunks
        for fig in chunk.get("figures", [])
        if fig.get("picture_ref")
    }

    injected = 0
    for meta_path in sorted(images_dir.glob("*_meta.json")):
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            ref = meta.get("picture_ref", "")
            if not ref or ref in assigned_refs:
                continue
            if meta.get("caption", "").strip():
                continue  # captioned figures should have been fuzzy-matched already
            page_no  = meta.get("page_number")
            img_name = meta_path.name[: -len("_meta.json")] + ".png"

            target_idx = next(
                (i for i, c in enumerate(chunks) if page_no in c.get("page_numbers", [])),
                None,
            )
            if target_idx is None:
                print(f"  WARNING: no chunk found for {ref} on page {page_no} — skipping")
                continue

            chunks[target_idx].setdefault("figures", []).append({
                "caption": "",
                "picture_ref": ref,
                "image_file": f"images/{img_name}",
            })
            assigned_refs.add(ref)
            print(f"  Fallback injection: {ref} → chunk {target_idx} (page {page_no})")
            injected += 1
        except Exception as e:
            print(f"  Warning: could not read {meta_path.name}: {e}")

    return injected


def postprocess_chunks(
    chunks: list[dict],
    doc,
    out_dir: Path,
    fuzzy_threshold: int = 85,
    max_tokens: int = 400,
    filename: str = "chunks.json",
) -> dict:
    """
    Run all post-processing steps on the in-memory chunks list and save.

    Steps:
      1. Fuzzy-match captions → inject missing figure refs
      2. Resolve image_file paths from _meta.json sidecars
      3. Deduplicate figures/tables within each chunk
      4. Cross-chunk deduplication (first occurrence wins)
      5. Fallback injection for uncaptioned figures (by page number)
      6. Second cross-chunk dedup pass (safety)
      7. Split oversized chunks at sentence boundaries (max_tokens)

    Returns stats dict.
    """
    images_dir = out_dir / "images"
    print(f"\n=== Post-processing Chunks ===")

    fuzzy_injected       = fix_figure_refs(chunks, doc, fuzzy_threshold)
    image_files_resolved = resolve_image_files(chunks, images_dir)
    dedup_per_chunk(chunks)
    cross_chunk_removed  = dedup_cross_chunk(chunks)
    fallback_injected    = inject_fallback_figures(chunks, images_dir)
    if fallback_injected:
        cross_chunk_removed += dedup_cross_chunk(chunks)

    chunks, oversized_split = split_oversized_chunks(chunks, max_tokens=max_tokens)
    if oversized_split:
        print(f"  Split {oversized_split} oversized chunk(s) at sentence boundaries.")

    (out_dir / filename).write_text(
        json.dumps(chunks, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Saved → {filename}")

    return {
        "fuzzy_injected":       fuzzy_injected,
        "fallback_injected":    fallback_injected,
        "image_files_resolved": image_files_resolved,
        "cross_chunk_removed":  cross_chunk_removed,
        "oversized_split":      oversized_split,
        "final_with_figures":   sum(1 for c in chunks if c["figures"]),
        "final_with_tables":    sum(1 for c in chunks if c["tables"]),
    }


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(
    page_count: int,
    table_stats: dict,
    picture_stats: dict,
    chunk_stats: dict,
    pp_stats: dict,
) -> None:
    print(f"""
=== Final Summary ===
  Pages:    {page_count}
  Tables:   {table_stats['total']} total  ({table_stats['ok']} ok, {table_stats['fail']} failed)
  Pictures: {picture_stats['total']} total  ({picture_stats['ok']} ok, {picture_stats['fail']} failed, {picture_stats['skipped']} skipped)
  Chunks:   {chunk_stats['total']}
    with figures : {pp_stats['final_with_figures']}
    with tables  : {pp_stats['final_with_tables']}
  Post-processing:
    figure refs injected (fuzzy)    : {pp_stats['fuzzy_injected']}
    figure refs injected (fallback) : {pp_stats['fallback_injected']}
    image files resolved            : {pp_stats['image_files_resolved']}
    cross-chunk duplicates removed  : {pp_stats['cross_chunk_removed']}
    oversized chunks split (≤400t)  : {pp_stats['oversized_split']}
""")


# ---------------------------------------------------------------------------
# doc_stats.json
# ---------------------------------------------------------------------------

def save_doc_stats(
    out_dir: Path,
    source_file: str,
    species: str,
    page_count: int,
    garbled_ratio: float,
    force_ocr: bool,
    table_stats: dict,
    picture_stats: dict,
    chunk_stats: dict,
    pp_stats: dict,
    filename: str = "doc_stats.json",
) -> None:
    """Write a single aggregated stats file for the document."""
    # Collect non-empty captions from saved image meta files
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
        "page_count":    page_count,
        "garbled_ratio": round(garbled_ratio, 3),
        "force_ocr":     force_ocr,
        "chunks": {
            "total":        chunk_stats["total"],
            "with_figures": pp_stats["final_with_figures"],
            "with_tables":  pp_stats["final_with_tables"],
        },
        "tables": {
            "total":    table_stats["total"],
            "ok":       table_stats["ok"],
            "fail":     table_stats["fail"],
            "captions": table_captions,
        },
        "images": {
            "total":    picture_stats["total"],
            "saved":    picture_stats["ok"],
            "skipped":  picture_stats["skipped"],
            "fail":     picture_stats["fail"],
            "captions": image_captions,
        },
    }
    (out_dir / filename).write_text(
        json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"Stats  → {filename}")


# ---------------------------------------------------------------------------
# Per-file processing
# ---------------------------------------------------------------------------

def process_pdf(pdf_path: Path, out_dir: Path, quiet: bool = False) -> bool:
    """
    Run the full pipeline for a single PDF.

    Output is written to out_dir/.  The caller is responsible for constructing
    the desired output path (with or without a species-level subfolder).
    A run.log is written to out_dir as well.

    quiet=True suppresses stdout mirroring (used when running inside a worker
    process so that output from parallel workers does not interleave on the
    terminal — the run.log still captures everything).

    Returns True on success, False on failure.
    """
    species = pdf_path.parent.name
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(out_dir / "run.log", "w", encoding="utf-8") as _log_fh:
        sys.stdout = _log_fh if quiet else Tee(sys.__stdout__, _log_fh)
        try:
            doc, n_pages, g_ratio, force_ocr = convert_pdf(pdf_path)

            table_slugs, table_stats = extract_tables(doc, out_dir, pdf_path.name, species)
            picture_stats            = extract_pictures(doc, out_dir, pdf_path.name, species)
            save_document_json(doc, out_dir)
            chunks, chunk_stats      = build_chunks(doc, out_dir, pdf_path.name, species, table_slugs)
            pp_stats                 = postprocess_chunks(chunks, doc, out_dir)

            save_doc_stats(
                out_dir, pdf_path.name, species, n_pages, g_ratio, force_ocr,
                table_stats, picture_stats, chunk_stats, pp_stats,
            )
            print_summary(n_pages, table_stats, picture_stats, chunk_stats, pp_stats)
            return True
        except Exception as exc:
            print(f"ERROR: {exc}")
            return False
        finally:
            sys.stdout = sys.__stdout__


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    INPUT_DIR = Path("/home/artie-sh/repos/spiders/classifier")
    OUT_BASE  = Path("/home/artie-sh/repos/spiders/classifier_processed")

    # Number of parallel worker processes.
    # On a GPU instance (e.g. EC2 g5.2xlarge, 1× A10G) keep this at 2–4:
    # Docling shares the GPU for TableFormer, so too many workers will OOM.
    # Override at runtime: WORKERS=2 python3 scripts/pdf_processor.py
    WORKERS = int(os.environ.get("WORKERS", 4))

    pdf_files = sorted(INPUT_DIR.rglob("*.pdf"))
    if not pdf_files:
        print(f"No PDF files found under {INPUT_DIR}")
        raise SystemExit(0)

    print(f"Found {len(pdf_files)} PDF(s) under {INPUT_DIR}\n")

    def out_dir_for(pdf_path: Path) -> Path:
        """Output folder for a PDF. Flat under OUT_BASE when PDFs sit directly
        inside INPUT_DIR; one species-level subfolder otherwise."""
        if pdf_path.parent == INPUT_DIR:
            return OUT_BASE / pdf_path.stem
        return OUT_BASE / pdf_path.parent.name / pdf_path.stem

    # Separate real files from symlinks.
    # Symlinks are duplicate references to already-canonical PDFs; we process
    # only real files and recreate the symlink structure in the output tree.
    real_pdfs    = [p for p in pdf_files if not p.is_symlink()]
    symlink_pdfs = [p for p in pdf_files if p.is_symlink()]
    print(f"Real PDFs  : {len(real_pdfs)}")
    print(f"Symlinks   : {len(symlink_pdfs)} (will mirror to output tree after processing)\n")

    # Pre-scan: cheap fitz page count for PDFs still needing processing.
    # PDFs whose output already contains chunks.json are skipped.
    print("Pre-scanning page counts...", end=" ", flush=True)
    pdf_info: list[tuple[Path, int | None]] = []
    for pdf_path in real_pdfs:
        if (out_dir_for(pdf_path) / "chunks.json").exists():
            pdf_info.append((pdf_path, None))   # None → skip
        else:
            pdf_info.append((pdf_path, page_count(pdf_path)))

    to_process  = sum(1 for _, n in pdf_info if n is not None)
    skipped_pre = len(pdf_files) - to_process
    total_pages = sum(n for _, n in pdf_info if n is not None)
    print("done.")
    print(f"To process : {to_process} PDF(s), {total_pages} pages total")
    print(f"Skipping   : {skipped_pre} (chunks.json already present)")
    print(f"Workers    : {WORKERS}\n")

    ok = fail = 0
    pages_done = 0
    t_batch_start = time.perf_counter()

    # Log skipped files upfront so they don't clutter the live progress output.
    for pdf_path, n_pages in pdf_info:
        if n_pages is None:
            tqdm.write(f"SKIP  {pdf_path.parent.name}/{pdf_path.name}")

    pending = [(pdf_path, n) for pdf_path, n in pdf_info if n is not None]

    with tqdm(
        total=total_pages,
        unit="pg",
        desc="Total progress",
        dynamic_ncols=True,
        file=sys.stderr,
    ) as pbar:
        with ProcessPoolExecutor(max_workers=WORKERS) as executor:
            future_to_info = {
                executor.submit(process_pdf, pdf_path, out_dir_for(pdf_path), True): (pdf_path, n_pages)
                for pdf_path, n_pages in pending
            }

            for future in as_completed(future_to_info):
                pdf_path, n_pages = future_to_info[future]
                elapsed = time.perf_counter() - t_batch_start  # wall time so far

                try:
                    success = future.result()
                except Exception as exc:
                    tqdm.write(f"ERROR  {pdf_path.name}: {exc}")
                    success = False

                status = "OK  " if success else "FAIL"
                if success:
                    ok += 1
                else:
                    fail += 1

                pages_done += n_pages
                tqdm.write(f"{status}  {pdf_path.parent.name}/{pdf_path.name}  ({n_pages} pg)")

                pbar.update(n_pages)
                pbar.set_postfix({
                    "done":    f"{ok + fail}/{to_process}",
                    "ok/fail": f"{ok}/{fail}",
                    "pg/s":    f"{pages_done / max(elapsed, 1):.1f}",
                })

    total_elapsed = time.perf_counter() - t_batch_start
    print(f"\n{'═' * 60}")
    print(f"Batch complete:  {ok} succeeded,  {fail} failed,  {skipped_pre} skipped")
    if pages_done:
        print(f"Pages processed: {pages_done}  |  {pages_done / total_elapsed:.1f} pg/s"
              f"  |  total {total_elapsed:.0f}s")
    print(f"{'═' * 60}")

    # Mirror symlinks into the output tree.
    # For each symlink in the input, create a symlink in the output that points
    # to the canonical output folder (relative path, same as input convention).
    if symlink_pdfs:
        print(f"\nMirroring {len(symlink_pdfs)} symlink(s) into output tree...")
        sym_ok = sym_skip = sym_fail = 0
        for link_path in symlink_pdfs:
            try:
                canonical_pdf = link_path.resolve()
                canonical_out = out_dir_for(canonical_pdf)

                # Only create the symlink if the canonical output was actually produced
                if not canonical_out.exists():
                    print(f"  SKIP  {link_path.name} — canonical output not found: {canonical_out}")
                    sym_skip += 1
                    continue

                link_out = out_dir_for(link_path)
                if link_out.exists() or link_out.is_symlink():
                    sym_skip += 1
                    continue

                link_out.parent.mkdir(parents=True, exist_ok=True)
                rel_target = os.path.relpath(canonical_out, link_out.parent)
                link_out.symlink_to(rel_target)
                print(f"  LINK  {link_out.parent.name}/{link_out.name} → {rel_target}")
                sym_ok += 1
            except Exception as exc:
                print(f"  FAIL  {link_path.name}: {exc}")
                sym_fail += 1

        print(f"Symlinks:  {sym_ok} created,  {sym_skip} skipped,  {sym_fail} failed")
