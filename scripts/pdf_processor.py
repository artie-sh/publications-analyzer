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
import re
import sys
from pathlib import Path


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

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.pdf_analyzer import looks_garbled  # noqa: E402

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

    Returns (DoclingDocument, page_count).
    """
    if ocr_langs is None:
        ocr_langs = ["eng", "rus", "deu", "spa", "fra", "ita", "lat"]

    ratio, page_count = garbled_ratio(source)
    force_ocr = ratio > garbled_threshold
    print(
        f"Pre-check: {ratio:.0%} of pages have garbled native text"
        f"  →  force_full_page_ocr={force_ocr}"
    )

    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = True
    pipeline_options.ocr_options = EasyOcrOptions()
    pipeline_options.do_table_structure = True
    pipeline_options.table_structure_options.mode = table_mode
    pipeline_options.generate_picture_images = True
    pipeline_options.generate_table_images = False
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
    return result.document, page_count


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

    for i, table in enumerate(tables, 1):
        caption = table.caption_text(doc) or ""
        slug = make_slug(caption) if caption else f"table_{i}"
        csv_name = f"{slug}.csv"
        table_slugs[table.self_ref] = f"{tables_subdir}/{csv_name}"
        pages = get_page_numbers(table)

        try:
            df = table.export_to_dataframe()
            df.to_csv(tables_dir / csv_name, index=False)

            meta = {
                "caption": caption,
                "page_number": pages[0] if pages else None,
                "rows": df.shape[0],
                "cols": df.shape[1],
                "source_file": source_file,
                "species": species,
            }
            (tables_dir / f"{slug}_meta.json").write_text(
                json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            print(
                f"  Table {i}: {caption[:80] or '(no caption)'}"
                f"  [{df.shape[0]}×{df.shape[1]}]  p.{pages}  ✓"
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
    ok = fail = 0

    for i, pic in enumerate(pictures, 1):
        caption = pic.caption_text(doc) or ""
        slug = make_slug(caption) if caption else f"figure_{i}"
        pages = get_page_numbers(pic)

        print(f"  Picture {i}: {caption[:120] or '(no caption)'}  p.{pages}")
        try:
            img = pic.get_image(doc)
            if img:
                img.save(images_dir / f"{slug}.png")
            else:
                print("    (no image data)")

            meta = {
                "caption": caption,
                "page_number": pages[0] if pages else None,
                "picture_ref": pic.self_ref,
                "source_file": source_file,
                "species": species,
            }
            (images_dir / f"{slug}_meta.json").write_text(
                json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            ok += 1
        except Exception as e:
            print(f"    image error: {e}")
            fail += 1

    return {"total": len(pictures), "ok": ok, "fail": fail}


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

    Returns stats dict.
    """
    images_dir = out_dir / "images"
    print(f"\n=== Post-processing Chunks ===")

    fuzzy_injected      = fix_figure_refs(chunks, doc, fuzzy_threshold)
    image_files_resolved = resolve_image_files(chunks, images_dir)
    dedup_per_chunk(chunks)
    cross_chunk_removed = dedup_cross_chunk(chunks)
    fallback_injected   = inject_fallback_figures(chunks, images_dir)
    if fallback_injected:
        cross_chunk_removed += dedup_cross_chunk(chunks)

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
  Pictures: {picture_stats['total']} total  ({picture_stats['ok']} ok, {picture_stats['fail']} failed)
  Chunks:   {chunk_stats['total']}
    with figures : {chunk_stats['with_figures']}
    with tables  : {chunk_stats['with_tables']}
  Post-processing:
    figure refs injected (fuzzy)    : {pp_stats['fuzzy_injected']}
    figure refs injected (fallback) : {pp_stats['fallback_injected']}
    image files resolved            : {pp_stats['image_files_resolved']}
    cross-chunk duplicates removed  : {pp_stats['cross_chunk_removed']}
""")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # SOURCE = Path(
    #     "/home/artie-sh/repos/spiders/exp_clean/Pardosa_abagensis/"
    #     "Tikader_B_K_1977d_Description_of_two_new_species_of_wolf-spider_family_Lycosidae_from_Ladakh_India_1.pdf"
    # )
    SOURCE = Path(
        "/home/artie-sh/repos/spiders/exp_clean/Pardosa_abagensis/"
        "Nadolny_A_A__Kovblyuk_M_M_2012_Members_of_Pardosa_amentata_and_P_lugubris_species_groups_in_Crimea_and_Caucasus_with_notes_on_P_abagensis_Aranei_Lycosidae.pdf"
    )
    OUT_DIR = Path("/home/artie-sh/repos/spiders/exp_clean_processed/exp_docling11")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with open(OUT_DIR / "run.log", "w", encoding="utf-8") as _log_fh:
        sys.stdout = Tee(sys.__stdout__, _log_fh)
        try:
            doc, page_count = convert_pdf(SOURCE)
            species = SOURCE.parent.name

            table_slugs, table_stats = extract_tables(doc, OUT_DIR, SOURCE.name, species)
            picture_stats = extract_pictures(doc, OUT_DIR, SOURCE.name, species)
            save_document_json(doc, OUT_DIR)
            chunks, chunk_stats = build_chunks(doc, OUT_DIR, SOURCE.name, species, table_slugs)
            pp_stats = postprocess_chunks(chunks, doc, OUT_DIR)

            print_summary(page_count, table_stats, picture_stats, chunk_stats, pp_stats)
        finally:
            sys.stdout = sys.__stdout__
