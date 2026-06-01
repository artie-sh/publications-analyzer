#!/usr/bin/env python3
"""
Recover chunks.json and doc_stats.json for output dirs that have document.json
but are missing chunks.json (processing was interrupted after PDF conversion but
before the chunking step).

Usage:
    python3 scripts/recover_chunks.py <processed_dir> [<input_dir>]

Example:
    python3 scripts/recover_chunks.py pardosa_processed/ pardosa/

<processed_dir>  Output tree (e.g. pardosa_processed/).
<input_dir>      Original PDF library (e.g. pardosa/). Used to compute
                 garbled_ratio; falls back to 0.0 if PDF not found or omitted.
"""

import json
import sys
from pathlib import Path

import fitz

# ---------------------------------------------------------------------------
# Import reusable functions from pdf_processor
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent))
from pdf_processor import (
    build_chunks,
    get_page_numbers,
    looks_garbled,
    postprocess_chunks,
    save_doc_stats,
)

from docling_core.types.doc import DoclingDocument, TableItem


def reconstruct_table_slugs(doc: DoclingDocument, tables_subdir: str = "tables") -> dict[str, str]:
    """
    Reconstruct the table_slugs mapping (self_ref → relative csv path) using
    the same slug logic as extract_tables(), so chunk table entries stay consistent.
    """
    page_counters: dict[int, int] = {}
    table_slugs: dict[str, str] = {}
    for item, _ in doc.iterate_items():
        if not isinstance(item, TableItem):
            continue
        pages = get_page_numbers(item)
        page_no = pages[0] if pages else 0
        page_counters[page_no] = page_counters.get(page_no, 0) + 1
        slug = f"page_{page_no:03d}_tbl_{page_counters[page_no]}"
        table_slugs[item.self_ref] = f"{tables_subdir}/{slug}.csv"
    return table_slugs


def compute_garbled_ratio(pdf_path: Path) -> float:
    try:
        doc = fitz.open(str(pdf_path))
        garbled, total = 0, 0
        for page in doc:
            text = page.get_text()
            nws = sum(1 for c in text if not c.isspace())
            if nws >= 50:
                total += 1
                if looks_garbled(text):
                    garbled += 1
        doc.close()
        return garbled / total if total else 0.0
    except Exception:
        return 0.0


def recover_dir(doc_dir: Path, input_dir: Path | None) -> bool:
    doc_json_path = doc_dir / "document.json"
    if not doc_json_path.exists():
        return False
    chunks_path = doc_dir / "chunks.json"
    if chunks_path.exists():
        try:
            chunks = json.loads(chunks_path.read_text(encoding="utf-8"))
            if chunks:  # non-empty list → already done
                return False
        except Exception:
            pass  # unreadable → fall through and recover

    print(f"  Recovering {doc_dir.parent.name}/{doc_dir.name} ...")

    try:
        raw = json.loads(doc_json_path.read_text(encoding="utf-8"))
        # Patch document version to match local SDK schema if needed
        if "version" in raw:
            raw["version"] = DoclingDocument.model_fields["version"].default
        doc = DoclingDocument.model_validate(raw)
    except Exception as exc:
        print(f"    ERROR loading document.json: {exc}")
        return False

    # Reconstruct table_slugs
    table_slugs = reconstruct_table_slugs(doc)

    # Page count and garbled ratio
    n_pages = len(doc.pages) if doc.pages else 0

    # Try to find the original PDF for garbled ratio
    g_ratio = 0.0
    if input_dir is not None:
        species   = doc_dir.parent.name
        pdf_stem  = doc_dir.name
        # Try species subdir first, then root
        for candidate in [
            input_dir / species / f"{pdf_stem}.pdf",
            input_dir / f"{pdf_stem}.pdf",
        ]:
            if candidate.exists():
                g_ratio = compute_garbled_ratio(candidate)
                break

    source_file = doc_dir.name + ".pdf"
    species     = doc_dir.parent.name

    # Table stats from saved meta files
    tables_dir = doc_dir / "tables"
    table_meta_files = list(tables_dir.glob("*_meta.json")) if tables_dir.exists() else []
    table_stats = {
        "total": len(table_meta_files),
        "ok":    len(table_meta_files),
        "fail":  0,
    }

    # Picture stats from saved meta files
    images_dir = doc_dir / "images"
    image_meta_files = list(images_dir.glob("*_meta.json")) if images_dir.exists() else []
    picture_stats = {
        "total":   len(image_meta_files),
        "ok":      len(image_meta_files),
        "skipped": 0,
        "fail":    0,
    }

    try:
        chunks, chunk_stats = build_chunks(doc, doc_dir, source_file, species, table_slugs)
        pp_stats            = postprocess_chunks(chunks, doc, doc_dir)
        save_doc_stats(
            doc_dir, source_file, species, n_pages, g_ratio, False,
            table_stats, picture_stats, chunk_stats, pp_stats,
        )
        print(f"    OK — {chunk_stats['total']} chunks, {n_pages} pages")
        return True
    except Exception as exc:
        print(f"    ERROR during recovery: {exc}")
        return False


def main():
    processed_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else PROCESSED_DIR
    input_dir     = Path(sys.argv[2]) if len(sys.argv) > 2 else INPUT_DIR

    if not processed_dir.is_dir():
        print(f"ERROR: {processed_dir} not found")
        sys.exit(1)

    # Find all dirs with document.json but missing or empty chunks.json
    def needs_recovery(doc_dir: Path) -> bool:
        chunks_path = doc_dir / "chunks.json"
        if not chunks_path.exists():
            return True
        try:
            return not json.loads(chunks_path.read_text(encoding="utf-8"))
        except Exception:
            return True  # unreadable

    candidates = [
        p.parent
        for p in sorted(processed_dir.rglob("document.json"))
        if needs_recovery(p.parent)
    ]

    if not candidates:
        print("Nothing to recover.")
        sys.exit(0)

    print(f"Found {len(candidates)} dir(s) to recover.\n")
    ok = fail = 0
    for doc_dir in candidates:
        if recover_dir(doc_dir, input_dir):
            ok += 1
        else:
            fail += 1

    print(f"\nDone: {ok} recovered, {fail} failed.")


REPO_ROOT     = Path(__file__).resolve().parent.parent
PROCESSED_DIR = REPO_ROOT / "pardosa_processed"
INPUT_DIR     = REPO_ROOT / "pardosa"

if __name__ == "__main__":
    main()
