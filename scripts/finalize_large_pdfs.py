#!/usr/bin/env python3
"""
Process split large PDFs from FINALIZATION_DIR and merge results into PROCESSED_DIR.

Expects FINALIZATION_DIR to contain subfolders (one per original doc) with
partXX.pdf files as produced by split_pdfs.py.

For each subfolder:
  1. Finds the original document's species by scanning PARDOSA_DIR
  2. Runs pdf_processor on the subfolder (all parts processed together)
  3. Merges part outputs with correct page offsets into
     PROCESSED_DIR/<species>/<doc_stem>/

Usage:
    python3 scripts/finalize_large_pdfs.py

Page numbers in chunks, and image/table filenames, are offset so the merged
output looks as if the whole document was processed as one.
"""

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REPO_ROOT        = Path(__file__).resolve().parent.parent
FINALIZATION_DIR = Path(os.environ.get("FINALIZATION_DIR", str(REPO_ROOT / "finalization")))
PARDOSA_DIR      = Path(os.environ.get("PARDOSA_DIR",      str(REPO_ROOT / "pardosa")))
PROCESSED_DIR    = Path(os.environ.get("PROCESSED_DIR",   str(REPO_ROOT / "pardosa_processed")))

# Optional JSON file mapping doc_stem -> species (used instead of scanning PARDOSA_DIR)
_SPECIES_MAP_FILE = Path(os.environ.get("SPECIES_MAP", str(Path(__file__).parent / "species_map.json")))
_SPECIES_MAP: dict | None = None
if _SPECIES_MAP_FILE.exists():
    import json as _json
    _SPECIES_MAP = _json.loads(_SPECIES_MAP_FILE.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def find_species(doc_stem: str) -> str:
    """Return species for doc_stem, using map file if available, else scanning PARDOSA_DIR."""
    if _SPECIES_MAP is not None:
        return _SPECIES_MAP.get(doc_stem, "-")
    for pdf in PARDOSA_DIR.rglob("*.pdf"):
        if pdf.stem == doc_stem and not pdf.is_symlink():
            return pdf.parent.name
    return "-"


def offset_page_filename(filename: str, offset: int) -> str:
    """
    Add offset to the page number embedded in a filename.
    e.g. page_001_img_1.png  + offset 50 → page_051_img_1.png
    """
    m = re.match(r"(page_)(\d+)(_.*)", filename)
    if not m:
        return filename
    new_no = int(m.group(2)) + offset
    return f"page_{new_no:03d}{m.group(3)}"


def offset_page_path(path_str: str, offset: int) -> str:
    """Apply offset to the filename component of a relative path."""
    p = Path(path_str)
    return str(p.parent / offset_page_filename(p.name, offset))


def copy_assets(src_dir: Path, dst_dir: Path, subdir: str, offset: int) -> None:
    """
    Copy all files from src_dir/subdir → dst_dir/subdir, renaming
    page_NNN_ prefixes with the given page offset.
    """
    src = src_dir / subdir
    dst = dst_dir / subdir
    if not src.exists():
        return
    dst.mkdir(parents=True, exist_ok=True)
    for f in src.iterdir():
        new_name = offset_page_filename(f.name, offset)
        shutil.copy2(f, dst / new_name)


def apply_offset_to_chunks(chunks: list[dict], offset: int) -> list[dict]:
    """Return a new chunk list with all page numbers and asset paths offset."""
    result = []
    for chunk in chunks:
        c = dict(chunk)
        c["page_numbers"] = [p + offset for p in c.get("page_numbers", [])]
        c["figures"] = [
            {**fig, "image_file": offset_page_path(fig["image_file"], offset)}
            if "image_file" in fig else fig
            for fig in c.get("figures", [])
        ]
        c["tables"] = [
            {**tbl, "csv_file": offset_page_path(tbl["csv_file"], offset)}
            if "csv_file" in tbl else tbl
            for tbl in c.get("tables", [])
        ]
        result.append(c)
    return result


def merge_doc_stats(stats_list: list[dict], source_file: str, species: str) -> dict:
    """Merge a list of doc_stats dicts into one, summing counts."""
    merged = {
        "source_file": source_file,
        "species":     species,
        "page_count":  sum(s.get("page_count", 0) for s in stats_list),
        "garbled_ratio": (
            sum(s.get("garbled_ratio", 0) * s.get("page_count", 0) for s in stats_list)
            / max(sum(s.get("page_count", 0) for s in stats_list), 1)
        ),
        "force_ocr": any(s.get("force_ocr", False) for s in stats_list),
        "chunks": {
            "total":        sum(s["chunks"]["total"]        for s in stats_list),
            "with_figures": sum(s["chunks"]["with_figures"] for s in stats_list),
            "with_tables":  sum(s["chunks"]["with_tables"]  for s in stats_list),
        },
        "tables": {
            "total": sum(s["tables"]["total"] for s in stats_list),
            "ok":    sum(s["tables"]["ok"]    for s in stats_list),
            "fail":  sum(s["tables"]["fail"]  for s in stats_list),
        },
        "images": {
            "total":   sum(s["images"]["total"]   for s in stats_list),
            "saved":   sum(s["images"]["saved"]   for s in stats_list),
            "skipped": sum(s["images"]["skipped"] for s in stats_list),
            "fail":    sum(s["images"]["fail"]    for s in stats_list),
        },
    }
    return merged


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_doc(subfolder: Path) -> bool:
    doc_stem = subfolder.name
    parts = sorted(subfolder.glob("part*.pdf"))
    if not parts:
        print(f"  No part*.pdf files found in {subfolder.name}, skipping.")
        return False

    # Find species
    species = find_species(doc_stem)
    print(f"  Species  : {species}")

    # Skip if already merged into PROCESSED_DIR
    final_dir = PROCESSED_DIR / species / doc_stem
    if (final_dir / "chunks.json").exists():
        chunks = json.loads((final_dir / "chunks.json").read_text(encoding="utf-8"))
        if chunks:
            print(f"  SKIP — already processed ({len(chunks)} chunks)")
            return True

    print(f"  Parts    : {[p.name for p in parts]}")

    # Part outputs go into subfolder/processed/
    part_out_base = subfolder / "processed"
    part_out_base.mkdir(exist_ok=True)

    # Run pdf_processor on this subfolder
    env = {
        **os.environ,
        "PDF_INPUT_DIR": str(subfolder),
        "PDF_OUT_BASE":  str(part_out_base),
        "MAX_PAGES":     "0",
        "SKIP_IMAGEONLY": "0",
        "WORKERS":       os.environ.get("WORKERS", "2"),
    }
    print(f"  Running pdf_processor...")
    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "pdf_processor.py")],
        env=env,
    )
    if result.returncode != 0:
        print(f"  ERROR: pdf_processor exited with code {result.returncode}")
        return False

    # Collect part output dirs in order — all parts must succeed before merging
    part_out_dirs = []
    missing_parts = []
    for part in parts:
        out_dir = part_out_base / part.stem
        if not (out_dir / "chunks.json").exists():
            missing_parts.append(part.name)
        else:
            part_out_dirs.append(out_dir)

    if missing_parts:
        print(f"  PENDING — {len(missing_parts)} part(s) not yet done: {missing_parts}")
        print(f"  Will merge on next run once all parts succeed.")
        return False

    if not part_out_dirs:
        print(f"  ERROR: no parts processed successfully")
        return False

    # Compute page offsets: each part's offset = sum of page counts of preceding parts
    offsets = []
    running = 0
    for out_dir in part_out_dirs:
        offsets.append(running)
        stats = json.loads((out_dir / "doc_stats.json").read_text(encoding="utf-8"))
        running += stats["page_count"]

    # Final output dir (already set above, just ensure it exists)
    final_dir.mkdir(parents=True, exist_ok=True)

    # Merge chunks
    all_chunks = []
    all_stats  = []
    for out_dir, offset in zip(part_out_dirs, offsets):
        chunks = json.loads((out_dir / "chunks.json").read_text(encoding="utf-8"))
        all_chunks.extend(apply_offset_to_chunks(chunks, offset))
        all_stats.append(json.loads((out_dir / "doc_stats.json").read_text(encoding="utf-8")))

    (final_dir / "chunks.json").write_text(
        json.dumps(all_chunks, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    merged_stats = merge_doc_stats(all_stats, doc_stem + ".pdf", species)
    (final_dir / "doc_stats.json").write_text(
        json.dumps(merged_stats, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print(f"  Merged   : {len(all_chunks)} chunks, {merged_stats['page_count']} pages → {final_dir}")
    return True


def main():
    subfolders = sorted(
        p for p in FINALIZATION_DIR.iterdir()
        if p.is_dir() and not p.name.startswith(".")
    )
    if not subfolders:
        print("No subfolders found in finalization/")
        sys.exit(0)

    print(f"Found {len(subfolders)} doc(s) to finalize\n")
    ok = fail = 0
    for subfolder in subfolders:
        print(f"=== {subfolder.name[:80]} ===")
        if process_doc(subfolder):
            ok += 1
        else:
            fail += 1
        print()

    print(f"Done: {ok} succeeded, {fail} failed.")


if __name__ == "__main__":
    main()
