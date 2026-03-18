#!/usr/bin/env python3
"""
LlamaParse batch parsing script.

Scans INPUT_DIR recursively for PDFs, parses each with LlamaParse, and writes
output to OUTPUT_DIR mirroring the source subfolder structure:

  OUTPUT_DIR/
    <subfolder>/
      <pdf_stem>-<tier>/       (truncated to 240 chars if needed)
        chunks.json
        raw_response.json
        images/
          <image_name>.png
          <image_name>_meta.json
        tables/
          <table_name>.csv
          <table_name>_meta.json

Skip logic: chunks.json presence = already parsed; skipped on re-run.

Requires:
  pip install llama-cloud-services pandas --break-system-packages
  export LLAMA_CLOUD_API_KEY='llx-...'
"""
from PIL import Image
import numpy as np
import json
import os
import sys
from pathlib import Path

import pandas as pd
from llama_cloud_services import LlamaParse

MAX_FOLDER_LEN = 240  # max output folder name length (OS limit is 255 bytes)


# ---------------------------------------------------------------------------
# Output directory helpers
# ---------------------------------------------------------------------------

def get_output_base(pdf_path: Path, input_dir: Path, output_dir: Path, tier: str) -> Path:
    subfolder = pdf_path.relative_to(input_dir).parent
    folder_name = f"{pdf_path.stem}-{tier}"
    if len(folder_name) > MAX_FOLDER_LEN:
        folder_name = folder_name[:MAX_FOLDER_LEN]
    return output_dir / subfolder / folder_name


def setup_output_dirs(base: Path) -> dict:
    dirs = {
        "base": base,
        "images": base / "images",
        "tables": base / "tables",
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return dirs


def is_done(pdf_path: Path, input_dir: Path, output_dir: Path, tier: str) -> bool:
    base = get_output_base(pdf_path, input_dir, output_dir, tier)
    return (base / "chunks.json").exists()


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def save_chunks(pages: list, out_dir: Path):
    chunks = []
    for page in pages:
        chunks.append({
            "chunk_id": f"page_{page.get('page', 0)}",
            "page_number": page.get("page"),
            "text": page.get("text", ""),
            "md": page.get("md", ""),
            "items": page.get("items", []),
        })
    out_path = out_dir / "chunks.json"
    out_path.write_text(json.dumps(chunks, indent=2, ensure_ascii=False))
    print(f"  [chunks] Saved {len(chunks)} chunks -> {out_path}")


def save_images(json_objs: list, parser: LlamaParse, images_dir: Path, source_file: str, pages: list):
    # Build lookup: clean image stem -> page_number + text items for caption matching
    page_image_map = {}
    for page in pages:
        page_num = page.get("page")
        text_items = [it.get("value", "") for it in page.get("items", []) if it.get("type") == "text"]
        for img in page.get("images", []):
            if img.get("type") == "full_page_screenshot":
                continue
            name = img.get("name", "")
            stem = Path(name).stem
            page_image_map[stem] = {
                "page_number": page_num,
                "text_items": text_items,
                "original_meta": img,
            }

    image_dicts = parser.get_images(json_objs, download_path=str(images_dir))

    saved = 0
    for img in image_dicts:
        img_path = Path(img["path"])

        # Strip job_id UUID prefix: downloaded name is "<uuid>-img_p11_1.jpg"
        raw_stem = img_path.stem
        if len(raw_stem) > 36 and raw_stem[36] == '-':
            clean_name = img_path.name[37:]
            new_path = images_dir / clean_name
            img_path.rename(new_path)
            img_path = new_path
        clean_stem = img_path.stem

        if clean_stem not in page_image_map:
            img_path.unlink(missing_ok=True)
            continue

        page_data = page_image_map[clean_stem]
        page_num = page_data["page_number"]
        original_meta = page_data["original_meta"]

        fix_inverted_image(img_path)

        caption = _find_caption(page_data["text_items"])

        meta = {
            "caption": caption,
            "page_number": page_num,
            "image_ref": img_path.name,
            "source_file": source_file,
            "width": original_meta.get("width"),
            "height": original_meta.get("height"),
            "original_meta": original_meta,
        }
        meta_path = images_dir / f"{clean_stem}_meta.json"
        meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False))
        saved += 1

    print(f"  [images] Saved {saved} images -> {images_dir}")


def fix_inverted_image(img_path: Path) -> None:
    try:
        img = Image.open(img_path).convert("RGB")
        arr = np.array(img)
        if arr.mean() < 127:
            Image.fromarray(255 - arr).save(img_path)
    except Exception as e:
        print(f"  [images] Warning: could not check inversion for {img_path.name}: {e}")


def _find_caption(text_items: list) -> str:
    fig_candidates = [t for t in text_items if t.strip().startswith(("Fig", "Рис"))]
    if fig_candidates:
        return " | ".join(fig_candidates)
    return ""


def save_tables(pages: list, tables_dir: Path, source_file: str):
    table_idx = 0
    for page in pages:
        page_num = page.get("page", 0)
        for item in page.get("items", []):
            if item.get("type") != "table":
                continue

            rows = item.get("rows", [])
            table_name = f"table_{table_idx:04d}_page{page_num}"

            if rows:
                try:
                    df = pd.DataFrame(rows[1:], columns=rows[0]) if len(rows) > 1 else pd.DataFrame(rows)
                    df.to_csv(tables_dir / f"{table_name}.csv", index=False)
                except Exception as e:
                    pd.DataFrame(rows).to_csv(tables_dir / f"{table_name}.csv", index=False, header=False)
                    print(f"  [tables] Warning: fallback CSV for {table_name}: {e}")
            else:
                (tables_dir / f"{table_name}.csv").write_text(item.get("md", ""))

            meta = {
                "table_name": table_name,
                "page_number": page_num,
                "source_file": source_file,
                "bbox": item.get("bBox"),
                "row_count": len(rows),
                "col_count": len(rows[0]) if rows else 0,
                "markdown": item.get("md", ""),
            }
            (tables_dir / f"{table_name}_meta.json").write_text(
                json.dumps(meta, indent=2, ensure_ascii=False)
            )
            table_idx += 1

    print(f"  [tables] Saved {table_idx} tables -> {tables_dir}")


# ---------------------------------------------------------------------------
# Per-PDF processing
# ---------------------------------------------------------------------------

def run_tier(pdf_path: Path, tier: str, api_key: str, version: str,
             input_dir: Path, output_dir: Path):
    base = get_output_base(pdf_path, input_dir, output_dir, tier)
    dirs = setup_output_dirs(base)

    parser = LlamaParse(
        api_key=api_key,
        tier=tier,
        version=version,
        result_type="json",
        verbose=True,
    )

    if USE_RAW_RESPONSE:
        raw_path = dirs["base"] / "raw_response.json"
        if not raw_path.exists():
            print(f"  Error: USE_RAW_RESPONSE=True but {raw_path} does not exist.", file=sys.stderr)
            return False
        json_objs = json.loads(raw_path.read_text())
        print(f"  [raw] Loaded existing response from {raw_path}")
    else:
        json_objs = parser.get_json_result(str(pdf_path))

        if not json_objs:
            print(f"  Error: LlamaParse returned empty result.", file=sys.stderr)
            return False

        raw_path = dirs["base"] / "raw_response.json"
        raw_path.write_text(json.dumps(json_objs, indent=2, ensure_ascii=False))
        print(f"  [raw] Full API response saved -> {raw_path}")

    pages = json_objs[0].get("pages", [])
    source_file = pdf_path.name

    save_chunks(pages, dirs["base"])
    save_images(json_objs, parser, dirs["images"], source_file, pages)
    save_tables(pages, dirs["tables"], source_file)

    print(f"  [done] {dirs['base']}")
    return True


# ---------------------------------------------------------------------------
# Main — batch processing
# ---------------------------------------------------------------------------

def main():
    if not API_KEY:
        print("Error: LLAMA_CLOUD_API_KEY not set.", file=sys.stderr)
        sys.exit(1)

    input_dir = Path(INPUT_DIR).resolve()
    output_dir = Path(OUTPUT_DIR).resolve()

    if not input_dir.exists():
        print(f"Error: INPUT_DIR not found: {input_dir}", file=sys.stderr)
        sys.exit(1)

    # Collect all (pdf, tier) work items
    all_pdfs = sorted(input_dir.rglob("*.pdf"))
    if not all_pdfs:
        print(f"No PDFs found under {input_dir}")
        sys.exit(0)

    # Build full work list and split into done / pending
    all_work  = [(pdf, tier) for pdf in all_pdfs for tier in TIERS]
    done_work = [(pdf, tier) for pdf, tier in all_work if is_done(pdf, input_dir, output_dir, tier)]
    todo_work = [(pdf, tier) for pdf, tier in all_work if not is_done(pdf, input_dir, output_dir, tier)]

    total       = len(all_work)
    n_done      = len(done_work)
    n_todo      = len(todo_work)
    session_ok  = 0
    session_fail = 0

    print(f"Found {len(all_pdfs)} PDF(s) × {len(TIERS)} tier(s) = {total} items total.")
    print(f"Already done: {n_done}  |  To process this session: {n_todo}")
    if n_todo == 0:
        print("Nothing to do.")
        return

    for idx, (pdf_path, tier) in enumerate(todo_work, start=1):
        rel = pdf_path.relative_to(input_dir)
        total_done_so_far = n_done + session_ok + session_fail

        print(f"\n{'='*64}")
        print(
            f"[session {idx}/{n_todo} | total {total_done_so_far + 1}/{total}]"
            f"  {rel}  (tier: {tier})"
        )
        print(f"{'='*64}")

        try:
            ok = run_tier(pdf_path, tier, API_KEY, VERSION, input_dir, output_dir)
            if ok:
                session_ok += 1
            else:
                session_fail += 1
        except Exception as exc:
            print(f"  ERROR: {exc}", file=sys.stderr)
            session_fail += 1

    total_done_final = n_done + session_ok
    print(f"\n{'='*64}")
    print(f"Session complete: {session_ok} parsed, {session_fail} failed.")
    print(f"Total done: {total_done_final}/{total}")


# --- CONFIG ---
INPUT_DIR  = "/home/artie-sh/repos/spiders/pardosa"
OUTPUT_DIR = "/home/artie-sh/repos/spiders/pardosa_parsed"
TIERS   = ["cost_effective"]
VERSION = "latest"     # "latest" or a pinned date e.g. "2026-01-08"
API_KEY = os.environ.get("LLAMA_CLOUD_API_KEY")
USE_RAW_RESPONSE = False  # True: skip API call, reload existing raw_response.json
# --------------

if __name__ == "__main__":
    main()
