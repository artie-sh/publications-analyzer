#!/usr/bin/env python3
"""
Classify processed documents using the Pardosa Literature Classifier prompt.

Usage:
    ANTHROPIC_API_KEY=sk-... python3 scripts/classify.py [PROCESSED_DIR] [OUTPUT_CSV]

Defaults:
    PROCESSED_DIR = pardosa_processed/  (relative to repo root)
    OUTPUT_CSV    = classification_results.csv  (relative to repo root)

Works with both flat (classifier_processed/) and nested (pardosa_processed/) structures.
Docs without chunks.json are written to CSV with status=failed (no API call).
Supports resume: already-written rows are skipped on restart.
"""

import csv
import json
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import anthropic

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_prompt() -> str:
    return PROMPT_FILE.read_text(encoding="utf-8")


def sample_chunks(chunks: list[dict], max_n: int) -> list[dict]:
    """Return up to max_n chunks spread evenly across the document."""
    if len(chunks) <= max_n:
        return chunks
    step = len(chunks) / max_n
    indices = {int(i * step) for i in range(max_n)}
    return [chunks[i] for i in sorted(indices)]


def build_user_message(doc_dir: Path) -> str:
    doc_stats = json.loads((doc_dir / "doc_stats.json").read_text(encoding="utf-8"))
    chunks    = json.loads((doc_dir / "chunks.json").read_text(encoding="utf-8"))
    sampled   = sample_chunks(chunks, MAX_CHUNKS)

    parts = [
        "## doc_stats.json\n```json",
        json.dumps(doc_stats, indent=2, ensure_ascii=False),
        "```\n",
        f"## chunks.json  ({len(chunks)} total chunks; showing {len(sampled)} sampled evenly)\n```json",
        json.dumps(sampled, indent=2, ensure_ascii=False),
        "```",
    ]
    return "\n".join(parts)


def parse_result(text: str) -> tuple[float | None, int | None]:
    """Extract weighted total and category from model response.

    Handles formats like:
      Weighted total: ... = **1.70**
      Category: **1**
    """
    # Score: find **X.XX** after "weighted total" (with or without = sign)
    score_match = re.search(
        r"weighted total.*?(?:=\s*)?\*\*([0-9]+\.?[0-9]*)\*\*",
        text, re.IGNORECASE | re.DOTALL
    )
    # Category: find Category: **X** (with optional bold markers)
    cat_match = re.search(
        r"category[:\s*]+\*{0,2}([1-4])\*{0,2}",
        text, re.IGNORECASE
    )
    score = float(score_match.group(1)) if score_match else None
    cat   = int(cat_match.group(1))     if cat_match   else None
    return score, cat


def species_and_doc(doc_dir: Path, processed_dir: Path) -> tuple[str, str]:
    """Return (species, document_name) for a doc dir."""
    rel = doc_dir.relative_to(processed_dir)
    parts = rel.parts
    if len(parts) == 1:
        return "-", parts[0]
    return parts[0], parts[1]


def out_dir_for(pdf_path: Path, input_dir: Path, output_base: Path) -> Path:
    """Mirror of pdf_processor.py's out_dir_for logic."""
    if pdf_path.parent == input_dir:
        return output_base / pdf_path.stem
    return output_base / pdf_path.parent.name / pdf_path.stem


def collect_docs(
    processed_dir: Path,
    input_dir: Path | None = None,
) -> tuple[list[Path], list[Path], list[Path]]:
    """
    Return (ready, failed, not_attempted):
      ready         — output dirs with chunks.json  → will be classified
      failed        — output dirs without chunks.json → were attempted, didn't finish
      not_attempted — input PDFs with no output dir at all (SKIP_OCR / early crash)

    If input_dir is provided, cross-references against the input PDF library so that
    every input PDF appears in exactly one of the three lists.
    """
    # Build index of all output dirs
    ready, failed = [], []
    for stats_file in sorted(processed_dir.rglob("doc_stats.json")):
        doc_dir = stats_file.parent
        if (doc_dir / "chunks.json").exists():
            ready.append(doc_dir)
        else:
            failed.append(doc_dir)

    if input_dir is None:
        return ready, failed, []

    # Cross-reference: find input PDFs whose expected output dir doesn't exist
    known_out_dirs = {d for d in ready + failed}
    not_attempted = []
    for pdf_path in sorted(input_dir.rglob("*.pdf")):
        if pdf_path.is_symlink():
            continue
        expected = out_dir_for(pdf_path, input_dir, processed_dir)
        if expected not in known_out_dirs:
            not_attempted.append(pdf_path)

    return ready, failed, not_attempted


def load_done_keys(csv_path: Path) -> set[tuple[str, str]]:
    """Return set of (species, document) already written to CSV."""
    if not csv_path.exists():
        return set()
    done = set()
    with csv_path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            done.add((row["species"], row["document"]))
    return done


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    processed_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else PROCESSED_DIR
    csv_path      = Path(sys.argv[2]) if len(sys.argv) > 2 else OUTPUT_CSV

    if not processed_dir.is_dir():
        print(f"ERROR: directory not found: {processed_dir}", file=sys.stderr)
        sys.exit(1)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set.", file=sys.stderr)
        sys.exit(1)

    client        = anthropic.Anthropic(api_key=api_key)
    system_prompt = load_prompt()

    # Use pardosa/ as input source of truth if it exists next to pardosa_processed/
    input_dir = processed_dir.parent / processed_dir.name.replace("_processed", "")
    if not input_dir.is_dir():
        input_dir = None

    ready, failed, not_attempted = collect_docs(processed_dir, input_dir)
    done_keys = load_done_keys(csv_path)

    total = len(ready) + len(failed) + len(not_attempted)
    print(f"Processed (ready)  : {len(ready)}")
    print(f"Failed (attempted) : {len(failed)}")
    print(f"Not attempted      : {len(not_attempted)}")
    print(f"Total              : {total}")
    print(f"Output CSV         : {csv_path}")
    if done_keys:
        print(f"Resuming           : {len(done_keys)} already written, skipping")
    print()

    write_header = not csv_path.exists()
    csv_file = csv_path.open("a", encoding="utf-8", newline="")
    writer   = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
    if write_header:
        writer.writeheader()

    done_count = len(done_keys)

    # --- Write failed docs (no API call needed) ---
    for doc_dir in failed:
        species, document = species_and_doc(doc_dir, processed_dir)
        if (species, document) in done_keys:
            continue
        writer.writerow({
            "species":   species,
            "document":  document,
            "score":     "",
            "category":  "",
            "status":    "failed",
            "reasoning": "",
        })
        csv_file.flush()
        done_count += 1
        print(f"[{done_count:4}/{total}] FAILED        {species}/{document}")

    # --- Write not-attempted docs (no API call needed) ---
    for pdf_path in not_attempted:
        species  = pdf_path.parent.name if pdf_path.parent != input_dir else "-"
        document = pdf_path.stem
        if (species, document) in done_keys:
            continue
        writer.writerow({
            "species":   species,
            "document":  document,
            "score":     "",
            "category":  "",
            "status":    "not_attempted",
            "reasoning": "",
        })
        csv_file.flush()
        done_count += 1
        print(f"[{done_count:4}/{total}] NOT_ATTEMPTED {species}/{document}")

    # --- Classify ready docs ---
    for doc_dir in ready:
        species, document = species_and_doc(doc_dir, processed_dir)
        if (species, document) in done_keys:
            continue

        user_msg = build_user_message(doc_dir)

        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=1024,
                system=system_prompt,
                messages=[{"role": "user", "content": user_msg}],
            )
            reply = response.content[0].text
        except (
            anthropic.AuthenticationError,
            anthropic.PermissionDeniedError,
        ) as exc:
            csv_file.close()
            print(f"\nFATAL: {exc}")
            print("Check your API key and account balance. Progress saved — re-run to resume.")
            sys.exit(1)
        except Exception as exc:
            print(f"[{done_count:4}/{total}] TRANSIENT_ERROR {species}/{document}: {exc} — skipping")
            continue

        score, cat = parse_result(reply)
        writer.writerow({
            "species":   species,
            "document":  document,
            "score":     f"{score:.2f}" if score is not None else "",
            "category":  str(cat) if cat is not None else "",
            "status":    "ok",
            "reasoning": reply.strip(),
        })
        csv_file.flush()
        done_count += 1

        score_str = f"{score:.2f}" if score is not None else "???"
        cat_str   = str(cat) if cat is not None else "?"
        print(f"[{done_count:4}/{total}] cat={cat_str} score={score_str:>6}  {species}/{document}")

    csv_file.close()
    print(f"\nDone. Results written to {csv_path}")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REPO_ROOT      = Path(__file__).resolve().parent.parent
PROMPT_FILE    = REPO_ROOT / "classifier_prompt.md"
MODEL          = "claude-sonnet-4-6"
MAX_CHUNKS     = 120   # cap to keep prompt size reasonable; spread across doc
PROCESSED_DIR  = REPO_ROOT / "/home/artie-sh/repos/spiders/pardosa_processed"   # folder to classify
OUTPUT_CSV     = REPO_ROOT / "classification_results.csv"

CSV_COLUMNS = ["species", "document", "score", "category", "status", "reasoning"]

if __name__ == "__main__":
    main()
