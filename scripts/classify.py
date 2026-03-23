#!/usr/bin/env python3
"""
Classify processed documents using the Pardosa Literature Classifier prompt.

Usage:
    ANTHROPIC_API_KEY=sk-... python3 scripts/classify.py [PROCESSED_DIR]

Defaults PROCESSED_DIR to classifier_processed/ relative to this script's repo root.
Prints a results table and compares against ground-truth category labels when the
folder name starts with a digit (e.g. "3 type_Tongiorgi_1966").
"""

import json
import os
import re
import sys
from pathlib import Path

import anthropic

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REPO_ROOT     = Path(__file__).resolve().parent.parent
PROMPT_FILE   = REPO_ROOT / "classifier_prompt.md"
MODEL         = "claude-opus-4-6"
MAX_CHUNKS    = 120   # cap to keep prompt size reasonable; spread across doc

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
    """Extract weighted total and category from model response."""
    score_match = re.search(r"weighted total[:\s]+([0-9]+\.?[0-9]*)", text, re.IGNORECASE)
    cat_match   = re.search(r"category[:\s]+([1-4])", text, re.IGNORECASE)
    score = float(score_match.group(1)) if score_match else None
    cat   = int(cat_match.group(1))     if cat_match   else None
    return score, cat


def ground_truth_cat(folder_name: str) -> int | None:
    """Extract ground-truth category from folder name prefix (e.g. '3 type_...')."""
    m = re.match(r"^([1-4])\s", folder_name)
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    processed_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else REPO_ROOT / "classifier_processed"

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set.", file=sys.stderr)
        sys.exit(1)

    client        = anthropic.Anthropic(api_key=api_key)
    system_prompt = load_prompt()

    doc_dirs = sorted(d for d in processed_dir.iterdir() if d.is_dir()
                      and (d / "doc_stats.json").exists()
                      and (d / "chunks.json").exists())

    if not doc_dirs:
        print(f"No processed documents found in {processed_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Classifying {len(doc_dirs)} documents with {MODEL}...\n")

    # Header
    col = "{:<35} {:>4} {:>6} {:>4} {:>4}"
    print(col.format("Document", "GT", "Score", "Cat", "OK?"))
    print("-" * 58)

    correct = 0
    total   = 0

    for doc_dir in doc_dirs:
        name    = doc_dir.name
        gt_cat  = ground_truth_cat(name)
        user_msg = build_user_message(doc_dir)

        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=1024,
                system=system_prompt,
                messages=[{"role": "user", "content": user_msg}],
            )
            reply = response.content[0].text
        except Exception as exc:
            print(f"  ERROR for {name}: {exc}")
            continue

        score, cat = parse_result(reply)
        score_str  = f"{score:.2f}" if score is not None else "???"
        cat_str    = str(cat) if cat is not None else "?"
        gt_str     = str(gt_cat) if gt_cat is not None else "-"

        if gt_cat is not None and cat is not None:
            match = cat == gt_cat
            ok    = "✓" if match else "✗"
            if match:
                correct += 1
            total += 1
        else:
            ok = "-"

        # Trim name for display
        display = name[name.index(" ")+1:] if " " in name else name
        if len(display) > 35:
            display = display[:32] + "..."
        print(col.format(display, gt_str, score_str, cat_str, ok))

        # Print full response for review
        print()
        print(reply)
        print()

    if total:
        print("-" * 58)
        print(f"Accuracy: {correct}/{total} ({100*correct/total:.0f}%)")


if __name__ == "__main__":
    main()
