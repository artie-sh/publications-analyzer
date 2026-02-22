#!/usr/bin/env python3
"""
Reconstruct downloads/ into a clean output folder where every duplicate is
replaced by a relative symlink pointing to the single kept copy.

The original downloads/ folder is never modified.

Linux/macOS: symlinks are created directly.
Windows:     run create_links.ps1 from the output root in PowerShell
             (requires Developer Mode or administrator rights).
"""

import csv
import os
import re
import shutil
from pathlib import Path

from tqdm import tqdm

# ---------------------------------------------------------------------------
CSV_FILE       = Path("/home/artie-sh/repos/spiders/duplicates_exp.csv")
DOWNLOADS_ROOT = Path("/home/artie-sh/repos/spiders/exp")
OUTPUT_ROOT    = Path("/home/artie-sh/repos/spiders/exp_clean")
PS1_OUT        = OUTPUT_ROOT / "create_links.ps1"
# ---------------------------------------------------------------------------

PS1_HEADER = """\
# Run this script from the root of the output folder in PowerShell.
# It recreates the relative symbolic links that were created on Linux.
# Requires either Developer Mode (Settings → Privacy → Developer Mode) or
# administrator rights; without one of these, symlink creation will fail.
#
# Each command uses -Force, so any stub files left behind when the Linux
# symlinks were copied to Windows are automatically replaced with proper
# Windows symbolic links.
#
# Usage:
#   cd /path/to/downloads_clean
#   .\\create_links.ps1
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def has_numeric_suffix(path: Path) -> bool:
    """True if the stem ends with _<digits>, e.g. Author_Year_Title_2."""
    return bool(re.search(r'_\d+$', path.stem))


def numeric_suffix_value(path: Path) -> int:
    m = re.search(r'_(\d+)$', path.stem)
    return int(m.group(1)) if m else 0


def select_keeper(paths: list[str]) -> str:
    """
    Choose the canonical file from a duplicate group.
    Priority:
      1. Files whose stem does NOT end with a numeric suffix.
         Among these, the first one in CSV order wins.
      2. If every file has a numeric suffix, pick the one with the lowest number.
    """
    no_suffix = [p for p in paths if not has_numeric_suffix(Path(p))]
    if no_suffix:
        return no_suffix[0]
    return min(paths, key=lambda p: numeric_suffix_value(Path(p)))


def win_path(absolute: Path, base: Path) -> str:
    """Relative path from base to absolute, using Windows backslashes."""
    return str(absolute.relative_to(base)).replace("/", "\\")


def win_rel(posix_rel: str) -> str:
    """Convert a POSIX relative path string to Windows backslashes."""
    return posix_rel.replace("/", "\\")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    # --- Read CSV ---
    groups: dict[str, list[str]] = {}
    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            groups.setdefault(row["hash"], []).append(row["path"])

    csv_paths: set[str] = {p for paths in groups.values() for p in paths}

    # Unique files: hash appears only once in the CSV
    unique_paths = [paths[0] for paths in groups.values() if len(paths) == 1]

    # Files present in downloads/ but absent from the CSV entirely
    not_in_csv = [
        str(p) for p in DOWNLOADS_ROOT.rglob("*.pdf")
        if str(p) not in csv_paths
    ]

    files_to_copy = unique_paths + not_in_csv

    # Duplicate groups: hash shared by 2+ files
    dup_groups = {h: ps for h, ps in groups.items() if len(ps) > 1}
    total_dup_files = sum(len(ps) for ps in dup_groups.values())

    print(f"Unique files (no duplicates): {len(files_to_copy)}")
    print(f"Duplicate groups:             {len(dup_groups)}")
    print(f"Total files in dup groups:    {total_dup_files}\n")

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    files_copied    = 0
    symlinks_created = 0
    ps1_commands    = 0
    errors          = 0
    ps1_lines       = [PS1_HEADER]

    total_work = len(files_to_copy) + total_dup_files

    with tqdm(total=total_work, desc="Processing", unit="file") as bar:

        # --- Unique files: straight copy ---
        for src_str in files_to_copy:
            src = Path(src_str)
            try:
                dest = OUTPUT_ROOT / src.relative_to(DOWNLOADS_ROOT)
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dest)
                files_copied += 1
            except Exception as e:
                tqdm.write(f"  [copy error] {src}: {e}")
                errors += 1
            bar.update(1)

        # --- Duplicate groups ---
        for paths in dup_groups.values():
            keeper_str  = select_keeper(paths)
            keeper_src  = Path(keeper_str)
            keeper_dest = OUTPUT_ROOT / keeper_src.relative_to(DOWNLOADS_ROOT)

            # Copy the keeper
            try:
                keeper_dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(keeper_src, keeper_dest)
                files_copied += 1
            except Exception as e:
                tqdm.write(f"  [copy error] {keeper_src}: {e}")
                errors += 1
            bar.update(1)

            # Symlinks for every non-keeper
            for p_str in paths:
                if p_str == keeper_str:
                    continue

                link_dest  = OUTPUT_ROOT / Path(p_str).relative_to(DOWNLOADS_ROOT)
                rel_target = os.path.relpath(keeper_dest, link_dest.parent)

                try:
                    link_dest.parent.mkdir(parents=True, exist_ok=True)
                    if link_dest.is_symlink() or link_dest.exists():
                        link_dest.unlink()
                    link_dest.symlink_to(rel_target)
                    symlinks_created += 1
                except Exception as e:
                    tqdm.write(f"  [symlink error] {link_dest}: {e}")
                    errors += 1

                # PowerShell equivalent
                ps1_link   = win_path(link_dest, OUTPUT_ROOT)
                ps1_target = win_rel(rel_target)
                ps1_lines.append(
                    f'New-Item -ItemType SymbolicLink -Path "{ps1_link}" -Target "{ps1_target}" -Force'
                )
                ps1_commands += 1
                bar.update(1)

    # --- Write PowerShell script ---
    PS1_OUT.write_text("\n".join(ps1_lines) + "\n", encoding="utf-8")

    # --- Summary ---
    print(f"\nFiles copied:          {files_copied}")
    print(f"Symlinks created:      {symlinks_created}")
    print(f"PowerShell commands:   {ps1_commands}")
    print(f"Errors:                {errors}")
    print(f"PowerShell script:     {PS1_OUT}")


if __name__ == "__main__":
    main()
