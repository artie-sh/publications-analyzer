# Project Summary: Pardosa Literature Analysis Pipeline

## 1. Data Source & Collection

- **Database:** World Spider Catalog (wsc.nmbe.ch) — the authoritative global registry of spider taxonomy publications
- Automated scraping (`web_scraper.py` via Playwright) downloaded all publications linked to the *Pardosa* genus
- Result: **3,709 PDF entries** across **517 Pardosa species** subfolders
- Many papers cover multiple species simultaneously, so the same file appeared repeatedly across folders

## 2. Deduplication

- `duplicates_remover.py` rebuilt the folder structure so each unique PDF is stored exactly once, with symlinks in all other species folders pointing back to it
- Result: **790 unique PDF files** + **2,919 symlinks** — an ~79% reduction in stored files while preserving the full species-organized structure

## 3. Document Processing Pipeline

Each of the 790 unique PDFs was run through a multi-stage pipeline (`pdf_processor.py`, Docling-based).

### Pre-check — garbled text detection

Before any heavy processing, PyMuPDF scans every page for font-encoding artifacts (common in Soviet-era Russian publications where Cyrillic was stored as Latin codepoints). If >30% of pages are garbled, full-page OCR is forced.

### Docling conversion

- **Digital-first PDFs** (no scan): native text extracted directly — fast and clean
- **Scanned or garbled PDFs:** Tesseract OCR applied (languages: English, Russian, Ukrainian, German, Spanish, French, Italian, Latin)

### Extraction outputs per document

| Output | Description |
|---|---|
| `chunks.json` | Text split into RAG-ready chunks (max 400 tokens), with section headings, page numbers, and figure/table references |
| `tables/` | Each table exported as CSV + PNG image + metadata |
| `images/` | Scientific figures extracted as PNG + metadata (caption, page, sharpness, resolution); decorative images filtered out |
| `doc_stats.json` | Document-level quality signals (garbled ratio, OCR mode, chunk/image/table counts, sharpness stats) |

### Large PDF handling

Very large PDFs (hundreds of pages) caused pipeline timeouts or crashes. `split_pdfs.py` broke them into 50-page parts; parts were processed individually; `finalize_large_pdfs.py` reassembled the outputs with correct page number offsets so the merged result is identical to a single-run output.

### Processing stats across 790 unique documents

**Document text quality (mutually exclusive tiers):**

| Text quality | Count | % |
|---|---|---|
| Clean digital-first (garbled ratio = 0) | 591 | 75% |
| Minor garbling (ratio 0–30%, OCR on blank/scanned pages only) | 158 | 20% |
| Heavy garbling (ratio >30%, full-page OCR forced) | 40 | 5% |

**Scale of extracted content:**

| Metric | Value |
|---|---|
| Total pages processed | 57,046 |
| Average pages per document | 72 |
| Longest document | 810 pages |
| Total text chunks extracted | **308,467** |
| Total figures extracted | **40,251** |
| Total tables extracted | **7,967** |

## 4. Classification Rubric & Scoring

A 6-dimension scoring rubric was developed iteratively: the biologist manually assigned categories (1–4) to 8 example publications; these were used to reverse-engineer objective, measurable criteria that reproduce those labels, then validated to achieve **100% agreement** on all 8 ground-truth examples.

### The 6 dimensions (weighted score 0–10)

| Dimension | Weight | What it measures |
|---|---|---|
| Pardosa relevance | 25% | Fraction of document genuinely focused on *Pardosa* |
| Morphological description depth | 25% | Presence of quantitative measurements combined with detailed anatomical terminology (palp, embolus, epigyne, spermathecae, etc.) |
| Genital figure quality | 20% | Whether figures illustrate genital structures, and how many views/sexes are covered |
| Taxonomic originality | 15% | New species descriptions, revisions, or redescriptions from type material |
| Supporting data | 10% | Specimen lists, distribution maps, molecular data (DNA barcoding) |
| Text/OCR quality | 5% | Readability of extracted text |

### Category definitions

| Category | Score | Description |
|---|---|---|
| 1 | 0.0–2.4 | Zero taxonomic value — pre-modern, non-*Pardosa*, or unreadable |
| 2 | 2.5–4.9 | Minimal — *Pardosa* mentioned but not substantively treated |
| 3 | 5.0–7.4 | Moderate — *Pardosa* treated with illustrations; incomplete coverage |
| 4 | 7.5–10.0 | Maximum — full morphological treatment, multi-view genital figures, original taxonomy |

### Classification process

`classify.py` fed each document's `doc_stats.json` + a sample of up to 120 chunks from `chunks.json` to Claude (Sonnet 4.6), using the rubric as the system prompt, and parsed the numeric score and category from the structured response. The run was resumable — already-scored documents were skipped on restart.

## 5. Classification Results

789 of 790 documents were successfully scored (1 had a response parse failure).

| Category | Count | % | Score range | Avg score |
|---|---|---|---|---|
| Cat 1 — no value | 87 | 11% | 1.05–2.45 | 2.02 |
| Cat 2 — minimal | 442 | 56% | 2.50–4.95 | 3.73 |
| Cat 3 — moderate | 188 | 24% | 5.00–7.45 | 6.16 |
| Cat 4 — high value | 72 | 9% | 7.50–8.90 | 8.02 |
| **Total** | **789** | **100%** | **1.05–8.90** | **4.51** |

### Ground-truth calibration examples

| Publication | Cat | Score | Key discriminators |
|---|---|---|---|
| Holmberg 1876 | 1 | 1.40 | Pre-modern Spanish; no *Pardosa* detected; decorative images only |
| Sundevall 1833 | 1 | 1.80 | 19th-century Latin; no *Pardosa* detected |
| Simon 1883 | 2 | 2.70 | French fauna list; single *Pardosa* entry; no genital figures |
| Tikader & Malhotra 1980 | 2 | 4.60 | Multi-genus monograph; *Pardosa* one of 9 genera; shallow treatment |
| Ovtsharenko 1979 | 3 | 6.60 | Caucasus faunal survey; 3 new species; genital figure plates; distribution table |
| Tongiorgi 1966 | 3 | 7.35 | *Pardosa*-focused Italian revision; multi-view figures; lacks integrated measurements |
| Nadolny et al. 2016 | 4 | 7.75 | New species; multi-view genital figures; distribution map; DNA barcoding |
| Naumova & Deltshev 2023 | 4 | 7.65 | Single-species redescription; comparative multi-view figures for 3 species |

## Key Takeaway

Out of 789 scored publications on *Pardosa* spiders collected from the World Spider Catalog, **72 documents (9%) are high-priority reading** — those with full morphological descriptions, multi-view genital illustrations, and original taxonomic contributions. Another **188 (24%) have moderate value**. The scoring system produces a ranked list so the most scientifically dense material can be prioritized, rather than reading blindly through hundreds of papers.
