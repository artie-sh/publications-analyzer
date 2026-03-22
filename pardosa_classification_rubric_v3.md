# Document Classification Rubric for Pardosa Taxonomy Literature
*Automated scoring criteria — v2, revised per domain expert review*

# Overview
This document describes a system for ranking taxonomic publications by their scientific usefulness for Pardosa spider systematics. It was developed by analyzing a set of publications manually classified by a domain expert into four informativeness categories, then deriving criteria that reproduce those classifications.

The system works as follows:

1. Each PDF is processed by an extraction pipeline that produces a structured text representation (chunks) and image metadata files (one per extracted figure, containing caption text and quality metrics).
2. An LLM evaluator is given the extracted content and scores each of six dimensions based on the criteria defined in this rubric. Each dimension receives a score from 1 to 9.
3. The six scores are combined into a single weighted score between 0 and 10. Dimensions more critical for taxonomic utility — Pardosa relevance and morphological description depth — carry the highest weight (25% each).
4. The final output is a spreadsheet listing every document with its score. Documents can be sorted by score to prioritize review. Optional category labels (1-4) can be applied using the thresholds in Section 3, but the raw score is the primary ranking signal.

The rubric was validated against 8 ground-truth labeled publications spanning all four categories, achieving 100% correct classification (see Section 4).

# 1. Category Definitions
Four categories of scientific informativeness, as defined by the domain expert. The categories reflect the practical utility of a publication for Pardosa taxonomy revision work, from zero value (Cat 1) to maximum value (Cat 4).

# 2. Scoring Rubric
Each dimension is scored independently on a 1-9 scale, then multiplied by its weight. Scores of 1-2 correspond to Cat 1 behavior on that dimension; 3-4 to Cat 2; 5-7 to Cat 3; 8-9 to Cat 4.

| Dimension | Weight | Cat 1 (1-2 pts) | Cat 2 (3-4 pts) | Cat 3 (5-7 pts) | Cat 4 (8-10 pts) |
| --- | --- | --- | --- | --- | --- |
| Pardosa relevance (normalized by doc size). Note: some Pardosa species may appear under misidentified names or synonyms in older literature. If this cannot yet be accounted for, the dimension will still capture the majority of useful content. | 25% | No mentions of Pardosa (score 1) | Incidental mention only (checklist entry, passing reference); or large multi-genus fauna book where Pardosa chunks represent <25% of total (score 3-5) | Pardosa present as one of several genera with moderate treatment; Pardosa chunks represent 10-24% of total document (score 7) | Document focused on Pardosa throughout; Pardosa chunks represent >=25% of total (score 9) |
| Morphological description depth | 25% | Absent or limited to pre-modern prose (19th-century Latin or equivalent) with no structured descriptions (score 1) | Modern morphological vocabulary present but no quantitative measurements (score 4) | Modern terms present together with quantitative measurements (e.g. body length, leg segment lengths in mm), but descriptions are incomplete or not co-occurring in the same passages (score 6) | Passages combining precise measurements with detailed morphological terms describing multiple structures. Relevant terms: palp, bulb, embolus, embolic division, tegular apophysis, terminal apophysis, conductor, epigyne, vulva, spermathecae, chelicera, carapace, abdomen, prosoma, opisthosoma, femur, patella, tibia, metatarsus, tarsus (score 8) |
| Genital figure quality | 20% | No figures, or only very small/low-quality images with no informative content (score 1). Note: absence of a caption does not automatically indicate low quality - captions may appear on adjacent pages and not be captured by the pipeline. | Figures present but show only habitus (whole-body) views; no genital structures illustrated (score 3) | At least one figure caption references genital structures (epigyne, vulva, palp, bulb) for either female or male (score 5) | >=2 figure captions reference genital structures with multi-view coverage for both sexes. Views may include: ventral, dorsal, lateral, prolateral, retrolateral, anterior, posterior, apical (score 9) |
| Taxonomic originality | 15% | No original taxonomic contribution (score 1) | Species listed or synonymised only; no new descriptions or revisions (score 3) | 1-5 new species described. Note: a single new species description is generally more valuable than multiple new locality records, which should be scored lower within this band (score 6) | >=6 new species with full diagnosis and comparison to related taxa; OR a thorough redescription or revision of a poorly known species based on new or type material - considered equally valuable to new species description (score 9) |
| Supporting data | 10% | No supporting data (score 1) | Locality data mentioned in prose only, without structured listing (score 4) | Structured distribution table, faunal checklist, or a formal list of examined specimens with collection localities (score 7) | Formal examined material list with collection localities + distribution map + molecular data (DNA barcoding, COI sequences, GenBank accessions) (score 9) |
| Text/OCR quality | 5% | Text heavily garbled; large portions unreadable due to poor scan quality or OCR failure (score 1) | Text parseable but noisy; older or non-English language may reduce extractability (score 4) | Text readable with minor OCR artifacts; most content extractable (score 6) | Clean, well-structured text; minimal noise; content fully extractable (score 9) |

# 3. Category Thresholds
Final score = sum of (dimension_score × weight). Range: 0.0 - 10.0. Equal-width bands of 2.5 points each.

| Category | Weighted Score | Description |
| --- | --- | --- |
| Category 1 | 0.0 - 2.4 | Zero taxonomic value. Pre-modern, non-Pardosa, or unreadable. |
| Category 2 | 2.5 - 4.9 | Minimal value. Pardosa mentioned but not substantively treated; figures rarely useful. |
| Category 3 | 5.0 - 7.4 | Moderate value. Pardosa treated with illustrations; missing details or incomplete coverage. |
| Category 4 | 7.5 - 10.0 | Maximum value. Full morphological treatment, multi-view genital figures, original or revisionary taxonomy. |

> Note: Pardosa relevance and morphological description depth together account for 50% of the score, reflecting their primacy for taxonomic utility.

> Note on figure quality: pixel-level image sharpness is not a reliable proxy for scientific figure quality — high-quality SEM and photographic images can score lower on sharpness than line drawings. Figure quality is therefore assessed through caption content and view coverage, not raw image metrics.

# 4. Calibration Examples
Ground-truth labeled examples used to derive and validate the rubric. All category assignments were made manually by the domain expert prior to rubric development.

| Publication | Cat | Score | Key discriminators |
| --- | --- | --- | --- |
| Holmberg 1876 | 1 | 1.40 | Pre-modern Spanish; no Pardosa detected; tiny decorative images |
| Sundevall 1833 | 1 | 1.80 | 19th-century Latin; no Pardosa detected; decorative images |
| Simon 1883 | 2 | 2.70 | French; single Pardosa checklist entry; modern morphological vocabulary present |
| Tikader & Malhotra 1980 | 2 | 4.60 | Multi-genus fauna monograph; Pardosa treated shallowly per species; no deep description passages |
| Ovtsharenko 1979 | 3 | 6.60 | Caucasus faunal survey; 2 genital figure plates; 5 new species; distribution table |
| Tongiorgi 1966 | 3 | 7.35 | Pardosa-focused revision (42% of chunks); identification key; deep description passages present |
| Nadolny et al. 2016 | 4 | 7.75 | New species description; multi-view genital figures; distribution map; DNA barcoding |
| Naumova & Deltshev 2023 | 4 | 7.65 | Single-species redescription; comparative multi-view figures for 3 species; intraspecific variation documented |
