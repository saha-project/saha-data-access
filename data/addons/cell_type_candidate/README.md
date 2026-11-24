# SAHA v1 add-on: candidate cell-type labels for lymph node, pancreas, liver and bone marrow

Supplementary, per-cell candidate cell-type columns for the 11 healthy CosMx 1K RNA acquisitions of lymph node (LN),
pancreas (PANC), liver (LIV) and bone marrow (BM). They sit **alongside** the released labels (`cell_type_broad`,
`cell_type`, `cell_type_detailed`, ...) in each object and never replace them. In these four organs the released labels
are not reliable at lineage level (bone marrow has none); see Table S3 of the SAHA Cell Resource paper.
Use them only within the scope given per acquisition below.

## Files

| file | content |
|---|---|
| `<sample_id>_cell_type_candidate.parquet` (×11) | one row per cell of the released object, same order |
| `acq_label_quality.csv` | per-acquisition flag, usable scope and the evidence behind it |
| `label_to_CL.csv` | Cell Ontology mapping of every release label (all organs, all label columns) and every candidate label |
| `export_check.csv` | row-count / cell-id check against the release objects |

Join to an object on `cell`:

```python
import pandas as pd, spatialdata as sd
sdata = sd.read_zarr("saha-v1-lymph-node/SAHA_CR_LN_A1_EC03.zarr")
obs = sdata.tables["table"].obs
cand = pd.read_parquet("SAHA_CR_LN_A1_EC03_cell_type_candidate.parquet")
ids = obs["cell"].astype(str) if "cell" in obs else obs.index.astype(str)   # the release cell id
assert (cand["cell"].values == ids.values).all()                           # same cells, same order
for c in cand.columns[1:]:
    obs[c] = cand[c].values
```

## Columns

| column | meaning |
|---|---|
| `cell` | release cell id (`obs['cell']`, or the obs index where that column is absent); identical to the released object, same row order |
| `cell_type_candidate` | fine candidate call: a Tabula Sapiens cell-type name; null when unresolved (fine posterior < 0.5 or < 20 transcripts) |
| `cell_type_candidate_broad` | lineage of the call (16 lineages, e.g. B, T/NK, Plasma, Myeloid, Epithelial, Hepatocyte, Endocrine); null when unresolved (lineage posterior < 0.5 or < 20 transcripts) |
| `cell_type_candidate_CL` | Cell Ontology id of `cell_type_candidate` (CELLxGENE-curated, from the reference); null when unresolved |
| `cell_type_candidate_confidence` | `high` = lineage posterior ≥ 0.9, ≥ 20 transcripts and the all-gene sensitivity model gives the same lineage; `medium` = lineage posterior ≥ 0.6 and ≥ 20 transcripts; `low` otherwise |
| `cell_type_candidate_hc` | `cell_type_candidate_broad` where confidence is `high`, else null. In SAHA_CR_LN_A1_EC03 it is filled only for B, T/NK and Plasma, the lineages with serial-section protein support; other high-confidence lymph-node calls (myeloid, stromal, endothelial, ...) stay in `cell_type_candidate_broad` with their confidence. The recommended column |

All cells of each object are included. No high-confidence call falls on a cell failing the release per-cell QC.

## Method

Each acquisition was classified against Tabula Sapiens references of the same organ (10x cells only, ambient-corrected
counts, restricted to the CosMx 1K panel; IgA plasma cells from Tabula Sapiens large intestine; lymph-node endothelial and
stromal profiles used for bone marrow) with a supervised, Insitutype-style multinomial likelihood. Each cell's expected
profile mixes the reference type profile with a negative-probe background term and an acquisition-level ambient
(segmentation spillover) term whose share is chosen per acquisition by marginal likelihood; per-gene platform efficiencies
are fitted against cluster pseudobulks with clusters weighted equally, and the cell-type prior is fitted by EM. The primary
model uses about 230–300 reference-informative genes and excludes protocol-sensitive transcripts (MALAT1, NEAT1, ribosomal
and mitochondrial genes) and probes with acquisition-specific artefact signal (for example POU5F1, NANOG and KRT17 in the
pre-commercial TAP runs); an all-gene model is the sensitivity check used in `confidence`. Labels were checked against
same-cell morphology stains (informative only in runs where release GI/prostate labels pass a positive control),
serial-section 64-plex protein per 240 µm tile, and spatial structure (homotypic neighbour enrichment). Code:
`M_reannot/scripts/` (`annot_lib.py`, `02_annotate.py`, `11_export_candidate.py`).

Labels were regenerated on 2026-11-23 after a fix to the reference-type → lineage map (mast, tuft, goblet and
myofibroblast cells had been matched to T/NK; pancreatic alpha and enterochromaffin-like cells to no lineage; intestinal
crypt stem cells to HSPC). Lymph node, pancreas and liver were re-run; bone marrow was not affected. The export asserts
that every reference type has a lineage.

## Where the labels are usable

| acquisition | median transcripts (QC cells) | flag | usable scope |
|---|---:|---|---|
| SAHA_CR_LN_A1_EC03 | 55 | partial | B, T/NK and plasma compartments (the only lineages in `_hc`). Not myeloid (40%, probably a spillover catch-all), not T or B subsets |
| SAHA_CR_PANC_A1_EC02 | 186 | partial | acinar, ductal, endocrine. Not immune or stromal |
| SAHA_CR_PANC_A1_SJ01 | 206 | partial | acinar, ductal, endocrine. Not immune: keep the release myeloid label (CD68 AUROC 0.90 vs 0.52) |
| SAHA_CR_LIV_A1_TAP01 | 491 | partial | hepatocyte vs other; not independently validated |
| SAHA_CR_LIV_A1_TAP02 | 54 | partial | hepatocyte vs other; not independently validated |
| SAHA_CR_LIV_A2_TAP01 | 188 | unreliable | do not use (hepatocyte 26%, cholangiocyte 16%, 18% unresolved) |
| SAHA_CR_LIV_A2_TAP02 | 50 | unreliable | do not use (33% unresolved) |
| SAHA_CR_BM_I17_TAP03 | 43 | unreliable | provisional only (basophil 16%, HSPC 11% implausible) |
| SAHA_CR_BM_I18_TAP03 | 65 | unreliable | provisional only |
| SAHA_CR_BM_I194082_TAP02 | 98 | unreliable | provisional only |
| SAHA_CR_BM_I195889_TAP02 | 57 | unreliable | provisional only (2,835 QC cells) |

In short:

- **LN**: use B, T/NK and plasma; `cell_type_candidate_hc` carries only these three, because only they track serial-section protein (CD19/CD20, CD3, CD138) while the myeloid call does not (CD68 tile ρ ≈ 0). The candidate B fraction tracks serial-section CD19 protein per tile (ρ 0.66; release
  labels 0.01). The release labels call 22,290 of 36,861 marker-defined plasma cells B; the candidate model calls 93% of
  them plasma. Ig-based plasma calls at this depth can include neighbours of plasma cells (immunoglobulin RNA spillover).
  Do not use myeloid calls, T-cell subsets (CD4/CD8/Treg/Tfh), germinal-centre B cells, or FDC/FRC (no reference).
  The section is a single LN TMA in which 78% of FOVs fail FOV QC.
- **PANC**: use acinar, ductal and endocrine (beta-cell and alpha-cell calls form islet-like clusters, 28–42× homotypic
  enrichment). Alpha rests on 48 reference cells from one donor; delta and PP cells are not in the reference. Do not use
  myeloid or other immune calls; keep the release myeloid label in pancreas.
- **LIV**: hepatocyte vs other in LIV_A1 only. No zonation, no Kupffer vs monocyte, no LSEC subsets. LIV_A2 unreliable.
- **BM**: provisional only; no lineage is independently supported and there is no megakaryocyte reference.

These are candidate labels. Counts are low (median 43–491 transcripts per cell) and the two models still disagree
substantially (κ 0.31–0.64 per acquisition, `acq_label_quality.csv`).

## Cell Ontology table (`label_to_CL.csv`)

Columns: `label_set` (`release v1`, `M_reannot fine (Tabula Sapiens)`, `M_reannot broad lineage`), `label`, `CL_id`, `CL_name`, `mapping_level`
(exact = the label names the CL term; parent = a cluster or subtype mapped to its closest parent term; none = no
defensible term, e.g. Unknown, MixedT), `lineage`, `cells` (summed over label columns), `label_columns`, `records`.
Release labels: 334 labels, 219 exact, 107 parent, 8 none; ids resolved by exact name match against EBI OLS4. Candidate
labels: CL ids curated by CELLxGENE with the Tabula Sapiens reference; broad lineages checked against OLS4
(T/NK = CL:0000084 | CL:0000623).
