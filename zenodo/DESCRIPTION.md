# SAHA v1.0 — Zenodo record description (source text)

Edit this file, not the JSON. `scripts/zenodo_upload.py` renders the PARENT and
CHILD sections below to HTML when it creates the records. `{BRACED}` names are
filled in per record.

---

## PARENT — collection record

The Spatial Atlas of Human Anatomy (SAHA) is a multimodal, subcellular-resolution
spatial reference atlas of human tissues. This release holds **12.4M cells** in
**126 acquisitions** (CosMx RNA 49, CosMx protein 19, Xenium 58) across **10 organs**
— appendix, bone marrow, colon, ileum, liver, lymph node, pancreas, prostate,
rectum and stomach — from **115 donor cores**, including disease samples
(colorectal cancer, prostate cancer, pancreatic ductal adenocarcinoma and
inflammatory bowel disease). RNA and protein are measured on the same tissue with
the NanoString/Bruker CosMx and 10x Genomics Xenium platforms, with matched H&E
where it exists.

This record is the **top-level collection** for the v1.0 release. It holds the
release documentation and the metadata tables. The SpatialData objects are in
**12 per-organ child records**, listed under *Related works*.

### Contents of this record

| File | Description |
|------|-------------|
| `README.md` | Release notes, file inventory and reading instructions |
| `samples.parquet` | One row per acquisition: organ, condition, platform, panel, cell counts, number of donor cores |
| `donors.parquet` | One row per donor **core**: slide, organ, condition. No demographics (see *Access tiers*) |
| `runs.parquet` | One row per acquisition run: instrument, institution, date, panel, protocol fields |
| `panels.parquet` | Panel configurations: platform, plex, gene and protein lists |
| `measurements.csv` | One row per acquisition x donor core: which cells came from which core |
| `cohort_coverage.csv` | Donor cores against the manuscript's cohort table |
| `file_manifest.csv` | Every object in the release: file, sample id, child record, platform, organ, condition, cell counts |
| `LICENSE-DATA` | Data licence and attribution statement |

### Donor identity is per cell, not per sample

Most SAHA slides are **tissue microarrays**: one acquisition images 2–28 donor
cores. `PANC_A1` or `APE_A9` names the *slide*, not a person, so
`samples.parquet` carries no scalar donor id — it records `n_donors`. Every object
carries donor identity per cell in `obs['donor_core']`, which joins to
`donors.parquet`; `measurements.csv` gives the cell count per object x core.

```python
import pandas as pd
donors = pd.read_parquet("donors.parquet")
obs = adata.obs.merge(donors, on="donor_core", how="left")
obs.groupby("donor_core").size()          # cells per donor core
```

The pancreatic-cancer slide also carries `obs['donor_patient']`: its 28 cores come
from 14 patients, two cores each. Group by `donor_patient` for per-patient
statistics.

### What is in each object

- **Table** — `sdata.tables["table"]`: counts (RNA) or per-cell intensities
  (protein), with per-cell `obs`.
- **Cell types** — `obs['cell_type_broad']`, one 13-type vocabulary across the
  release where available. `cell_type_broad_source` says how each label was made
  (expert annotation, mapping of earlier labels, or label transfer);
  transferred labels carry `cell_type_broad_confidence`, and cells without a
  confident label are `NA`. The original labels (`cell_type`,
  `cell_type_detailed`, ...) are kept. A few objects have only placeholder
  `leiden` clusters, which are not cell types. **Treat labels as a starting point
  and check them for your own analysis.**
- **Images** — morphology images, and H&E where it exists, as `he_same_section`
  (this slide, cell-level), `he_serial_section` (an adjacent cut, tissue-level) or
  `he_unaligned` (a scan whose placement could not be verified, in its own
  coordinate system). Registration provenance and an accuracy note are in
  `uns['<element>_registration']`.
- **Cells** — segmentation polygons and, for RNA, transcript locations. CosMx
  objects also carry cell and compartment label masks; `obs['label_id']`
  (`fov * 100000 + fov_cell_id`) links a table row to its pixels.

### Cell counts and `qc_pass`

Objects contain **every cell the instrument segmented**. For the 18 acquisitions
with a published QC-filtered h5ad, `obs['qc_pass']` marks the cells it kept, and
`samples.parquet` gives both `n_cells` and `n_cells_qc_pass`. Other objects carry
no `qc_pass` column; the CosMx per-cell QC fields (`nCount_RNA`, `nFeature_RNA`,
`nCount_negprobes`, `Area.um2`) are in `obs` for your own filtering.

### Pre-commercial acquisitions

The `TAP*` acquisitions (bone marrow, liver, colorectal cancer, and some appendix,
colon, ileum and prostate objects) were made on a **pre-commercial CosMx
instrument**. Their yields are not comparable to commercial runs — the
pre-commercial colon acquisition recovers 35,267 cells from a slide on which a
commercial run recovers 92,636 — so compare cell density and detection across
that boundary with care; `runs.parquet` identifies each run. Colorectal-cancer
channel names are the standard-panel fallback (that instrument recorded no
channel dictionary), and `SAHA_CP_COL_A1_TAP01` holds background-subtracted
intensities rather than raw counts (`uns['x_is_raw_counts']` is `False`). Some
TAP objects have circular cell placeholders (centroid and area) rather than
segmented boundaries.

### How to read the data

Each object is a [SpatialData](https://spatialdata.scverse.org/) store in Zarr v3
format, distributed as an uncompressed ZIP (one per acquisition; Zarr chunks are
already compressed). Written with `spatialdata` 0.8.0 and `zarr` 3.3.0; older
readers may not open Zarr v3 stores.

```python
# unzip SAHA_XR_APE_A9_MS01_01_RNA.zarr.zip first
import spatialdata as sd
sdata = sd.read_zarr("SAHA_XR_APE_A9_MS01_01_RNA.zarr")
adata = sdata.tables["table"]
print(adata.obs[["donor_core", "cell_type_broad"]].value_counts().head())
```

### Image resolution

CosMx morphology images in this Zenodo release are **downsampled 4x**
(0.48 µm/px) so each record fits Zenodo's size limit; label masks are at full
resolution. The same objects with **full-resolution imagery**, and the raw
instrument data, are planned for AWS Open Data.

### Access tiers

Institutional and consent approvals for public sharing are complete. This record
carries no donor demographics. Age group, sex and ethnicity (where available and
deidentifiable) are at the registered tier, and exact age at the controlled tier;
see `docs/ACCESS_TIERS.md` in the repository.

### Licence and citation

Data are released under CC BY 4.0. If you use SAHA data, please cite:

> Park J, et al. Spatial Atlas of Human Anatomy (SAHA). bioRxiv 2025.06.16.658716.
> https://doi.org/10.1101/2025.06.16.658716

and this dataset by its concept DOI (which always resolves to the latest version).
We ask users to make no re-identification attempts and to cite the paper; these
are community expectations, not licence conditions.

### Related resources

- Project website: https://www.saha-project.org/
- Code, schemas and tutorials: https://github.com/saha-project/saha-data-access
- Object gallery: https://saha-project.github.io/saha-data-access/gallery/

---

## CHILD — per-organ record

This record is part of the SAHA v1.0 release: **{N_SAMPLES} SpatialData objects**
({PLATFORM}) from **{ORGAN_GROUP}**, {N_CELLS} cells from {N_DONOR_CORES} donor
cores, {MEASURED_GB} GB.

Each file is one acquisition's SpatialData store (Zarr v3) as an uncompressed
ZIP. `file_manifest.csv` lists the files with their sample ids, and
`MD5SUMS.txt` their checksums. The metadata tables, release notes and reading
instructions are in the parent collection record: {PARENT_LINK}.

CC BY 4.0. Please cite the paper (Park J, et al., bioRxiv 2025.06.16.658716,
https://doi.org/10.1101/2025.06.16.658716) and the parent collection's concept DOI.
