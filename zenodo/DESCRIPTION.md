# SAHA v1.0 — Zenodo record description (source text)

Edit this file, not the JSON. The description field in `record_metadata.json`
is HTML; regenerate it from this file when depositing.

Two variants below: the parent collection record and the per-part child records.

---

## PARENT — collection record

**Spatial Atlas of Human Anatomy (SAHA): SpatialData objects, v1.0**

The Spatial Atlas of Human Anatomy (SAHA) is a multimodal, subcellular-resolution
spatial reference atlas of human tissues, profiling 8.5M+ annotated cells from
normal and diseased donor tissue across the gastrointestinal tract and associated
organs. SAHA captures matched RNA and protein measurements at ~50 nm resolution
using the NanoString CosMx and 10x Genomics Xenium platforms.

This record is the **top-level collection** for the v1.0 SpatialData release. It
contains the release documentation, the four metadata tables that describe every
sample, donor, acquisition run, and panel, and a manifest mapping each data file
to its sample. The SpatialData objects themselves are deposited as separate child
records, listed under *Related works* below.

### Contents of this record

| File | Description |
|------|-------------|
| `README.md` | Release notes, file inventory, and reading instructions |
| `samples.parquet` | One row per sample — organ, condition, platform, donor, cell count, QC status |
| `donors.parquet` | One row per donor — age group, sex, tissue source, consent level |
| `runs.parquet` | One row per acquisition run — instrument, institution, date, panel, protocol parameters |
| `panels.parquet` | Panel configurations — platform, plex, gene and protein lists |
| `file_manifest.csv` | Maps every data file in the collection to sample_id, organ, platform, child record DOI |
| `MD5SUMS.txt` | Checksums for every file across the collection |
| `LICENSE-DATA` | Data licence terms and attribution statement |

### Child records

Each child record holds zipped SpatialData (OME-NGFF/Zarr) objects for one
platform and organ group. See *Related works*.

### How to read the data

Data objects are [SpatialData](https://spatialdata.scverse.org/) stores written in
Zarr v3 format, distributed as uncompressed ZIP archives (one archive per sample).
Zarr chunks are already compressed; the ZIP is a container only.

```python
# unzip SAHA_XR_APE_A9_MS01_01_RNA.zarr.zip first
import spatialdata as sd

sdata = sd.read_zarr("SAHA_XR_APE_A9_MS01_01_RNA.zarr")
print(sdata)

adata = sdata.tables["table"]          # cells x genes, AnnData
adata.layers["raw"]                    # raw counts
sdata.points                           # transcript locations
sdata.shapes                           # cell and nucleus boundaries
sdata.images                           # morphology images
```

Written with `spatialdata` vX.Y.Z and `zarr` vX.Y.Z — **TODO: pin actual
versions before deposit.** Readers older than these may not open Zarr v3 stores.

### What each object contains, by platform

| Element | Xenium | CosMx RNA | CosMx protein |
|---------|--------|-----------|---------------|
| `tables/table` (cells x features, raw counts) | yes | yes | yes |
| `shapes` (cell boundaries) | yes | yes (polygons) | yes (polygons) |
| `points/transcripts` | yes | yes | n/a — protein assay |
| `images/morphology` | yes | yes (5-channel) | yes (5-channel) |
| `labels/cell_labels` | n/a | yes | yes |
| `labels/compartment_labels` | n/a | yes | yes |

CosMx morphology carries five immunofluorescence channels — typically PanCK,
CD68, Membrane, CD45 and DNA. Channel names are read per sample from the
instrument's own channel dictionary; where a run left a target unnamed the
channel is labelled by its optical ID (e.g. `Channel-G`) rather than assigned a
marker by inference.

### Image resolution, and where to get full resolution

CosMx images in **this Zenodo release are downsampled 4x**, to 0.48 um/px, which
keeps each record within Zenodo's per-record size limit. Cell and compartment
label masks are at **full resolution** — downsampling them would destroy small
cells and corrupt the integer IDs.

The **full-resolution copies (0.12 um/px, ~623 GB)** are published separately via
AWS Open Data at `s3://saha-open-data`. They are otherwise identical objects:
same tables, same polygons, same transcripts, same coordinate system.

Images were stitched from the per-FOV instrument exports into each sample's
`global` coordinate system, so they align with the cell polygons and transcript
locations already in the object. Alignment was verified per sample by checking
that each cell's centroid lands inside its own segmentation label; all samples
scored 99.7–100%.

### Linking cells to the label image

`labels/cell_labels` stores a globally unique integer per cell, because CosMx
`cell_ID` restarts at 1 in every FOV. The obs column `label_id` holds the same
value:

```python
adata = sdata.tables["table"]
adata.obs["label_id"]           # == fov * 100000 + fov_cell_id
```

Use it to move between a row of the table and its pixels in the label image.

### Cell filtering and the `qc_pass` column

CosMx objects contain **every segmented cell**, including cells that did not
survive QC into the published organ-level h5ad files. The boolean obs column
`qc_pass` marks which cells are in the published set:

```python
adata = sdata.tables["table"]
adata[adata.obs["qc_pass"]]     # the published, QC-filtered cell set
adata                           # every segmented cell
```

For protein samples `qc_pass` is True for all cells. For RNA samples it is True
for 91–97% of them. Cell counts reported in the manuscript and in
`samples.parquet` correspond to the `qc_pass == True` subset.

Four disease samples have no published h5ad to compare against and therefore
**carry no `qc_pass` column**: `SAHA_CR_IBD_A1_EC04`, `SAHA_CR_IBD_A2_EC04`,
`SAHA_CR_PDAC_RC_EC02`, `SAHA_CP_PDAC_RC_EC02`. Their tables are the complete
segmented cell set, released unfiltered by design. Users wanting a QC-filtered
subset of these samples should apply their own thresholds — the per-cell QC
fields from the CosMx export (`nCount_RNA`, `nFeature_RNA`,
`nCount_negprobes`, `Area.um2`) are all present in obs.

### Access

Institutional and consent approvals for public sharing are complete. Release is
gated only on acceptance of the associated manuscript.

Deposits are held as unpublished drafts with reserved DOIs until acceptance, so
the DOI can be cited in the manuscript while files remain replaceable. On
acceptance they are published — open, or with a short embargo if the journal
requires one. Record metadata and DOIs are public and citable from the moment of
publication; embargoed files open automatically at midnight UTC on the embargo
end date.

### Licence and citation

Data are released under CC-BY 4.0. If you use SAHA data, please cite:

> Park J, et al. Spatial Atlas of Human Anatomy (SAHA). bioRxiv 2025.06.16.658716.
> https://doi.org/10.1101/2025.06.16.658716

and cite this dataset by its concept DOI (the DOI that always resolves to the
latest version).

Users are asked to observe the SAHA Data Use Statement: no re-identification
attempts, citation of the marker paper, research use only. These are community
expectations, not additional licence conditions — CC-BY 4.0 permits no
restrictions beyond attribution.

### Related resources

- Project website: https://www.saha-project.org/
- Code, schemas, and tutorials: https://github.com/masonlab/saha-data-access
- Full-fidelity data on AWS Open Data: `s3://saha-open-data`

---

## CHILD — per-part record

**Spatial Atlas of Human Anatomy (SAHA): {PLATFORM} SpatialData objects — {ORGAN_GROUP}, v1.0**

This record is part of the SAHA v1.0 SpatialData release. It contains
{N_SAMPLES} SpatialData objects from the {PLATFORM} platform covering
{ORGAN_GROUP} tissue ({N_CELLS} cells total).

For release documentation, sample/donor/run/panel metadata tables, and the full
file manifest, see the parent collection record: {PARENT_DOI}.

### Contents

{FILE_TABLE — one row per zipped zarr: filename, sample_id, organ, condition,
donor_id, n_cells, size}

### How to read the data

Identical to the parent record — see {PARENT_DOI}.

### Licence and citation

CC-BY 4.0. Cite the parent collection concept DOI and the marker paper:

> Park J, et al. Spatial Atlas of Human Anatomy (SAHA). bioRxiv 2025.06.16.658716.
> https://doi.org/10.1101/2025.06.16.658716
