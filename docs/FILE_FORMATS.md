# SAHA File Formats

This document describes the file formats used for raw, processed, and metadata files in the SAHA open data release.

---

## Metadata Tables

| File | Format | Description |
|------|--------|-------------|
| `metadata/samples/` | Parquet (partitioned by `organ`, `platform`) | Sample-level metadata |
| `metadata/donors/`  | Parquet (flat) | Donor demographics |
| `metadata/runs/`    | Parquet (flat) | Acquisition run metadata |
| `metadata/panels/`  | Parquet (flat) | Panel gene/protein lists |

All Parquet files use Snappy compression and are readable with any Parquet reader (pandas, R arrow, DuckDB), and queryable with Amazon Athena once the bucket is live and the tables are registered in your own AWS account.

```python
import pandas as pd
df = pd.read_parquet("s3://saha-open-data/metadata/samples/",
                     storage_options={"anon": True})
```

---

## CosMx (NanoString)

Raw output from the CosMx Spatial Molecular Imager.

| File | Format | Description |
|------|--------|-------------|
| `*_exprMat_file.csv.gz` | Gzip CSV | Cell × gene count matrix |
| `*_metadata_file.csv.gz` | Gzip CSV | Per-cell metadata (x/y, FOV, area) |
| `*_fov_positions_file.csv` | CSV | FOV stage coordinates |
| `*_polygons.csv.gz` | Gzip CSV | Cell segmentation polygons |
| `*CellComposite_F*.jpg` | JPEG | Composite morphology image per FOV |
| `*CellLabels_F*.tif` | TIFF | Per-cell label mask per FOV |

**Loading raw CosMx data:**
```python
import spatialdata_io as sdio

sdata = sdio.cosmx(
    path="path/to/cosmx_run_folder",
    dataset_id="SAHA_CR_COL_A1_TAP01",
)
```

**Processed CosMx (SpatialData):**
```python
import spatialdata
sdata = spatialdata.read_zarr(
    "s3://saha-open-data/processed/saha-v1-colon/SAHA_CR_COL_A1_TAP01.zarr")
table = sdata.tables["table"]
# obs: cell_id, fov, cell_type, organ, condition, donor_core, ...
# var: gene symbols (HGNC)
# obsm["spatial"]: n_obs x 2 XY coords
```

---

## Xenium (10x Genomics)

Raw output from the 10x Xenium Analyzer.

| File | Format | Description |
|------|--------|-------------|
| `cell_feature_matrix.h5` | HDF5 | Sparse cell × gene matrix |
| `cells.csv.gz` | Gzip CSV | Per-cell metadata |
| `transcripts.csv.gz` | Gzip CSV | Per-transcript locations |
| `cell_boundaries.csv.gz` | Gzip CSV | Cell polygon boundaries |
| `morphology_focus.ome.tif` | OME-TIFF | Multi-z morphology image |
| `experiment.xenium` | JSON | Run metadata |

**Loading raw Xenium data:**
```python
import spatialdata_io as sdio

sdata = sdio.xenium(
    path="path/to/xenium_output_folder",
    n_jobs=4,
)
```

---

## GeoMx (NanoString)

| File | Format | Description |
|------|--------|-------------|
| `*.xlsx` | Excel | NanoString DCC count matrices |
| `*.dcc` | DCC | Per-ROI raw count files |
| `*_annotation.xlsx` | Excel | ROI annotations |
| `*.ome.tiff` | OME-TIFF | High-resolution tissue scan |

**Loading GeoMx data:**
```python
from geomxtools import GeoMxDataset
ds = GeoMxDataset.from_dcc("path/to/dcc_files/", config="path/to/pkc/")
```

---

## RNAscope

| File | Format | Description |
|------|--------|-------------|
| `*.czi` | Zeiss CZI | Multi-channel confocal images |
| `*.nd2` | Nikon ND2  | Confocal image stacks |
| `*_spot_calls.csv` | CSV | Per-spot gene calls and XYZ coords |
| `*_cell_boundaries.geojson` | GeoJSON | Cell segmentation from QuPath/Cellpose |

---

## G4X

| File | Format | Description |
|------|--------|-------------|
| `*.zarr/` | Zarr store | Multi-resolution image pyramid |
| `*_cells.parquet` | Parquet | Cell-level gene expression and coordinates |

---

## Images

Large tissue images are stored as Cloud-Optimized GeoTIFF (COG) or OME-Zarr to enable partial reads.

| Format | Extension | Use |
|--------|-----------|-----|
| Cloud-Optimized GeoTIFF | `.tif` / `.tiff` | Single-section morphology images |
| OME-Zarr | `.zarr/` | Multi-channel, multi-z image stacks |
| OME-TIFF | `.ome.tif` | Xenium morphology, GeoMx scans |

**Reading a COG tile:**
```python
import rioxarray
img = rioxarray.open_rasterio(
    "s3://saha-open-data/images/cosmx/TAP01/SAHA_CR_COL_A1_TAP01_morphology.tif",
    overview_level=2,   # downsampled thumbnail
    lock=False,
)
```

**Reading OME-Zarr:**
```python
import zarr, dask.array as da

store = zarr.open("s3://saha-open-data/images/cosmx/TAP01/SAHA_CR_COL_A1_TAP01.zarr",
                  mode="r")
img_dask = da.from_zarr(store["0"])  # full-resolution
```

---

## Processed Objects

| Format | Extension | Loaded with |
|--------|-----------|-------------|
| AnnData | `.h5ad` | `scanpy.read_h5ad()` |
| Seurat | `.rds` | `readRDS()` |
| SpatialData | `.zarr/` | `spatialdata.read_zarr()` |

The released objects are SpatialData stores; the table inside each one is an
AnnData, so `sdata.tables["table"]` behaves as you expect:
- `.X` — normalized log1p counts
- `.raw` — raw integer counts
- `.obs` — cell metadata (cell type, spatial coords, organ, condition,
  **`donor_core`** — donor identity is per cell, not per sample)
- `adata.var` — gene metadata (HGNC symbol, Ensembl ID)
- `adata.obsm["spatial"]` — XY tissue coordinates (microns)
- `adata.obsm["X_umap"]` — 2D UMAP embedding

---

## Release Layout — Zenodo and AWS

The same objects are published twice, for two different jobs. Zenodo is the
**citable** copy: DOI'd, versioned, immutable, sized to fit the platform's
50 GB per-record cap. AWS is the **working** copy: a strict superset, with
full-resolution imagery, the raw instrument output, and metadata laid out for
Athena. Nothing is on Zenodo that is not also on AWS.

### Zenodo

One parent collection record plus one child record per organ, so matched RNA,
protein and both platforms for a tissue arrive together. Two exceptions:
pancreas is split by platform (both together are 64 GB, over the cap), and
colorectal cancer is its own record rather than being buried inside normal
colon.

```
saha-v1-<organ>/                     one child record per organ
  SAHA_<..>.zarr.zip                 one archive per acquisition
  file_manifest.csv                  per-file sample, donors, cells, size, md5
  MD5SUMS.txt
  README.md                          contents, how to read, caveats

saha-v1 (parent collection)
  samples.parquet  donors.parquet  runs.parquet  panels.parquet
  measurements.csv                  one row per acquisition x donor core
  cohort_coverage.csv               one row per donor core, vs Table 1
  LICENSE-DATA  README.md
```

Archives are ZIP_STORED — a container, not compression; zarr chunks are already
compressed. Morphology images are downsampled 4x (0.48 um/px) so the records fit;
the full-resolution copies live on AWS.

The metadata tables are **flat single files** here, because a Zenodo record is a
flat list of files and a person downloading one wants one file.

### AWS — `s3://saha-open-data/`

```
metadata/
  samples/organ=<o>/platform=<p>/*.parquet     Hive-partitioned for Athena
  donors/   runs/   panels/
  measurements.csv   cohort_coverage.csv
raw/
  <platform>/<generation_institution>/<run_id>/<sample_id>/
processed/
  <record_slug>/<sample_id>.zarr/                 full-resolution imagery
docs/
  README.md  DATA_DICTIONARY.md  QUICK_START.md  ACCESS_TIERS.md  FILE_FORMATS.md
```

The metadata tables are **Hive-partitioned directories** here, not flat files:
that is what makes `organ` and `platform` partition keys in Glue, so an Athena
query that filters on them scans only the matching prefixes. A flat
`metadata/samples.parquet` would work for pandas and defeat the catalog, so the
two layouts are deliberately different and neither is a mistake.

**`processed/` is grouped by Zenodo record slug** (decided 2026-10-05), with
plain names rather than Hive `organ=`/`platform=` syntax: Hive partitioning earns
its noise only where a query engine discovers partitions from path names, and no
Athena query scans zarr stores. The slug is computed by `record_slug()` from
`samples.parquet`, never parsed from the sample id -- the three-letter codes are
not one-to-one with the organ enum (`PROS` -> prostate, `LN` -> lymph_node, `IBD`
-> ileum or rectum depending on cohort).

There is no `zarr/` level under `processed/`: SpatialData is the only processed
format, so a one-child directory would only lengthen every path.

The alternative, `processed/<organ>/<platform>/`, was considered and
rejected. It would keep two Zenodo-only constraints (pancreas split by platform
for the 50 GB cap, CRC lifted out of colon) out of S3, but at the cost of a second
layout that users must map back to the citable records. Identical membership in
both distributions was judged worth more.

`raw/` keeps the `platform / institution / run / sample` hierarchy instead. The
institution and run levels matter because the same slide is re-run across sites
and batches, and a flat `raw/<platform>/<sample_id>/` cannot express that. The
two trees are shaped differently because they answer different questions: `raw/`
is "which acquisition produced this", `processed/` is "what tissue is this".

There is no `processed/h5ad/` tree. It was in an earlier draft of this layout,
but `data/h5ad_public/` was never built and the SpatialData objects are the
primary format; advertising a prefix that ships empty is worse than omitting it.

### Finding the same data in both places

The S3 prefix **is** the Zenodo record slug, produced by the same `record_slug()`
the packaging uses, so the two distributions have identical membership and there
is no mapping to consult. `saha-v1-appendix` on Zenodo is the same 13 objects as
`processed/saha-v1-appendix/` on S3.

```
processed/saha-v1-appendix/         13      saha-v1-pancreas-cosmx/    6
          saha-v1-bone-marrow/       7      saha-v1-pancreas-xenium/   9
          saha-v1-colon/             6      saha-v1-prostate/          5
          saha-v1-crc/               3      saha-v1-rectum/           40
          saha-v1-ileum/            27      saha-v1-stomach/           2
          saha-v1-liver/             6
          saha-v1-lymph-node/        2
```

**Condition is metadata, never a prefix.** CRC has its own record because Zenodo
gave it one for citation visibility, but that is the exception, not a rule:
PDAC sits inside `saha-v1-pancreas-cosmx` and IBD inside `saha-v1-ileum` and
`saha-v1-rectum`, exactly as in the records. To select by disease state, filter
`condition` in `samples.parquet` rather than reading it off a path.

`s3_processed_path` in `samples.parquet` gives the exact object path for any
sample_id, so a path never has to be constructed by hand.

### Access tiers

`saha-open-data` carries the processed release: public, no credentials,
CC-BY 4.0.

**`raw/` is private for now.** The bucket policy grants anonymous read to
`metadata/`, `processed/` and `docs/` only; `raw/` requires credentials. It is
3.61 TB of vendor output across 2.67M objects, and opening it is a separate
decision from publishing the objects -- note that AWS Open Data egress support
would need to cover it explicitly.

**v2 (phase 2) is private.** It is pre-publication and is not in this bucket at
all; see `data/v2/STATUS.md`.

Two further buckets are reserved for data whose consent does not permit open
release: `saha-registered-data` (DUA acknowledgement) and `saha-controlled-data`
(full IRB + DUA review). Both are empty as of v1. See ACCESS_TIERS.md.

### Raw data on AWS

Raw instrument output ships to S3 as well, under

    raw/<platform>/<institution>/<run_id>/<sample_id>/

The institution and run levels are load-bearing: the same slide is re-run across
sites and batches -- APE_A9 alone appears under three run ids at two
institutions -- so a flat `raw/<platform>/<sample_id>/` would collide re-runs on
one prefix.

The problem raw poses is not the S3 layout but the source side. It lives in three
unrelated trees with three different shapes:

    /athena/project-saha/data/<sample_id>/            one dir per acquisition
    /athena/project-saha/data_TAP/<organ>/...         by organ, depth varies per run
    /athena/project-saha/data_IBD/<panel>/            by panel; RDS only, no raw

Nothing derives `sample_id` from those paths reliably -- that is the same
irregularity that made `stitch_tap_images.py` necessary. So raw upload needs its
own manifest rather than a directory walk, generated once and reviewed:

    aws/raw_manifest.csv
        sample_id, source_path, s3_prefix, bytes, n_files, include, note

It lives under `aws/`, not `zenodo/`: raw never goes to Zenodo. Zenodo carries
the versioned, citable SpatialData release and nothing else, so 3.6 TB of vendor
output would be both impossible under the 50 GB cap and wrong for what that
record is for.

with `include` defaulting to false for anything a human has not confirmed. The
exclusions that already exist in `scripts/s3_upload_exclude.txt` apply on top:
vendor TileDB stores, AtoMx intermediates and per-FOV decoding artefacts are
large and are not what anyone means by "the raw data".

Three things to settle before uploading raw:

1. **Size.** The CosMx raw trees are far larger than the processed objects --
   `SAHA_CP_PRCA_UA_EC02` alone is 36 GB of RawFiles against a 2 GB object. The
   full raw corpus has not been measured; do that before committing to egress.
2. **What counts as raw.** Settled: the whole of `RawFiles/` ships, and
   `TileDB/` ships too (94.6 GB, 283K files). TileDB is the AtoMx vendor store
   and is **only readable with NanoString's AtoMx stack** -- it is included for
   completeness and reproducibility, not because it is generally usable, and the
   per-record README says so. Note that publishing `RawFiles/` alongside the
   objects does publish the same pixels twice, since the stitched morphology is
   derived from those per-FOV TIFFs; that is accepted as the cost of shipping
   genuine vendor-native raw.
3. **Dulai has no raw to publish.** Only Seurat RDS and H&E were ever shared.

### What is NOT yet uniform

Recorded here because it is the difference between the design and the build, and
it governs what "AWS is a superset" currently means:

- **Full-resolution imagery covers 49 of 126 acquisitions**: the 22 v1 CosMx, the
  24 TAP (two of which have no imagery at all), and the 3 PRCA. Verified
  2026-09-25 by comparing `images/morphology` between tiers, e.g.
  `SAHA_CR_APE_A9_TAP03` 515 MB -> 6.4 GB and `SAHA_CR_PRCA_UA_EC02` 606 MB ->
  7.5 GB, the 4x linear swap `build_aws_tier.py` performs.
  The remaining 77 are identical in both tiers for good reasons rather than as a
  gap: the 52 Dulai acquisitions have no imagery in existence (only Seurat RDS
  and H&E were ever shared) and the 25 Xenium objects carry the vendor's own
  native-resolution images, so there is nothing to downsample from.
  Tier sizes as built: AWS 818 GB (aws_objects 607, tap_aws 123, xenium 68,
  prca_aws 19, dulai 0.5) against Zenodo's 165 GB.
- There is no `processed/h5ad/` tree; see the layout above. `data/h5ad_public/`
  was never built and the SpatialData objects are the primary format.

---

## File Naming Conventions

```
SAHA_{consortium_code}_{organ_code}_{sample_index}_{run_id}
```

| Part | Example | Meaning |
|------|---------|---------|
| `SAHA` | `SAHA` | Fixed consortium prefix |
| `{consortium_code}` | `CR` | Two-letter institution+region code |
| `{organ_code}` | `COL` | Three-letter organ abbreviation |
| `{sample_index}` | `A1` | Slide letter + section number |
| `{run_id}` | `TAP01` | Run batch identifier |

Common organ codes: `COL` (colon), `ILE` (ileum), `LIV` (liver), `LNG` (lung), `LYN` (lymph node), `SPL` (spleen), `THY` (thymus), `APE` (appendix)
