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

All Parquet files use Snappy compression and are queryable via Amazon Athena or any Parquet reader (pandas, R arrow, DuckDB).

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

**Processed CosMx (AnnData):**
```python
import scanpy as sc
adata = sc.read_h5ad("s3://saha-open-data/processed/by_sample/SAHA_CR_COL_A1_TAP01.h5ad",
                     storage_options={"anon": True})
# obs columns: cell_id, fov, x_centroid, y_centroid, cell_type, organ, condition, ...
# var: gene symbols (HGNC)
# obsm: X_umap, spatial (n_obs × 2 XY coords)
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

All processed H5AD files include:
- `adata.X` — normalized log1p counts
- `adata.raw` — raw integer counts
- `adata.obs` — cell metadata (cell type, spatial coords, organ, condition, donor_id)
- `adata.var` — gene metadata (HGNC symbol, Ensembl ID)
- `adata.obsm["spatial"]` — XY tissue coordinates (microns)
- `adata.obsm["X_umap"]` — 2D UMAP embedding

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
