# SAHA Data Access

Data schemas, metadata infrastructure, and access documentation for the **Spatial Atlas of Human Anatomy (SAHA)** — hosted on the [AWS Open Data Program](https://registry.opendata.aws/).

## About SAHA

SAHA is a multimodal, subcellular-resolution spatial reference atlas of human tissues, profiling 15M+ cells from 100+ donors across 16 tissue types. It captures matched RNA (up to 19,000 genes), protein (67 markers), and histological data at ~50 nm resolution using CosMx, Xenium, GeoMx, and RNAscope spatial omics platforms. The atlas includes both healthy and disease tissues (colorectal cancer, pancreatic ductal adenocarcinoma, inflammatory bowel disease).

- **Website**: [saha-project.org](https://www.saha-project.org/)
- **Preprint**: [bioRxiv 2025.06.16.658716](https://www.biorxiv.org/content/10.1101/2025.06.16.658716v3)
- **Data deposits**: [zenodo.org/communities/saha](https://zenodo.org/communities/saha)

## Repository Contents

```
data/             # Metadata Parquet tables (samples, donors, runs, panels)
schemas/          # JSON Schema definitions for each metadata table
scripts/          # Data standardization and validation scripts
docs/             # Data dictionary, quick start, access tiers, file formats
tutorials/        # Jupyter notebooks for querying and analyzing SAHA data
dataset.yaml      # AWS Open Data Registry submission file
```

## Metadata Tables

Four Parquet tables are provided in `data/` and mirrored at `s3://saha-open-data/metadata/`.

| Table | Rows | Description |
|-------|------|-------------|
| `samples.parquet` | 47 | One row per sample — organ, condition, platform, donor, n_cells, QC status, S3 paths |
| `donors.parquet` | 5 | One row per donor — age group, sex, tissue source, consent level |
| `runs.parquet` | 11 | One row per acquisition run — instrument, institution, date, panel, protocol fields |
| `panels.parquet` | 6 | Panel configurations — platform, plex, gene/protein lists |

## Data Organization on S3

```
s3://saha-open-data/
├── processed/
│   ├── h5ad/
│   │   ├── cosmx/RNA/        # Organ-level CosMx RNA AnnData (e.g. SAHA_COL_RNA.h5ad)
│   │   ├── cosmx/PRT/        # Organ-level CosMx Protein AnnData
│   │   └── xenium/           # Sample-level Xenium AnnData
│   └── zarr/
│       ├── xenium/           # Xenium SpatialData zarr objects
│       └── cosmx/            # CosMx SpatialData zarr objects
├── raw/
│   ├── cosmx/{sample_id}/    # Raw CosMx flat files (controlled access)
│   └── xenium/{sample_id}/   # Raw Xenium output (controlled access)
├── metadata/                 # Parquet tables (samples, donors, runs, panels)
└── docs/                     # Documentation
```

CosMx processed data is aggregated to organ level (all samples from one organ in one h5ad). Xenium processed data is kept at sample level. Raw data requires registration — see [docs/ACCESS_TIERS.md](docs/ACCESS_TIERS.md).

## Quick Start

**Load metadata locally** (no AWS credentials needed):

```python
import pandas as pd

samples = pd.read_parquet("data/samples.parquet")
colon = samples.query("organ == 'colon' and qc_pass == True")
print(colon[["sample_id", "donor_id", "n_cells", "s3_processed_path"]])
```

**Load metadata from S3** (anonymous access):

```python
import pandas as pd

samples = pd.read_parquet(
    "s3://saha-open-data/metadata/samples.parquet",
    storage_options={"anon": True},
)
```

**Load a processed h5ad** (anonymous access):

```python
import scanpy as sc

adata = sc.read_h5ad(
    "s3://saha-open-data/processed/h5ad/cosmx/RNA/SAHA_COL_RNA.h5ad",
    backed="r",
)
```

**Query metadata with Athena:**

```sql
SELECT s.sample_id, s.organ, s.n_cells, d.consent_level
FROM saha.samples s
JOIN saha.donors d ON s.donor_id = d.donor_id
WHERE s.qc_pass = true
ORDER BY s.organ;
```

See [tutorials/](tutorials/) for full worked examples.

## Scripts

| Script | Purpose |
|--------|---------|
| `standardize_h5ad.py` | Produce public h5ad files from internal objects (whitelist obs columns, add `sample_id` and `donor_id`) |
| `create_cosmx_zarr.py` | Build CosMx SpatialData zarr from raw flat files |
| `ingest_samples.py` | Validate and convert sample manifest → `samples.parquet` |
| `ingest_donors.py` | Validate and convert donor metadata → `donors.parquet` |
| `ingest_runs.py` | Validate and convert run metadata → `runs.parquet` |
| `ingest_panels.py` | Validate and convert panel definitions → `panels.parquet` |
| `validate_release.py` | Cross-check parquet referential integrity and S3 object existence |
| `test_athena.py` | Run validation queries against the Glue/Athena catalog |
| `generate_data_dictionary.py` | Auto-generate `docs/DATA_DICTIONARY.md` from schemas |

## Contributing Data

Institutions contributing spatial omics data to SAHA should see [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) for the submission process, required metadata fields, and validation instructions.

## License

Code and schemas in this repository: MIT.
SAHA datasets: CC-BY-NC-ND 4.0. See [docs/ACCESS_TIERS.md](docs/ACCESS_TIERS.md) for details on open, registered, and controlled access tiers.

## Citation

> Park J, et al. Spatial Atlas of Human Anatomy (SAHA). in revision. Preprint: bioRxiv 2025.06.16.658716v3.

## Contact

Jiwoon Park — [saha-project.org](https://www.saha-project.org/)
