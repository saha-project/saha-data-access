# SAHA Data Access

Data schemas, metadata infrastructure, and access documentation for the **Spatial Atlas of Human Anatomy (SAHA)** — hosted on the [AWS Open Data Program](https://registry.opendata.aws/).

> **Status (November 2026):** the AWS Open Data bucket `s3://saha-open-data` is not live yet. A copy
> with the same layout is readable now at `s3://saha-public` (Wasabi, anonymous). See [docs/QUICK_START.md](docs/QUICK_START.md).

## About SAHA

SAHA is a multimodal, multi-platform spatial reference atlas of human tissues. The v1 release holds 12.4M cells from 126 acquisitions of 115 donor cores (86 patients) across 10 organs, measured on CosMx (1,000- and 6,000-plex RNA; 64-plex protein) and Xenium (377-plex, colon and 5K panels), with RNA, protein and a second platform on adjacent sections of the same blocks. The atlas includes both healthy and disease tissues (colorectal cancer, pancreatic ductal adenocarcinoma, inflammatory bowel disease).

### Releases

| release | contents | acquisitions | cells | donors | access |
|---|---|---:|---:|---:|---|
| `v1` | breadth tier: 10 organs, CosMx RNA + protein and Xenium on shared blocks | 126 | 12,408,790 | 86 (115 donor cores) | open (CC-BY 4.0) |
| `v2-preview` | prostate depth tier: CosMx WTx RNA + 64-plex protein on the same section, registered H&E, Xenium 5K on a serial section | 6 | 1,683,380 | 7 (69 cores) | reviewers and collaborators now; open at publication |
| `pbmc-stamp` | healthy-donor PBMCs, CosMx WTx STAMP | 3 flow cells (36 spots) | 3,220,395 | 9 | open at publication |
| **total** | | **135** | **17,312,565** | **102** | |

Object ids and bucket prefixes keep their original names (`saha-v1-*`, `phase2/`, `saha-v2-*`).

Most slides are **tissue microarrays** carrying 2-28 donor cores, so donor identity is a per-**cell** property held in `obs['donor_core']`, not a column on `samples.parquet`. See [docs/DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md).

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

## Dataset Tally

Current cohort and run counts, with breakdowns by organ, status, assay, panel, and site:
**[View the live tally →](https://saha-project.github.io/saha-data-access/stats.html)** — built from publication Tables 1 & 2; regenerate with `python scripts/generate_stats.py`.

## Metadata Tables

Four Parquet tables are provided in `data/` and mirrored under `metadata/` at the data root.

| Table | Rows | Description |
|-------|------|-------------|
| `samples.parquet` | 126 | One row per acquisition — organ, condition, platform, n_donors, n_cells, QC status, S3 paths |
| `donors.parquet` | 121 | One row per donor **core** — slide, organ, condition; `donor_id` is one per patient, so cores from the same patient share it (demographics are registered-tier; see [ACCESS_TIERS](docs/ACCESS_TIERS.md)) |
| `runs.parquet` | 19 | One row per acquisition run — instrument, institution, date, panel, protocol fields |
| `panels.parquet` | 8 | Panel configurations — platform, plex, gene/protein lists |

## Data Organization

```
<data root>/                 s3://saha-public (Wasabi, now) or s3://saha-open-data (AWS, once live)
├── metadata/                samples, donors, runs, panels (Parquet) + measurements.csv
└── processed/<record>/      one SpatialData (zarr) store per acquisition, e.g. processed/saha-v1-colon/SAHA_CR_COL_A1_TAP01.zarr
```

`samples.s3_processed_path` gives each store's path relative to the root. Raw instrument output is not
in the public copy; see [docs/ACCESS_TIERS.md](docs/ACCESS_TIERS.md).

## Note on Cell Types

Cell types were assigned by a mix of manual and automated methods: expert annotation where it
existed, mapping of earlier reference-based labels onto one shared vocabulary, and label
transfer (logistic regression) from annotated SAHA objects for objects that had no labels.
Each cell records how its label was made in `cell_type_broad_source`; transferred labels also
carry `cell_type_broad_confidence`, and cells without a confident label are `NA`. The original
labels of the source objects (`cell_type`, `cell_type_detailed`, ...) are kept unchanged.

**These labels are a starting point, not ground truth. We strongly recommend evaluating them,
and re-labelling, for your own analysis.**

## Quick Start

**Load metadata locally** (no AWS credentials needed):

```python
import pandas as pd

samples = pd.read_parquet("data/samples.parquet")
colon = samples.query("organ == 'colon'")
print(colon[["sample_id", "n_donors", "n_cells", "s3_processed_path"]])

# donor identity is per cell, inside the objects:
#   adata.obs.merge(donors, on="donor_core", how="left")
```

**Load metadata and one cell table from the data root** (anonymous): see [docs/QUICK_START.md](docs/QUICK_START.md),
which also shows how to open a full SpatialData object.

**Query the metadata tables** (works today on the copies in `data/`):

```python
import pandas as pd
samples = pd.read_parquet("data/samples.parquet")
samples.groupby(["organ", "platform"]).n_cells.agg(["count", "sum"])
```

Amazon Athena (SQL over S3) will work once the AWS bucket is live, and only after you register the
tables in your own AWS account (setup steps will be published with the bucket); there is no
shared `saha` database to connect to.

See [tutorials/](tutorials/) for full worked examples.

## Scripts

| Script | Purpose |
|--------|---------|
| `standardize_h5ad.py` | Produce public h5ad files from internal objects (whitelist obs columns, add `sample_id` and `donor_core`) |
| `create_cosmx_zarr.py` | Build CosMx SpatialData zarr from raw flat files |
| `ingest_samples.py` | Validate and convert sample manifest → `samples.parquet` |
| `build_donors_table.py` | Build `donors.parquet`, one row per donor core, from the resolved assignments + Table 1 |
| `donor_map.py` | Single source for sample → donor cores; used by ingest, build and validate |
| `release_tally.py` | Generate `zenodo/release_breakdown.csv` and refresh the record split |
| `ingest_runs.py` | Validate and convert run metadata → `runs.parquet` |
| `ingest_panels.py` | Validate and convert panel definitions → `panels.parquet` |
| `fill_processed_paths.py` | Fill `samples.s3_processed_path` (relative to the data root) from the uploaded stores |
| `validate_release.py` | Cross-check parquet referential integrity and S3 object existence |
| `test_athena.py` | Run validation queries against the Glue/Athena catalog |
| `generate_data_dictionary.py` | Auto-generate `docs/DATA_DICTIONARY.md` from schemas |
| `generate_stats.py` | Auto-generate `docs/stats.html` dataset tally from publication Tables 1 & 2 |

## Contributing Data

Institutions contributing spatial omics data to SAHA should see [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) for the submission process, required metadata fields, and validation instructions.

## License

Code and schemas in this repository: MIT.
SAHA datasets (open and registered tiers): [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/). Controlled-tier data are released under a signed DUA, not a public license. See [docs/ACCESS_TIERS.md](docs/ACCESS_TIERS.md) for details on the three access tiers and the SAHA Data Use Statement.

## Citation

> Park J, et al. Spatial Atlas of Human Anatomy (SAHA). Preprint: bioRxiv 2025.06.16.658716v3.

## Contact

Jiwoon Park — [saha-project.org](https://www.saha-project.org/)
