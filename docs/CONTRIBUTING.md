# Contributing Data to SAHA

This guide explains how consortium institutions can contribute new spatial omics data to the SAHA open data release.

---

## Overview

New data submissions go through four stages:

```
1. Pre-submission check  →  2. Metadata preparation  →  3. Upload  →  4. Validation & ingest
```

---

## 1. Pre-Submission Checklist

Before preparing files, confirm:

- [ ] IRB approval (or equivalent) covers data sharing
- [ ] Donor consent permits open or registered data release
- [ ] Samples passed internal QC at your institution
- [ ] Run IDs follow the SAHA naming convention (see below)
- [ ] Your institution has a SAHA data custodian registered with Mason Lab

If you need a new institution code assigned, email [data@saha-project.org](mailto:data@saha-project.org).

---

## 2. Metadata Preparation

### 2a. Required CSV files

Prepare one CSV per table. Column names must match the schema exactly (snake_case).

#### `sample_metadata.csv`

Minimum required columns:

| Column | Example | Notes |
|--------|---------|-------|
| `sample_id` | `SAHA_CR_COL_A1_TAP01` | Must match naming convention |
| `run_id` | `TAP01` | Links to runs table |
| `platform` | `cosmx` | lowercase: cosmx, xenium, g4x, geomx, rnascope |
| `organ` | `colon` | See allowed values in `schemas/samples.json` |
| `condition` | `normal` | normal / cancer / disease / other |
| `generation_institution` | `cornell` | Must be a registered institution code |

See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for all optional columns.

#### `run_metadata.csv`

| Column | Example |
|--------|---------|
| `run_id` | `TAP01` |
| `platform` | `cosmx` |
| `generation_institution` | `cornell` |
| `run_date` | `2024-03-15` (ISO 8601) |
| `panel_name` | `UCC 1K` |
| `n_samples` | `8` |

#### `donor_metadata.csv`

Only needed if new donors are being added. Donor age group, sex, and ethnicity if available and deidentifiable are released at the registered tier; exact age and clinical fields (comorbidities, clinical annotations) are controlled. Submit donor demographics via secure channel, never in a pull request -- this repository is public.

| Column | Example |
|--------|---------|
| `donor_core` | `COL_A1_7` |
| `slide_id` | `COL_A1` |
| `organ` | `colon` |
| `sex` | `M` |
| `age_group` | `adult` |
| `tissue_source_institution` | `Cornell` |

A row is **one donor core on one slide**, not one person. SAHA slides are mostly
tissue microarrays carrying 2-28 cores, so a submission must say which core is
which — `donor_core` is the key the released objects carry per cell in
`obs['donor_core']`. Use `donor_patient` if several cores come from the same
person. Do **not** submit a scalar `donor_id` per sample: a slide is not a donor,
and collapsing the two is how 32 samples were once attributed to one fictional
donor.

### 2b. Validate your CSVs

```bash
# Clone the repo
git clone https://github.com/saha-consortium/saha-data-access
cd saha-data-access
pip install pandas pyarrow jsonschema

# Validate samples
python scripts/ingest_samples.py \
    --input  your_sample_metadata.csv \
    --output /tmp/test_samples.parquet \
    --strict

# Validate donors
python scripts/ingest_donors.py \
    --input  your_donor_metadata.csv \
    --output /tmp/test_donors.parquet \
    --strict

# Validate runs
python scripts/ingest_runs.py \
    --input  your_run_metadata.csv \
    --mode   direct \
    --output /tmp/test_runs.parquet \
    --strict
```

Fix any errors reported before uploading.

---

## 3. Upload Raw Data

### S3 path structure

Upload raw instrument output under:

```
s3://saha-open-data/raw/{platform}/{institution}/{run_id}/{sample_id}/
```

Example for a CosMx run from Cornell:
```
s3://saha-open-data/raw/cosmx/cornell/TAP01/SAHA_CR_COL_A1_TAP01/
```

### Uploading with AWS CLI

You will need upload credentials scoped to your institution's prefix. Request these from [data@saha-project.org](mailto:data@saha-project.org).

```bash
# Set credentials
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# Upload one sample
aws s3 cp ./SAHA_CR_COL_A1_TAP01/ \
    s3://saha-open-data/raw/cosmx/cornell/TAP01/SAHA_CR_COL_A1_TAP01/ \
    --recursive \
    --storage-class INTELLIGENT_TIERING

# Verify upload
aws s3 ls s3://saha-open-data/raw/cosmx/cornell/TAP01/SAHA_CR_COL_A1_TAP01/ \
    --recursive --human-readable
```

### Uploading processed objects

The release format is SpatialData (OME-NGFF / Zarr), one store per acquisition,
filed under its Zenodo record slug:
```
s3://saha-open-data/processed/{record_slug}/{sample_id}.zarr/
```
`record_slug` comes from `record_slug()` in `package_for_zenodo.py`, computed
from `samples.parquet` -- never from parsing the sample id; the three-letter codes
are not one-to-one with the organ enum.

H5AD and Seurat RDS are accepted as *submission* formats and converted; they are
not what the release ships. `s3_processed_path` is populated after upload by the
backfill step, so submitters leave it blank.

---

## 4. Validation & Ingest

After upload, notify the SAHA data team:

1. Email [data@saha-project.org](mailto:data@saha-project.org) with:
   - Your institution name and run IDs
   - Number of new samples and donors
   - S3 upload paths
   - Your validated metadata CSVs attached

2. The data team will:
   - Run the ingest scripts against your CSVs
   - Append records to the central Parquet tables
   - Update the Glue Catalog
   - Confirm inclusion in the next data release

Turnaround is typically 1–2 weeks.

---

## Naming Conventions

### Run IDs

Format: `{institution_code}{sequential_number}`

| Institution | Code | Example run IDs |
|-------------|------|-----------------|
| Cornell / TAP | `TAP` | TAP01, TAP02 |
| Weill Cornell Medicine | `WCM` | WCM01 |
| St. Jude | `SJ` | SJJP01 |
| MSK | `MS` | MS01 |
| Broad | `BR` | BR01 |

### Sample IDs

Format: `SAHA_{consortium_code}_{organ_code}_{sample_index}_{run_id}`

- `consortium_code`: two-letter code assigned by Mason Lab
- `organ_code`: three-letter abbreviation (COL, ILE, LIV, LNG, LYN, SPL, THY, APE, BOM, KID, PAN, PRO, STO)
- `sample_index`: letter (slide) + number (section), e.g. A1, A2, B1
- `run_id`: batch run ID

Example: `SAHA_CR_COL_A1_TAP01`

### Donor IDs

Format: `SAHA_D{zero-padded number}` — assigned centrally by Mason Lab to preserve de-identification.

---

## Questions

Open a GitHub issue or contact Jiwoon Park at [data@saha-project.org](mailto:data@saha-project.org).
