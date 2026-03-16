# DATA_DICTIONARY.md

> Auto-generated from `schemas/*.json` on 2026-03-16. Edit the JSON Schema
> files to update this document, then re-run
> `python scripts/generate_data_dictionary.py`.

This document describes the four Parquet metadata tables that form the
SAHA spatial omics open-data catalog.  All column names use `snake_case`;
dates follow ISO 8601 (`YYYY-MM-DD`); institution codes and platform names
are lowercase per SAHA conventions.


## Tables

- [samples.parquet](#samplesparquet)

- [donors.parquet](#donorsparquet)

- [runs.parquet](#runsparquet)

- [panels.parquet](#panelsparquet)


---

## samples.parquet

One row per tissue sample.  Joins to `donors.parquet` on `donor_id`, to `runs.parquet` on `run_id`, and to `panels.parquet` on `panel_name`.


> **Partitioning:** Partitioned by `organ` and `platform`.


| Column | Type | Required | Constraints | Description |
|--------|------|:--------:|-------------|-------------|
| `sample_id` | `string` | yes | pattern: `^SAHA_[A-Z]{2}_[A-Z0-9]+_.+$` | Unique sample identifier derived from folder name (e.g. SAHA_CR_COL_A1_TAP01). |
| `run_id` | `string` | yes |  | Run/batch identifier linking to runs.parquet (e.g. TAP01, EC02, SJJP01). |
| `platform` | `string` | yes | enum: `cosmx`, `xenium`, `g4x`, `geomx`, `rnascope` | Spatial omics instrument platform. |
| `assay_type` | `string | null` |  | enum: `rna`, `protein`, `multiome` | Molecular modality measured (rna or protein). |
| `generation_institution` | `string` | yes | enum: `cornell`, `wcm`, `st_jude`, `msk`, `broad`, `other` | Institution that performed the data acquisition run. |
| `tissue_source_institution` | `string | null` |  |  | Institution that sourced the tissue (ISO 3166-1 alpha-2 country appended if needed). |
| `organ` | `string` | yes | enum: `appendix`, `bone_marrow`, `colon`, `ileum`, `kidney`, `liver`, `lung`, `lymph_node`, `pancreas`, `prostate`, `spleen`, `stomach`, `thymus`, `other` | Organ of tissue origin. |
| `condition` | `string` | yes | enum: `normal`, `cancer`, `disease`, `other` | Gross tissue condition. |
| `anatomical_region` | `string | null` |  |  | Short tissue-type code from the source manifest (e.g. COL, LIV, APE). |
| `donor_id` | `string | null` |  |  | Donor identifier linking to donors.parquet. |
| `slide_id` | `string | null` |  |  | Instrument slide or section identifier. |
| `panel_name` | `string | null` |  |  | Panel name linking to panels.parquet. |
| `panel_plex` | `integer | null` |  | min=1 | Number of targets (genes or proteins) in the panel. |
| `n_cells` | `integer | null` |  | min=0 | Number of cells detected after segmentation. |
| `n_fovs` | `integer | null` |  | min=0 | Number of fields of view (CosMx) or sections (Xenium) in this sample. |
| `qc_pass` | `boolean | null` |  |  | Whether the sample passed QC review. |
| `processing_date` | `string | null` |  | format: date | Instrument run start date (ISO 8601 YYYY-MM-DD). |
| `slide_prepared_date` | `string | null` |  | format: date | Slide preparation date (ISO 8601 YYYY-MM-DD). |
| `s3_raw_prefix` | `string | null` |  |  | S3 URI prefix for raw instrument output (e.g. s3://saha-open-data/cosmx/wcm/TAP01/SAHA_CR_COL_A1_TAP01/). |
| `s3_processed_path` | `string | null` |  |  | S3 URI for the processed AnnData/Seurat object. |
| `on_wasabi` | `boolean | null` |  |  | Whether raw data is currently mirrored on Wasabi object storage. |


---

## donors.parquet

One row per de-identified tissue donor.  Controlled-access columns (age, ethnicity, comorbidities) are omitted from the open tier.


| Column | Type | Required | Constraints | Description |
|--------|------|:--------:|-------------|-------------|
| `donor_id` | `string` | yes |  | Unique donor identifier (de-identified, e.g. SAHA_D001). |
| `age` | `integer | null` |  | min=0; max=120 | Donor age in years at time of tissue collection. |
| `age_group` | `string | null` |  | enum: `pediatric`, `adult`, `elderly` | Broad age category for access-tier reporting. |
| `sex` | `string | null` |  | enum: `M`, `F`, `unknown` | Biological sex (M/F/unknown). |
| `ethnicity` | `string | null` |  |  | Self-reported ethnicity using NIH standard categories. |
| `comorbidities` | `array[string] | null` |  |  | List of relevant comorbidities or diagnoses. |
| `tissue_source_institution` | `string | null` |  |  | Institution that collected or provided the tissue. |
| `tissue_source_country` | `string | null` |  | pattern: `^[A-Z]{2}$` | Country of tissue collection (ISO 3166-1 alpha-2, e.g. US, DE). |
| `consent_level` | `string | null` |  | enum: `open`, `registered`, `controlled` | Maximum data access tier permitted by donor consent. |


---

## runs.parquet

One row per instrument acquisition run (batch).  Links to `samples.parquet` via `run_id`.


| Column | Type | Required | Constraints | Description |
|--------|------|:--------:|-------------|-------------|
| `run_id` | `string` | yes |  | Unique run/batch identifier (e.g. TAP01, EC02, SJJP01, MS01). |
| `platform` | `string` | yes | enum: `cosmx`, `xenium`, `g4x`, `geomx`, `rnascope` | Spatial omics instrument platform used for this run. |
| `instrument_id` | `string | null` |  |  | Specific instrument serial number or lab-assigned ID. |
| `generation_institution` | `string` | yes | enum: `cornell`, `wcm`, `st_jude`, `msk`, `broad`, `other` | Institution that performed the run. |
| `run_date` | `string | null` |  | format: date | Date the instrument run started (ISO 8601 YYYY-MM-DD). |
| `panel_name` | `string | null` |  |  | Primary panel used in this run (links to panels.parquet). |
| `panel_version` | `string | null` |  |  | Panel lot or version number. |
| `n_samples` | `integer | null` |  | min=1 | Number of tissue samples processed in this run. |
| `instrument_software_version` | `string | null` |  |  | Instrument acquisition software version. |
| `slide_prep_manual` | `string | null` |  |  | NanoString/10x slide preparation manual part number. |
| `instrument_manual` | `string | null` |  |  | Instrument user manual part number. |
| `qc_summary` | `string | null` |  |  | Free-text QC summary or flag for the run. |
| `s3_raw_prefix` | `string | null` |  |  | S3 URI prefix for all raw data from this run. |


---

## panels.parquet

One row per panel configuration.  `gene_list` and `protein_list` are stored as Parquet list columns.


| Column | Type | Required | Constraints | Description |
|--------|------|:--------:|-------------|-------------|
| `panel_name` | `string` | yes |  | Canonical panel name as used in samples/runs tables (e.g. UCC 1K, 6K Discovery). |
| `platform` | `string` | yes | enum: `cosmx`, `xenium`, `g4x`, `geomx`, `rnascope` | Platform for which this panel was designed. |
| `assay_type` | `string` |  | enum: `rna`, `protein`, `multiome` | Whether the panel measures RNA or protein targets. |
| `plex` | `integer` | yes | min=1 | Total number of targeted analytes (genes + proteins). |
| `version` | `string | null` |  |  | Panel design version or catalog revision. |
| `gene_list` | `array[string] | null` |  |  | List of targeted gene symbols (HGNC). |
| `protein_list` | `array[string] | null` |  |  | List of targeted protein names or antibody clones. |
| `catalog_number` | `string | null` |  |  | Vendor catalog number for this panel. |
| `notes` | `string | null` |  |  | Free-text notes about panel design or modifications. |


---
