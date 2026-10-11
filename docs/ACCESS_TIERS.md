# SAHA Data Access Tiers

SAHA data are organized into three access tiers based on donor consent and data sensitivity.


## Tier Summary

| Tier | Who can access | What's available | License |
|------|---------------|------------------|---------|
| **Open** | Anyone, no account | Expression matrices, cell coordinates, metadata (organ, platform, condition, QC), donor-core identity, tissue source country | CC-BY 4.0 |
| **Registered** | Researchers who accept DUA | Open tier + donor age group, sex, and ethnicity if available and deidentifiable; tissue source institution | CC-BY 4.0 |
| **Controlled** | Approved projects with IRB | Registered tier + exact age, comorbidities, clinical annotations | Full DUA required |

Open- and registered-tier data are released under [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/) — free to use, modify, and redistribute, including commercially, provided you give attribution. Controlled-tier data are not publicly licensed; access is granted under a signed Data Use Agreement.

All users of SAHA data are asked to observe the [SAHA Data Use Statement](#saha-data-use-statement) below.

---

## Open Tier

No registration required. All data are hosted on the AWS Open Data Program (S3) and can be accessed without AWS credentials.

**Includes:**
- Raw instrument output (CosMx flat files, Xenium HDF5, etc.)
- Processed AnnData/Seurat objects with gene expression
- Cell segmentation masks and spatial coordinates
- Metadata: `sample_id`, `organ`, `platform`, `condition`, `panel_name`, `n_cells`, `n_cells_qc_pass`
- Donor-core identity: `donors.donor_id`, `donor_core`, `donor_patient`, `slide_id`, `organ`, `condition`, `in_release` -- no demographics
- `donors.tissue_source_country`
- Normal tissue samples **and** disease/cancer samples (CRC, PDAC, IBD) where donor consent permits open release

> **Note:** Some future disease/cancer samples may be restricted to the registered or controlled tier based on consent level.

**Access:**
```bash
# No credentials needed
aws s3 ls s3://saha-open-data/ --no-sign-request
```

```python
import pandas as pd
samples = pd.read_parquet("s3://saha-open-data/metadata/samples/",
                          storage_options={"anon": True})
```

---

## Registered Tier

Researchers who complete a short data use acknowledgment gain access to donor age group, sex, and ethnicity if available and deidentifiable.

**Additional data beyond open tier:**
- `donors.age_group` (pediatric / adult / elderly)
- `donors.sex` (M / F / unknown)
- `donors.ethnicity` (NIH standard categories)
- `donors.tissue_source_institution`

Each field is released only where it was recorded and where, combined with organ, condition and the other released fields, it does not single out an individual. Institution sits here rather than in the open tier because, alongside age group, sex, ethnicity and a small disease cohort, it narrows a donor down.

**To register:**
1. Visit [saha-project.org/access](https://www.saha-project.org/access)
2. Sign in with your institutional email
3. Accept the Registered Data Use Acknowledgment
4. Receive AWS credentials scoped to the registered S3 prefix

**S3 prefix:** `s3://saha-registered-data/`

---

## Controlled Tier

Projects requiring exact age or clinical detail must apply for controlled access.

**Additional data beyond registered tier:**
- `donors.age` (exact age in years)
- `donors.comorbidities` (free-text list)
- Clinical annotations linked to tissue samples (e.g. treatment response, pathology detail)

**Requirements:**
- Institutional Review Board (IRB) approval or equivalent
- Signed Data Use Agreement (DUA) with Mason Lab / SAHA Consortium
- Named data custodian at applicant institution

**To apply:**
1. Download the DUA template from [saha-project.org/dua](https://www.saha-project.org/dua)
2. Complete with your PI and institutional signing official
3. Submit to [data@saha-project.org](mailto:data@saha-project.org)
4. Approval typically takes 2–4 weeks
5. Upon approval, receive time-limited AWS credentials

**S3 prefix:** `s3://saha-controlled-data/`

---

## `consent_level` Field

The `donors.consent_level` field records the maximum tier permitted by each donor's consent:

| Value | Meaning |
|-------|---------|
| `open` | Data can be shared without restriction |
| `registered` | Requires DUA acknowledgment |
| `controlled` | Requires full IRB and DUA review |

Metadata tables automatically include only the columns permissible at each tier. The `open` version of `donors.parquet` carries donor-core identity, organ and condition only. The registered version adds `age_group`, `sex` and `ethnicity`; the controlled version adds exact `age`, `comorbidities` and clinical annotations. `measurements.csv` and `cohort_coverage.csv` ship in the open tier and carry no demographics.

---

## SAHA Data Use Statement

Open- and registered-tier data are licensed CC-BY 4.0, which imposes no conditions beyond attribution. The following are **community expectations, not license terms** — we ask that all users observe them:

1. **Do not attempt re-identification.** Do not attempt to identify individual donors, contact them, or link SAHA data to other datasets for the purpose of re-identification.
2. **Please cite the SAHA marker paper.** CC-BY requires attribution; we additionally ask that you cite Park J et al., *Spatial Atlas of Human Anatomy (SAHA)* (see [readme.md](../readme.md#citation)) so that data contributions remain traceable.
3. **Not for clinical or diagnostic use.** SAHA data are provided for research purposes only. They are not validated for clinical decision-making, diagnosis, or treatment.
4. **No warranty.** Data are provided "as is," without warranty of any kind, as set out in Section 5 of the CC-BY 4.0 license.

Legally binding restrictions apply only to the **controlled tier**, where they are set out in the signed Data Use Agreement.

---

## Questions

Contact Jiwoon Park at [data@saha-project.org](mailto:data@saha-project.org) or open an issue in this repository.
