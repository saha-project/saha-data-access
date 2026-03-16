# SAHA Data Access Tiers

SAHA data are organized into three access tiers based on donor consent and data sensitivity.

## Tier Summary

| Tier | Who can access | What's available | License |
|------|---------------|------------------|---------|
| **Open** | Anyone, no account | Expression matrices, cell coordinates, metadata (organ, platform, QC) | CC-BY-NC-ND 4.0 |
| **Registered** | Researchers who accept DUA | Open tier + donor demographics (age group, sex) | CC-BY-NC-ND 4.0 |
| **Controlled** | Approved projects with IRB | Registered tier + exact age, ethnicity, comorbidities, clinical data | Full DUA required |

---

## Open Tier

No registration required. All data are hosted on the AWS Open Data Program (S3) and can be accessed without AWS credentials.

**Includes:**
- Raw instrument output (CosMx flat files, Xenium HDF5, etc.)
- Processed AnnData/Seurat objects with gene expression
- Cell segmentation masks and spatial coordinates
- Metadata: `sample_id`, `organ`, `platform`, `condition`, `panel_name`, `n_cells`, `qc_pass`

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

Researchers who complete a short data use acknowledgment gain access to donor-level demographic metadata.

**Additional data beyond open tier:**
- `donors.age_group` (pediatric / adult / elderly)
- `donors.sex` (M / F / unknown)
- `donors.tissue_source_institution`
- `donors.tissue_source_country`

**To register:**
1. Visit [saha-project.org/access](https://www.saha-project.org/access)
2. Sign in with your institutional email
3. Accept the Registered Data Use Acknowledgment
4. Receive AWS credentials scoped to the registered S3 prefix

**S3 prefix:** `s3://saha-registered-data/`

---

## Controlled Tier

Projects requiring the most sensitive donor attributes must apply for controlled access.

**Additional data beyond registered tier:**
- `donors.age` (exact age in years)
- `donors.ethnicity` (NIH standard categories)
- `donors.comorbidities` (free-text list)
- Clinical annotations linked to tissue samples

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

Metadata tables automatically include only the columns permissible at each tier. The `open` version of `donors.parquet` omits `age`, `ethnicity`, and `comorbidities`.

---

## Questions

Contact [data@saha-project.org](mailto:data@saha-project.org) or open an issue in this repository.
