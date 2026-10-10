# SAHA v2 (phase 2) — status

Last updated 2026-10-09. Tables here are written by `scripts/build_v2_tables.py`.

**v2 is pre-publication and staged privately.** Nothing is deposited to Zenodo
and nothing is uploaded to any S3 bucket. Because there is no Zenodo record,
there is no 50 GB cap, no 4x-downsampled tier and no zip packaging — one
full-resolution tier is the whole release. v1's tables and outputs are untouched
by anything in this directory.

## What exists

    6 acquisitions · 2 slides · 1,683,380 cells
    4 release objects · 35.4 GB in derived/v2_objects/
    4 of 6 acquisitions carry donor_core (all CosMx; A3's recovered geometrically)
    0 of 6 have a resolved condition

| sample_id | platform | assay | slide | cells | FOVs | cores |
|---|---|---|---|---:|---:|---:|
| `SAHA_XR_PROS_A2_UP01` | xenium | rna | PROS_A2 | 305,676 | — | — |
| `SAHA_XR_PROS_A3_UP01` | xenium | rna | PROS_A3 | 477,932 | — | — |
| `SAHA_CR_PROS_A2_EC05` | cosmx | rna | PROS_A2 | 174,190 | 185 | **31** |
| `SAHA_CR_PROS_A3_EC05` | cosmx | rna | PROS_A3 | 214,435 | 220 | 38 (geo) |
| `SAHA_CP_PROS_A2_EC05` | cosmx | protein | PROS_A2 | 221,374 | 185 | 31 |
| `SAHA_CP_PROS_A3_EC05` | cosmx | protein | PROS_A3 | 289,773 | 220 | 38 (geo) |

Source: `/athena/masonlab/scratch/users/jip2007/2026_SAHA_data`.

**The SpatialData objects are already built.** `processed/zarr/` holds 6
per-assay stores; `processed/export/` holds the 2 merged CosMx deliverables,
each carrying both tables (`rna`, `protein`), both modalities' boundaries
(`rna_cell_boundaries`, `prt_cell_boundaries`) and a registered H&E.

H&E attachment, as built:

| Object | H&E element |
|---|---|
| `export/SAHA_CosMx_PROS_A2` | `he_same_section` [3, 11562, 15935] |
| `export/SAHA_CosMx_PROS_A3` | `he_same_section` [3, 11886, 17429] |
| `zarr/EC_JP_7881_*` (4) | none |
| `zarr/SAHA_XR_PROS_A{2,3}` | none |

So the same-section H&E is attached to the merged CosMx objects only. The
serial-section SVS in `processed/he/` is not attached to anything, and the Xenium
objects carry no H&E even though `code/register_he_xenium.py` and
`qc/registration_xenium/` exist — that registration was computed but never
written into the store.

## Traps in this dataset — do not parse the directory names

- `20260616_SAHA_CR_PROS_A2_EC05/` contains **both** A2 and A3 objects, for both
  RNA and protein. Its name mentions only A2.
- The Xenium run directory lists region `0108728` first, but that region is
  **A3**. Mapping taken from cell counts and confirmed against the vendor QC
  workbook: `ML108208` → A2 (305,676), `ML108728` → A3 (477,932).
- CosMx `slide_ID` in obs is an internal index running **opposite** to the SAHA
  slide name: A2 → `slide_ID` 2, A3 → `slide_ID` 1. Anything keyed off it swaps
  the two slides.
- `export/` objects carry **two tables** (`protein`, `rna`). Reading only the
  first gives the protein cell count and looks like the whole object.

## Done (2026-09-26)

Release-tier objects are built in `/athena/project-saha/derived/v2_objects/`.
There is no 4x tier: v2 is S3-only, so one full-resolution tier is the release.

- **Xenium H&E attached, both slides.** The serial-section H&E was registered
  twice by different tools; the st2he-toolkit fit is the better one and is what
  was used:

      register_he_xenium.py   on-tissue 0.581, pass-1 mask fit, ~10-20 um
      st2he-toolkit align.py  on-tissue 0.764 (A2) / 0.783 (A3), refined affine

  st2he also ships `warped_spots_level0.csv`, per-cell H&E coordinates, so the
  applied transform was verified cell by cell rather than by bounding box:

      A2  305,662 cells  median residual 0.645 um  p95 1.711
      A3  477,931 cells  median residual 0.651 um  p95 1.637

  and the stored transform was checked algebraically against the source affine --
  `inv(A_global) @ A_stored` is diagonal to 2e-13 (A2) and 6e-17 (A3), so the
  write preserved it exactly. `is_self_contained()` is True for both; H&E and
  morphology pixels both decode; cell counts unchanged (305,676 / 477,932).

  Units matter here and are easy to get wrong: the object's `global` system is
  PIXELS (shapes carry Scale 4.70588 = 1/0.2125), while the stored affine targets
  MICRONS, so it needs `diag(1/px_um)` applied before use. Do not read
  `obsm['spatial']` as evidence of the coordinate system -- obsm carries no
  transform.

- **A2 protein cores transferred from A2 RNA** (`transfer_cores_v2.py`):
  221,028 of 221,374 cells labelled (99.8%), median nearest-neighbour distance
  2.11 um, all 31 cores represented, per-core protein/RNA ratios 0.94-1.54 against
  a 1.27 global ratio. Computed to a sidecar parquet first and inspected as a
  figure before being written into the release object (see below).
  The transfer uses SHAPE centroids: in the merged object the protein->RNA affine
  is baked into vector geometry only, and `CenterX_global_px` in obs still holds
  raw per-modality values (protein 4442..83480 vs rna 13..79031), so matching on
  obs would misplace nearly every cell while appearing to succeed.

- **CosMx H&E needs nothing** -- `he_same_section` is already attached to both
  merged objects, which is better than a serial section. Both `align/runs/`
  entries are Xenium runs, so no CosMx serial registration exists.

- **CosMx release objects built** (`build_v2_cosmx.py`), cores written in and the
  public obs whitelist applied per table (107 -> 45 protein, 107 -> 46 rna):

      SAHA_CosMx_PROS_A2   2 tables, 371 images, 2 shapes
                           donor_core: protein 221,028 · rna 174,190
      SAHA_CosMx_PROS_A3   2 tables, 441 images, 2 shapes
                           donor_core: protein 0 · rna 0   (correct -- none exist)

  `is_self_contained()` True for both. Dropped columns backed up to
  `derived/v2_obs_dropped/`.

  Two bugs were caught by the post-write re-read, both of which would have
  shipped silently on a presence-only check:

    * the whitelist dropped `cell_index`, these tables' `instance_key`, and
      `write()` then died with KeyError after serialising the whole object. The
      script now takes the join keys from each table's own `spatialdata_attrs`
      rather than from v1's hardcoded STRUCTURAL list, which names the keys v1's
      objects happened to use.
    * `donor_core` written as object-dtype with None does NOT round-trip: the
      Nones return as strings. A3 protein went 0 -> 289,773 non-null across the
      write, i.e. a slide with no annotation claiming a core for every cell.
      Now written as a pandas Categorical, where missing is code -1.

- **Protein panel resolved**: v1's `IO Protein (64-plex)`. The 69 variables are
  64 targets (including the isotype controls Ms IgG1 and Rb IgG) plus five
  `Channel-*` morphology imaging channels -- DNA, PanCK, Membrane, CD45, G --
  which are not antibody targets. Counted from the names, not inferred.

- **Core transfer inspected as a figure**, `data/v2/qc/v2_core_transfer_qc.png`.
  The 31 cores occupy the same grid positions in both modalities, including the
  glandular voids inside cores; per-core centroid offset is 15.6 um median
  against ~1 mm core spacing and is unstructured across the slide (a real affine
  error would grow with distance from the origin); the transfer-distance
  histogram has no second mode at core spacing, which is what a one-position
  shift would produce.

## Blockers before v2 can be released

1. **`condition` is unresolved.** One slide is cancer and one is normal; which is
   which is not established. Transcriptomic markers do **not** separate them —
   A2's own 31 cores span 5.29–14.10% AMACR-positive while the A2-vs-A3 slide
   difference is 8.58% vs 11.20%, so the between-slide difference is smaller
   than the within-slide spread on one slide. `KLK3` ranges 2.5–72.5% across
   A2's cores, so apparent marker shifts track epithelial content per core
   rather than biology. Needs the H&E in `processed/he/` read by a pathologist,
   or the blocks' accession record.
2. **RESOLVED 2026-10-09 (see below).** **Core identity is missing AT SOURCE for 3 of 4 CosMx acquisitions.** Verified
   against the AtoMx flatFile metadata, not just the objects:

       EC_JP_7881_PROS_A2_RNA_metadata_file.csv.gz   174,190 / 174,190 filled
       EC_JP_7881_PROS_A3_RNA_metadata_file.csv.gz         0 filled
       EC_JP_7881_PROS_A2_PRT_metadata_file.csv.gz         0 filled
       EC_JP_7881_PROS_A3_PRT_metadata_file.csv.gz         0 filled

   All four exports carry the same 106-column schema including `Core_ID`,
   `Composite_Core_ID`, `SAHA_Block_ID` and `Serial_Section_N` — only the A2 RNA
   study was ever annotated in AtoMx. This is **not** a build bug: `Core_ID`
   appears nowhere in `build_sd.py` or `merge_slide.py`, only in `st2he-toolkit`.
   Recovery: A2 PRT is the same slide as A2 RNA, so cores transfer by FOV
   position (what v1's `transfer_donor_core.py` does). A3 has no annotated
   source at all and needs fresh annotation, or transfer from its H&E.
   The Xenium objects have no core column at all.
3. **Per-FOV morphology is by design, not a gap.** The `export/` objects carry
   `rna_fovNNN_morphology` + `prt_fovNNN_morphology` (370 elements for A2, 440
   for A3) plus the registered H&E, rather than v1's single stitched
   `morphology`. Stitching is therefore a presentation choice for v2, not a
   prerequisite. Recorded because an earlier revision of this file wrongly
   listed it as a blocker.
4. **Demographics unlinked** for all 31 A2 cores.
5. **RESOLVED 2026-10-09 (38, one donor per punch -- see below).** **A3 `n_donors` unknown.** Cores could be derived geometrically -- TMA punches
   are spatially separated, so clustering centroids recovers how many there are
   and which cells belong to each. That yields `n_donors` and per-core grouping
   but NOT donor identity, which exists only wherever the TMA was laid out.

Resolved since this file was first written: the obs whitelist is applied, the
protein panel is identified, and the CosMx release objects are built -- see
"Done" above.

## Deliberately null

`condition` on all 6 samples and all 69 donor rows; `n_donors` on the 2 Xenium
samples; `panel_name`/`panel_plex` on the two protein samples. These are not
oversights — writing a plausible value into any of them is how v1 collapsed 32
samples onto one fictional donor. A field that exists is not a field that is
populated.

## 2026-10-06 — format, layout, raw

- **All four objects are now zarr v3** (`zarr.json`), like every v1 object
  (`scripts/convert_zarr_v3.py`). Each copy was checked element by element against
  its source -- table shapes, X sums, every obs value, image/label shapes, shape
  counts, transforms -- before replacing it; originals are in
  `derived/_replaced/v2_objects_zarr2_20261006/`. 31.5 GB, down from 36.6.
- **Queued S3 layout:** `processed/saha-v2-prostate/<object>.zarr/` (v1 convention,
  no `zarr/` level).
- **Shared-bucket naming (decided 2026-10-09):** `s3://saha-data-access/phase2/processed/saha-phase2-prostate/`.
  Bucket prefixes are `phase1/` / `phase2/` and the collection is `saha-phase2-prostate`: "v2" would read as a new
  version of the Zenodo v1.0 records. The phase-1 record names stay `saha-v1-*` (already deposited), and the internal
  paths (`data/v2`, `release/s3_v2`) are unchanged.
- **Shared-bucket naming (decided 2026-10-09):** `s3://saha-data-access/phase2/processed/saha-phase2-prostate/`.
  Bucket prefixes are `phase1/` / `phase2/` and the collection is `saha-phase2-prostate`: "v2" would read as a new
  version of the Zenodo v1.0 records. The phase-1 record names stay `saha-v1-*` (already deposited), and the internal
  paths (`data/v2`, `release/s3_v2`) are unchanged.
- **Raw is mapped:** `aws/raw_manifest_v2.csv` (`scripts/build_raw_manifest_v2.py`),
  693 GB / 157,628 files under `raw/<platform>/<institution>/<run>/<sample_id>/`.
  AtoMx TileDB and md5 lists cover both slides and sit under `.../<run>/_run/`.

## 2026-10-09 — finalized as objects (blocker 1, condition, still open)

Release stores: `/athena/project-saha/derived/release/s3_v2/processed/saha-v2-prostate/`.

- **H&E re-tiled** (`scripts/fix_he_chunks.py`, rewritten to work on zarr arrays;
  the spatialdata path refused). All 4 H&E elements went from `[3,2048,3]` chunks to
  `[3,1024,1024]`, 99,379 -> 623 files. Every pixel of every level was compared before the
  directory swap, then metadata was re-consolidated and the objects read back. The old
  elements and root `zarr.json` are in `release_qc/he_v1/v2_retile/backup/`.
- **Xenium H&E upgraded to full resolution** (`scripts/upgrade_he_level.py`). It had been attached from
  svs level 2 (~4 um/px, 16x down). It is now level 0 (0.2476 um/px): A2 47,739x61,751, A3
  51,334x73,703, [3,1024,1024] tiles, 3,876 / 4,941 files. The registration is unchanged: the stored
  affine was rescaled by the svs downsample. Checks: footprint corners within 1-4 global px
  (<1 um) of the old element, the stored transform read back exactly, and 8 tiles per object
  match the svs pixel for pixel. Re-check after the swap: NCC 0.14/0.16, cells on H&E tissue 0.84
  (the same serial-section reading as before), and the gate is clean. Old elements are in
  `release_qc/v2_he_l0/real/backup/`. CosMx H&E upgraded the same way afterwards (svs level 1 -> 0; A2 46,251x63,743, A3 47,546x69,719;
  3,899 / 4,384 files; backups in `release_qc/v2_he_l0/real_cosmx/backup/`). Gate is clean; H&E check: A2 NCC 0.44 with
  12/12 windows within 2.5 um, A3 NCC 0.36 with 9/12 (the same windows as before; two edge windows moved 2 -> 3 um,
  which is pixel granularity, not a worse fit). **All four v2 H&E images now ship at full resolution (0.2476 um/px).**
- **H&E alignment measured** (`he_alignment_check.py`, `release_qc/he_check/v2pros/`).
  CosMx same-section: A2 has 12/12 windows within ~2 um at zero shift (NCC 0.41), A3 has
  11/12 (NCC 0.39), so it is cell-level. Xenium serial sections: NCC 0.17/0.20, with
  5-25 um shifts scattered across windows, so tissue-level, as their notes already say.
  The CosMx objects had no provenance record. `uns['he_same_section_registration']`
  (source svs, method, offset, accuracy_note, same_section_confirmed) is now written to
  both tables of both objects (`scripts/write_he_provenance.py`).
- **A3 cores recovered geometrically** (`scripts/derive_cores_geometric.py`, outputs in
  `release_qc/v2_cores/`). Method: tissue islands of the cell centroids (25 um bins,
  75 um closing, components >= 300 cells).
  *Validated on A2* against AtoMx `Core_ID`: 31 vs 31, median purity 1.0. There is one
  failure: two touching cores (AtoMx 1+8) fused and one fragment split off. Expect that
  error mode wherever FOV blocks of neighbouring cores touch.
  *A3*: 40 islands. Two pairs were joined with `--merge` after reading the same-section
  H&E: a punch split by a diagonal crack (row 5, col 5), and a faint piece of the row 8,
  col 3 punch. That gives 38 cores, matching the H&E punch layout row by row
  (4,4,5,5,5,5,5,5). Written as `donor_core` = `PROS_A3_geo01..38` (Categorical, reading
  order) into both A3 tables; the all-null column was replaced (`write_obs_columns.py
  --replace-null`). rna 214,434/214,435 assigned, protein 289,757/289,773; the rest are
  debris islands, left NA. These are grouping labels, not AtoMx numbers and not donor
  identity. Backups: `release_qc/obs_write_backup/saha-v2-prostate__SAHA_CosMx_PROS_A3.zarr/`.
- **Tables**: `build_v2_tables.py` now gives A3 CosMx `n_donors` = 38 (one donor per
  punch, the same assumption as A2's 31) and 38 `PROS_A3_geoNN` donor rows.
- **Release gate** (`qc_release_objects.py --dir release/s3_v2/...`, `release_qc/v2_gate/`):
  4/4 objects clean. `--dir` used to label every store tier "both", which checked v2
  objects against v1's samples.parquet. That is fixed.

**Signed off 2026-10-09 (Jiwoon): ready for collaborator access.** This is packaging for
collaborators, not a full analysis. The open items below are known gaps for the shared
copy, not blockers to sharing it.

Still open: blocker 1 (`condition`, needs a pathologist or the block accession),
demographics for all 69 cores, cell types (`cell_type_broad`: no prostate reference
labels exist in v1 or v2; objects ship with `leiden`), and Xenium core labels (different
section; would need a registration to the CosMx slide).

## 2026-10-09 — shared on Wasabi

`s3://saha-data-access/phase2/` (README = `data/v2/BUCKET_README.md`, metadata = the 4 v2 parquet tables,
`processed/saha-phase2-prostate/` = the 4 stores). Uploaded from Cayuga (`release_qc/wasabi_phase2/upload.sbatch`,
`aws s3 sync --delete`). Verified: per-store file counts and bytes equal the source (119,909 files, 58.57 GB);
the bucket holds exactly the stores + 4 metadata files + README (119,914 objects); 91/91 md5-comparable files
match, including full-res H&E tiles. `spatialdata` 0.8.0 with zarr 3.3.0 cannot open remote s3:// stores
(`_resolve_zarr_store` TypeError), so the README documents sync-then-read only.

2026-10-09 22:10: the H&E backups (`release_qc/he_v1/v2_retile/backup/`, `release_qc/v2_he_l0/{real,real_cosmx}/backup/`)
and the test copies were removed with Jiwoon's OK. They were lower-resolution copies; the full-res elements are live
and in the bucket, and the source .svs scans in `2026_SAHA_data/processed/he/` are untouched.

