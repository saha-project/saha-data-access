# SAHA phase 2 — prostate TMA (collaborator early access)

Pre-publication SpatialData objects shared with collaborators. Please do not redistribute.
Phase 2 is new data alongside phase 1 (`phase1/`, the Zenodo v1.0 records `saha-v1-*`), not a new version of it.
This is a packaging release for access and validation, not a finished analysis — see "Known gaps".

```
s3://<SAHA AWS bucket>/phase2/          bucket location to be announced
├── README.md                          this file
├── metadata/
│   ├── samples.parquet                6 acquisitions (one row per platform x assay x slide)
│   ├── donors.parquet                 69 TMA cores (31 on PROS_A2, 38 on PROS_A3)
│   ├── runs.parquet                   2 runs (Xenium UP01, CosMx EC05)
│   └── panels.parquet                 Xenium 5K, CosMx 19K RNA, CosMx IO Protein 64-plex
└── processed/saha-phase2-prostate/
    ├── SAHA_CosMx_PROS_A2.zarr        CosMx RNA + protein, slide PROS_A2, 31 cores
    ├── SAHA_CosMx_PROS_A3.zarr        CosMx RNA + protein, slide PROS_A3, 38 cores
    ├── SAHA_XR_PROS_A2.zarr           Xenium 5K, slide PROS_A2
    └── SAHA_XR_PROS_A3.zarr           Xenium 5K, slide PROS_A3
```

Two prostate TMA slides (PROS_A2, PROS_A3), each measured by Xenium 5K and by CosMx
(19K RNA and 64-plex protein, merged per slide in a shared frame); 1,683,380 cells. Every store is
zarr v3 SpatialData.

## What is in each object

| Object | Tables | Images | Other |
|---|---|---|---|
| `SAHA_CosMx_PROS_A{2,3}` | `rna` (19,244 genes; `X` = transcript counts) and `protein` (69: 64 antibodies incl. 2 isotype controls + 5 `Channel-*` imaging channels; `X` = per-cell intensities, not counts) | `he_same_section` (H&E of this slide after the run, full resolution 0.25 µm/px) + per-FOV morphology `rna_fovNNN_morphology`, `prt_fovNNN_morphology` | per-FOV `*_labels` / `*_compartments`; `rna_cell_boundaries`, `prt_cell_boundaries` |
| `SAHA_XR_PROS_A{2,3}` | `table` (5,001 features; `X` = transcript counts) | `morphology_focus`, `he_serial_section` (H&E of an adjacent section, full resolution 0.25 µm/px) | `transcripts`, `cell_labels`, `nucleus_labels`, `cell_boundaries`, `nucleus_boundaries` |

- **Cores** — `obs['donor_core']` on both CosMx tables. PROS_A2: AtoMx core IDs
  (`PROS_A2_1`..`31`). PROS_A3: AtoMx core IDs were never recorded, so cores were
  recovered from tissue geometry and checked against the H&E (`PROS_A3_geo01`..`38`,
  reading order); these group cells by punch but are not AtoMx numbers. Xenium objects
  have no core column.
- **H&E** — registration provenance and an accuracy note are in each table's
  `uns['he_same_section_registration']` / `uns['he_serial_section_registration']`.
  CosMx H&E aligns cell-to-cell (~1 µm); Xenium H&E is a different cut, so it aligns
  at tissue level only.
- **Clusters** — `obs['leiden']` is an automatic placeholder, not a cell-type annotation.
- **Coordinates** — `global` is in pixels (CosMx 0.1203 µm/px, Xenium 0.2125 µm/px);
  `uns['pixel_size_um']` on each table. In the CosMx objects, use the shapes (not
  `obs['CenterX_global_px']`) for positions shared between RNA and protein.

## Reading

The AWS bucket name and access instructions will be added here once the bucket is set up.

```bash
aws s3 sync s3://<SAHA AWS bucket>/phase2/processed/saha-phase2-prostate/SAHA_CosMx_PROS_A2.zarr \
    ./SAHA_CosMx_PROS_A2.zarr
```

```python
import spatialdata as sd
sdata = sd.read_zarr("SAHA_CosMx_PROS_A2.zarr")
rna, protein = sdata.tables["rna"], sdata.tables["protein"]
```

H&E images are multiscale (`scale0` = full resolution, each further level halves it) and
carry their registration as a transform to `global`, so they line up with the cells
without any extra step:

```python
from spatialdata import bounding_box_query
from spatialdata.transformations import get_transformation

he = sdata.images["he_same_section"]          # Xenium objects: "he_serial_section"
print(list(he.children))                      # scale0 .. scaleN
rgb = he["scale2"].image                      # (c, y, x) uint8, 4x downsampled; lazy until computed
print(get_transformation(he, "global"))       # H&E pixels -> object pixels

# a 500 x 500 um window (CosMx global = pixels of 0.1203 um) with H&E, cells and polygons together
x0, y0, w = 30000, 30000, int(500 / 0.1203)
crop = bounding_box_query(sdata, axes=("x", "y"), min_coordinate=[x0, y0],
                          max_coordinate=[x0 + w, y0 + w], target_coordinate_system="global")

print(sdata.tables["rna"].uns["he_same_section_registration"]["accuracy_note"])
```

How far to trust the overlay is recorded in `uns[...]["accuracy_note"]` (see "What is in
each object"): cell-to-cell on CosMx, tissue-level on Xenium. With `spatialdata-plot`
installed, `crop.pl.render_images("he_same_section").pl.render_shapes("rna_cell_boundaries").pl.show()`
draws the H&E with the cell outlines on top.

Access is by invitation; phase 2 is not public.

## Known gaps

- `condition` is not yet resolved: one slide is cancer and one normal, and which is
  which has not been confirmed. Do not infer it from the data.
- Donor demographics are not linked to cores.
- No cell-type annotation yet (`leiden` only).

Contact Jiwoon Park, Ph.D. for any questions/suggestions.
