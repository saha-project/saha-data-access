#!/usr/bin/env python3
"""
apply_obs_whitelist.py
Reduce obs in the released SpatialData objects to the documented public columns.

standardize_h5ad.py has carried public obs whitelists (RNA_KEEP, PRT_KEEP,
XENIUM_KEEP) since the h5ad release, but create_cosmx_zarr.py never applied
them. The objects therefore ship whatever AtoMx exported: across the 22 CosMx
v1 objects that is 551 distinct obs columns, only 34 of which appear in all of
them, including internal run-tracking fields named after pipeline UUIDs --

    RNA_spatialclust_66dd9eb7.47d3.4655.a223.4704491f867a_1_neighbours_Tfh
    nn_8541dce7.0524.45b3.b0dc.2e54ab3e348d_1_cluster_cluster_f7e62af4...

Publishing those means every object has a different, undocumented schema and
the data dictionary cannot describe obs at all. This applies the same whitelist
the h5ads use, plus the columns the SpatialData layout itself needs.

Nothing is computed here and no cell is dropped -- only columns are removed.
The full obs of every object is written to <out>/obs_dropped_<sample>.csv first
so the removal is reversible and auditable.

Usage:
    python scripts/apply_obs_whitelist.py --zarr-dir ... --report-dir ... --dry-run
    python scripts/apply_obs_whitelist.py --zarr-dir ... --report-dir ...
"""

import argparse
import re
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from standardize_h5ad import RNA_KEEP, PRT_KEEP, XENIUM_KEEP

# Columns the SpatialData object needs regardless of assay. Dropping any of
# these breaks the link between the table and the shapes/labels elements.
STRUCTURAL = {
    "region",          # spatialdata's region_key -- the table/shapes join
    "cell", "cell_id", "cell_ID", "fov", "fov_cell_id",
    "label_id",        # matches labels/cell_labels pixel values
    "Run_Tissue_name",
    "cellSegmentationSetId", "cellSegmentationSetName",
}

# SAHA release columns, documented in DESCRIPTION.md and the data dictionary.
SAHA = {
    "sample_id", "assay_type", "platform", "modality",
    "donor_core", "donor_patient",   # per-cell donor identity
    "qc_pass",                       # the published QC subset
    "cell_type", "cell_type_detailed",
    # Sample context written by the builders. For the IBD cohorts `disease` and
    # `disease_state` (CD/UC, inflamed/non-inflamed) are the whole point of the
    # data, and dropping them would leave the objects unable to answer the
    # question they were collected for.
    "organ", "condition", "disease", "disease_state",
}

# Derived annotations that are results, not run-tracking. The h5ad whitelists
# predate these columns, so matching on the KEEP sets alone would silently drop
# published analysis: the CRC pathology calls, the niche assignments, the
# clustering labels, and the slide-millimetre coordinates the H&E alignment
# works in. Keeping them is the whole reason the object is worth downloading.
ANNOTATION = {
    "spatial_niche", "spaVAE_niche", "niche",
    "pathology_detail", "pathology_region",
    "cluster", "leiden",
    "x_slide_mm", "y_slide_mm", "x_FOV_px", "y_FOV_px",
    "x_centroid", "y_centroid", "x_centroid_px", "y_centroid_px",
}

# Seurat names per-assay QC counts `nCount_<assay>` / `nFeature_<assay>`, and
# the assay differs by platform (Nanostring, Xenium, BlankCodeword, ...). The
# h5ad whitelists list only the CosMx spellings, so matching literally drops
# every Xenium QC column. Match the convention instead.
KEEP_PATTERNS = (
    r"^n(Count|Feature)_",
    r"^gene_expression_(graphclust|kmeans_\d+_clusters)$",
)

# donor_id is deliberately absent: donor is per-cell via donor_core, and a
# scalar donor id is what collapsed whole microarrays onto one fictional donor.
DROP_ALWAYS = {"donor_id"}


def keep_set(sample: str, present: set) -> set:
    if sample.startswith("SAHA_XR"):
        base = XENIUM_KEEP
    elif sample.startswith("SAHA_CP"):
        base = PRT_KEEP
    else:
        base = RNA_KEEP
    keep = (set(base) | STRUCTURAL | SAHA | ANNOTATION) - DROP_ALWAYS
    # Keep every measured marker channel: panels differ per run, so listing
    # them exhaustively in the whitelist would silently drop new ones.
    keep |= {c for c in present
             if c.startswith(("Mean.", "Max.")) and "." in c}
    keep |= {c for c in present if any(re.match(p, c) for p in KEEP_PATTERNS)}
    return keep & present


def apply_one(zarr_path: Path, report_dir: Path, dry: bool, backup: bool = True):
    import pandas as pd
    import spatialdata as sd
    from spatialdata.models import TableModel

    sample = zarr_path.stem
    sdata = sd.read_zarr(zarr_path)
    if not sdata.tables:
        raise ValueError("object has no table -- refusing to touch it")
    name = list(sdata.tables)[0]          # not always "table" (the Xenium reader differs)
    table = sdata.tables[name]
    present = set(map(str, table.obs.columns))

    # Whatever the table declares as its own keys must survive: dropping the
    # region or instance key severs the table from the shapes and the object
    # will not re-parse. Read them rather than assuming the usual names.
    attrs = dict(table.uns.get("spatialdata_attrs", {}))
    required = {attrs.get("region_key"), attrs.get("instance_key")} - {None}

    keep = keep_set(sample, present) | (required & present)
    drop = sorted(present - keep)
    if not drop:
        return {"sample": sample, "before": len(present), "after": len(present),
                "dropped": 0}

    if not dry:
        reduced = table[:, :].copy()
        reduced.obs = reduced.obs[[c for c in reduced.obs.columns if c in keep]]

        # Validate BEFORE touching disk. delete_element_from_disk() followed by a
        # write that raises leaves the object with no table at all -- that is how
        # SAHA_XR_PRCA_UA_MS01 lost its table, because the Xenium reader's
        # instance key had been dropped and parse failed after the delete.
        # A table read from disk already carries spatialdata_attrs, and parse()
        # rejects the keys being passed again alongside it -- so let it validate
        # against what the table itself declares.
        try:
            TableModel.parse(reduced)
        except Exception as e:
            raise ValueError(f"reduced table fails validation, nothing written: {e}")
        for k in ("region_key", "instance_key"):
            col = attrs.get(k)
            if col and col not in reduced.obs.columns:
                raise ValueError(f"{k} '{col}' was dropped, nothing written")

        if backup:
            # Preserve what is being removed before removing it.
            report_dir.mkdir(parents=True, exist_ok=True)
            table.obs[drop].to_csv(report_dir / f"obs_dropped_{sample}.csv.gz",
                                   index=True, compression="gzip")

        sdata.tables[name] = reduced
        sdata.delete_element_from_disk(name)
        sdata.write_element(name)

    return {"sample": sample, "before": len(present),
            "after": len(keep), "dropped": len(drop), "drop_list": drop}


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--zarr-dir", type=Path, required=True)
    ap.add_argument("--report-dir", type=Path, required=True)
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--no-backup", action="store_true",
                    help="skip writing the dropped columns; use for a mirrored "
                         "tier whose twin has already been backed up")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    zarrs = sorted(args.zarr_dir.glob("*.zarr"))
    if args.only:
        zarrs = [z for z in zarrs if z.stem in args.only]
    if not zarrs:
        sys.exit(f"ERROR: no .zarr under {args.zarr_dir}")

    print(f"{len(zarrs)} object(s)\n")
    kept_union, dropped_union = set(), set()
    ok = fail = 0
    for z in zarrs:
        try:
            r = apply_one(z, args.report_dir, args.dry_run,
                          backup=not args.no_backup)
        except Exception as e:
            print(f"  {z.stem:26s} FAILED {type(e).__name__}: {e}")
            fail += 1
            continue
        print(f"  {r['sample']:26s} {r['before']:>4} -> {r['after']:>3} obs "
              f"({r['dropped']} dropped)")
        dropped_union |= set(r.get("drop_list", []))
        ok += 1

    print(f"\n{ok} object(s) processed, {fail} failed"
          + ("   [dry-run — nothing written]" if args.dry_run else ""))
    print(f"{len(dropped_union)} distinct obs columns removed across the release")
    if fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
