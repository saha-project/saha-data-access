#!/usr/bin/env python3
"""
transfer_cores_v2.py
Carry donor-core identity between the two modalities of one v2 CosMx slide.

Why this is not transfer_donor_core.py
--------------------------------------
v1's transfer works between two SCANS of a slide that share no coordinate frame,
so it recovers the transform from the tissue footprint by FFT. Here the frame is
already known: `merge_slide.py` bakes the measured protein->RNA affine into the
merged object's vector geometry. So the transfer is a nearest-neighbour lookup
in one frame, with no registration step to get wrong.

The trap this script exists to avoid
------------------------------------
In the merged object the affine is baked into the SHAPES ONLY. The obs columns
`CenterX_global_px` / `CenterY_global_px` still hold each modality's own raw
values, and for PROS_A2 they disagree outright:

    obs   protein  x  4442..83480    rna  x    13..79031
    shape protein  x  11.3..79062    rna  x  12.1..79033

Matching on obs would offset every protein cell by the whole affine and assign
confidently wrong cores. Centroids therefore come from `sd.shapes[...]`, never
from obs. The assertion below refuses to run if the two footprints do not
overlap, so this cannot regress silently.

Output is a sidecar parquet, not a write into the object. Compute and inspect
first; apply once, when the release objects are built.

Usage:
    P=~/.venv-sd08/bin/python
    $P scripts/transfer_cores_v2.py --object .../export/SAHA_CosMx_PROS_A2.zarr \\
        --source rna --target protein --out cores_A2_protein.parquet
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# CosMx global pixels. v1 uses the same value in transfer_donor_core.py.
PX_UM = 0.12028


def centroids(sd, table_name: str) -> tuple[np.ndarray, pd.Index]:
    """Cell centroids in the OBJECT's shared frame, plus the table's obs index.

    Uses the shapes element the table is annotated against, because that is the
    element the affine was baked into -- see the module docstring.
    """
    t = sd.tables[table_name]
    attrs = t.uns["spatialdata_attrs"]
    region = attrs["region"]
    region = region[0] if isinstance(region, (list, tuple)) else region
    inst_key = attrs["instance_key"]
    gdf = sd.shapes[region]

    inst = t.obs[inst_key].to_numpy()
    missing = ~pd.Index(inst).isin(gdf.index)
    if missing.any():
        raise ValueError(f"{table_name}: {int(missing.sum())} cells have no shape in "
                         f"'{region}' -- cannot place them in the shared frame")
    c = gdf.loc[inst].geometry.centroid
    return np.column_stack([c.x.to_numpy(), c.y.to_numpy()]), t.obs.index


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--object", type=Path, required=True)
    ap.add_argument("--source", default="rna", help="table carrying Core_ID")
    ap.add_argument("--target", default="protein", help="table to label")
    ap.add_argument("--core-col", default="Core_ID")
    ap.add_argument("--composite-col", default="Composite_Core_ID")
    ap.add_argument("--max-dist-um", type=float, default=50.0,
                    help="target cells further than this from any labelled "
                         "source cell get NA rather than a guess")
    ap.add_argument("--min-assigned", type=float, default=0.90,
                    help="fail if a smaller fraction of target cells is labelled")
    ap.add_argument("--out", type=Path)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    import warnings
    warnings.filterwarnings("ignore")
    from spatialdata import read_zarr
    from scipy.spatial import cKDTree

    sd = read_zarr(args.object)
    for name in (args.source, args.target):
        if name not in sd.tables:
            sys.exit(f"ERROR: no table '{name}' in {args.object} "
                     f"(have: {list(sd.tables)})")

    src_t, tgt_t = sd.tables[args.source], sd.tables[args.target]
    if args.core_col not in src_t.obs.columns:
        sys.exit(f"ERROR: source table has no '{args.core_col}'")

    labelled = src_t.obs[args.core_col].notna().to_numpy()
    if not labelled.any():
        sys.exit(f"ERROR: source '{args.source}' has no non-null {args.core_col} -- "
                 f"nothing to transfer")

    S, _ = centroids(sd, args.source)
    T, tgt_index = centroids(sd, args.target)
    S = S[labelled]
    src_core = src_t.obs.loc[labelled, args.core_col].astype(str).to_numpy()
    src_comp = (src_t.obs.loc[labelled, args.composite_col].astype(str).to_numpy()
                if args.composite_col in src_t.obs.columns else None)

    # Refuse to proceed if the two footprints do not sit on top of each other.
    # This is the guard against silently matching across an un-applied affine.
    for ax, si, ti in ((0, S[:, 0], T[:, 0]), (1, S[:, 1], T[:, 1])):
        lo = max(si.min(), ti.min()); hi = min(si.max(), ti.max())
        overlap = (hi - lo) / max(si.max() - si.min(), ti.max() - ti.min())
        name = "xy"[ax]
        print(f"  {name}: source {si.min():10.1f}..{si.max():10.1f}  "
              f"target {ti.min():10.1f}..{ti.max():10.1f}  overlap {overlap:.3f}")
        if overlap < 0.80:
            sys.exit(f"ERROR: {name} footprints overlap only {overlap:.2f} -- the two "
                     f"tables are not in a shared frame; refusing to transfer")

    max_dist_px = args.max_dist_um / PX_UM
    dist, idx = cKDTree(S).query(T, k=1)
    ok = dist <= max_dist_px

    core = np.where(ok, src_core[idx], None)
    out = pd.DataFrame({
        "obs_index": tgt_index,
        "core": core,
        "dist_um": dist * PX_UM,
        "assigned": ok,
    })
    if src_comp is not None:
        out["composite_core"] = np.where(ok, src_comp[idx], None)

    frac = float(ok.mean())
    print(f"\n  source {args.source}: {labelled.sum():,} labelled cells, "
          f"{len(set(src_core))} cores")
    print(f"  target {args.target}: {len(T):,} cells, {ok.sum():,} assigned "
          f"({frac:.1%})")
    print(f"  distance um: median {np.median(dist)*PX_UM:.2f}  "
          f"p95 {np.percentile(dist, 95)*PX_UM:.2f}  max {dist.max()*PX_UM:.1f}")

    vc = pd.Series(core[ok]).value_counts()
    print(f"\n  per-core target counts ({len(vc)} cores):")
    for k, v in vc.sort_index(key=lambda s: s.astype(int)).items():
        n_src = int((src_core == k).sum())
        print(f"    core {k:>3}  target {v:>7,}   source {n_src:>7,}   "
              f"ratio {v / n_src:5.2f}")

    # Every source core should receive target cells. A core that vanishes means
    # the modalities disagree about where tissue is, not a clean transfer.
    lost = sorted(set(src_core) - set(core[ok].tolist()), key=lambda x: int(x))
    if lost:
        print(f"\n  WARNING: {len(lost)} source core(s) received no target cells: {lost}")
    if frac < args.min_assigned:
        print(f"\nFAILED: only {frac:.1%} assigned, below --min-assigned "
              f"{args.min_assigned:.0%}")
        return 1

    if args.out and not args.dry_run:
        out.to_parquet(args.out, index=False)
        print(f"\nwrote {args.out}  ({len(out):,} rows)")
    else:
        print("\n[dry-run] nothing written")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
