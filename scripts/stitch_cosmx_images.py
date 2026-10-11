#!/usr/bin/env python3
"""
stitch_cosmx_images.py
Stitch per-FOV CosMx images into SpatialData image/label elements.

Runs on Cayuga, where both the flat files and the raw AtoMx export live:

    /athena/project-saha/data/<sample>/
        flatFiles/<run>/<run>_metadata_file.csv.gz     FOV offsets come from here
        RawFiles/<run>/<timestamp>_S?/CellStatsDir/
            Morphology2D/*_F###.TIF                    (5, 4256, 4256) uint16
            FOV###/CellLabels_F###.tif                 (4256, 4256) uint16
            FOV###/CompartmentLabels_F###.tif          (4256, 4256) uint8
            RunSummary/Morphology_ChannelID_Dictionary.txt

Geometry
--------
The metadata file carries both local and global cell centroids, which pins the
per-FOV offset exactly — no guessing at the mm->px scale or at axis flips:

    x_global = x_local + ox_fov          (ox constant within a FOV to 1e-4 px)
    y_global = oy_fov - y_local          (y is FLIPPED)

Defining the canvas as row = Y0 - y_global absorbs that flip, so tiles are
placed directly with no vertical mirroring, and the canvas maps back to the
`global` coordinate system of the existing shapes/points by

    x_global = X0 + col * D
    y_global = Y0 - row * D

FOVs do NOT lie on a regular lattice — offsets differ by half-tiles in places —
so each tile is placed at its own rounded offset. The bounding box is typically
only ~20% covered; zarr stores the empty chunks for almost nothing.

Label IDs
---------
CosMx `cell_ID` restarts at 1 in every FOV, so a stitched label image would
collide. Labels are remapped to `fov * FOV_STRIDE + cell_ID`, which is stable,
collision-free, and reproducible without depending on table row order. The same
value is written to obs as `label_id` by merge_cosmx_images.py.

Usage
-----
    P=~/.venv-sd08/bin/python

    # report canvas geometry and projected sizes, write nothing
    $P scripts/stitch_cosmx_images.py --sample SAHA_CR_ILE_A1_EC01 --dry-run

    # Zenodo tier: morphology downsampled 4x, labels at full resolution
    $P scripts/stitch_cosmx_images.py --sample SAHA_CR_ILE_A1_EC01 \
        --out-dir /athena/project-saha/derived/build/images/d4/v1_cosmx --downsample 4 --verify

    # AWS tier: everything at full resolution
    $P scripts/stitch_cosmx_images.py --sample SAHA_CR_ILE_A1_EC01 \
        --out-dir /athena/project-saha/derived/build/images/full/v1_cosmx --downsample 1
"""

import argparse
import json
import re
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd

DATA_ROOT = Path("/athena/project-saha/data")

FOV_TILE = 4256          # CosMx FOV is 4256 x 4256 px
FOV_STRIDE = 100_000     # label_id = fov * FOV_STRIDE + cell_ID
PIXEL_UM = 0.12028       # CosMx SMI pixel size, from the 4256 px / 0.5119 mm FOV pitch
CHUNK = 1024          # overridden by --chunk

# Channel UID -> biological target, when RunSummary has no dictionary.
FALLBACK_CHANNELS = {"B": "PanCK", "G": "CD68", "Y": "Membrane", "R": "CD45", "U": "DNA"}


# --------------------------------------------------------------------------
# discovery
# --------------------------------------------------------------------------

def tile_shape(p: dict):
    """(height, width) of one FOV tile. The commercial instrument writes
    4256x4256; the pre-commercial CRC runs write 3648x5472, so this is read
    from the data rather than assumed."""
    import tifffile
    tifs = [f for f in sorted(p["morphology"].glob("*.TIF"))
            if re.search(r"_F\d+\.TIF$", f.name)] if p["morphology"].is_dir() else []
    if not tifs:
        return FOV_TILE, FOV_TILE
    a = tifffile.TiffFile(tifs[0]).asarray()
    return (a.shape[-2], a.shape[-1])


def sample_paths(sample: str, root: Path = DATA_ROOT,
                 metadata: Path = None, cellstats: Path = None) -> dict:
    """Locate the flat-file metadata and the CellStatsDir for one sample.

    data_TAP does not follow the <sample>/RawFiles/<run>/ layout of the v1
    tree, and its raw directory name is not derivable from the sample id, so
    both paths can be supplied explicitly (see zenodo/tap_audit/tap_crosswalk.csv).
    """
    if metadata and cellstats:
        d = Path(cellstats)
        if not d.is_dir():
            sys.exit(f"ERROR: no such CellStatsDir: {d}")
        # Newer exports put the flattened per-FOV tiles in Morphology2D/; older
        # ones leave them in CellStatsDir/ alongside the _Z### focal stacks.
        # The _F###.TIF regex already excludes the _Z files either way.
        morph = d / "Morphology2D"
        return {"sample": sample, "metadata": Path(metadata), "cellstats": d,
                "morphology": morph if morph.is_dir() else d,
                "runsummary": d.parent / "RunSummary"}

    base = root / sample
    if not base.is_dir():
        sys.exit(f"ERROR: no such sample directory: {base}")

    # Most samples nest one run directory under flatFiles/ and RawFiles/; a few
    # (e.g. SAHA_CR_APE_A9_EC01) store the run's contents one level higher.
    meta = sorted(base.glob("flatFiles/*/*_metadata_file.csv.gz")) or \
        sorted(base.glob("flatFiles/*_metadata_file.csv.gz"))
    if not meta:
        sys.exit(f"ERROR: no *_metadata_file.csv.gz under {base}/flatFiles")

    cs = sorted(base.glob("RawFiles/*/*/CellStatsDir")) or \
        sorted(base.glob("RawFiles/*/CellStatsDir"))
    if not cs:
        sys.exit(f"ERROR: no CellStatsDir under {base}/RawFiles")

    d = cs[0]
    return {
        "sample": sample,
        "metadata": meta[0],
        "cellstats": d,
        "morphology": d / "Morphology2D",
        "runsummary": d.parent / "RunSummary",
    }


def channel_names(p: dict) -> list:
    """Channel order comes from the TIFF header; names from RunSummary."""
    order = "BGYRU"
    tifs = [f for f in sorted(p["morphology"].glob("*.TIF"))
            if re.search(r"_F\d+\.TIF$", f.name)] if p["morphology"].is_dir() else []
    if tifs:
        import tifffile
        with tifffile.TiffFile(tifs[0]) as t:
            n_chan = t.asarray().shape[0] if t.asarray().ndim == 3 else 1
            try:
                declared = json.loads(t.pages[0].description).get("ChannelOrder", "")
            except Exception:
                declared = ""
        # The pre-commercial CRC runs write ChannelOrder "Unknown" literally.
        # Iterating that string yields one bogus channel per character, so only
        # accept an order made of known codes whose length matches the array.
        if (declared and len(declared) == n_chan
                and all(c in FALLBACK_CHANNELS for c in declared)):
            order = declared
        elif declared:
            print(f"    WARNING: ignoring ChannelOrder {declared!r} "
                  f"({n_chan}-channel image); assuming {order}")
        order = order[:n_chan]

    # Two vendor quirks: the 2023 runs ship no dictionary at all, and some files
    # leave a target unfilled (Channel-G -> "G"). Never invent a marker for
    # either case -- an unnamed channel is reported as Channel-<UID>.
    names = {}
    dict_file = p["runsummary"] / "Morphology_ChannelID_Dictionary.txt"
    if dict_file.exists():
        for line in dict_file.read_text().splitlines()[1:]:
            parts = line.replace("\r", "").split("\t")
            if len(parts) >= 2:
                uid, target = parts[0].replace("Channel-", "").strip(), parts[1].strip()
                if target and target != uid:
                    names[uid] = target
    else:
        names = dict(FALLBACK_CHANNELS)      # standard 5-channel morphology panel

    return [names.get(u, f"Channel-{u}") for u in order]


def fov_geometry(metadata_csv: Path) -> pd.DataFrame:
    """Per-FOV (ox, oy) offsets, derived exactly from paired local/global centroids."""
    cols = ["fov", "CenterX_local_px", "CenterY_local_px",
            "CenterX_global_px", "CenterY_global_px"]
    d = pd.read_csv(metadata_csv, usecols=cols)
    d["ox"] = d.CenterX_global_px - d.CenterX_local_px
    d["oy"] = d.CenterY_global_px + d.CenterY_local_px

    g = d.groupby("fov")[["ox", "oy"]]
    spread = max(g.std().max().max(), 0.0)
    if spread > 1.0:
        sys.exit(f"ERROR: FOV offsets are not constant (max sd {spread:.3f} px). "
                 "The local/global centroid relationship does not hold for this sample.")
    return g.mean()


def canvas_geometry(off: pd.DataFrame, downsample: int, tile=None) -> dict:
    th, tw = tile if tile else (FOV_TILE, FOV_TILE)
    x0, y0 = off.ox.min(), off.oy.max()
    width = int(np.ceil(off.ox.max() - x0)) + tw
    height = int(np.ceil(y0 - off.oy.min())) + th
    return {
        "x0": float(x0), "y0": float(y0),
        "full": (height, width),
        "shape": (int(np.ceil(height / downsample)), int(np.ceil(width / downsample))),
        "n_fov": len(off),
        "fill": len(off) * th * tw / (height * width),
    }


# --------------------------------------------------------------------------
# stitching
# --------------------------------------------------------------------------

def block_mean(tile: np.ndarray, d: int) -> np.ndarray:
    """Downsample by an integer factor with a block mean (d divides 4256)."""
    if d == 1:
        return tile
    *lead, h, w = tile.shape
    out = tile.reshape(*lead, h // d, d, w // d, d).mean(axis=(-3, -1))
    return out.astype(tile.dtype)


def place(dst, tile: np.ndarray, r0: int, c0: int) -> None:
    """Write a tile into the canvas, clipped to its bounds."""
    h, w = tile.shape[-2:]
    r1, c1 = min(r0 + h, dst.shape[-2]), min(c0 + w, dst.shape[-1])
    if r1 <= r0 or c1 <= c0:
        return
    sub = tile[..., : r1 - r0, : c1 - c0]
    if dst.ndim == 3:
        dst[:, r0:r1, c0:c1] = sub
    else:
        dst[r0:r1, c0:c1] = sub


def stitch(p: dict, off: pd.DataFrame, geom: dict, downsample: int,
           label_downsample: int, out_zarr: Path, verbose: bool = True,
           no_labels: bool = False) -> dict:
    """Fill zarr canvases tile by tile. Returns the created zarr arrays."""
    import tifffile
    import zarr

    store = zarr.storage.LocalStore(str(out_zarr))
    root = zarr.create_group(store=store, overwrite=True)

    chans = channel_names(p)
    hs, ws = geom["shape"]
    hl = int(np.ceil(geom["full"][0] / label_downsample))
    wl = int(np.ceil(geom["full"][1] / label_downsample))

    arrs = {}
    if p["morphology"].is_dir():
        arrs["morphology"] = root.create_array(
            "morphology", shape=(len(chans), hs, ws), dtype="uint16",
            chunks=(1, CHUNK, CHUNK), fill_value=0)
    if not no_labels:
        arrs["cell_labels"] = root.create_array(
            "cell_labels", shape=(hl, wl), dtype="uint32",
            chunks=(CHUNK, CHUNK), fill_value=0)
        arrs["compartment_labels"] = root.create_array(
            "compartment_labels", shape=(hl, wl), dtype="uint8",
            chunks=(CHUNK, CHUNK), fill_value=0)

    morph = {}
    if p["morphology"].is_dir():
        for f in p["morphology"].glob("*.TIF"):
            m = re.search(r"_F(\d+)\.TIF$", f.name)
            if m:
                morph[int(m.group(1))] = f

    # FOV directories are zero-padded to 3 digits on older runs and 5 on newer
    # ones (FOV001 vs FOV00001), so map them by value rather than by format.
    fov_dirs = {}
    for d in p["cellstats"].glob("FOV*"):
        m = re.fullmatch(r"FOV(\d+)", d.name)
        if m and d.is_dir():
            fov_dirs[int(m.group(1))] = d

    missing = {"morphology": 0, "cell_labels": 0, "compartment_labels": 0}

    for n, (fov, row) in enumerate(off.iterrows(), 1):
        fov = int(fov)
        rc = lambda d: (int(round((geom["y0"] - row.oy) / d)),
                        int(round((row.ox - geom["x0"]) / d)))

        if "morphology" in arrs:
            f = morph.get(fov)
            if f is None:
                missing["morphology"] += 1
            else:
                tile = tifffile.imread(f)
                if tile.ndim == 2:
                    tile = tile[None]
                r0, c0 = rc(downsample)
                place(arrs["morphology"], block_mean(tile, downsample), r0, c0)

        if no_labels:
            if verbose and (n % 25 == 0 or n == len(off)):
                print(f"    {n}/{len(off)} FOVs", flush=True)
            continue

        fdir = fov_dirs.get(fov)
        r0, c0 = rc(label_downsample)
        if fdir is None:
            missing["cell_labels"] += 1
            missing["compartment_labels"] += 1
            continue

        cl = sorted(fdir.glob("CellLabels_*.tif"))
        if not cl:
            missing["cell_labels"] += 1
        else:
            lab = tifffile.imread(cl[0]).astype("uint32")
            lab[lab > 0] += fov * FOV_STRIDE          # make IDs globally unique
            place(arrs["cell_labels"], lab[::label_downsample, ::label_downsample], r0, c0)

        cp = sorted(fdir.glob("CompartmentLabels_*.tif"))
        if not cp:
            missing["compartment_labels"] += 1
        else:
            cmp_ = tifffile.imread(cp[0])
            place(arrs["compartment_labels"],
                  cmp_[::label_downsample, ::label_downsample], r0, c0)

        if verbose and (n % 25 == 0 or n == len(off)):
            print(f"    {n}/{len(off)} FOVs", flush=True)

    for k, v in missing.items():
        if v:
            print(f"    WARNING: {v} FOV(s) had no {k}")
    # An element that is empty for every FOV means the inputs were not found,
    # not that the sample lacks them. Fail loudly rather than ship a blank layer.
    for k, v in missing.items():
        if v == len(off):
            sys.exit(f"ERROR: no {k} found for any of the {len(off)} FOVs — "
                     f"check the layout under {p['cellstats']}")
    return {"arrays": arrs, "channels": chans, "missing": missing}


# --------------------------------------------------------------------------
# assembly into a SpatialData store
# --------------------------------------------------------------------------

def to_spatialdata(res: dict, geom: dict, downsample: int, label_downsample: int,
                   out: Path, scale_factors: list) -> None:
    import dask.array as da
    import spatialdata as sd
    from spatialdata.models import Image2DModel, Labels2DModel
    from spatialdata.transformations import Affine

    def affine(d):
        # canvas (col, row) -> global (x, y):  x = X0 + col*d,  y = Y0 - row*d
        return Affine(
            np.array([[d, 0, geom["x0"]], [0, -d, geom["y0"]], [0, 0, 1]]),
            input_axes=("x", "y"), output_axes=("x", "y"),
        )

    images, labels = {}, {}
    arrs = res["arrays"]

    if "morphology" in arrs:
        images["morphology"] = Image2DModel.parse(
            da.from_array(arrs["morphology"], chunks=(1, CHUNK, CHUNK)),
            dims=("c", "y", "x"), c_coords=res["channels"],
            scale_factors=scale_factors,
            transformations={"global": affine(downsample)},
        )

    for name, dtype in (("cell_labels", "uint32"), ("compartment_labels", "uint8")):
        if name not in arrs:                 # --no-labels: none were stitched
            continue
        labels[name] = Labels2DModel.parse(
            da.from_array(arrs[name], chunks=(CHUNK, CHUNK)),
            dims=("y", "x"), scale_factors=scale_factors,
            transformations={"global": affine(label_downsample)},
        )

    sdata = sd.SpatialData(images=images, labels=labels)
    sdata.write(out, overwrite=True)


# --------------------------------------------------------------------------
# verification
# --------------------------------------------------------------------------

def verify_morphology(out: Path, p: dict, n: int = 3000) -> bool:
    """Without labels, check that cells land on tissue rather than background.

    Same purpose as verify(): if the offsets, the y-flip or the affine were
    wrong, centroids would fall on empty canvas. Compares the nuclear channel
    at cell centroids against the same number of random in-canvas points --
    a correct alignment separates them by a wide margin.
    """
    import spatialdata as sd
    from spatialdata.transformations import get_transformation

    sdata = sd.read_zarr(out)
    img = sdata.images["morphology"]
    arr = img["scale0"]["image"] if hasattr(img, "children") else img
    chans = list(arr.coords["c"].values) if "c" in arr.coords else []
    ci = chans.index("DNA") if "DNA" in chans else len(chans) - 1

    d = pd.read_csv(p["metadata"], usecols=["CenterX_global_px", "CenterY_global_px"])
    d = d.sample(min(n, len(d)), random_state=0)
    m = np.asarray(get_transformation(img, "global").to_affine_matrix(
        input_axes=("x", "y"), output_axes=("x", "y")))
    cr = np.c_[d.CenterX_global_px, d.CenterY_global_px, np.ones(len(d))] @ np.linalg.inv(m).T
    col = np.round(cr[:, 0]).astype(int)
    row = np.round(cr[:, 1]).astype(int)
    H, W = arr.shape[-2], arr.shape[-1]
    ok = (row >= 0) & (row < H) & (col >= 0) & (col < W)
    if ok.sum() < 100:
        print(f"  FAILED: only {ok.sum()} of {len(d)} centroids fall inside the canvas")
        return False

    at_cells = np.asarray(arr.data.vindex[ci, row[ok], col[ok]]).astype(float)
    rng = np.random.default_rng(0)
    rr = rng.integers(0, H, ok.sum()); rc = rng.integers(0, W, ok.sum())
    at_random = np.asarray(arr.data.vindex[ci, rr, rc]).astype(float)

    frac_signal = float((at_cells > 0).mean())
    ratio = (np.median(at_cells) + 1) / (np.median(at_random) + 1)
    print(f"  verify: {ok.sum()}/{len(d)} centroids in bounds; "
          f"{frac_signal:.1%} on non-zero {chans[ci] if chans else 'last'} signal; "
          f"median cell/random = {ratio:.1f}x")
    if frac_signal < 0.90 or ratio < 2.0:
        print("  FAILED: centroids do not land on tissue — alignment is wrong")
        return False
    print("  PASS")
    return True


def verify(out: Path, p: dict, n: int = 3000) -> bool:
    """A cell's global centroid must land on its own label in the stitched image.

    This is the end-to-end check on the offsets, the y-flip and the affine: if
    any of them were wrong the hit rate would collapse to ~0.
    """
    import spatialdata as sd
    from spatialdata.transformations import get_transformation

    sdata = sd.read_zarr(out)
    lab = sdata.labels["cell_labels"]
    arr = lab["scale0"]["image"] if hasattr(lab, "children") else lab

    d = pd.read_csv(p["metadata"],
                    usecols=["fov", "cell_ID", "CenterX_global_px", "CenterY_global_px"])
    d = d.sample(min(n, len(d)), random_state=0)

    m = np.asarray(get_transformation(lab, "global").to_affine_matrix(
        input_axes=("x", "y"), output_axes=("x", "y")))
    inv = np.linalg.inv(m)
    xy1 = np.c_[d.CenterX_global_px, d.CenterY_global_px, np.ones(len(d))]
    cr = xy1 @ inv.T                      # -> (col, row)

    col = np.round(cr[:, 0]).astype(int)
    row = np.round(cr[:, 1]).astype(int)
    ok = (row >= 0) & (row < arr.shape[-2]) & (col >= 0) & (col < arr.shape[-1])

    got = np.zeros(len(d), dtype="uint32")
    got[ok] = np.asarray(arr.data.vindex[row[ok], col[ok]])
    want = (d.fov.to_numpy() * FOV_STRIDE + d.cell_ID.to_numpy()).astype("uint32")

    hit = (got == want).mean()
    print(f"  verify: {hit:.1%} of {len(d)} sampled centroids land on their own label "
          f"({ok.sum()} in bounds)")
    if hit < 0.90:
        print("  FAILED: alignment is wrong — do not use this output")
        return False
    print("  PASS")
    return True


# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sample", required=True)
    ap.add_argument("--root", type=Path, default=DATA_ROOT)
    ap.add_argument("--metadata", type=Path,
                    help="flat-file *_metadata_file.csv.gz (overrides discovery)")
    ap.add_argument("--cellstats", type=Path,
                    help="raw CellStatsDir (overrides discovery)")
    ap.add_argument("--no-labels", action="store_true",
                    help="sample has no CellLabels/CompartmentLabels (all of "
                         "data_TAP); build the morphology image only")
    ap.add_argument("--out-dir", type=Path)
    ap.add_argument("--downsample", type=int, default=4,
                    help="morphology downsample factor; must divide 4256 (default 4)")
    ap.add_argument("--label-downsample", type=int, default=1,
                    help="label downsample; keep at 1, small cells vanish otherwise")
    ap.add_argument("--scale-factors", type=int, nargs="*", default=[2, 2, 2],
                    help="pyramid levels above the base (default 2 2 2)")
    ap.add_argument("--chunk", type=int, default=1024,
                    help="zarr chunk edge in px; larger reads better from S3 "
                         "and cuts dask task count at full resolution")
    ap.add_argument("--scratch", type=Path, default=Path("/tmp"))
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    global CHUNK
    CHUNK = args.chunk

    p = sample_paths(args.sample, args.root, args.metadata, args.cellstats)
    tile = tile_shape(p)
    if any(t % args.downsample or t % args.label_downsample for t in tile):
        sys.exit(f"ERROR: downsample must divide the {tile[0]}x{tile[1]} FOV tile")

    off = fov_geometry(p["metadata"])
    geom = canvas_geometry(off, args.downsample, tile)

    px = PIXEL_UM * args.downsample
    print(f"=== {args.sample} ===")
    print(f"  FOV tile {tile[0]} x {tile[1]} px")
    print(f"  {geom['n_fov']} FOVs, canvas {geom['full'][1]:,} x {geom['full'][0]:,} px "
          f"full res, {geom['fill']:.0%} covered")
    print(f"  morphology  {args.downsample}x -> {geom['shape'][1]:,} x {geom['shape'][0]:,} "
          f"px  ({px:.2f} um/px)")
    print(f"  channels    {', '.join(channel_names(p))}")

    if args.dry_run:
        n = len(channel_names(p))
        dense = n * np.prod(geom["shape"]) * 2 / 1e9
        print(f"  dense morphology would be {dense:.1f} GB; "
              f"~{dense * geom['fill']:.1f} GB of it is real signal")
        return

    if not args.out_dir:
        sys.exit("ERROR: --out-dir is required unless --dry-run")

    out = args.out_dir / f"{args.sample}.zarr"
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = args.scratch / f"{args.sample}.canvas.zarr"

    print(f"  stitching -> {tmp}")
    res = stitch(p, off, geom, args.downsample, args.label_downsample, tmp,
                 no_labels=args.no_labels)

    print(f"  writing SpatialData -> {out}")
    to_spatialdata(res, geom, args.downsample, args.label_downsample,
                   out, args.scale_factors)

    if tmp.exists():
        shutil.rmtree(tmp)          # scratch canvas we created ourselves

    size = sum(f.stat().st_size for f in out.rglob("*") if f.is_file())
    print(f"  wrote {size / 1e9:.2f} GB")

    check = verify_morphology if args.no_labels else verify
    if args.verify and not check(out, p):
        sys.exit(1)


if __name__ == "__main__":
    main()
