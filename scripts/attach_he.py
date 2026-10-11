#!/usr/bin/env python3
"""
attach_he.py
Write a registered H&E into a v1 release object, from a register_he_object.py
result. One store per call; the source is never modified.

Element name follows the MEASURED fit quality, not the scan's file name
(register_he_object.py metrics):
    he_same_section     cell-level: refined nuclear offset <= 6 um, and >= 20 windows agree
                        (or >= 8 agree with >= 90% of cells on H&E tissue -- small imaged areas)
    he_serial_section   tissue-level: an adjacent section, OR the same section where
                        re-staining tore/shifted tissue locally (seen on IBD_A1, STO,
                        PANC TMA) -- architecture corresponds, single cells do not
    refused             cells on H&E tissue no better than 2x a random H&E pixel

Resolution per tier, so the Zenodo records stay under their 50 GB cap:
    --tier aws      full resolution (H&E level 0)
    --tier zenodo   ~1 um/px
    --tier both     ~0.5 um/px  (Dulai/Xenium stores that serve both tiers)
The H&E is cropped to the object's extent (+200 um), which matters for Xenium,
where one slide scan covers four region objects.

How the store is edited (see add_leiden.py; delete-then-write lesson in
project memory): a hardlinked copy `<dir>_he/<store>.partial` shares every
existing file; root metadata, images/ group metadata and tables/ are un-shared
first, so nothing written can reach the source. The new image is ADDED
(write_element), the table is rewritten only to record the registration in
uns, both validated before writing. Read back and checked, source root
metadata checksummed before and after, then renamed. Swapping in is separate.

Usage (on Cayuga):
    P=~/.venv-sd08/bin/python
    $P scripts/attach_he.py --registration .../SAHA_CR_COL_A1_EC02.HECR.he_registration.json \
        --store .../build/zenodo_tier/saha-v1-colon/SAHA_CR_COL_A1_EC02.zarr --tier zenodo
"""

import argparse
import hashlib
import json
import os
import shutil
import sys
import warnings
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
TIER_UM = {"aws": 0.0, "zenodo": 1.0, "both": 0.5}


def classify(rec):
    off, on, bg = rec.get("nuclear_offset_um_refined"), rec["cells_on_he_tissue"], rec["he_tissue_fraction_random"]
    if on < 2 * bg and (1 - on) > 0.5 * (1 - bg):   # same rule as the release gate: a section filling >50% of
        return None                                  # the frame can't reach 2x random on-tissue, so also accept
                                                     # off-tissue <= 0.5x random off-tissue
    w = rec.get("refine_windows") or 0
    if off is not None and off <= 6.0 and (w >= 20 or (w >= 8 and on >= 0.90)):
        return "he_same_section"     # small imaged areas (TAP) give few windows but agree tightly
    return "he_serial_section"


def _tiles(read, H, W, chunk=2048):
    """Lazy (H, W, 3) uint8 dask array built from a region reader -- delayed tasks only, so
    nothing in the graph points at a foreign zarr store (spatialdata refuses to write those)."""
    import dask
    import dask.array as da
    rows = []
    for y0 in range(0, H, chunk):
        h = min(chunk, H - y0)
        rows.append(da.concatenate([da.from_delayed(dask.delayed(read)(y0, x0, h, min(chunk, W - x0)),
                                                    shape=(h, min(chunk, W - x0), 3), dtype=np.uint8)
                                    for x0 in range(0, W, chunk)], axis=1))
    return da.concatenate(rows, axis=0)


def he_lazy(path: Path, target_um: float, mpp_hint=None):
    """(dask yxc uint8, level um/px, full-res mpp).

    SVS -> openslide tiles. OME-TIFF -> tifffile pyramid (openslide sees only level 0
    of those). Flat TIFF exports with no scale (SJ01) -> whole image, at the scale the
    registration estimated (mpp_hint, from the registration JSON).
    """
    name = str(path).lower()
    if name.endswith((".ome.tif", ".ome.tiff")):
        import tifffile
        import xml.etree.ElementTree as ET
        with tifffile.TiffFile(path) as tf:
            px = next(el for el in ET.fromstring(tf.ome_metadata).iter() if el.tag.endswith("Pixels"))
            mpp = float(px.get("PhysicalSizeX"))
            shapes = [l.shape for l in tf.series[0].levels]
        rgb_last = shapes[0][-1] in (3, 4)
        w = [sh[-2] if rgb_last else sh[-1] for sh in shapes]
        hgt = [sh[-3] if rgb_last else sh[-2] for sh in shapes]
        lvl = 0 if target_um <= 0 else int(np.argmin([abs(w[0] / x - target_um / mpp) for x in w]))

        def read(y0, x0, h, ww):
            import zarr
            with tifffile.TiffFile(path) as tf:
                g = zarr.open(tf.series[0].aszarr(), mode="r")
                z = g if hasattr(g, "shape") else g[str(lvl)]
                a = z[y0:y0 + h, x0:x0 + ww] if rgb_last else np.moveaxis(z[:, y0:y0 + h, x0:x0 + ww], 0, -1)
                return np.ascontiguousarray(np.asarray(a)[..., :3], dtype=np.uint8)
        return _tiles(read, hgt[lvl], w[lvl]), mpp * w[0] / w[lvl], mpp
    try:
        import openslide
        sl = openslide.OpenSlide(str(path))
        mpp = float(sl.properties.get(openslide.PROPERTY_NAME_MPP_X) or 0)
        ds_all = [float(d) for d in sl.level_downsamples]
        sl.close()
        if not mpp:
            raise ValueError("no mpp")
        lvl = 0 if target_um <= 0 else int(np.argmin([abs(d - target_um / mpp) for d in ds_all]))
        from attach_he_xenium_v2 import he_dask
        return he_dask(path, lvl)
    except Exception:
        if not mpp_hint:
            raise
        import dask.array as da
        import tifffile
        img = np.ascontiguousarray(tifffile.imread(path)[..., :3], dtype=np.uint8)   # small flat export
        return da.from_array(img, chunks=(2048, 2048, 3)), float(mpp_hint), float(mpp_hint)


def md5(p):
    return hashlib.md5(p.read_bytes()).hexdigest() if p.exists() else None


def unshare(p: Path):
    tmp = p.with_name(p.name + ".unshare")
    shutil.copy2(p, tmp)
    os.replace(tmp, p)


def attach(reg_path: Path, store: Path, tier: str, dry_run=False, element=None, in_place=False):
    warnings.filterwarnings("ignore")
    from spatialdata import read_zarr
    from spatialdata.models import Image2DModel, TableModel
    from spatialdata.transformations import Affine, get_transformation

    rec = json.loads(reg_path.read_text())
    measured = classify(rec)
    if measured is None:
        sys.exit(f"{store.name}: registration is not better than chance "
                 f"({rec['cells_on_he_tissue']:.2f} vs {rec['he_tissue_fraction_random']:.2f}) -- refusing")
    if element is None:
        element = measured
    elif element == "he_same_section" and measured != "he_same_section" and not rec.get("same_section_confirmed"):
        # same_section_confirmed: the H&E is known to be of this very section (post-run stain of the
        # slide) but too coarse to pass the nuclear test; its accuracy_note must say so
        sys.exit(f"{store.name}: plan says cell-level but the fit measures {measured} -- refusing")
    A_full = np.array(rec["affine_he_fullres_px_to_global_px"], float)

    src = store.resolve()
    out_dir = src.parent.with_name(src.parent.name + "_he")
    final, tmp = out_dir / src.name, out_dir / (src.name + ".partial")
    if final.exists():
        print(f"{src.name}: already done, skipping")
        return

    sd = read_zarr(src)
    if element in sd.images:
        sys.exit(f"{src.name}: already has {element} -- refusing to overwrite")

    # crop window: the object's own image extent in global px (cells if it has no image), + 200 um
    base = next((k for k in ("morphology", "morphology_focus") if k in sd.images), None)
    if base:
        el = sd.images[base]
        b0 = next(iter(el[list(el.children)[0]].data_vars.values()))
        M = get_transformation(b0, "global").to_affine_matrix(("x", "y"), ("x", "y"))
        h, w = b0.shape[-2:]
        cg = M @ np.array([[0, w, 0, w], [0, 0, h, h], [1, 1, 1, 1]], float)
    else:
        from register_he_object import cell_xy
        cx, cy = cell_xy(sd, rec["global_px_um"])
        cg = np.array([[cx.min(), cx.max()], [cy.min(), cy.max()]])
    pad = 200.0 / rec["global_px_um"]
    gx0, gx1, gy0, gy1 = cg[0].min() - pad, cg[0].max() + pad, cg[1].min() - pad, cg[1].max() + pad

    arr, he_um, mpp = he_lazy(Path(rec["he_file"]), TIER_UM[tier], rec.get("he_mpp"))
    ds = he_um / mpp
    A_lvl = A_full @ np.diag([ds, ds, 1.0])                    # H&E level px -> global
    inv = np.linalg.inv(A_lvl)
    hc = inv @ np.array([[gx0, gx1, gx0, gx1], [gy0, gy0, gy1, gy1], [1, 1, 1, 1]])
    x0, x1 = int(max(0, np.floor(hc[0].min()))), int(min(arr.shape[1], np.ceil(hc[0].max())))
    y0, y1 = int(max(0, np.floor(hc[1].min()))), int(min(arr.shape[0], np.ceil(hc[1].max())))
    if x1 - x0 < 64 or y1 - y0 < 64:
        sys.exit(f"{src.name}: object extent maps outside the H&E -- refusing")
    crop = arr[y0:y1, x0:x1]
    T = np.array([[1, 0, x0], [0, 1, y0], [0, 0, 1]], float)
    A_crop = A_lvl @ T
    n_scales = max(0, int(np.floor(np.log2(min(crop.shape[:2]) / 512))))
    img = Image2DModel.parse(
        crop, dims=("y", "x", "c"), c_coords=["r", "g", "b"],
        transformations={"global": Affine(A_crop, input_axes=("x", "y"), output_axes=("x", "y"))},
        # chunks keyed by NAME: a tuple is applied in the stored (c, y, x) order, and
        # (1024, 1024, 3) became 3-px-wide chunks -- millions of tiny files per H&E,
        # which filled the lab's 175M-file quota on 2026-10-07.
        scale_factors=[2] * min(n_scales, 5), chunks={"c": 3, "y": 1024, "x": 1024})

    tname = list(sd.tables)[0]
    t = sd.tables[tname]
    off = rec.get("nuclear_offset_um_refined")
    note = rec.get("accuracy_note") or {"he_same_section": f"Cell-level: nuclei agree to ~{off:.1f} um across "
                               f"{rec.get('refine_windows')} windows; cells can be compared to H&E one to one."
                               if off is not None else "",
            "he_serial_section": "Tissue-level only: an adjacent section, or the same section with local "
                                 "distortion after re-staining. Architecture and regions correspond; individual "
                                 "cells do not. Measured nuclear offset "
                                 + (f"~{off:.0f} um." if off is not None else "not measurable (no nuclear image).")
            }[element]
    new_t = t.copy()
    new_t.uns[f"{element}_registration"] = {
        "source": Path(rec["he_file"]).name, "section": element.removeprefix("he_").removesuffix("_section"),
        "method": "register_he_object.py: tissue-mask fit + RANSAC nuclear refinement",
        "orientation": rec["orientation"], "nuclear_offset_um": rec.get("nuclear_offset_um_refined"),
        "cells_on_he_tissue": rec["cells_on_he_tissue"], "he_um_per_px": he_um,
        "accuracy_note": note}
    attrs = new_t.uns.pop("spatialdata_attrs")
    new_t = TableModel.parse(new_t, region=attrs["region"], region_key=attrs["region_key"],
                             instance_key=attrs["instance_key"])  # validated before any write
    print(f"{src.name} [{tier}]: {element}, H&E {crop.shape[1]}x{crop.shape[0]} at {he_um:.2f} um/px, "
          f"{len(img.children) if hasattr(img, 'children') else 1} scales")
    if dry_run:
        return
    if in_place:
        return add_in_place(src, sd, element, img, A_crop, tname, t, new_t.uns[f"{element}_registration"])

    out_dir.mkdir(exist_ok=True)
    if tmp.exists():
        shutil.rmtree(tmp)
    meta = [n for n in ("zarr.json", ".zmetadata", ".zattrs", ".zgroup") if (src / n).exists()]
    before = {n: md5(src / n) for n in meta}
    shutil.copytree(src, tmp, copy_function=os.link, symlinks=True)
    for n in meta:
        unshare(tmp / n)
    for d in ("images", "tables"):
        for n in ("zarr.json", ".zattrs", ".zgroup"):
            if (tmp / d / n).exists():
                unshare(tmp / d / n)
    for f in (tmp / "tables").rglob("*"):
        if f.is_file():
            unshare(f)

    sd = read_zarr(tmp)
    sd.images[element] = img
    sd.write_element(element)
    sd.tables[tname] = new_t
    sd.delete_element_from_disk(tname)
    sd.write_element(tname)

    assert {n: md5(src / n) for n in meta} == before, "source metadata changed"
    back = read_zarr(tmp)
    assert element in back.images
    bt = get_transformation(next(iter(back.images[element][list(back.images[element].children)[0]].data_vars.values())) if hasattr(back.images[element], "children") else back.images[element], "global")
    assert np.allclose(bt.to_affine_matrix(("x", "y"), ("x", "y")), A_crop, atol=1e-6), "transform changed"
    assert back.tables[tname].n_obs == t.n_obs and list(back.tables[tname].obs.columns) == list(t.obs.columns)
    assert f"{element}_registration" in back.tables[tname].uns
    assert set(back.images) == set(sd.images) and set(back.shapes) == set(sd.shapes)
    tmp.rename(final)
    print(f"{src.name}: verified -> {final}")


def add_in_place(src, sd, element, img, A_crop, tname, t, reg_note):
    """ADD the H&E to the existing store -- nothing is deleted or rewritten.

    The hardlinked-copy route duplicates every directory of a store (directories cannot
    be hardlinked): ~0.5M+ new inodes per full-resolution AWS copy, which pushed the lab's
    shared 175M-file quota to its hard limit. Adding an element creates only the H&E's
    own files. The registration note goes into the table as a NEW uns key, written with
    anndata.write_elem into the existing uns group, so the table is never deleted and
    rewritten (the step that destroyed two objects before -- see project memory).
    Root zarr.json is backed up first; zarr writes it via temp file + rename.
    """
    import anndata as ad
    import zarr
    from spatialdata import read_zarr
    from spatialdata.transformations import get_transformation
    key = f"{element}_registration"
    if key in t.uns:
        sys.exit(f"{src.name}: uns already has {key} -- refusing")
    bak = Path("/athena/masonlab/scratch/users/jip2007/release_qc/he_v1/inplace_backup")
    bak.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src / "zarr.json", bak / f"{src.name}.zarr.json")
    before = {k: set(getattr(sd, k)) for k in ("images", "labels", "shapes", "points", "tables")}

    sd.images[element] = img
    sd.write_element(element)
    g = zarr.open_group(str(src / "tables" / tname / "uns"), mode="a")
    ad.io.write_elem(g, key, {k: ("NA" if v is None else v) for k, v in reg_note.items()})

    back = read_zarr(src)
    assert element in back.images
    el = back.images[element]
    b0 = next(iter(el[list(el.children)[0]].data_vars.values())) if hasattr(el, "children") else el
    assert np.allclose(get_transformation(b0, "global").to_affine_matrix(("x", "y"), ("x", "y")), A_crop, atol=1e-6)
    bt = back.tables[tname]
    assert bt.n_obs == t.n_obs and list(bt.obs.columns) == list(t.obs.columns) and key in bt.uns
    for k, names in before.items():
        extra = {element} if k == "images" else set()
        assert set(getattr(back, k)) == names | extra, f"{k} changed"
    print(f"{src.name}: H&E added in place and verified")


UNALIGNED = "he_unaligned"


def attach_unaligned(he: Path, store: Path, tier: str, note: str, mpp_hint=None, replace=False):
    """ADD a whole H&E scan that is NOT registered to the cells, in place.

    It goes into its own coordinate system UNALIGNED (pixels -> um), never "global", so no
    viewer overlays it on the cells as if it were aligned. Used where an H&E of the block
    exists but no fit holds (Dulai IBD biopsies: several fragments per block, no nuclear image).
    """
    warnings.filterwarnings("ignore")
    import anndata as ad
    import zarr
    from spatialdata import read_zarr
    from spatialdata.models import Image2DModel
    from spatialdata.transformations import Scale, get_transformation
    src = store.resolve()
    sd = read_zarr(src)
    tname = list(sd.tables)[0]
    key = f"{UNALIGNED}_registration"
    if replace and UNALIGNED in sd.images:   # it is re-creatable from the scan; only this element + its uns key go
        del sd[UNALIGNED]                    # spatialdata refuses to delete an element still held in memory
        sd.delete_element_from_disk(UNALIGNED)
        uns = zarr.open_group(str(src / "tables" / tname / "uns"), mode="a")
        if key in uns:
            del uns[key]
        sd = read_zarr(src)
    if UNALIGNED in sd.images or key in sd.tables[tname].uns:
        sys.exit(f"{src.name}: already has {UNALIGNED} -- refusing")
    arr, he_um, _ = he_lazy(he, TIER_UM[tier], mpp_hint)
    n_scales = max(0, int(np.floor(np.log2(min(arr.shape[:2]) / 512))))
    img = Image2DModel.parse(arr, dims=("y", "x", "c"), c_coords=["r", "g", "b"],
                             transformations={UNALIGNED: Scale([he_um, he_um], axes=("x", "y"))},
                             scale_factors=[2] * min(n_scales, 5), chunks={"c": 3, "y": 1024, "x": 1024})
    reg = {"source": he.name, "section": "serial (tissue block)", "method": "none -- not registered",
           "coordinate_system": UNALIGNED, "he_um_per_px": he_um, "accuracy_note": note}
    bak = Path("/athena/masonlab/scratch/users/jip2007/release_qc/he_v1/inplace_backup")
    bak.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src / "zarr.json", bak / f"{src.name}.zarr.json")
    before = {k: set(getattr(sd, k)) for k in ("images", "labels", "shapes", "points", "tables")}
    n_obs, cols = sd.tables[tname].n_obs, list(sd.tables[tname].obs.columns)
    print(f"{src.name} [{tier}]: {UNALIGNED}, H&E {arr.shape[1]}x{arr.shape[0]} at {he_um:.2f} um/px")
    sd.images[UNALIGNED] = img
    sd.write_element(UNALIGNED)
    ad.io.write_elem(zarr.open_group(str(src / "tables" / tname / "uns"), mode="a"), key, reg)

    back = read_zarr(src)
    el = back.images[UNALIGNED]
    b0 = next(iter(el[list(el.children)[0]].data_vars.values())) if hasattr(el, "children") else el
    ts = get_transformation(b0, get_all=True)
    assert set(ts) == {UNALIGNED}, f"unexpected coordinate systems {set(ts)}"
    assert np.allclose(ts[UNALIGNED].to_affine_matrix(("x", "y"), ("x", "y")), np.diag([he_um, he_um, 1.0]))
    bt = back.tables[tname]
    assert bt.n_obs == n_obs and list(bt.obs.columns) == cols and key in bt.uns
    for k, names in before.items():
        assert set(getattr(back, k)) == names | ({UNALIGNED} if k == "images" else set()), f"{k} changed"
    print(f"{src.name}: unaligned H&E added in place and verified")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--unaligned", type=Path, metavar="HE_FILE",
                    help="attach this scan WITHOUT registration (own coordinate system); needs --store, --tier, --note")
    ap.add_argument("--note", help="accuracy_note for --unaligned")
    ap.add_argument("--he-mpp", type=float, help="um/px for --unaligned scans with no stored scale")
    ap.add_argument("--replace", action="store_true", help="--unaligned: replace an existing he_unaligned (other tier resolution)")
    ap.add_argument("--registration", type=Path)
    ap.add_argument("--store", type=Path, required=True)
    ap.add_argument("--tier", choices=list(TIER_UM), required=True)
    ap.add_argument("--element", choices=["he_same_section", "he_serial_section"],
                    help="from he_attach_plan.py; may only DOWNGRADE the measured label")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--in-place", action="store_true",
                    help="add to the existing store instead of a hardlinked copy (no inode blow-up)")
    a = ap.parse_args()
    if a.unaligned:
        if not a.note:
            ap.error("--unaligned needs --note")
        return attach_unaligned(a.unaligned, a.store, a.tier, a.note, a.he_mpp, a.replace)
    if not a.registration:
        ap.error("--registration is required")
    attach(a.registration, a.store, a.tier, a.dry_run, a.element, a.in_place)


if __name__ == "__main__":
    main()
