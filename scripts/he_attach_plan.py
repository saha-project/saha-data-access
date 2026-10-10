#!/usr/bin/env python3
"""
he_attach_plan.py
Pick one H&E registration per v1 object and list the stores to write it into.
Writes the attach plan (for attach_he.py, one store per row) and he_status.csv
(every v1 object: cell-level / tissue-level / no H&E), from the same rules, so
the status table and what gets attached cannot disagree.

Candidates are register_he_object.py results in --reg. Per object, the best is
the cell-level fit if there is one (attach_he.classify), else the clearest
tissue-level fit (most cells on H&E tissue relative to a random H&E pixel).

EXCLUDED holds candidates shown wrong on inspection, so a later rerun cannot
quietly bring them back:
  * Xenium_WCM_APE/ILE contain only 2 of each slide's 4 MS01 regions; for the
    others every orientation gives spurious or zero overlap.
  * The same scans do not belong to the SJJP02 run at all.
  * SJ01 low-res H&E TIFF exports other than PANC protein: whole-slide images of
    unscaled TIFFs; the tissue-area scale estimate fails, fits are at chance.

Usage (on Cayuga):
    python scripts/he_attach_plan.py --reg .../he_v1/reg --out .../he_v1
"""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from attach_he import classify  # noqa: E402
from package_for_zenodo import record_slug, sample_id_of  # noqa: E402

D = Path("/athena/project-saha/derived")
EXCLUDED = {
    "SAHA_XR_APE_A9_MS01_02_RNA", "SAHA_XR_APE_A9_MS01_03_RNA",
    "SAHA_XR_ILE_A1_MS01_01_RNA", "SAHA_XR_ILE_A1_MS01_02_RNA",
    "SAHA_XR_APE_A9_SJJP02_01_RNA", "SAHA_XR_ILE_A1_SJJP02_01_RNA",
}
# Scans that are physically a DIFFERENT section from the one measured: SJ01's
# SAHA_HE_*.svs are dated 2023-12-20, the CosMx run 2024-01-27. Their fits can
# never be cell-level, however small the measured offset (5.1 um on 9 windows for
# APE_A9 is a coincidence of similar architecture, not nuclear correspondence).
ADJACENT_SCANS = {f"SAHA_HE_{t}_SJ01.svs" for t in ("APE_A9", "COL_A1", "ILE_A1")}
EXCLUDED_SCANS = {f"SAHA_HE{a}_{t}_SJ01.tif" for a in ("CR", "CP") for t in ("APE_A9", "COL_A1", "ILE_A1")} \
    | {"SAHA_HECR_PANC_A1_SJ01.tif"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reg", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--samples", type=Path, required=True)
    a = ap.parse_args()

    cand = {}
    for f in sorted(a.reg.glob("*.he_registration.json")):
        r = json.loads(f.read_text())
        if r["sample_id"] in EXCLUDED or Path(r["he_file"]).name in EXCLUDED_SCANS:
            continue
        r["_file"], r["_class"] = str(f), classify(r)
        if r["_class"] and Path(r["he_file"]).name in ADJACENT_SCANS:
            r["_class"] = "he_serial_section"
        if r["_class"]:
            cand.setdefault(r["sample_id"], []).append(r)

    def rank(r):
        return (r["_class"] == "he_same_section",
                r["cells_on_he_tissue"] / max(r["he_tissue_fraction_random"], 1e-3),
                -(r.get("nuclear_offset_um_refined") or 1e9))

    s = pd.read_parquet(a.samples)
    s["slug"] = s.apply(record_slug, axis=1)
    plan, status = [], []
    for row in s.itertuples():
        stores = sorted((D / "release/s3/processed" / row.slug).glob(f"{row.sample_id}*.zarr"))
        store = next((p for p in stores if sample_id_of(p) == row.sample_id), None)
        name = store.name.removesuffix(".zarr") if store else row.sample_id
        rs = cand.get(name)
        if not rs:
            status.append(dict(sample_id=row.sample_id, slug=row.slug, he="no H&E"))
            continue
        best = max(rs, key=rank)
        lvl = "cell-level" if best["_class"] == "he_same_section" else "tissue-level"
        status.append(dict(sample_id=row.sample_id, slug=row.slug, he=lvl,
                           element=best["_class"], scan=Path(best["he_file"]).name,
                           nuclear_offset_um=best.get("nuclear_offset_um_refined"),
                           windows=best.get("refine_windows"),
                           cells_on_he=round(best["cells_on_he_tissue"], 3),
                           registration=best["_file"]))
        zen = D / "build/zenodo_tier" / row.slug / store.name
        if zen.is_symlink():                       # Dulai/Xenium: one store serves both tiers
            plan.append((best["_file"], str(store), "both", best["_class"]))
        else:
            plan.append((best["_file"], str(zen), "zenodo", best["_class"]))
            plan.append((best["_file"], str(store), "aws", best["_class"]))

    a.out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(plan, columns=["registration", "store", "tier", "element"]).to_csv(
        a.out / "attach_plan.tsv", sep="\t", index=False, header=False)
    st = pd.DataFrame(status)
    st.to_csv(a.out / "he_status.csv", index=False)
    print(st.he.value_counts().to_string())
    print(f"{len(plan)} stores to write")


if __name__ == "__main__":
    main()
