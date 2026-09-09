#!/usr/bin/env python3
"""
build_donors_table.py
Rebuild donors.parquet with one row per donor core, from the published cohort table.

The previous donors.parquet claimed 5 donors and samples.parquet mapped every
`*_A1` slide to a single `SAHA_D001` -- 32 of 47 samples. That was wrong: A1/A9
are SLIDE identifiers, and these slides are tissue microarrays carrying 2-28
donor cores each. Publishing it would have asserted that 32 samples across five
organs came from one person.

Donor identity is per-CELL (obs `donor_core`, written by apply_donor_core.py),
so this table is keyed by `donor_core` and joins to the objects on that column.
Demographics come from Table_1 of the manuscript, so the release and the paper
describe the same cohort.

`donor_id` is retained as the de-identified key required by the schema. Where
two cores are known to be the same person it is shared: the PDAC slide carries
28 cores from 14 patients (two each, KRAS G12R / G12D), recorded in
ProjectConfig_PANC_Combined.yaml and independently in RNA_PDAC_scVI.h5ad.
Elsewhere one core is treated as one donor.

Usage:
    python scripts/build_donors_table.py --qc-dir zenodo/donor_qc \
        --out data/donors.parquet --dry-run
"""

import argparse
import glob
import re
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

TABLE1 = Path("/Users/jiwoonpark/Dropbox (Personal)/2025- MasonLab/2025_SAHA/"
              "Publication/Revision/Submission/Upload/Tables/Table_1.xlsx")
CONFIG = Path("/Users/jiwoonpark/Dropbox (Personal)/2025- MasonLab/SAHA/02_Code/"
              "Configs/ProjectConfigs/ProjectConfig_PANC_Combined.yaml")

STATUS_TO_CONDITION = {"healthy": "normal", "disease": "disease", "cancer": "cancer"}
AGE_GROUP = {"<21": "pediatric", "21-60": "adult", ">60": "elderly"}


def cohort():
    """SampleID -> demographics, from the published cohort table."""
    import pandas as pd
    d = pd.read_excel(TABLE1, sheet_name="Cohort")
    d = d.drop(columns=[c for c in d.columns if str(c).startswith("Unnamed")])
    d["SampleID"] = d["SampleID"].astype(str).str.strip()
    return d.set_index("SampleID")


def core_to_patient():
    """PDAC core -> patient, from Patient_maps (KRAS G12R / G12D)."""
    import yaml
    if not CONFIG.exists():
        return {}
    d = yaml.safe_load(open(CONFIG))
    pm = d.get("Patient_maps")
    out = {}
    if isinstance(pm, dict):
        def walk(x):
            for k, v in x.items():
                if isinstance(v, dict):
                    yield from walk(v)
                elif isinstance(v, list):
                    yield k, v
        for patient, cores in walk(pm):
            for c in cores:
                out[str(c).replace("-", "_")] = str(patient).replace("-", "_")
    return out


def cohort_key(core: str) -> str:
    """Map a release donor_core onto its SampleID in the cohort table.

    They differ for PDAC only: the release says PANC_CA_A1_28 (slide included),
    the cohort table says PANC_CA_28.
    """
    m = re.fullmatch(r"PANC_CA_A1_(\d+)", core)
    return f"PANC_CA_{m.group(1)}" if m else core


def clean(v):
    s = str(v).strip()
    return None if s in ("", "U", "nan", "None", "NA") else s


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--qc-dir", type=Path, required=True)
    ap.add_argument("--out", type=Path, default=Path("data/donors.parquet"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    import pandas as pd

    # the cores actually present in the release
    cores = set()
    for f in glob.glob(str(args.qc_dir / "*.donor_core.csv")):
        s = pd.read_csv(f, usecols=["donor_core"])["donor_core"].dropna().astype(str)
        cores |= {c.replace("-", "_") for c in s.unique()}
    if not cores:
        sys.exit(f"ERROR: no donor cores found under {args.qc_dir}")

    tab = cohort()
    c2p = core_to_patient()

    rows, missing = [], []
    for core in sorted(cores):
        key = cohort_key(core)
        if key not in tab.index:
            missing.append(core)
            continue
        r = tab.loc[key]
        if isinstance(r, pd.DataFrame):
            r = r.iloc[0]
        slide = core.rsplit("_", 1)[0]
        rows.append({
            "donor_core": core,
            "slide_id": slide,
            "donor_patient": c2p.get(core),
            "organ": (clean(r.get("Organ")) or "").lower().replace(" ", "_") or None,
            "condition": STATUS_TO_CONDITION.get(
                str(r.get("Status", "")).strip().lower()),
            "age": int(r["Age"]) if str(r.get("Age")).strip().isdigit() else None,
            "age_group": AGE_GROUP.get(str(r.get("AgeGroup", "")).strip()),
            "sex": clean(r.get("Sex")) or "unknown",
            "ethnicity": clean(r.get("Ethnicity")),
        })

    df = pd.DataFrame(rows)
    if missing:
        print(f"WARNING: {len(missing)} core(s) absent from the cohort table: "
              f"{', '.join(missing[:8])}{' ...' if len(missing) > 8 else ''}\n")

    # donor_id: one per core. Each core is an independent case -- PANC_A1 has
    # 12 and PDAC 28 -- so cores are not collapsed even where two share a
    # donor_patient grouping. donor_patient is kept alongside for anyone who
    # wants the PDAC cores grouped by the patient they came from.
    df["_person"] = df["donor_core"]
    ids = {p: f"SAHA_D{i:03d}" for i, p in enumerate(sorted(df["_person"].unique()), 1)}
    df["donor_id"] = df["_person"].map(ids)
    df = df.drop(columns="_person")
    df = df[["donor_id", "donor_core", "donor_patient", "slide_id", "organ",
             "condition", "age", "age_group", "sex", "ethnicity"]]

    print(f"{len(df)} donor cores -> {df.donor_id.nunique()} distinct donors\n")
    print(df.groupby(["slide_id"]).agg(
        cores=("donor_core", "size"), donors=("donor_id", "nunique"),
        known_age=("age", "count")).to_string())
    print(f"\nsex: {df.sex.value_counts().to_dict()}")
    print(f"age: {int(df.age.min())}-{int(df.age.max())} "
          f"({df.age.notna().sum()} of {len(df)} known)")
    print(f"ethnicity recorded for {df.ethnicity.notna().sum()} of {len(df)}")

    if args.dry_run:
        print("\n[dry-run] nothing written")
        return
    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.out, index=False)
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
