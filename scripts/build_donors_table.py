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

Demographics are tiered (docs/ACCESS_TIERS.md). `--out` gets the OPEN table:
identity, organ and condition only. The registered table (+ age group, sex,
ethnicity) and the controlled table (+ exact age) go to private/, which is
gitignored -- this repo is public, and Table 1 keys demographics by the same
core ids, so an open copy with them is a re-identification table.

Usage:
    python scripts/build_donors_table.py --qc-dir zenodo/donor_qc \
        --out data/donors.parquet --dry-run
"""

import argparse
import re
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).resolve().parent))
import donor_map

# Columns withheld from each tier's rendering. Open carries no demographics;
# registered adds age group, sex and ethnicity; controlled adds exact age.
TIER_DROP = {
    "open":       ["age", "age_group", "sex", "ethnicity"],
    "registered": ["age"],
    "controlled": [],
}
PRIVATE = Path(__file__).resolve().parent.parent / "private"

TABLE1 = Path("/Users/jiwoonpark/Dropbox (Personal)/2025- MasonLab/2025_SAHA/"
              "Publication/Revision/Submission/Upload/Tables/Table_1.xlsx")
CONFIG = Path("/Users/jiwoonpark/Dropbox (Personal)/2025- MasonLab/SAHA/02_Code/"
              "Configs/ProjectConfigs/ProjectConfig_PANC_Combined.yaml")

STATUS_TO_CONDITION = {"healthy": "normal", "disease": "disease", "cancer": "cancer"}
AGE_GROUP = {"<21": "pediatric", "21-60": "adult", ">60": "elderly"}

CROSSWALK = Path("zenodo/tap_audit/tap_crosswalk.csv")
DULAI = Path("zenodo/dulai/dulai_manifest.csv")

# The TAP exports and the cohort table spell the bone-marrow accessions
# differently: the release uses the accession, the cohort table suffixes it.
#
# Both BM specimens are I19. The TAP export directory for the second is named
# `I19-5886`, which is a typo -- the cohort table and the H&E file both say
# I19-5889, and that reading is settled (Jiwoon, 2026-09-15). The release uses
# I19-5889 throughout; only the raw directory on disk keeps the old spelling,
# recorded in tap_crosswalk.csv.
COHORT_ALIAS = {
    "BM_I194082": "I19-4082_BM",
    "BM_I195889": "I19-5889_BM",
    # The TAP exports name these by accession; the cohort table names them by
    # their position on the bone-marrow microarray. raw_from_tracker/
    # patient_blocks.csv carries the mapping (BM_A1_3 = I18-4753B1, 89 M;
    # BM_A2_2 = I17-1673B1, 16 F), and the ages and sexes agree on both sides.
    "BM_I184753": "BM_A1_3",
    "BM_I171673": "BM_A2_2",
}

# Cohort members whose block sits on a slide we hold but which no released
# acquisition covers yet. They stay in the table -- the tissue exists and the
# donor is part of the published cohort -- flagged `in_release = False` so a
# join against the objects still resolves to nothing rather than silently
# implying data that is not there.
AWAITING_DATA = {
    "BM_A1_1":   "block I19-943B1 - not yet acquired",
    "BM_A1_2":   "block I19-5155B1 - not yet acquired",
    "BM_A2_1":   "block I18-3244B1 - not yet acquired",
    "LIV_A1_2":  "on LIV_A1; outside the FOVs acquired so far (runs cover cores 1,3)",
    "LIV_A2_2":  "on LIV_A2; outside the FOVs acquired so far (runs cover cores 1,3)",
    "PROS_A1_1": "on PROS_A1; outside the FOVs acquired so far (run covers cores 2-5)",
}

# Cores with no cohort-table entry at all: new material beyond the published
# 150. They get identity rows so the objects can still join, with demographics
# left null rather than invented.
# Prostate cancer, WCM/MSK-SAIL, specimen PCa_9224. Table 1 carries only the
# healthy PROS_A1 microarray, so this donor ships with identity and no
# demographics -- the same footing as the CRC slides.
SPECIMEN_CONTEXT = {
    "PRCA_UA": ("PrCa_UA", "prostate", "cancer"),
}

NO_COHORT_ENTRY = {
    "PRCA_UA": "prostate cancer specimen PCa_9224, not in the published cohort",
    "CRC_C16": "cohort table carries a single pooled 'CRC' row for three slides",
    "CRC_C17": "cohort table carries a single pooled 'CRC' row for three slides",
    "CRC_C18": "cohort table carries a single pooled 'CRC' row for three slides",
}


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

    PDAC: the release says PANC_CA_A1_28 (slide included), the cohort table
    says PANC_CA_28. Bone marrow uses accession-number aliases.
    """
    if core in COHORT_ALIAS:
        return COHORT_ALIAS[core]
    m = re.fullmatch(r"PANC_CA_A1_(\d+)", core)
    return f"PANC_CA_{m.group(1)}" if m else core


def core_slides() -> dict:
    """core -> slide_id, for cores whose label is not `<SLIDE>_<n>`.

    BM_I171673 and CRC_C16 carry no trailing core number, so the slide has to
    come from the TAP crosswalk rather than by splitting the label.
    """
    import pandas as pd
    if not CROSSWALK.exists():
        return {}
    d = pd.read_csv(CROSSWALK)
    out = {}
    for _, r in d.iterrows():
        slide = str(r.get("slide_id", "")).strip()
        for c in str(r.get("donor_cores", "")).split(";"):
            c = c.strip().replace("-", "_")
            if c and c not in ("nan", "None") and slide and slide != "nan":
                out.setdefault(c, slide)
    return out


def tap_context() -> dict:
    """core -> (organ, condition), from the TAP release breakdown."""
    import pandas as pd
    p = Path("zenodo/tap_audit/tap_release_breakdown.csv")
    if not p.exists():
        return {}
    d = pd.read_csv(p)
    out = {}
    for _, r in d.iterrows():
        # CRC is recorded there as its own "organ"; it is a colon cancer.
        organ = str(r["organ"])
        condition = str(r["condition"])
        if organ == "colorectal_cancer":
            organ, condition = "colon", "cancer"
        for c in str(r.get("donor_cores", "")).split(";"):
            c = c.strip().replace("-", "_")
            if c and c not in ("nan", "None"):
                out.setdefault(c, (organ, condition))
    return out


def dulai_context() -> dict:
    """core -> (slide_id, organ, condition, patient) for the Dulai cohort.

    Table 1 lists all 52 IBD_PD rows with Sex, Age, Ethnicity and AgeGroup as
    `U`, so these cores ship with identity and provenance but no demographics.
    `donor_patient` is the accession: five patients contributed both an ileal
    and a rectal block, and those two cores are the same person.
    """
    import pandas as pd
    if not DULAI.exists():
        return {}
    d = pd.read_csv(DULAI)
    out = {}
    for _, r in d.iterrows():
        out.setdefault(str(r["donor_core"]),
                       (str(r["donor_core"]), str(r["organ"]),
                        str(r["condition"]), str(r["donor_patient"])))
    return out


PATIENT_BLOCKS = Path("raw_from_tracker/patient_blocks.csv")


def block_demographics() -> dict:
    """core -> (age, sex) from the tracker's per-block sheet.

    Table 1 is the published cohort and stays authoritative, but it records `U`
    for some cores that the tracker does know -- every prostate core has a sex
    there, for instance. Used only to fill a gap, never to overwrite Table 1.
    """
    import pandas as pd
    if not PATIENT_BLOCKS.exists():
        return {}
    d = pd.read_csv(PATIENT_BLOCKS)
    out = {}
    for _, r in d.iterrows():
        age = r.get("Age")
        sex = clean(r.get("Sex"))
        out[str(r.get("Sample")).strip()] = (
            int(age) if pd.notna(age) else None, sex)
    return out


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

    # the cores actually present in the release -- v1 per-cell assignments plus
    # the TAP acquisitions, through the one shared mapping
    sample_cores = donor_map.sample_cores(qc_dir=args.qc_dir)
    cores = {c for v in sample_cores.values() for c in v}
    if not cores:
        sys.exit(f"ERROR: no donor cores found under {args.qc_dir}")

    tab = cohort()
    c2p = core_to_patient()
    slides = core_slides()
    tapctx = tap_context()
    dulai = dulai_context()
    blocks = block_demographics()

    rows, no_demographics = [], []
    dulai_cores = set(dulai)
    released = set(cores)
    cores = sorted(set(cores) | set(AWAITING_DATA))
    for core in sorted(cores):
        key = cohort_key(core)
        r = tab.loc[key] if key in tab.index else None
        if isinstance(r, pd.DataFrame):
            r = r.iloc[0]

        slide = slides.get(core) or core.rsplit("_", 1)[0]
        organ, condition = tapctx.get(core, (None, None))
        if core in SPECIMEN_CONTEXT:
            slide, organ, condition = SPECIMEN_CONTEXT[core]
        if core in dulai:
            slide, organ, condition, patient = dulai[core]
            c2p[core] = patient

        if r is None:
            # Keep the core: the objects reference it, so a missing row would
            # break the join. Demographics stay null rather than guessed.
            if core not in dulai_cores:
                no_demographics.append(core)
            rows.append({
                "donor_core": core, "slide_id": slide,
                "donor_patient": c2p.get(core),
                "organ": organ, "condition": condition,
                "age": None, "age_group": None, "sex": "unknown", "ethnicity": None,
            })
            continue

        rows.append({
            "donor_core": core,
            "slide_id": slide,
            "donor_patient": c2p.get(core),
            "organ": ((clean(r.get("Organ")) or "").lower().replace(" ", "_")
                      or organ or None),
            "condition": STATUS_TO_CONDITION.get(
                str(r.get("Status", "")).strip().lower()) or condition,
            "age": int(r["Age"]) if str(r.get("Age")).strip().isdigit() else None,
            "age_group": AGE_GROUP.get(str(r.get("AgeGroup", "")).strip()),
            "sex": clean(r.get("Sex")) or "unknown",
            "ethnicity": clean(r.get("Ethnicity")),
        })
        # fill only what Table 1 left unknown
        fb_age, fb_sex = blocks.get(key, (None, None))
        if rows[-1]["age"] is None and fb_age is not None:
            rows[-1]["age"] = fb_age
            rows[-1]["age_group"] = ("pediatric" if fb_age < 21
                                     else "adult" if fb_age <= 60 else "elderly")
        if rows[-1]["sex"] == "unknown" and fb_sex:
            rows[-1]["sex"] = fb_sex

    df = pd.DataFrame(rows)
    df["in_release"] = df.donor_core.isin(released)
    df["status_note"] = df.donor_core.map(AWAITING_DATA)
    if no_demographics:
        print(f"{len(no_demographics)} core(s) shipped with identity but no "
              f"demographics (absent from the cohort table):")
        for c in no_demographics:
            print(f"    {c:14s} {NO_COHORT_ENTRY.get(c, 'no cohort-table entry')}")
        print()

    # donor_id: one per core. Each core is an independent case -- PANC_A1 has
    # 12 and PDAC 28 -- so cores are not collapsed even where two share a
    # donor_patient grouping. donor_patient is kept alongside for anyone who
    # wants the PDAC cores grouped by the patient they came from.
    df["_person"] = df["donor_core"]
    ids = {p: f"SAHA_D{i:03d}" for i, p in enumerate(sorted(df["_person"].unique()), 1)}
    df["donor_id"] = df["_person"].map(ids)
    df = df.drop(columns="_person")
    df = df[["donor_id", "donor_core", "donor_patient", "slide_id", "organ",
             "condition", "age", "age_group", "sex", "ethnicity",
             "in_release", "status_note"]]

    n_await = int((~df.in_release).sum())
    print(f"{len(df)} donor cores -> {df.donor_id.nunique()} distinct donors "
          f"({len(df) - n_await} with released data, {n_await} awaiting acquisition)\n")
    print(df.groupby(["slide_id"]).agg(
        cores=("donor_core", "size"), donors=("donor_id", "nunique"),
        known_age=("age", "count")).to_string())
    print(f"\nsex: {df.sex.value_counts().to_dict()}")
    if df.age.notna().any():
        print(f"age: {int(df.age.min())}-{int(df.age.max())} "
              f"({df.age.notna().sum()} of {len(df)} known)")
    print(f"ethnicity recorded for {df.ethnicity.notna().sum()} of {len(df)}")

    if args.dry_run:
        print("\n[dry-run] nothing written")
        return
    for tier, path in (("controlled", PRIVATE / "donors.controlled.parquet"),
                       ("registered", PRIVATE / "donors.registered.parquet"),
                       ("open", args.out)):
        t = df.drop(columns=TIER_DROP[tier])
        path.parent.mkdir(parents=True, exist_ok=True)
        t.to_parquet(path, index=False)
        print(f"wrote {tier:10s} {path}  ({len(t.columns)} columns)")


if __name__ == "__main__":
    main()
