#!/usr/bin/env python3
"""
cohort_coverage.py
Write zenodo/cohort_coverage.csv -- one row per donor core, reconciled against
Table 1 of the manuscript.

Answers two questions that kept being asked in prose and answered differently
each time: how many donors are actually in the release, and which of the
published cohort are missing.

Table 1 has 150 rows, but they are not all the same kind of thing: 98 are donor
cores, and 52 are per-acquisition rows for the Northwestern/Dulai IBD cohort
(20 blocks x 7 panel runs). So the cohort is 118 distinct tissue cores, not 150
donors, and a release with 114 cores is not 36 short.

`cells_in_core` is attributed per core, never per acquisition: a 28-core PDAC
acquisition contributes its cells to 28 different cores, and counting the whole
acquisition against each of them was inflating the totals sixfold.

Both outputs ship in the open tier, so neither carries demographics (age, sex):
those are registered-tier fields in private/donors.registered.parquet.
"""
import collections, glob, json, os, re, sys
from pathlib import Path
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
import donor_map

TABLE1 = Path("/Users/jiwoonpark/Dropbox (Personal)/2025- MasonLab/2025_SAHA/"
              "Publication/Revision/Submission/Upload/Tables/Table_1.xlsx")

# Table 1 spells some cores differently from the release.
ALIAS = {"I19-4082_BM": "BM_I194082", "I19-5889_BM": "BM_I195889",
         "BM_A1_3": "BM_I184753", "BM_A2_2": "BM_I171673"}
POOLED = {"CRC": ["CRC_C16", "CRC_C17", "CRC_C18"]}   # one row, three slides

WHY = {
 "BM_A1_1": "block I19-943B1 - no raw data on the cluster",
 "BM_A1_2": "block I19-5155B1 - no raw data on the cluster",
 "BM_A2_1": "block I18-3244B1 - no raw data on the cluster",
 "LIV_A1_2": "slide scanned; core outside the acquired FOVs (LIV_A1 covers cores 1,3)",
 "LIV_A2_2": "slide scanned; core outside the acquired FOVs (LIV_A2 covers cores 1,3)",
 "PROS_A1_1": "slide scanned; core outside the acquired FOVs (PROS_A1 covers cores 2-5)",
}


def cores_of(sid):
    if sid in ALIAS:
        return [ALIAS[sid]]
    if sid in POOLED:
        return POOLED[sid]
    m = re.fullmatch(r"PANC_CA_(\d+)", sid)
    return [f"PANC_CA_A1_{m.group(1)}"] if m else [sid]


def measurements(samples, cores, tap_counts):
    """One record per acquisition x donor core -- the unit actually measured.

    A donor core imaged in seven runs is seven separate measurements of seven
    separate populations of cells; an acquisition spanning 28 cores is 28. Neither
    "one row per acquisition" nor "one row per donor" can express that, and
    aggregating to either loses the cell counts that only exist at the crossing.
    """
    qc = {os.path.basename(f).split(".")[0]: f
          for f in glob.glob(str(REPO / "zenodo/donor_qc/*.donor_core.csv"))}
    cells = collections.Counter()
    acqs = collections.defaultdict(list)
    recs = []
    for _, r in samples.iterrows():
        cs = donor_map.cores_for(r.sample_id, cores)
        key = (r.sample_id if r.sample_id in qc
               else f"{r.sample_id}_RNA" if f"{r.sample_id}_RNA" in qc else None)
        if key:                                    # exact per-cell assignment
            counts = (pd.read_csv(qc[key], usecols=["donor_core"])["donor_core"]
                      .map(donor_map.norm_core).dropna().value_counts().to_dict())
        elif r.sample_id in tap_counts:            # read back from the object
            counts = tap_counts[r.sample_id]
        elif len(cs) == 1 and pd.notna(r.n_cells):  # one acquisition, one core
            counts = {cs[0]: int(r.n_cells)}
        else:
            raise SystemExit(f"ERROR: no per-core rule for {r.sample_id} "
                             f"({len(cs)} cores) -- refusing to guess")
        for c, n in counts.items():
            cells[c] += n
            acqs[c].append(r.sample_id)
            recs.append({"measurement_id": f"{r.sample_id}::{c}",
                         "sample_id": r.sample_id, "donor_core": c,
                         "cells": int(n), "platform": r.platform,
                         "assay_type": r.assay_type, "panel_name": r.panel_name,
                         "run_id": r.run_id, "organ": r.organ,
                         "condition": r.condition, "slide_id": r.slide_id,
                         "n_donors_in_acquisition": int(r.n_donors or 1)})
    return cells, acqs, pd.DataFrame(recs)


def main():
    tap_file = REPO / "zenodo/tap_audit/tap_core_cells.jsonl"
    tap = {}
    if tap_file.exists():
        for line in tap_file.read_text().splitlines():
            if line.strip().startswith("{"):
                r = json.loads(line)
                tap[r["sample"]] = {donor_map.norm_core(k): v
                                    for k, v in r["counts"].items()
                                    if donor_map.norm_core(k)}

    samples = pd.read_parquet(REPO / "data/samples.parquet")
    donors = pd.read_parquet(REPO / "data/donors.parquet")
    cores = donor_map.sample_cores()
    cells, acqs, meas = measurements(samples, cores, tap)

    # the measurement-grain table, joined to donor identity
    dm = donors.set_index("donor_core")
    meas["donor_patient"] = meas.donor_core.map(dm.donor_patient)
    meas["cohort"] = meas.sample_id.map(
        lambda x: "NU_Dulai_IBD" if "IBD_PD" in x
        else "TAP_precommercial" if "TAP" in x else "SAHA")
    meas = meas.sort_values(["sample_id", "donor_core"])
    mout = REPO / "zenodo/measurements.csv"
    meas.to_csv(mout, index=False)
    print(f"{len(meas)} measurements (acquisition x core) -> {mout}")

    plat = samples.set_index("sample_id").platform
    # donors.parquet now also lists cohort members awaiting acquisition, so
    # presence in the table no longer means presence in the release.
    released = set(donors.loc[donors.in_release, "donor_core"]) if "in_release" in donors else set(donors.donor_core)
    have, byc, rows = set(donors.donor_core), donors.set_index("donor_core"), []

    t = pd.read_excel(TABLE1, sheet_name="Cohort")
    t = t.drop(columns=[c for c in t.columns if str(c).startswith("Unnamed")])
    t["SampleID"] = t.SampleID.astype(str).str.strip()

    for _, r in t[~t.SampleID.str.startswith("IBD_PD")].iterrows():
        cs = [c for c in cores_of(r.SampleID) if c in released]
        awaiting = [c for c in cores_of(r.SampleID) if c in have and c not in released]
        if not cs:
            rows.append(dict(table1_id=r.SampleID, donor_core=None, cohort="SAHA",
                             organ=str(r.Organ).lower().replace(" ", "_"),
                             condition=str(r.Status).lower(),
                             in_release=False, n_acquisitions=0, cells_in_core=0,
                             platforms=None, acquisitions=None,
                             donor_core_awaiting=awaiting[0] if awaiting else None,
                             why_absent=(byc.loc[awaiting[0], "status_note"] if awaiting
                                         else WHY.get(r.SampleID, "not acquired"))))
            continue
        for c in cs:
            rows.append(_row(c, r.SampleID, "SAHA", byc, cells, acqs, plat))

    for c in sorted(x for x in donors.donor_core if re.match(r"I0\d+_", x) and x in released):
        rows.append(_row(c, f"IBD_PD (block {c.replace('_', ' ')})",
                         "NU_Dulai_IBD", byc, cells, acqs, plat))

    # Anything released that neither Table 1 nor the Dulai block list accounts
    # for -- material beyond the published cohort. Dropping these silently is how
    # a core ends up in the objects but in no register.
    listed = {r["donor_core"] for r in rows if r["donor_core"]}
    for c in sorted(released - listed):
        rows.append(_row(c, "not in Table 1", "SAHA", byc, cells, acqs, plat))

    cc = pd.DataFrame(rows).sort_values(["cohort", "organ", "donor_core"])
    out = REPO / "zenodo/cohort_coverage.csv"
    cc.to_csv(out, index=False)

    n_in = int(cc.in_release.sum())
    print(f"{len(cc)} rows -> {out}")
    print(f"cores in release: {n_in} | absent: {len(cc) - n_in}")
    print(f"cells attributed to a core: {cc.cells_in_core.sum():,} "
          f"of {int(samples.n_cells.sum()):,} released")
    return cc


def _row(core, table1_id, cohort, byc, cells, acqs, plat):
    d = byc.loc[core]
    a = sorted(set(acqs.get(core, [])))
    return dict(table1_id=table1_id, donor_core=core, cohort=cohort,
                organ=d.organ, condition=d.condition,
                in_release=True, n_acquisitions=len(a),
                cells_in_core=int(cells.get(core, 0)),
                platforms=",".join(sorted({plat[x] for x in a})),
                acquisitions=";".join(a), donor_core_awaiting=None, why_absent=None)


if __name__ == "__main__":
    main()
