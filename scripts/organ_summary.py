#!/usr/bin/env python3
"""
organ_summary.py
Write zenodo/organ_summary.csv -- one row per organ (Dulai IBD split out), plus
the two cohorts not yet in measurements.csv: PRCA (build pending) and
astroSTAMP PBMC (deferred).

samples = measurements = donor cores x acquisitions.
"""
from pathlib import Path
import pandas as pd

REPO = Path(__file__).resolve().parent.parent

PLATFORM = {"UCC 1K": "CosMx UCC", "6K Discovery": "CosMx 6K",
            "IO Protein (64-plex)": "CosMx Protein",
            "Multi-tissue (377-plex)": "Xenium Multi",
            "Xenium 5K": "Xenium 5K", "Xenium Colon": "Xenium Colon"}
STATE = {"normal": "Healthy", "cancer": "Cancer", "disease": "IBD"}
ORDER = list(PLATFORM.values()) + ["CosMx WTx"]

# ponytail: hand-entered until this cohort reaches measurements.csv; drop then.
# (PRCA reached it on 2026-09-25 and now sits in the prostate row.)
EXTRA = [
    # 25 spots = crew x timepoint, on 3 slides (zenodo/stamp/DEFERRED.md).
    dict(organ="PBMC", cohort="astroSTAMP (deferred)", samples=25, acquisitions=3,
         cells=4855308, platforms="CosMx WTx", states="Healthy"),
]


def join(vals, order):
    return ", ".join(sorted(set(vals), key=order.index))


def main():
    m = pd.read_csv(REPO / "zenodo/measurements.csv")
    unknown = set(m.panel_name) - set(PLATFORM)
    assert not unknown, f"unmapped panels: {unknown}"
    m["plat"] = m.panel_name.map(PLATFORM)
    m["state"] = m.condition.map(STATE)
    m["group"] = m.cohort.map(lambda c: "Dulai IBD" if c == "NU_Dulai_IBD" else "SAHA + TAP")

    rows = [dict(organ=o, cohort=g, samples=len(x), acquisitions=x.sample_id.nunique(),
                 cells=int(x.cells.sum()), platforms=join(x.plat, ORDER),
                 states=join(x.state, ["Healthy", "Cancer", "IBD"]))
            for (o, g), x in m.groupby(["organ", "group"])]
    out = pd.DataFrame(rows + EXTRA)
    assert out.samples.iloc[:len(rows)].sum() == len(m)
    out["cells"] = out.cells.astype("Int64")
    out.to_csv(REPO / "zenodo/organ_summary.csv", index=False)
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()
