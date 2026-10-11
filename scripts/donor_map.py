#!/usr/bin/env python3
"""
donor_map.py
One answer to "which donor cores does this sample cover?"

These slides are tissue microarrays, so a single acquisition can span 1-28
donors. Three scripts need the same mapping and used to derive it separately:
ingest_samples.py (for n_donors), build_donors_table.py (for which cores exist)
and validate_release.py (to check the donor join). Deriving it three ways is how
samples.parquet ended up claiming that 32 samples across five organs came from
one person.

Sources, in the order they are trusted:

1. `zenodo/donor_qc/<sample>.donor_core.csv` -- the per-cell assignments
   resolved by resolve_donor_cores.py. Authoritative wherever it exists.
2. `zenodo/tap_audit/tap_release_breakdown.csv` -- the `donor_cores` column,
   read from the recorded FOV->core maps in the TAP FOV_Metadata CSVs.
3. The Xenium sample-id suffix rule (SAHA_XR_APE_A9_MS01_03 -> APE_A9_3).
   Xenium acquisitions are already one core each, so no per-cell file exists.

Core labels are normalised to snake_case on the way out: the PDAC config files
spell them `PANC-CA_A1_7` and donors.parquet keys on `PANC_CA_A1_7`.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
QC_DIR = REPO / "zenodo" / "donor_qc"
TAP_BREAKDOWN = REPO / "zenodo" / "tap_audit" / "tap_release_breakdown.csv"
DULAI_MANIFEST = REPO / "zenodo" / "dulai" / "dulai_manifest.csv"


def norm_core(v) -> "str | None":
    """`PANC-CA_A1_7` -> `PANC_CA_A1_7`; blanks and NA spellings -> None."""
    if v is None:
        return None
    s = str(v).strip()
    if s in ("", "nan", "None", "NA", "<NA>"):
        return None
    return s.replace("-", "_")


def xenium_core(sample_id: str) -> "str | None":
    """SAHA_XR_APE_A9_MS01_03 -> APE_A9_3, or None when there is no core number.

    Mirrors core_of() in apply_donor_core_xenium.py, which wrote these labels
    into the objects. SAHA_XR_PANC_A1_MS01 is a whole-slide scan with no
    trailing core number and is covered by its donor_qc CSV instead.
    """
    stem = re.sub(r"_(RNA|PRT)$", "", str(sample_id))
    if stem in XENIUM_CORE_OVERRIDE:
        return XENIUM_CORE_OVERRIDE[stem]
    parts = stem.split("_")
    if len(parts) < 4:
        return None
    m = re.search(r"_(\d+)$", stem)
    return f"{parts[2]}_{parts[3]}_{int(m.group(1))}" if m else None


# Xenium regions whose number is NOT their core number. The suffix rule assumes region i = core i;
# for the PANC_A1 SJJP02 slide that is false for 7 of 8 regions, and for appendix/ileum 7 of 8 each. Identity checked 2026-10-10 two
# independent ways against the MS01 scan (authoritative per-cell cores): H&E core matching on the
# MS01 slide H&E and pseudobulk expression correlation (release_qc/he_check/panc_core_expr.csv).
XENIUM_CORE_OVERRIDE = {
    "SAHA_XR_PANC_A1_SJJP02_01": "PANC_A1_4",
    "SAHA_XR_PANC_A1_SJJP02_02": "PANC_A1_8",
    "SAHA_XR_PANC_A1_SJJP02_03": "PANC_A1_3",
    "SAHA_XR_PANC_A1_SJJP02_04": "PANC_A1_2",
    "SAHA_XR_PANC_A1_SJJP02_05": "PANC_A1_7",
    "SAHA_XR_PANC_A1_SJJP02_06": "PANC_A1_6",
    "SAHA_XR_PANC_A1_SJJP02_07": "PANC_A1_1",
    "SAHA_XR_PANC_A1_SJJP02_08": "PANC_A1_9",
    # Appendix / ileum (approved 2026-10-10): expression vs CosMx cannot separate these cores, so MS01
    # identity is core SHAPE on H&E vs the CosMx per-cell cores of the same block (release_qc/he_check/
    # geo_shape.py; one shared ~270 deg turn, all four H&E comparisons agree). Each SJJP02 region follows
    # its MS01 partner (pseudobulk r ~0.997, runner-up <= 0.97). Listed in full, unchanged ones included.
    "SAHA_XR_APE_A9_MS01_01": "APE_A9_2",
    "SAHA_XR_APE_A9_MS01_02": "APE_A9_4",
    "SAHA_XR_APE_A9_MS01_03": "APE_A9_3",
    "SAHA_XR_APE_A9_MS01_04": "APE_A9_1",
    "SAHA_XR_APE_A9_SJJP02_01": "APE_A9_3",
    "SAHA_XR_APE_A9_SJJP02_02": "APE_A9_4",
    "SAHA_XR_APE_A9_SJJP02_03": "APE_A9_2",
    "SAHA_XR_APE_A9_SJJP02_04": "APE_A9_1",
    "SAHA_XR_ILE_A1_MS01_01": "ILE_A1_4",
    "SAHA_XR_ILE_A1_MS01_02": "ILE_A1_3",
    "SAHA_XR_ILE_A1_MS01_03": "ILE_A1_1",
    "SAHA_XR_ILE_A1_MS01_04": "ILE_A1_2",
    "SAHA_XR_ILE_A1_SJJP02_01": "ILE_A1_2",
    "SAHA_XR_ILE_A1_SJJP02_02": "ILE_A1_4",
    "SAHA_XR_ILE_A1_SJJP02_03": "ILE_A1_3",
    "SAHA_XR_ILE_A1_SJJP02_04": "ILE_A1_1",
}


# Acquisitions that image a single specimen rather than a microarray. Donor is
# still written per cell; there is simply one core for the whole acquisition.
SINGLE_SPECIMEN = {
    "SAHA_CR_PRCA_UA_EC02": "PRCA_UA",
    "SAHA_CP_PRCA_UA_EC02": "PRCA_UA",
    "SAHA_XR_PRCA_UA_MS01": "PRCA_UA",
}


def sample_cores(qc_dir: Path = QC_DIR,
                 tap_breakdown: Path = TAP_BREAKDOWN) -> dict:
    """sample_id -> sorted list of donor_core labels covering it."""
    import pandas as pd

    out: dict = {}

    for f in sorted(Path(qc_dir).glob("*.donor_core.csv")):
        sample = f.name.split(".")[0]
        col = pd.read_csv(f, usecols=["donor_core"])["donor_core"]
        cores = {c for c in (norm_core(v) for v in col.unique()) if c}
        if cores:
            out[sample] = sorted(cores)

    # Northwestern / Dulai IBD: one acquisition images one FFPE block, so the
    # whole acquisition is a single core. Still per-cell in the object, so it
    # joins donors.parquet exactly like the microarray acquisitions do.
    if DULAI_MANIFEST.exists():
        m = pd.read_csv(DULAI_MANIFEST)
        for _, r in m.iterrows():
            c = norm_core(r["donor_core"])
            if c:
                out.setdefault(str(r["sample_id"]), [c])

    # Single-specimen acquisitions: one core for the whole object. Registered
    # here as well as in cores_for() so build_donors_table.py sees them -- a core
    # only cores_for() knows about resolves for samples but never gets a row in
    # donors.parquet, and the join then fails validation.
    for sid, core in SINGLE_SPECIMEN.items():
        out.setdefault(sid, [core])

    if Path(tap_breakdown).exists():
        t = pd.read_csv(tap_breakdown)
        for _, r in t.iterrows():
            cores = {c for c in (norm_core(x)
                                 for x in str(r.get("donor_cores", "")).split(";")) if c}
            if cores:
                out.setdefault(str(r["sample_id"]), sorted(cores))

    return out


def cores_for(sample_id: str, cores: dict) -> list:
    """Cores for one sample, falling back to the Xenium suffix rule."""
    # donor_qc files for Xenium carry the `_RNA` suffix the objects use.
    for key in (sample_id, f"{sample_id}_RNA"):
        if key in cores:
            return cores[key]
    if sample_id in SINGLE_SPECIMEN:
        return [SINGLE_SPECIMEN[sample_id]]
    c = xenium_core(sample_id)
    return [c] if c else []


def demo():
    """Self-check: the mapping must reproduce the release's known shape."""
    cores = sample_cores()
    assert cores, "no donor cores found -- is zenodo/donor_qc populated?"

    # One acquisition spans many donors; that is the whole point of the table.
    assert len(cores_for("SAHA_CP_PDAC_RC_EC02", cores)) == 28
    assert len(cores_for("SAHA_CR_PANC_A1_SJ01", cores)) == 12
    assert len(cores_for("SAHA_CP_LN_A1_EC03", cores)) == 10

    # Xenium falls through to the suffix rule and yields exactly one core.
    assert cores_for("SAHA_XR_APE_A9_MS01_03", cores) == ["APE_A9_3"]
    assert cores_for("SAHA_XR_ILE_A1_SJJP02_04", cores) == ["ILE_A1_1"]
    # ...unless the region was shown not to be its suffix core.
    assert cores_for("SAHA_XR_PANC_A1_SJJP02_01", cores) == ["PANC_A1_4"]
    assert xenium_core("SAHA_XR_PANC_A1_SJJP02_08_RNA") == "PANC_A1_9"
    # ...except the whole-slide scan, which has a per-cell file.
    assert len(cores_for("SAHA_XR_PANC_A1_MS01", cores)) == 12

    # Dulai: one block per acquisition, snake_cased from the block id.
    assert cores_for("SAHA_CR_IBD_PD_A1_1_NU01", cores) == ["I0294_ITDF00A"]
    # The same block imaged on both platforms resolves to the same donor.
    assert (cores_for("SAHA_XR_IBD_PD_A1_1_NU01", cores)
            == cores_for("SAHA_CR_IBD_PD_A1_1_NU01", cores))

    # A single-specimen acquisition still resolves to a core.
    assert cores_for("SAHA_XR_PRCA_UA_MS01", cores) == ["PRCA_UA"]

    # TAP comes from the recorded FOV maps.
    assert cores_for("SAHA_CR_CRC_C16_TAP00", cores) == ["CRC_C16"]
    assert len(cores_for("SAHA_CP_PROS_A1_TAP01", cores)) == 4

    # PDAC cores are snake_case so they join donors.parquet.
    assert all("-" not in c for v in cores.values() for c in v)
    assert "PANC_CA_A1_7" in cores_for("SAHA_CP_PDAC_RC_EC02", cores)

    # A slide id is not a donor: every APE_A9 core is distinct.
    ape = cores_for("SAHA_CR_APE_A9_SJ01", cores)
    assert len(set(ape)) == len(ape) == 4

    print(f"OK: {len(cores)} samples mapped, "
          f"{len({c for v in cores.values() for c in v})} distinct cores")


if __name__ == "__main__":
    demo()
