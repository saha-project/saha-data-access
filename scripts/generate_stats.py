#!/usr/bin/env python3
"""
generate_stats.py
Generate docs/stats.html — a self-contained tally of current SAHA numbers
(cohort samples, acquisition runs, breakdowns by organ / status / assay /
panel / site) read live from the two publication supplementary tables:

    Table_1.xlsx  sheet "Cohort"  — one row per tissue sample + demographics
    Table_2.xlsx  sheet "Sheet1"  — one row per acquisition run + protocol

Only aggregate counts are written to the page; the raw per-sample tables
(which contain age/sex/ethnicity) are never copied into this repo.

Re-run whenever the tables change to refresh the page:
    python scripts/generate_stats.py
    python scripts/generate_stats.py --tables-dir /path/to/Tables
"""

import argparse
from datetime import date
from pathlib import Path

import pandas as pd

# Default location of the publication supplementary tables (outside the repo).
DEFAULT_TABLES_DIR = Path(
    "/Users/jiwoonpark/Dropbox (Personal)/2025- MasonLab/2025_SAHA/"
    "Publication/Revision/Submission/Upload/Tables"
)

# Cell counts live in the run tracker (in-repo). CosMx only for now —
# Xenium and other assays are not yet tallied here.
DEFAULT_RUNS_CSV = Path("raw_from_tracker/runs.csv")
CELL_COLS = ["CR_Cells", "CP_Cells", "Other_Cells"]


def counts(series, normalize=None):
    """Ordered {value: count} dict, most common first, dropping blanks."""
    s = series.dropna().astype(str).str.strip()
    if normalize:
        s = s.map(normalize)
    s = s[s != ""]
    return s.value_counts().to_dict()


def stat(label, value):
    return f'<div class="stat"><span class="v">{value}</span><span class="l">{label}</span></div>'


def bars(d):
    mx = max(d.values()) or 1
    out = []
    for k, v in d.items():
        out.append(
            f'<div class="row"><span class="k" title="{k}">{k}</span>'
            f'<span class="track"><span class="fill" style="width:{100*v/mx:.1f}%"></span></span>'
            f'<span class="n">{v}</span></div>'
        )
    return "\n".join(out)


def block(title, d):
    return f'<section class="block"><h3>{title}</h3><div class="bars">{bars(d)}</div></section>'


def title_case(x):
    """Normalize organ labels: merges e.g. 'ileum' and 'Ileum'."""
    return x.title()


def build(tables_dir: Path, runs_csv: Path) -> str:
    cohort = pd.read_excel(tables_dir / "Table_1.xlsx", sheet_name="Cohort")
    runs = pd.read_excel(tables_dir / "Table_2.xlsx", sheet_name="Sheet1")
    tracker = pd.read_csv(runs_csv)

    n_samples = len(cohort)
    n_runs = len(runs)
    organ_counts = counts(cohort["Organ"], normalize=title_case)
    n_organs = len(organ_counts)
    n_assays = runs["Assay"].nunique()
    n_sites = runs["Run location"].nunique()
    n_cells = int(
        tracker[CELL_COLS].apply(pd.to_numeric, errors="coerce").sum().sum()
    )

    caption = (
        f"cells segmented across CosMx runs<sup>*</sup> · {n_samples} samples · "
        f"{n_organs} organs · {n_assays} assays · {n_sites} sites"
    )

    strip = "".join([
        stat("samples", n_samples),
        stat("runs", n_runs),
        stat("organs", n_organs),
        stat("assays", n_assays),
        stat("sites", n_sites),
    ])

    grid = "".join([
        block("Samples by organ", organ_counts),
        block("Samples by status", counts(cohort["Status"])),
        block("Runs by assay", counts(runs["Assay"])),
        block("Runs by panel", counts(runs["Panel"])),
        block("Runs by site", counts(runs["Run location"])),
        block("Samples by sex", counts(cohort["Sex"].replace({"U": "Unknown", "M": "Male", "F": "Female"}))),
    ])

    html = TEMPLATE
    html = html.replace("%%HERO%%", f"{n_cells:,}")
    html = html.replace("%%CAPTION%%", caption)
    html = html.replace("%%STRIP%%", strip)
    html = html.replace("%%GRID%%", grid)
    html = html.replace("%%DATE%%", date.today().isoformat())
    return html


TEMPLATE = """\
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SAHA — Dataset Tally</title>
<style>
  :root {
    --paper:#fbfaf8; --ink:#211b2e; --muted:#8a8398; --line:#e9e5ee;
    --card:#ffffff; --eosin:#c9366b; --hematox:#4b3f7a; --track:#efeaf3;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --paper:#15111d; --ink:#ece8f2; --muted:#9a92ad; --line:#2a2338;
      --card:#1d1730; --eosin:#ef6aa3; --hematox:#b3a4e0; --track:#271f3a;
    }
  }
  * { box-sizing:border-box; }
  html { -webkit-text-size-adjust:100%; }
  body {
    margin:0; background:var(--paper); color:var(--ink);
    font:16px/1.55 system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  }
  .mono { font-family:ui-monospace,"SF Mono",Menlo,Consolas,monospace;
    font-variant-numeric:tabular-nums; letter-spacing:-.02em; }
  .wrap { max-width:920px; margin:0 auto; padding:clamp(1.5rem,4vw,3.5rem) 1.25rem 4rem; }

  /* Hero — cell count is the thesis, on its own row so nothing collides */
  .eyebrow { font-size:.72rem; font-weight:600; letter-spacing:.18em;
    text-transform:uppercase; color:var(--hematox); margin:0 0 1.5rem; }
  .hero {
    position:relative; border:1px solid var(--line); border-radius:16px;
    background:var(--card); padding:clamp(1.75rem,4vw,2.75rem);
    overflow:hidden; margin-bottom:2rem;
    /* faint field-of-view grid — the microscope's tiling */
    background-image:
      radial-gradient(circle, color-mix(in srgb, var(--hematox) 12%, transparent) 1px, transparent 1px);
    background-size:22px 22px;
  }
  .hero .big { display:block; margin:0; color:var(--eosin);
    font-size:clamp(3rem,13vw,6.5rem); line-height:.92; font-weight:600; }
  .hero .cap { display:block; margin-top:.9rem; color:var(--muted);
    font-size:clamp(.95rem,2.5vw,1.15rem); max-width:40ch; }
  .hero .cap sup { color:var(--eosin); font-weight:600; }
  .note { margin:1.75rem 0 0; color:var(--muted); font-size:.8rem; }
  .note sup { color:var(--eosin); font-weight:600; }

  /* Stat strip */
  .strip { display:grid; grid-template-columns:repeat(auto-fit,minmax(120px,1fr));
    border:1px solid var(--line); border-radius:12px; overflow:hidden;
    background:var(--card); margin-bottom:3rem; }
  .stat { padding:1.1rem 1.25rem; border-right:1px solid var(--line);
    display:flex; flex-direction:column; gap:.2rem; }
  .stat:last-child { border-right:0; }
  .stat .v { font-family:ui-monospace,"SF Mono",Menlo,Consolas,monospace;
    font-variant-numeric:tabular-nums; font-size:1.7rem; font-weight:600; }
  .stat .l { font-size:.78rem; color:var(--muted); text-transform:uppercase;
    letter-spacing:.06em; }

  /* Breakdowns */
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(300px,1fr));
    gap:2rem 3rem; }
  .block h3 { font-size:.72rem; font-weight:600; letter-spacing:.12em;
    text-transform:uppercase; color:var(--hematox); margin:0 0 .9rem;
    padding-bottom:.5rem; border-bottom:1px solid var(--line); }
  .bars { display:flex; flex-direction:column; gap:.55rem; }
  .row { display:grid; grid-template-columns:8.5rem 1fr 2.5rem; align-items:center;
    gap:.75rem; font-size:.9rem; }
  .row .k { color:var(--ink); white-space:nowrap; overflow:hidden;
    text-overflow:ellipsis; }
  .track { height:7px; background:var(--track); border-radius:4px; overflow:hidden; }
  .fill { display:block; height:100%; border-radius:4px;
    background:linear-gradient(90deg, var(--hematox), var(--eosin)); min-width:3px; }
  .row .n { text-align:right; color:var(--muted);
    font-family:ui-monospace,"SF Mono",Menlo,Consolas,monospace;
    font-variant-numeric:tabular-nums; }

  footer { margin-top:3.5rem; padding-top:1.25rem; border-top:1px solid var(--line);
    color:var(--muted); font-size:.8rem; }
  footer a { color:var(--eosin); text-decoration:none; }
  footer a:hover { text-decoration:underline; }
  @media (prefers-reduced-motion:no-preference) {
    .fill { animation:grow .9s cubic-bezier(.2,.7,.2,1) both; }
    @keyframes grow { from { transform:scaleX(0); transform-origin:left; } }
  }
</style>
</head>
<body>
<div class="wrap">
  <p class="eyebrow">SAHA · Spatial Atlas of Human Anatomy</p>

  <div class="hero">
    <span class="big mono">%%HERO%%</span>
    <span class="cap">%%CAPTION%%</span>
  </div>

  <div class="strip">%%STRIP%%</div>

  <div class="grid">%%GRID%%</div>

  <p class="note"><sup>*</sup> Cell count reflects CosMx segmentation only
    (RNA&nbsp;+&nbsp;protein runs). Xenium and other assays are not yet
    tallied and will be added.</p>

  <footer>
    Cohort as of %%DATE%%. Auto-generated from publication Tables 1 &amp; 2
    by <code>scripts/generate_stats.py</code> — re-run after updating the
    tables. · <a href="https://www.saha-project.org/">saha-project.org</a>
  </footer>
</div>
</body>
</html>
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables-dir", type=Path, default=DEFAULT_TABLES_DIR,
                    help="Directory holding Table_1.xlsx and Table_2.xlsx")
    ap.add_argument("--runs-csv", type=Path, default=DEFAULT_RUNS_CSV,
                    help="Run tracker CSV with CosMx cell counts")
    ap.add_argument("--output", type=Path, default=Path("docs/stats.html"))
    args = ap.parse_args()

    html = build(args.tables_dir, args.runs_csv)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
