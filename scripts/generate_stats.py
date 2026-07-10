#!/usr/bin/env python3
"""
generate_stats.py
Generate docs/stats.html — a self-contained tally of current SAHA numbers
(sample counts, cell counts, breakdowns by platform / organ / institution)
read live from the four Parquet metadata tables.

Re-run whenever the metadata changes to refresh the page:
    python scripts/generate_stats.py
"""

import argparse
from datetime import date
from pathlib import Path

import pandas as pd


def counts(series):
    """Ordered {value: count} dict, most common first."""
    return series.value_counts().to_dict()


def stat(label, value):
    return f'<div class="stat"><span class="v">{value}</span><span class="l">{label}</span></div>'


def bars(d):
    mx = max(d.values()) or 1
    out = []
    for k, v in d.items():
        out.append(
            f'<div class="row"><span class="k">{k}</span>'
            f'<span class="track"><span class="fill" style="width:{100*v/mx:.1f}%"></span></span>'
            f'<span class="n">{v}</span></div>'
        )
    return "\n".join(out)


def block(title, d):
    return f'<section class="block"><h3>{title}</h3><div class="bars">{bars(d)}</div></section>'


def build(data_dir: Path) -> str:
    s = pd.read_parquet(data_dir / "samples.parquet")
    d = pd.read_parquet(data_dir / "donors.parquet")
    r = pd.read_parquet(data_dir / "runs.parquet")
    p = pd.read_parquet(data_dir / "panels.parquet")

    n_cells = int(s["n_cells"].sum())
    n_qc = int((s["qc_pass"] == True).sum())  # noqa: E712

    caption = (
        f"cells profiled across {len(s)} samples · {len(d)} donors · "
        f"{s['organ'].nunique()} organs · {s['platform'].nunique()} platforms"
    )

    strip = "".join([
        stat("samples", len(s)),
        stat("donors", len(d)),
        stat("runs", len(r)),
        stat("panels", len(p)),
        stat("QC&#8209;passed", n_qc),
    ])

    grid = "".join([
        block("By platform", counts(s["platform"])),
        block("By organ", counts(s["organ"])),
        block("By generating institution", counts(s["generation_institution"])),
        block("By condition", counts(s["condition"])),
        block("By panel", counts(s["panel_name"])),
    ])

    html = TEMPLATE
    html = html.replace("%%CELLS%%", f"{n_cells:,}")
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
    font-size:clamp(.95rem,2.5vw,1.15rem); max-width:34ch; }

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
    <span class="big mono">%%CELLS%%</span>
    <span class="cap">%%CAPTION%%</span>
  </div>

  <div class="strip">%%STRIP%%</div>

  <div class="grid">%%GRID%%</div>

  <footer>
    Current release contents · updated %%DATE%%. Auto-generated from
    <code>data/*.parquet</code> by <code>scripts/generate_stats.py</code> —
    re-run after updating metadata. ·
    <a href="https://www.saha-project.org/">saha-project.org</a>
  </footer>
</div>
</body>
</html>
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--output", type=Path, default=Path("docs/stats.html"))
    args = ap.parse_args()

    html = build(args.data_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
