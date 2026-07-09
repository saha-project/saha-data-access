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


def rows(d):
    total = sum(d.values()) or 1
    return "\n".join(
        f'<tr><td>{k}</td><td class="num">{v}</td>'
        f'<td class="bar"><span style="width:{100*v/total:.1f}%"></span></td></tr>'
        for k, v in d.items()
    )


def stat_card(label, value):
    return f'<div class="stat"><div class="v">{value}</div><div class="l">{label}</div></div>'


def table(title, d):
    return f'<section><h2>{title}</h2><table>{rows(d)}</table></section>'


def build(data_dir: Path) -> str:
    s = pd.read_parquet(data_dir / "samples.parquet")
    d = pd.read_parquet(data_dir / "donors.parquet")
    r = pd.read_parquet(data_dir / "runs.parquet")
    p = pd.read_parquet(data_dir / "panels.parquet")

    n_cells = int(s["n_cells"].sum())
    n_qc = int((s["qc_pass"] == True).sum())  # noqa: E712

    headline = "".join([
        stat_card("Samples", f"{len(s):,}"),
        stat_card("Cells profiled", f"{n_cells:,}"),
        stat_card("Donors", f"{len(d):,}"),
        stat_card("Acquisition runs", f"{len(r):,}"),
        stat_card("Panels", f"{len(p):,}"),
        stat_card("QC-passed samples", f"{n_qc:,}"),
    ])

    sections = "".join([
        table("Samples by platform", counts(s["platform"])),
        table("Samples by organ", counts(s["organ"])),
        table("Samples by generating institution", counts(s["generation_institution"])),
        table("Samples by condition", counts(s["condition"])),
        table("Samples by panel", counts(s["panel_name"])),
    ])

    return TEMPLATE.format(
        date=date.today().isoformat(),
        headline=headline,
        sections=sections,
    )


TEMPLATE = """\
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SAHA — Dataset Tally</title>
<style>
  :root {{
    --bg:#0d1117; --card:#161b22; --fg:#e6edf3; --muted:#8b949e;
    --accent:#3fb950; --border:#30363d;
  }}
  @media (prefers-color-scheme: light) {{
    :root {{ --bg:#ffffff; --card:#f6f8fa; --fg:#1f2328; --muted:#656d76;
             --accent:#1a7f37; --border:#d0d7de; }}
  }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; padding:2rem 1rem; background:var(--bg); color:var(--fg);
    font:16px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif; }}
  .wrap {{ max-width:820px; margin:0 auto; }}
  h1 {{ font-size:1.8rem; margin:0 0 .25rem; }}
  .sub {{ color:var(--muted); margin:0 0 2rem; font-size:.9rem; }}
  .stats {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr));
    gap:.75rem; margin-bottom:2.5rem; }}
  .stat {{ background:var(--card); border:1px solid var(--border); border-radius:10px;
    padding:1.1rem 1rem; }}
  .stat .v {{ font-size:1.9rem; font-weight:700; color:var(--accent); }}
  .stat .l {{ color:var(--muted); font-size:.85rem; margin-top:.2rem; }}
  section {{ margin-bottom:2rem; }}
  h2 {{ font-size:1.05rem; margin:0 0 .6rem; }}
  table {{ width:100%; border-collapse:collapse; }}
  td {{ padding:.35rem .5rem; border-bottom:1px solid var(--border); }}
  td.num {{ text-align:right; font-variant-numeric:tabular-nums; width:4rem;
    color:var(--muted); }}
  td.bar {{ width:45%; }}
  td.bar span {{ display:block; height:8px; border-radius:4px; background:var(--accent);
    opacity:.7; min-width:2px; }}
  a {{ color:var(--accent); }}
  footer {{ color:var(--muted); font-size:.8rem; margin-top:2.5rem;
    border-top:1px solid var(--border); padding-top:1rem; }}
</style>
</head>
<body>
<div class="wrap">
  <h1>SAHA — Dataset Tally</h1>
  <p class="sub">Spatial Atlas of Human Anatomy · current release contents · updated {date}</p>
  <div class="stats">{headline}</div>
  {sections}
  <footer>
    Auto-generated from <code>data/*.parquet</code> by
    <code>scripts/generate_stats.py</code>. Re-run after updating metadata.
    · <a href="https://www.saha-project.org/">saha-project.org</a>
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
