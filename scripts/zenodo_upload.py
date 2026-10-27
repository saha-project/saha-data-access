#!/usr/bin/env python3
"""
zenodo_upload.py
Create Zenodo draft deposits, reserve DOIs, and upload the packaged records.

Drafts are NOT published. Publishing is a separate, explicit act (--publish).
Records are published with an embargo on files (access in record_metadata.json):
metadata and DOIs go public, files open on embargo.until. The date can be edited
after publishing -- extend it, or make files public on acceptance. Files cannot be
replaced after publishing; corrections go in a new version.

Flow per record directory:
    1. create a draft            POST /api/records
    2. reserve its DOI           POST {draft}/pids/doi
    3. upload each file          POST/PUT/POST {draft}/files...
    4. link the parent draft to every child DOI (haspart)
    5. stop. Review in the web UI, then publish when ready.

State is written to <out-dir>/.zenodo_state.json so re-runs resume instead of
creating duplicate drafts.

Usage:
    export ZENODO_TOKEN=...            # sandbox and production tokens differ

    # always rehearse on sandbox first — free, throwaway DOIs
    python scripts/zenodo_upload.py --sandbox \
        --records-dir ~/SAHA_zenodo --metadata zenodo/record_metadata.json

    # one record, to check the shape before committing 97 GB
    python scripts/zenodo_upload.py --sandbox \
        --records-dir ~/SAHA_zenodo --metadata zenodo/record_metadata.json \
        --only cosmx-protein

    # production drafts
    python scripts/zenodo_upload.py \
        --records-dir ~/SAHA_zenodo --metadata zenodo/record_metadata.json

Requirements:
    pip install requests markdown
"""

import argparse
import json
import os
import sys
from pathlib import Path

import markdown
import requests

PROD = "https://zenodo.org"
SANDBOX = "https://sandbox.zenodo.org"

# Order matters only for readability in the web UI.
DOC_FILES = ["README.md", "file_manifest.csv", "MD5SUMS.txt"]


class Zenodo:
    def __init__(self, base: str, token: str):
        self.base = base.rstrip("/")
        self.s = requests.Session()
        self.s.headers.update({"Authorization": f"Bearer {token}"})

    def _check(self, r, what: str):
        if r.status_code >= 400:
            try:
                detail = json.dumps(r.json(), indent=2)[:1500]
            except Exception:
                detail = r.text[:1500]
            sys.exit(f"ERROR: {what} failed ({r.status_code})\n{detail}")
        return r

    def whoami(self) -> None:
        r = self.s.get(f"{self.base}/api/user/records", params={"size": 1})
        if r.status_code == 403:
            sys.exit("ERROR: token rejected. Sandbox and production tokens are "
                     "separate — check which one ZENODO_TOKEN holds.")
        self._check(r, "token check")

    def create_draft(self, metadata: dict) -> dict:
        r = self.s.post(f"{self.base}/api/records", json=metadata)
        return self._check(r, "create draft").json()

    def reserve_doi(self, draft: dict) -> str:
        link = draft["links"]["self"] + "/pids/doi"
        r = self.s.post(link)
        if r.status_code == 400 and "already" in r.text.lower():
            return draft.get("pids", {}).get("doi", {}).get("identifier", "")
        out = self._check(r, "reserve DOI").json()
        return out.get("pids", {}).get("doi", {}).get("identifier", "")

    def existing_files(self, draft: dict) -> dict:
        r = self.s.get(draft["links"]["files"])
        if r.status_code >= 400:
            return {}
        return {e["key"]: e for e in r.json().get("entries", [])}

    def upload_file(self, draft: dict, path: Path) -> None:
        files_link = draft["links"]["files"]
        key = path.name

        self._check(self.s.post(files_link, json=[{"key": key}]),
                    f"declare {key}")
        # Stream from disk — these archives run to tens of GB.
        with open(path, "rb") as fh:
            self._check(
                self.s.put(f"{files_link}/{key}/content", data=fh,
                           headers={"Content-Type": "application/octet-stream"}),
                f"upload {key}",
            )
        self._check(self.s.post(f"{files_link}/{key}/commit"), f"commit {key}")

    def update_draft(self, draft_url: str, mutate) -> None:
        cur = self._check(self.s.get(draft_url), "read draft").json()
        mutate(cur)
        self._check(self.s.put(draft_url, json=cur), "update draft")

    def publish(self, draft: dict) -> dict:
        r = self.s.post(draft["links"]["publish"])
        return self._check(r, "publish").json()


def build_metadata(spec: dict, slug: str, parent_doi: str = "") -> dict:
    """Assemble the API payload for one record from record_metadata.json."""
    parent = spec["parent"]
    is_parent = slug == "parent-collection"

    meta = {k: v for k, v in parent["metadata"].items()
            if not k.startswith("_") and k != "description"}

    if is_parent:
        meta["title"] = parent["metadata"]["title"]
    else:
        child = next(c for c in spec["_proposed_split"]["children"]
                     if c["slug"] == slug)
        meta["title"] = child["title"]
    meta["description"] = describe(slug, spec, parent_doi)

    if not is_parent:
        rel = [r for r in meta.get("related_identifiers", [])
               if not r.get("_comment") and r["relation_type"]["id"] != "haspart"]
        if parent_doi:
            rel.append({
                "identifier": parent_doi, "scheme": "doi",
                "relation_type": {"id": "ispartof"},
                "resource_type": {"id": "dataset"},
            })
        meta["related_identifiers"] = rel
    else:
        meta["related_identifiers"] = [
            r for r in meta.get("related_identifiers", [])
            if not r.get("_comment")
        ]

    meta["related_identifiers"] = [
        {k: v for k, v in r.items() if not k.startswith("_")}
        for r in meta["related_identifiers"]
        if "TODO" not in json.dumps(r)
    ]
    meta["funding"] = [f for f in meta.get("funding", [])
                       if "TODO" not in json.dumps(f)]
    if not meta["funding"]:
        meta.pop("funding")
    meta.pop("dates", None)

    # Files restricted under an embargo; metadata public once published.
    access = spec["parent" if is_parent else "child_template"]["access"]
    return {"access": access, "metadata": meta}


DESCRIPTION_MD = Path(__file__).resolve().parent.parent / "zenodo" / "DESCRIPTION.md"


def describe(slug: str, spec: dict, parent_doi: str) -> str:
    """Render the PARENT or CHILD section of zenodo/DESCRIPTION.md to HTML."""
    text = DESCRIPTION_MD.read_text()
    parent_part, child_part = text.split("## PARENT", 1)[1].split("## CHILD", 1)
    if slug == "parent-collection":
        body = parent_part.split("\n", 1)[1].rstrip().removesuffix("---")
    else:
        c = next(c for c in spec["_proposed_split"]["children"] if c["slug"] == slug)
        link = (f"[{parent_doi}](https://doi.org/{parent_doi})" if parent_doi
                else "see *Related works*")
        body = child_part.split("\n", 1)[1].format(
            N_SAMPLES=c["n_samples"], PLATFORM=" + ".join({"cosmx": "CosMx", "xenium": "Xenium"}[p] for p in c["platform"].split("+")),
            ORGAN_GROUP=c["organ_group"].replace("-", " "),
            N_CELLS=f"{c['n_cells']:,}", N_DONOR_CORES=c["n_donor_cores"],
            MEASURED_GB=c["measured_gb"], PARENT_LINK=link)
    return markdown.markdown(body, extensions=["tables", "fenced_code"])


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--records-dir", type=Path, required=True)
    p.add_argument("--metadata", type=Path, required=True)
    p.add_argument("--sandbox", action="store_true",
                   help="use sandbox.zenodo.org — always rehearse here first")
    p.add_argument("--only", nargs="*", help="restrict to these record slugs")
    p.add_argument("--publish", action="store_true",
                   help="IRREVERSIBLE. Publishes the drafts. Files become "
                        "uneditable 45 days later. Only after acceptance.")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    token = os.environ.get("ZENODO_TOKEN")
    if not token and not args.dry_run:
        sys.exit("ERROR: set ZENODO_TOKEN (sandbox and production tokens differ)")

    spec = json.loads(args.metadata.read_text())
    base = SANDBOX if args.sandbox else PROD

    slugs = sorted(d.name for d in args.records_dir.iterdir()
                   if d.is_dir() and not d.name.startswith("."))
    # Parent first, so children can reference its DOI.
    slugs.sort(key=lambda s: (s != "parent-collection", s))
    if args.only:
        slugs = [s for s in slugs if s in args.only]
    if not slugs:
        sys.exit(f"ERROR: no record directories in {args.records_dir}")

    state_path = args.records_dir / ".zenodo_state.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    env_state = state.setdefault("sandbox" if args.sandbox else "production", {})

    print(f"target: {base}")
    print(f"records: {', '.join(slugs)}\n")

    if args.dry_run:
        for slug in slugs:
            md = build_metadata(spec, slug)
            files = sorted(p for p in (args.records_dir / slug).iterdir()
                           if p.is_file())
            gb = sum(f.stat().st_size for f in files) / 1e9
            print(f"[dry-run] {slug}: {md['metadata']['title']}")
            print(f"          {len(files)} files, {gb:.1f} GB, "
                  f"access={md['access']}")
        print("\n[dry-run] nothing sent")
        return

    z = Zenodo(base, token)
    z.whoami()

    parent_doi = env_state.get("parent-collection", {}).get("doi", "")

    for slug in slugs:
        rec_dir = args.records_dir / slug
        print(f"\n=== {slug} ===")
        entry = env_state.setdefault(slug, {})

        if entry.get("draft_url"):
            r = z.s.get(entry["draft_url"])
            draft = r.json() if r.status_code < 400 else None
            if draft:
                print(f"  resuming existing draft {entry.get('doi', '')}")
        else:
            draft = None

        if draft is None:
            md = build_metadata(spec, slug, parent_doi)
            draft = z.create_draft(md)
            entry["draft_url"] = draft["links"]["self"]
            entry["html"] = draft["links"].get("self_html", "")
            entry["doi"] = z.reserve_doi(draft)
            print(f"  created draft, DOI reserved: {entry['doi']}")
            state_path.write_text(json.dumps(state, indent=2))

        if slug == "parent-collection":
            parent_doi = entry.get("doi", "")

        done = z.existing_files(draft)
        files = sorted(p for p in rec_dir.iterdir() if p.is_file())
        # Docs first so a partially uploaded record still reads sensibly.
        files.sort(key=lambda p: (p.name not in DOC_FILES, p.name))

        for f in files:
            if f.name in done and done[f.name].get("status") == "completed":
                print(f"  [skip] {f.name}")
                continue
            gb = f.stat().st_size / 1e9
            print(f"  uploading {f.name} ({gb:.2f} GB) …", flush=True)
            z.upload_file(draft, f)
            entry.setdefault("uploaded", []).append(f.name)
            state_path.write_text(json.dumps(state, indent=2))

        print(f"  draft ready: {entry.get('html') or entry['draft_url']}")

        if args.publish:
            out = z.publish(draft)
            entry["published"] = out["links"].get("self_html", "")
            print(f"  PUBLISHED: {entry['published']}")
            state_path.write_text(json.dumps(state, indent=2))

    # The parent draft is created first, so it learns the child DOIs only now.
    pe = env_state.get("parent-collection", {})
    kids = [env_state[s]["doi"] for s in sorted(env_state)
            if s != "parent-collection" and env_state[s].get("doi")]
    if pe.get("draft_url") and kids and not args.publish:
        def add_children(rec):
            rel = [r for r in rec["metadata"].get("related_identifiers", [])
                   if r.get("relation_type", {}).get("id") != "haspart"]
            rel += [{"identifier": k, "scheme": "doi",
                     "relation_type": {"id": "haspart"},
                     "resource_type": {"id": "dataset"}} for k in kids]
            rec["metadata"]["related_identifiers"] = rel
        z.update_draft(pe["draft_url"], add_children)
        print(f"\nparent linked to {len(kids)} child DOIs")

    state_path.write_text(json.dumps(state, indent=2))
    print(f"\n{'=' * 60}")
    print(f"state: {state_path}")
    for slug in slugs:
        e = env_state.get(slug, {})
        print(f"  {slug:22s} {e.get('doi', '—')}")
    if not args.publish:
        print("\nDrafts are NOT published. Review them in the web UI, add them to "
              "the SAHA community, then publish on acceptance.")


if __name__ == "__main__":
    main()
