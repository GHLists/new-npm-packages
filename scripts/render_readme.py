#!/usr/bin/env python3
"""Render a README with the latest npm package list."""

import argparse
import csv
import datetime as dt
import json
import subprocess
import sys
import urllib.parse
from pathlib import Path

INTRO = """\
# New npm Packages

Hourly lists of packages newly published to the [npm registry](https://www.npmjs.com/),
taken from the [registry replication feed](https://github.com/npm/registry/blob/main/docs/REPLICATE-API.md).
A GitHub Actions workflow runs every hour, fetches the packages published since
the previous list and commits one CSV per run to [`data/`](data/), e.g.
[`data/new-packages-<timestamp>.csv`](data/).

Read the latest list below.
"""

SECTION = """\
## Latest list \u2014 {end}

New packages published between {start} and {end}.

[Full CSV]({csv_path})

{body}
"""

TABLE_HEADER = """\
| Created (UTC) | Package | Version | Publisher | Size | Description |
| :------------ | :------ | :------ | :-------- | ---: | :---------- |"""


def parse_iso(value):
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(text)
    except ValueError:
        return None


def display_timestamp(value):
    moment = parse_iso(value)
    if moment is None:
        return str(value)
    return moment.strftime("%Y-%m-%d %H:%M UTC")


def display_time(value):
    moment = parse_iso(value)
    if moment is None:
        return str(value)
    return moment.strftime("%Y-%m-%d %H:%M:%S")


def display_size(value):
    try:
        size = int(value)
    except (TypeError, ValueError):
        return ""
    if size < 1000:
        return f"{size} B"
    if size < 1000 * 1000:
        return f"{size / 1000:.1f} kB"
    return f"{size / (1000 * 1000):.1f} MB"


def clean_cell(value, limit=80):
    text = " ".join(str(value or "").split())
    if len(text) > limit:
        text = text[: limit - 1].rstrip() + "\u2026"
    return text.replace("|", "\\|")


def package_link(name):
    url = "https://www.npmjs.com/package/" + urllib.parse.quote(name, safe="@/")
    return f"[{name}]({url})"


def publisher_link(publisher):
    if not publisher:
        return ""
    url = "https://www.npmjs.com/~" + urllib.parse.quote(publisher)
    label = str(publisher).replace("|", "\\|")
    return f"[{label}]({url})"


def read_csv_text(path):
    file = Path(path)
    if file.exists():
        return file.read_text(encoding="utf-8")
    try:
        result = subprocess.run(
            ["git", "show", f"HEAD:{path}"],
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout


def render_rows(rows):
    return "\n".join(
        f"| {display_time(row.get('created_at'))} | {package_link(row.get('package', ''))} "
        f"| {clean_cell(row.get('version'), 20)} | {publisher_link(row.get('publisher'))} "
        f"| {display_size(row.get('unpacked_size'))} | {clean_cell(row.get('description'))} |"
        for row in rows
    )


def load_manifest(path):
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def render_section(entry, rows, limit):
    path = entry.get("path")
    if rows is None:
        body = "_The latest CSV could not be read; open it for the full list._"
    elif not rows:
        body = "_No packages were published in this window._"
    else:
        body = TABLE_HEADER + "\n" + render_rows(rows[:limit])
        if len(rows) > limit:
            body += (
                f"\n\n_Showing the first {limit:,} of {len(rows):,} packages; "
                f"see the [full CSV]({path})._"
            )
    return SECTION.format(
        end=display_timestamp(entry.get("to")),
        start=display_timestamp(entry.get("from")),
        csv_path=path,
        body=body,
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", default="latest.json")
    parser.add_argument("--output", default="README.md")
    parser.add_argument("--limit", type=int, default=200)
    args = parser.parse_args(argv)

    manifest = load_manifest(args.manifest)
    entry = manifest.get("list")
    if not isinstance(entry, dict):
        entry = None
    content = INTRO + "\n"
    if entry and entry.get("path"):
        text = read_csv_text(entry["path"])
        rows = list(csv.DictReader(text.splitlines())) if text is not None else None
        content += render_section(entry, rows, args.limit)
    else:
        content += "_No list has been generated yet._\n"
        print(
            "no list found in the manifest; rendering an empty README",
            file=sys.stderr,
        )

    Path(args.output).write_text(content, encoding="utf-8")
    print(f"wrote README to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
