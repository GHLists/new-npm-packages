#!/usr/bin/env python3
"""Fetch npm packages created between the previous list and now.

New packages are detected with the npm registry replication feed at
replicate.npmjs.com: every package whose document only has a handful of
revisions is inspected through the registry API and kept when its
``time.created`` timestamp falls inside the requested window. The feed
sequence of the last processed change is stored in the manifest so the next
run can resume exactly where the previous one stopped.
"""

import argparse
import csv
import datetime as dt
import json
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

CHANGES_URL = "https://replicate.npmjs.com/registry/_changes"
INFO_URL = "https://replicate.npmjs.com/registry/"
PACKAGE_URL = "https://registry.npmjs.org/{name}"
DEFAULT_USER_AGENT = (
    "new-npm-packages/1.0 (https://github.com/GHLists/new-npm-packages)"
)

DESCRIPTION_LIMIT = 200
CSV_HEADER = (
    "created_at",
    "package",
    "version",
    "publisher",
    "license",
    "unpacked_size",
    "description",
)


class NotFound(Exception):
    pass


def iso(moment):
    return moment.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_timestamp(value):
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    moment = dt.datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=dt.timezone.utc)
    return moment.astimezone(dt.timezone.utc).replace(microsecond=0)


def timestamp_filename(moment):
    return moment.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")


def fetch_json(url, user_agent, retries=3, backoff=5.0):
    last_error = None
    for attempt in range(1, retries + 1):
        request = urllib.request.Request(
            url,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            if error.code == 404:
                raise NotFound(url) from error
            last_error = error
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
            last_error = error
        if attempt < retries:
            print(f"attempt {attempt} failed ({last_error}), retrying", file=sys.stderr)
            time.sleep(backoff * attempt)
    raise RuntimeError(f"failed to fetch {url}: {last_error}")


def fetch_latest_seq(user_agent, retries):
    payload = fetch_json(INFO_URL, user_agent, retries=retries)
    return int(payload["update_seq"])


def random_page_size():
    """Pick a page size that differs between runs.

    The replication feed sits behind a CDN that caches responses by URL, so
    asking for the exact same sequence and page size twice can return a stale
    response after an earlier run failed.
    """
    return random.randint(1000, 5000)


def fetch_changes(start, page_size, user_agent, retries, max_pages=500):
    rows = []
    since = start
    for _ in range(max_pages):
        url = f"{CHANGES_URL}?since={since}&limit={page_size}"
        payload = fetch_json(url, user_agent, retries=retries)
        results = payload.get("results") or []
        if not results:
            break
        rows.extend(results)
        since = int(payload.get("last_seq", since))
        if len(results) < page_size:
            break
    return rows, since


def revision_generation(change):
    revisions = change.get("changes") or []
    if not revisions:
        return None
    head = str(revisions[0].get("rev") or "").partition("-")[0]
    return int(head) if head.isdigit() else None


def collect_candidates(rows, max_revision):
    candidates = []
    seen = set()
    for row in rows:
        name = row.get("id")
        if not name or name in seen or row.get("deleted"):
            continue
        generation = revision_generation(row)
        if generation is None or generation > max_revision:
            continue
        seen.add(name)
        candidates.append(name)
    return candidates


def fetch_package(name, user_agent, retries):
    url = PACKAGE_URL.format(name=urllib.parse.quote(name, safe="@"))
    try:
        return fetch_json(url, user_agent, retries=retries)
    except NotFound:
        return None


def package_created(doc):
    created = (doc.get("time") or {}).get("created")
    if not created:
        return None
    try:
        return parse_timestamp(created)
    except ValueError:
        return None


def first_version(doc):
    times = doc.get("time") or {}
    versions = doc.get("versions") or {}
    ordered = sorted(
        (version for version in versions if version in times),
        key=lambda version: times[version],
    )
    return ordered[0] if ordered else None


def clean_text(value, limit=DESCRIPTION_LIMIT):
    text = " ".join(str(value or "").split())
    if len(text) > limit:
        text = text[: limit - 1].rstrip() + "\u2026"
    return text


def build_row(name, doc, created):
    versions = doc.get("versions") or {}
    first = first_version(doc)
    latest = (doc.get("dist-tags") or {}).get("latest")
    if latest not in versions:
        latest = first
    latest_doc = versions.get(latest) or {}
    first_doc = versions.get(first) or {}
    publisher = (first_doc.get("_npmUser") or {}).get("name") or ""
    if not publisher:
        maintainers = doc.get("maintainers") or []
        if maintainers:
            publisher = maintainers[0].get("name") or ""
    license_name = latest_doc.get("license") or doc.get("license") or ""
    if isinstance(license_name, dict):
        license_name = license_name.get("type") or ""
    unpacked = (latest_doc.get("dist") or {}).get("unpackedSize")
    return {
        "created_at": iso(created),
        "package": name,
        "version": latest or "",
        "publisher": publisher,
        "license": clean_text(license_name, 100),
        "unpacked_size": unpacked if isinstance(unpacked, int) else "",
        "description": clean_text(
            doc.get("description") or latest_doc.get("description")
        ),
    }


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADER)
        writer.writeheader()
        writer.writerows(rows)


def load_manifest(path):
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def save_manifest(path, manifest):
    text = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    Path(path).write_text(text, encoding="utf-8")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--since",
        help="UTC start timestamp as ISO 8601 (default: end of the last list)",
    )
    parser.add_argument(
        "--until",
        help="UTC end timestamp as ISO 8601 (default: now)",
    )
    parser.add_argument(
        "--since-seq",
        type=int,
        help="registry change sequence to resume from (default: stored in the manifest)",
    )
    parser.add_argument(
        "--max-revision",
        type=int,
        default=8,
        help="ignore documents with more revisions than this (default: 8)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="parallel package metadata requests (default: 8)",
    )
    parser.add_argument("--output-dir", default="data")
    parser.add_argument("--manifest", default="latest.json")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument(
        "--lookback-hours",
        type=float,
        default=1.0,
        help="window length when no previous list exists (default: 1)",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    until = parse_timestamp(args.until) if args.until else now
    manifest = load_manifest(args.manifest)

    if args.since:
        since = parse_timestamp(args.since)
    else:
        try:
            since = parse_timestamp(manifest["window"])
        except (KeyError, TypeError, ValueError):
            since = until - dt.timedelta(hours=args.lookback_hours)
    if since >= until:
        print(f"nothing to do ({iso(since)} >= {iso(until)})", file=sys.stderr)
        return 0

    cursor = args.since_seq if args.since_seq is not None else manifest.get("seq")
    try:
        cursor = int(cursor)
    except (TypeError, ValueError):
        cursor = None
    if cursor is None:
        cursor = fetch_latest_seq(args.user_agent, args.retries)
        print(f"no stored sequence; starting at registry update_seq {cursor}")

    page_size = random_page_size()
    changes, end_seq = fetch_changes(cursor, page_size, args.user_agent, args.retries)
    candidates = collect_candidates(changes, args.max_revision)
    print(
        f"scanned {len(changes)} changes in sequence {cursor}..{end_seq}; "
        f"{len(candidates)} packages to inspect"
    )

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        documents = list(
            executor.map(
                lambda name: fetch_package(name, args.user_agent, args.retries),
                candidates,
            )
        )

    rows = []
    missing = 0
    for name, doc in zip(candidates, documents):
        if doc is None:
            missing += 1
            continue
        created = package_created(doc)
        if created is None or created <= since:
            continue
        if args.until and created > until:
            continue
        rows.append(build_row(name, doc, created))
    rows.sort(key=lambda row: row["created_at"])
    if missing:
        print(
            f"skipped {missing} packages without registry metadata", file=sys.stderr
        )

    manifest["seq"] = end_seq
    manifest["window"] = iso(until)
    if rows:
        output = Path(args.output_dir) / f"new-packages-{timestamp_filename(until)}.csv"
        write_csv(output, rows)
        manifest["list"] = {
            "path": output.as_posix(),
            "from": iso(since),
            "to": iso(until),
            "count": len(rows),
        }
        print(
            f"wrote {len(rows)} packages created between {iso(since)} "
            f"and {iso(until)} to {output}"
        )
    else:
        print(f"no new packages between {iso(since)} and {iso(until)}")
    save_manifest(args.manifest, manifest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
