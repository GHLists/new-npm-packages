#!/usr/bin/env python3
"""Fetch npm packages created between the previous list and now.

New packages are detected with the npm registry replication feed at
replicate.npmjs.com: every changed package is inspected through the registry
API and kept when its ``time.created`` timestamp falls inside the requested
window. The feed sequence of the last processed change and unresolved package
names are stored in the manifest so the next run can resume without losing
records.
"""

import argparse
import csv
import datetime as dt
import json
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

CHANGES_URL = "https://replicate.npmjs.com/registry/_changes"
PACKAGE_URL = "https://registry.npmjs.org/{name}"
DEFAULT_USER_AGENT = (
    "new-npm-packages/1.0 (https://github.com/GHLists/new-npm-packages)"
)

DESCRIPTION_LIMIT = 200
MAX_PENDING_ATTEMPTS = 5
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
    moment = moment.astimezone(dt.timezone.utc)
    if moment.microsecond:
        return moment.strftime("%Y-%m-%dT%H:%M:%S.%f").rstrip("0") + "Z"
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_timestamp(value):
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    moment = dt.datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=dt.timezone.utc)
    return moment.astimezone(dt.timezone.utc)


def timestamp_filename(moment):
    moment = moment.astimezone(dt.timezone.utc)
    stamp = moment.strftime("%Y-%m-%dT%H-%M-%S")
    if moment.microsecond:
        stamp += "-" + f"{moment.microsecond:06d}".rstrip("0")
    return stamp + "Z"


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


def random_page_size():
    """Pick a page size that differs between runs.

    The replication feed sits behind a CDN that caches responses by URL, so
    asking for the exact same sequence and page size twice can return a stale
    response after an earlier run failed.
    """
    return random.randint(1000, 5000)


def fetch_changes(start, page_size, user_agent, retries, max_pages=500):
    rows = []
    cursor = start
    for _ in range(max_pages):
        url = f"{CHANGES_URL}?since={cursor}&limit={page_size}"
        payload = fetch_json(url, user_agent, retries=retries)
        if not isinstance(payload, dict) or not isinstance(payload.get("results"), list):
            raise RuntimeError("registry change response has an invalid results field")
        results = payload["results"]
        if not results:
            return rows, cursor, True
        try:
            page_cursor = max(int(row["seq"]) for row in results)
        except (KeyError, TypeError, ValueError) as error:
            raise RuntimeError("registry change response contains an invalid seq") from error
        if page_cursor <= cursor:
            raise RuntimeError("registry change sequence did not advance")
        rows.extend(results)
        cursor = page_cursor
    return rows, cursor, False


def collect_candidates(rows):
    candidates = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError("registry change row must be an object")
        name = row.get("id")
        if row.get("deleted"):
            continue
        if not isinstance(name, str) or not name:
            raise RuntimeError("registry change row is missing its package name")
        if name in seen:
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
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADER)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def load_manifest(path):
    manifest_path = Path(path)
    try:
        text = manifest_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    except OSError as error:
        raise RuntimeError(f"could not read manifest {manifest_path}: {error}") from error
    try:
        data = json.loads(text)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"manifest {manifest_path} is not valid JSON") from error
    if not isinstance(data, dict):
        raise RuntimeError(f"manifest {manifest_path} must contain a JSON object")
    version = data.get("state_version", 1)
    if version != 1:
        raise RuntimeError(f"manifest {manifest_path} has an unsupported state version")
    return data


def save_manifest(path, manifest):
    manifest_path = Path(path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = manifest_path.with_name(f".{manifest_path.name}.tmp")
    text = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, manifest_path)


def load_pending(manifest):
    pending = {}
    raw_pending = manifest.get("pending", [])
    if not isinstance(raw_pending, list):
        raise RuntimeError("manifest pending must be a list")
    for item in raw_pending:
        if not isinstance(item, dict):
            raise RuntimeError("manifest pending entries must be objects")
        name = item.get("package")
        if not isinstance(name, str) or not name:
            raise RuntimeError("manifest pending entry has an invalid package")
        if "since" not in item:
            raise RuntimeError(f"manifest pending entry for {name} is missing since")
        if name in pending:
            raise RuntimeError(f"manifest contains duplicate pending package {name}")
        try:
            candidate_since = parse_timestamp(item["since"])
        except (TypeError, ValueError) as error:
            raise RuntimeError(
                f"manifest pending entry for {name} has an invalid since timestamp"
            ) from error
        attempts = item.get("attempts", 0)
        if not isinstance(attempts, int) or attempts < 0:
            raise RuntimeError(
                f"manifest pending entry for {name} has an invalid attempts count"
            )
        pending[name] = {
            "package": name,
            "since": candidate_since,
            "attempts": attempts,
        }
    return pending


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
        help="registry change sequence to resume from; requires --since",
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
    now = dt.datetime.now(dt.timezone.utc)
    until = parse_timestamp(args.until) if args.until else now
    manifest = load_manifest(args.manifest)

    if args.since_seq is not None and args.since is None:
        raise RuntimeError("--since-seq requires an explicit --since timestamp")
    if args.since:
        since = parse_timestamp(args.since)
        if "window" in manifest and args.since_seq is None:
            stored_window = parse_timestamp(manifest["window"])
            if since < stored_window:
                raise RuntimeError(
                    "timestamp-only backfill cannot move the registry cursor; "
                    "provide --since-seq"
                )
    elif "window" in manifest:
        since = parse_timestamp(manifest["window"])
    else:
        since = until - dt.timedelta(hours=args.lookback_hours)

    if args.since_seq is not None:
        cursor = args.since_seq
    elif "seq" in manifest:
        cursor = manifest["seq"]
    else:
        raise RuntimeError("no stored registry sequence; provide --since-seq")
    try:
        cursor = int(cursor)
    except (TypeError, ValueError) as error:
        raise RuntimeError("manifest contains an invalid registry sequence") from error
    if cursor < 0:
        raise RuntimeError("registry sequence cannot be negative")

    pending = load_pending(manifest)
    if since >= until:
        print(f"nothing to do ({iso(since)} >= {iso(until)})", file=sys.stderr)
        return 0

    page_size = random_page_size()
    changes, end_seq, exhausted = fetch_changes(
        cursor, page_size, args.user_agent, args.retries
    )
    new_candidates = collect_candidates(changes)
    for name in new_candidates:
        pending.setdefault(name, {"package": name, "since": since, "attempts": 0})
    print(
        f"scanned {len(changes)} changes in sequence {cursor}..{end_seq}; "
        f"{len(new_candidates)} new candidates and {len(pending)} pending candidates"
    )

    manifest["seq"] = end_seq
    manifest["source_truncated"] = not exhausted
    if not exhausted:
        manifest["window"] = iso(since)
        manifest["pending"] = [
            {
                "package": name,
                "since": iso(pending[name]["since"]),
                "attempts": pending[name]["attempts"],
            }
            for name in sorted(pending)
        ]
        save_manifest(args.manifest, manifest)
        print(
            "registry scan reached its page limit; candidates were persisted "
            "for the next run",
            file=sys.stderr,
        )
        return 0

    candidates = sorted(pending)
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        documents = list(
            executor.map(
                lambda name: fetch_package(name, args.user_agent, args.retries),
                candidates,
            )
        )

    rows = []
    next_pending = {}
    missing = 0
    invalid = 0
    deferred = 0
    dropped = 0
    for name, doc in zip(candidates, documents):
        candidate = pending[name]
        created = package_created(doc) if doc is not None else None
        if created is not None:
            if created <= candidate["since"]:
                continue
            if created <= until:
                rows.append(build_row(name, doc, created))
                continue
            deferred += 1
        elif doc is None:
            missing += 1
        else:
            invalid += 1
        candidate["attempts"] += 1
        if candidate["attempts"] < MAX_PENDING_ATTEMPTS:
            next_pending[name] = candidate
        else:
            dropped += 1
    rows.sort(key=lambda row: row["created_at"])
    if missing or invalid:
        print(
            f"kept {missing + invalid} packages pending after missing or invalid "
            "registry metadata",
            file=sys.stderr,
        )
    if deferred:
        print(f"deferred {deferred} packages created after {iso(until)}")
    if dropped:
        print(
            f"dropped {dropped} packages unresolved after {MAX_PENDING_ATTEMPTS} "
            "attempts",
            file=sys.stderr,
        )

    manifest["window"] = iso(until)
    manifest["source_truncated"] = False
    manifest["pending"] = [
        {
            "package": name,
            "since": iso(next_pending[name]["since"]),
            "attempts": next_pending[name]["attempts"],
        }
        for name in sorted(next_pending)
    ]
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
