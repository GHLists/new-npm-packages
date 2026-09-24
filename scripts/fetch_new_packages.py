#!/usr/bin/env python3
"""Fetch npm packages created between the previous list and now.

New packages are detected with the npm registry replication feed at
replicate.npmjs.com: every changed package is inspected through the registry
API and kept when its ``time.created`` timestamp falls inside the requested
window. The feed sequence of the last processed change and unresolved package
names are stored in the manifest so the next run can resume without losing
records.

Because the feed reports every mutation, most changed packages are existing
packages publishing a new version. Their names are remembered in a local
SQLite store so later scans only inspect packages that were never seen
before; a brand-new package always shows up in the feed as its first change.
Backfills with an explicit ``--since-seq`` still inspect every changed
package so that re-listing an old window is never incomplete.
"""

import argparse
import csv
import datetime as dt
import http.client
import json
import os
import random
import sqlite3
import subprocess
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
DEFAULT_STATE_DB = "~/.cache/new-npm-packages/seen-packages.sqlite3"

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


TRANSIENT_ERRORS = (
    urllib.error.URLError,
    TimeoutError,
    json.JSONDecodeError,
    http.client.HTTPException,
    OSError,
)


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
        except TRANSIENT_ERRORS as error:
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
    """Return (created, row) without keeping the registry document.

    Registry documents can be several megabytes each, so the document is
    reduced to the small CSV row inside the worker thread instead of being
    collected for every candidate in the main thread.
    """
    url = PACKAGE_URL.format(name=urllib.parse.quote(name, safe="@"))
    try:
        document = fetch_json(url, user_agent, retries=retries)
    except NotFound:
        return None, None
    created = package_created(document)
    if created is None:
        return None, None
    return created, build_row(name, document, created)


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


def read_manifest_text(path):
    """Read the manifest from disk, or fall back to the committed copy.

    The workflow checks out only ``scripts`` from the repository, so the
    manifest can be missing from the working tree even though it is committed.
    """
    manifest_path = Path(path)
    try:
        return manifest_path.read_text(encoding="utf-8")
    except OSError:
        pass
    try:
        result = subprocess.run(
            ["git", "show", f"HEAD:{manifest_path.as_posix()}"],
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout


def load_manifest(path):
    text = read_manifest_text(path)
    if text is None:
        return {}
    try:
        data = json.loads(text)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"manifest {path} is not valid JSON") from error
    if not isinstance(data, dict):
        raise RuntimeError(f"manifest {path} must contain a JSON object")
    version = data.get("state_version", 1)
    if version != 1:
        raise RuntimeError(f"manifest {path} has an unsupported state version")
    return data


def save_manifest(path, manifest):
    manifest_path = Path(path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = manifest_path.with_name(f".{manifest_path.name}.tmp")
    text = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, manifest_path)


def serialize_pending(pending):
    return [
        {
            "package": name,
            "since": iso(pending[name]["since"]),
            "attempts": pending[name]["attempts"],
        }
        for name in sorted(pending)
    ]


def open_seen_store(path):
    """Open the local store of package names inspected by earlier scans."""
    store_path = Path(path).expanduser()
    store_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(store_path)
    connection.execute(
        "CREATE TABLE IF NOT EXISTS packages (name TEXT PRIMARY KEY) WITHOUT ROWID"
    )
    connection.commit()
    return connection


def select_unseen(connection, names):
    """Return the subset of names that earlier scans have not inspected."""
    unseen = []
    for start in range(0, len(names), 500):
        chunk = names[start : start + 500]
        placeholders = ",".join("?" * len(chunk))
        rows = connection.execute(
            f"SELECT name FROM packages WHERE name IN ({placeholders})", chunk
        )
        seen = {row[0] for row in rows}
        unseen.extend(name for name in chunk if name not in seen)
    return unseen


def remember_seen(connection, names):
    connection.executemany(
        "INSERT OR IGNORE INTO packages (name) VALUES (?)",
        ((name,) for name in names),
    )
    connection.commit()


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
    parser.add_argument(
        "--state-db",
        default=DEFAULT_STATE_DB,
        help="SQLite store of package names seen by earlier scans "
        "(default: ~/.cache/new-npm-packages/seen-packages.sqlite3)",
    )
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

    seen_store = open_seen_store(args.state_db)
    page_size = random_page_size()
    changes, end_seq, exhausted = fetch_changes(
        cursor, page_size, args.user_agent, args.retries
    )
    changed = collect_candidates(changes)
    if args.since_seq is not None:
        unseen = changed
    else:
        unseen = select_unseen(seen_store, changed)
    for name in unseen:
        pending.setdefault(name, {"package": name, "since": since, "attempts": 0})
    print(
        f"scanned {len(changes)} changes in sequence {cursor}..{end_seq}; "
        f"{len(changed)} changed packages, {len(unseen)} unseen; "
        f"{len(pending)} pending candidates"
    )

    manifest["seq"] = end_seq
    manifest["source_truncated"] = not exhausted
    manifest["pending"] = serialize_pending(pending)
    if not exhausted:
        manifest["window"] = iso(since)
        save_manifest(args.manifest, manifest)
        print(
            "registry scan reached its page limit; candidates were persisted "
            "for the next run",
            file=sys.stderr,
        )
        seen_store.close()
        return 0

    save_manifest(args.manifest, manifest)

    candidates = sorted(pending)
    rows = []
    next_pending = {}
    resolved = []
    unresolved = 0
    deferred = 0
    dropped = 0
    total = len(candidates)
    executor = ThreadPoolExecutor(max_workers=max(1, args.workers))
    try:
        results = executor.map(
            lambda name: fetch_package(name, args.user_agent, args.retries),
            candidates,
        )
        for done, (name, (created, row)) in enumerate(zip(candidates, results), 1):
            if done % 1000 == 0:
                print(f"resolved {done}/{total} packages", file=sys.stderr)
                remember_seen(seen_store, resolved)
                resolved.clear()
            candidate = pending[name]
            if created is None:
                unresolved += 1
            else:
                resolved.append(name)
                if created <= candidate["since"]:
                    continue
                if created <= until:
                    rows.append(row)
                    continue
                deferred += 1
            candidate["attempts"] += 1
            if candidate["attempts"] < MAX_PENDING_ATTEMPTS:
                next_pending[name] = candidate
            else:
                dropped += 1
    finally:
        executor.shutdown(wait=True, cancel_futures=True)
        remember_seen(seen_store, resolved)
        seen_store.close()
    rows.sort(key=lambda row: row["created_at"])
    if unresolved:
        print(
            f"kept {unresolved} packages pending after missing or invalid "
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
    manifest["pending"] = serialize_pending(next_pending)
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
