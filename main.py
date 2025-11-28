import os
import sys
import csv
import json
import argparse
import requests
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

DEFAULT_TIMEOUT = 15.0


def is_valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def resolve_filepath(url: str, out_folder: str, preserve_path: bool) -> str:
    parsed = urlparse(url)
    if preserve_path:
        path = parsed.path.lstrip("/")
        if not path or path.endswith("/"):
            path = os.path.join(path, "downloaded_file")
        safe_path = os.path.normpath(path)
        if safe_path.startswith(".."):
            safe_path = safe_path.replace("..", "_")
        return os.path.join(out_folder, safe_path)
    else:
        filename = os.path.basename(parsed.path) or "downloaded_file"
        return os.path.join(out_folder, filename)


def download_and_save(url: str, out_dir: str, preserve_path: bool, skip_existing: bool) -> None:
    filepath = resolve_filepath(url, out_dir, preserve_path)

    # Ensure directory exists
    dirpath = os.path.dirname(filepath)
    if dirpath and not os.path.isdir(dirpath):
        os.makedirs(dirpath, exist_ok=True)

    if skip_existing and os.path.exists(filepath):
        print(f"[Skip] Exists: {filepath}")
        return

    try:
        print(f"[Get] {url}")
        resp = requests.get(url, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        with open(filepath, "wb") as f:
            f.write(resp.content)
        print(f"[Saved] {filepath}")
    except Exception as e:
        print(f"[Error] {url}: {e}")


def load_from_file_lines(path: str) -> set[str]:
    if not os.path.isfile(path):
        print(f"URL file not found: {path}")
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip() and is_valid_url(line.strip())}


def load_from_stdin() -> set[str]:
    return {line.strip() for line in sys.stdin if line.strip() and is_valid_url(line.strip())}


def load_from_csv(path: str, column: str) -> set[str]:
    urls = set()
    if not os.path.isfile(path):
        print(f"CSV file not found: {path}")
        return urls
    with open(path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        if column not in (reader.fieldnames or []):
            print(f"CSV column '{column}' not found in {path}. Columns: {reader.fieldnames}")
            return urls
        for row in reader:
            u = (row.get(column) or "").strip()
            if u and is_valid_url(u):
                urls.add(u)
    return urls


def _get_by_keypath(obj, keypath: str):
    cur = obj
    for part in keypath.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def _collect_urls_any(obj, acc: set[str]):
    if isinstance(obj, str):
        if is_valid_url(obj):
            acc.add(obj)
    elif isinstance(obj, list):
        for v in obj:
            _collect_urls_any(v, acc)
    elif isinstance(obj, dict):
        for v in obj.values():
            _collect_urls_any(v, acc)


def load_from_json(path: str, key: str | None) -> set[str]:
    urls = set()
    if not os.path.isfile(path):
        print(f"JSON file not found: {path}")
        return urls
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Failed to parse JSON from {path}: {e}")
        return urls

    if key:
        val = _get_by_keypath(data, key)
        if isinstance(val, list):
            for v in val:
                if isinstance(v, str) and is_valid_url(v):
                    urls.add(v)
        elif isinstance(val, str) and is_valid_url(val):
            urls.add(val)
    else:
        _collect_urls_any(data, urls)
    return urls


def load_from_sitemap(source: str) -> set[str]:
    urls = set()
    try:
        if is_valid_url(source):
            resp = requests.get(source, timeout=DEFAULT_TIMEOUT)
            resp.raise_for_status()
            content = resp.content
        else:
            if not os.path.isfile(source):
                print(f"Sitemap not found: {source}")
                return urls
            with open(source, "rb") as f:
                content = f.read()
        root = ET.fromstring(content)
        for elem in root.iter():
            if isinstance(elem.tag, str) and elem.tag.lower().endswith("loc"):
                if elem.text:
                    u = elem.text.strip()
                    if is_valid_url(u):
                        urls.add(u)
    except Exception as e:
        print(f"Failed to load sitemap {source}: {e}")
    return urls


def collect_urls(args) -> set[str]:
    urls: set[str] = set()

    if args.urls:
        urls |= {u for u in args.urls if is_valid_url(u)}
        for u in args.urls:
            if not is_valid_url(u):
                print(f"Ignored invalid URL: {u}")

    if args.file:
        urls |= load_from_file_lines(args.file)

    if args.stdin:
        urls |= load_from_stdin()

    if args.csv:
        urls |= load_from_csv(args.csv, args.csv_column)

    if args.json:
        urls |= load_from_json(args.json, args.json_key)

    if args.sitemap:
        urls |= load_from_sitemap(args.sitemap)

    return urls


def main():
    parser = argparse.ArgumentParser(description="Parallel file downloader (simplified).")
    # Inputs
    parser.add_argument("--urls", nargs="*", help="List of URLs")
    parser.add_argument("--file", help="File with URLs (one per line)")
    parser.add_argument("--stdin", action="store_true", help="Read URLs from stdin (one per line)")
    parser.add_argument("--csv", help="CSV file containing URLs")
    parser.add_argument("--csv-column", default="url", help="CSV column name with URLs (default: url)")
    parser.add_argument("--json", help="JSON file containing URLs")
    parser.add_argument("--json-key", help="Key or dotted path in JSON pointing to URL(s)")
    parser.add_argument("--sitemap", help="Sitemap path or URL to extract URLs from")

    # Output and behavior
    parser.add_argument("--out", default="downloaded", help="Output directory (default: downloaded)")
    parser.add_argument("--preserve-path", action="store_true",
                        help="Preserve URL path structure under output directory")
    parser.add_argument("--skip-existing", action="store_true", help="Skip downloading files that already exist")

    # Concurrency (use producers as worker count for backward compatibility)
    parser.add_argument("--producers", type=int, default=6, help="Number of worker threads (default: 6)")

    args = parser.parse_args()

    urls = collect_urls(args)
    if not urls:
        print("No URLs provided.")
        return

    os.makedirs(args.out, exist_ok=True)

    print(f"Downloading {len(urls)} file(s) with {args.producers} worker(s)...")
    with ThreadPoolExecutor(max_workers=args.producers) as executor:
        futures = {executor.submit(download_and_save, u, args.out, args.preserve_path, args.skip_existing): u for u in
                   urls}
        for _ in as_completed(futures):
            pass

    print("All downloads completed.")


if __name__ == "__main__":
    main()
