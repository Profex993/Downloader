import os
import sys
import csv
import json
import argparse
import threading
import requests
import xml.etree.ElementTree as ET
from queue import Queue
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


def producer(url, queue, filepath):
    try:
        print(f"[Producer] Downloading: {url}")
        response = requests.get(url, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        queue.put((url, response, filepath))
        print(f"[Producer] Done: {url}")
    except Exception as e:
        print(f"[Producer] Error downloading {url}: {e}")


def consumer(queue, skip_existing):
    while True:
        item = queue.get()
        if item is None:
            queue.task_done()
            break

        url, response, filepath = item

        dirpath = os.path.dirname(filepath)
        if dirpath and not os.path.isdir(dirpath):
            try:
                os.makedirs(dirpath, exist_ok=True)
            except Exception as e:
                print(f"[Consumer] Error creating directory {dirpath}: {e}")
                queue.task_done()
                continue

        if skip_existing and os.path.exists(filepath):
            print(f"[Consumer] Skipped (exists): {filepath}")
            queue.task_done()
            continue

        try:
            with open(filepath, "wb") as f:
                f.write(response.content)
            print(f"[Consumer] Saved: {filepath}")
        except Exception as e:
            print(f"[Consumer] Error saving {filepath}: {e}")

        queue.task_done()


def load_from_file_lines(path: str) -> set:
    urls = set()
    if not os.path.isfile(path):
        print(f"URL file not found: {path}")
        return urls
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and is_valid_url(line):
                urls.add(line)
    return urls


def load_from_stdin() -> set:
    urls = set()
    for line in sys.stdin:
        line = line.strip()
        if line and is_valid_url(line):
            urls.add(line)
    return urls


def load_from_csv(path: str, column: str) -> set:
    urls = set()
    if not os.path.isfile(path):
        print(f"CSV file not found: {path}")
        return urls
    with open(path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        if column not in reader.fieldnames:
            print(f"CSV column '{column}' not found in {path}. Columns: {reader.fieldnames}")
            return urls
        for row in reader:
            u = (row.get(column) or "").strip()
            if u and is_valid_url(u):
                urls.add(u)
    return urls


def load_from_json(path: str, key: str | None) -> set:
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

    def add_url(val):
        if isinstance(val, str) and is_valid_url(val):
            urls.add(val)

    def get_by_keypath(obj, keypath: str):
        parts = keypath.split(".")
        cur = obj
        for part in parts:
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return None
        return cur

    if isinstance(data, list):
        if all(isinstance(x, str) for x in data):
            for x in data:
                add_url(x)
        elif key:
            for item in data:
                if isinstance(item, dict):
                    val = get_by_keypath(item, key)
                    if isinstance(val, list):
                        for v in val:
                            add_url(v)
                    else:
                        add_url(val)
    elif isinstance(data, dict):
        if key:
            val = get_by_keypath(data, key)
            if isinstance(val, list):
                for v in val:
                    add_url(v)
            else:
                add_url(val)
        else:
            for v in data.values():
                if isinstance(v, str):
                    add_url(v)
                elif isinstance(v, list):
                    for vv in v:
                        add_url(vv)
    return urls


def load_from_sitemap(source: str) -> set:
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


def load_urls(args):
    urls = set()

    if args.urls:
        for u in args.urls:
            if is_valid_url(u):
                urls.add(u)
            else:
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
    parser = argparse.ArgumentParser(description="Parallel file downloader.")
    # Input sources
    parser.add_argument("--urls", nargs="*", help="List of URLs")
    parser.add_argument("--file", help="File with URLs (one per line)")
    parser.add_argument("--stdin", action="store_true", help="Read URLs from stdin (one per line)")
    parser.add_argument("--csv", help="CSV file containing URLs")
    parser.add_argument("--csv-column", default="url", help="CSV column name with URLs (default: url)")
    parser.add_argument("--json", help="JSON file containing URLs")
    parser.add_argument("--json-key",
                        help="Key or dotted path in JSON pointing to URL(s). If omitted, tries best-effort extraction.")
    parser.add_argument("--sitemap", help="Sitemap path or URL to extract URLs from")

    # Output and behavior
    parser.add_argument("--out", help="Output directory", default="downloaded")
    parser.add_argument("--preserve-path", action="store_true",
                        help="Preserve URL path structure under output directory")
    parser.add_argument("--skip-existing", action="store_true", help="Skip downloading files that already exist")

    # Concurrency
    parser.add_argument("--producers", type=int, default=3, help="Number of producer (download) threads")
    parser.add_argument("--consumers", type=int, default=3, help="Number of consumer (save) threads")

    args = parser.parse_args()
    urls = load_urls(args)

    if not urls:
        print("No URLs provided.")
        return

    os.makedirs(args.out, exist_ok=True)

    url_to_path = {u: resolve_filepath(u, args.out, args.preserve_path) for u in urls}
    if args.skip_existing:
        to_download = [u for u in urls if not os.path.exists(url_to_path[u])]
        skipped = len(urls) - len(to_download)
        if skipped:
            print(f"Skipping {skipped} existing file(s).")
    else:
        to_download = list(urls)

    if not to_download:
        print("Nothing to download.")
        return

    queue = Queue()
    consumer_threads = []
    producer_threads = []

    for _ in range(args.consumers):
        t = threading.Thread(target=consumer, args=(queue, args.skip_existing), daemon=True)
        t.start()
        consumer_threads.append(t)

    for url in to_download:
        filepath = url_to_path[url]
        t = threading.Thread(target=producer, args=(url, queue, filepath))
        t.start()
        producer_threads.append(t)

    for t in producer_threads:
        t.join()

    for _ in range(args.consumers):
        queue.put(None)

    queue.join()
    print("All downloads completed.")


if __name__ == "__main__":
    main()