import os
import argparse
import threading
import requests
from queue import Queue
from urllib.parse import urlparse


def producer(url, queue):
    try:
        print(f"[Producer] Downloading: {url}")
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        queue.put((url, response))
        print(f"[Producer] Done: {url}")
    except Exception as e:
        print(f"[Producer] Error downloading {url}: {e}")


def consumer(queue, out_folder):
    while True:
        item = queue.get()
        if item is None:
            queue.task_done()
            break

        url, response = item
        filename = os.path.basename(urlparse(url).path) or "downloaded_file"
        filepath = os.path.join(out_folder, filename)

        try:
            with open(filepath, "wb") as f:
                f.write(response.content)
            print(f"[Consumer] Saved: {filepath}")
        except Exception as e:
            print(f"[Consumer] Error saving {filepath}: {e}")

        queue.task_done()


def load_urls(args):
    urls = set()

    if args.urls:
        urls.update(args.urls)

    if args.file:
        if not os.path.isfile(args.file):
            print(f"URL file not found: {args.file}")
            return set()
        with open(args.file, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    urls.add(line)

    return urls


def main():
    parser = argparse.ArgumentParser(description="Parallel file downloader.")
    parser.add_argument("--urls", nargs="*", help="List of URLs")
    parser.add_argument("--file", help="File with URLs (one per line)")
    parser.add_argument("--out", help="Output directory", default="downloaded")
    parser.add_argument("--producers", type=int, default=3)
    parser.add_argument("--consumers", type=int, default=3)

    args = parser.parse_args()
    urls = load_urls(args)

    if not urls:
        print("No URLs provided.")
        return

    os.makedirs(args.out, exist_ok=True)

    queue = Queue()
    consumer_threads = []
    producer_threads = []

    for _ in range(args.consumers):
        t = threading.Thread(target=consumer, args=(queue, args.out), daemon=True)
        t.start()
        consumer_threads.append(t)

    for url in urls:
        t = threading.Thread(target=producer, args=(url, queue))
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
