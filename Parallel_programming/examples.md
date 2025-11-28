downloader.py — Options, descriptions and examples
===============================================
Options
-------

--urls
  What it does:
    Provide one or more URLs directly on the command line. The script accepts multiple URLs.
  Default:
    None
  Example (single URL):
    python downloader.py --urls https://i.pinimg.com/736x/e4/08/9e/e4089e5007c2177db368470329a6e5be.jpg
  Example (multiple URLs):
    python downloader.py --urls https://i.pinimg.com/736x/e4/08/9e/e4089e5007c2177db368470329a6e5be.jpg https://example.com/another.jpg

--file
  What it does:
    Read URLs from a plain text file, one URL per line.
  Default:
    None
  Example:
    # Put the URL in urls.txt (one URL per line)
    python downloader.py --file urls.txt

--stdin
  What it does:
    Read URLs from standard input (stdin), one URL per line. Useful for piping.
  Default:
    False (not reading stdin)
  Example (using echo):
    echo "https://i.pinimg.com/736x/e4/08/9e/e4089e5007c2177db368470329a6e5be.jpg" | python downloader.py --stdin
  Example (using a file and cat):
    cat urls.txt | python downloader.py --stdin

--csv
  What it does:
    Read URLs from a CSV file. Use --csv-column to specify the column name that holds the URL.
  Default:
    None
  Example (CSV with default column name 'url'):
    # links.csv contents:
    # url
    # https://i.pinimg.com/736x/e4/08/9e/e4089e5007c2177db368470329a6e5be.jpg
    python downloader.py --csv links.csv
  Example (CSV with custom column name):
    # CSV has column called image_link
    python downloader.py --csv links.csv --csv-column image_link

--csv-column
  What it does:
    Specify the CSV column name that contains the URLs (used with --csv).
  Default:
    url
  Example:
    python downloader.py --csv links.csv --csv-column image_link

--json
  What it does:
    Read URLs from a JSON file. The JSON can be an array of URL strings, a nested structure, or objects.
  Default:
    None
  Example (JSON array of strings):
    # urls.json:
    # ["https://i.pinimg.com/736x/e4/08/9e/e4089e5007c2177db368470329a6e5be.jpg"]
    python downloader.py --json urls.json

--json-key
  What it does:
    Specify a dotted key path into a JSON structure to find URL(s), e.g. items.pictures.
    If omitted, the script attempts a best-effort extraction of URL-like strings from the JSON.
  Default:
    None
  Example (nested key):
    # data.json:
    # {"items": {"pictures": ["https://i.pinimg.com/736x/e4/08/9e/e4089e5007c2177db368470329a6e5be.jpg"]}}
    python downloader.py --json data.json --json-key items.pictures

--sitemap
  What it does:
    Read URLs from an XML sitemap. The value can be a local file path or an HTTP/HTTPS URL to a sitemap.
    The script extracts <loc> elements from the sitemap.
  Default:
    None
  Example (local sitemap file):
    # sitemap.xml contains a <loc> with the example URL
    python downloader.py --sitemap sitemap.xml
  Example (remote sitemap URL):
    python downloader.py --sitemap https://example.com/sitemap.xml

--out
  What it does:
    Set the output directory where downloaded files are saved.
  Default:
    downloaded
  Example:
    python downloader.py --urls https://i.pinimg.com/736x/e4/08/9e/e4089e5007c2177db368470329a6e5be.jpg --out images

--preserve-path
  What it does:
    Preserve the URL path structure inside the output directory. For example, the URL path
    /736x/e4/08/9e/e4089e5007c2177db368470329a6e5be.jpg becomes OUT_DIR/736x/e4/08/9e/e4089e5007c2177db368470329a6e5be.jpg
  Default:
    False
  Example:
    python downloader.py --urls https://i.pinimg.com/736x/e4/08/9e/e4089e5007c2177db368470329a6e5be.jpg --preserve-path --out downloads
  Resulting file:
    downloads/736x/e4/08/9e/e4089e5007c2177db368470329a6e5be.jpg

--skip-existing
  What it does:
    If a target file already exists, skip downloading and saving it again.
  Default:
    False
  Example:
    python downloader.py --urls https://i.pinimg.com/736x/e4/08/9e/e4089e5007c2177db368470329a6e5be.jpg --skip-existing --out images

--producers
  What it does:
    Number of worker threads used to download files concurrently. (In the simplified script this sets the ThreadPoolExecutor max workers.)
  Default:
    6 (in the simplified script)
  Example:
    python downloader.py --urls https://i.pinimg.com/736x/e4/08/9e/e4089e5007c2177db368470329a6e5be.jpg --producers 8

Combined examples
-----------------

1) Single direct URL into default folder:
   python downloader.py --urls https://i.pinimg.com/736x/e4/08/9e/e4089e5007c2177db368470329a6e5be.jpg

2) Read from a file and preserve URL path:
   # urls.txt contains the example URL
   python downloader.py --file urls.txt --preserve-path --out downloads

3) Pipe via stdin and skip existing files:
   echo "https://i.pinimg.com/736x/e4/08/9e/e4089e5007c2177db368470329a6e5be.jpg" | python downloader.py --stdin --skip-existing --out images

4) CSV input with custom column and 10 workers:
   python downloader.py --csv links.csv --csv-column image_link --producers 10

5) JSON nested key extraction:
   python downloader.py --json data.json --json-key items.pictures --out pics

6) Sitemap (remote) and save using default options:
   python downloader.py --sitemap https://example.com/sitemap.xml

Notes and tips
--------------
- You can combine several input options: e.g. --file urls.txt --csv links.csv --json urls.json
- If multiple input sources include the same URL, duplicates are deduplicated.
- The script uses a fixed internal timeout (DEFAULT_TIMEOUT) to prevent hanging requests.
- If you use --preserve-path, the script creates subdirectories as needed under the output directory.
- When using CSV or JSON, ensure the file encoding is UTF-8 or compatible.

End of file