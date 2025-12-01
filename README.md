downloader.py — Options, descriptions and examples
===============================================

This is very simple app for downloading multiple URLs from meny inputs.
It was originally ment to be just school project but maybe someone finds it useful.

Options
-------

--urls             : Provide one or more URLs on the command line.
--file             : Path to a text file with one URL per line.
--stdin            : Read URLs from standard input (one URL per line).
--csv              : Read URLs from a CSV file.
--csv-column       : CSV column name that contains URLs (default: url).
--json             : Read URLs from a JSON file.
--json-key         : Dotted key path to extract URL(s) from JSON (e.g., items.pictures).
--sitemap          : Path or URL to an XML sitemap; extracts <loc> entries.
--out              : Output directory where downloaded files are saved.
--preserve-path    : Preserve the URL path structure under the output directory.
--skip-existing    : Skip downloading if the destination file already exists.
--producers        : Number of producer (download) threads.
--consumers        : Number of consumer (save) threads.
--timeout          : HTTP request timeout in seconds.
--max-retries      : Number of retry attempts on download failure.
--retry-backoff    : Base seconds for exponential backoff between retries.