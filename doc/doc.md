Parallel File Downloader

A simple Python script to download files from URLs using a producer-consumer model with multiple threads.

Features:
- Download multiple files in parallel
- Supports URLs from command line or text file
- Saves files into a specified output folder

Usage:

1. Download from URLs directly:
python main.py --urls https://example.com/file1.txt https://example.com/file2.jpg --out downloads

2. Download from a text file:
python main.py --file urls.txt --out downloads

3. Example with all arguments:s
python main.py --urls https://example.com/file1.txt https://example.com/file2.jpg --file urls.txt --out downloads --producers 4 --consumers 2

Arguments:

--urls       : List of URLs to download directly
--file       : Path to a text file containing URLs, one per line
--out        : Output folder where files will be saved (required)
--producers  : Number of producer threads (default: 3)
--consumers  : Number of consumer threads (default: 3)
