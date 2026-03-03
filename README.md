# Minerva DPN Worker

A high-performance, single-file Python worker client for the Minerva Distributed Preservation Network. This tool allows volunteers to help archive the internet by efficiently downloading files from source servers and uploading them safely to the Minerva archive.

## Features

* **High-Speed Downloads:** Seamless integration with `aria2c` for accelerated downloading.
* **Network Saturation:** Process multiple files concurrently to maximize high-bandwidth connections.
* **Intelligent Rate Limiting:** Hardcoded connection limits and staggered task execution to prevent HTTP 429 (Too Many Requests) IP bans from aggressive source servers.
* **Real-Time Monitoring:** Live terminal progress bars tracking both `aria2c` downloads and chunked server uploads.
* **Chunked Uploads:** Safely uploads massive files by breaking them into 8MB chunks, bypassing standard reverse-proxy (e.g., Cloudflare) file size limits.
* **Auto-Cleanup:** Automatically purges temporary files immediately after a successful upload to preserve local disk space.

## Prerequisites

* **Python:** Version 3.10 or higher.
* **aria2c:** Highly recommended. The script will fall back to standard Python downloads (`httpx`) if not found, but `aria2c` is required for maximum stability and speed.

## Installation

**1. Install Python Dependencies**
The script requires a few lightweight libraries to handle terminal UI and network requests:

```cmd
pip install httpx rich click

```

**2. Install aria2c**
Ensure `aria2c` is installed and added to your system's PATH.

* **Windows:** `winget install aria2.aria2`
* **macOS:** `brew install aria2`
* **Linux (Debian/Ubuntu):** `sudo apt install aria2`

*(Note: On Windows, you must restart your terminal after installing for the PATH to update).*

## Usage

### Authentication

Before running the worker, you must authenticate your client with Discord. This generates a secure token saved to `~/.minerva-dpn/token`.

```cmd
python minerva.py login

```

This will open a browser window. Once authorized, you can close the tab.

### Running the Worker

To start the automated download and upload process with the default settings (optimized for 4 concurrent files with 1 safe connection each):

```cmd
python minerva.py run

```

### CLI Options

You can customize the worker's behavior using the following flags:

| Flag | Long Name | Default | Description |
| --- | --- | --- | --- |
| `-c` | `--concurrency` | `4` | Number of files to download/upload simultaneously. |
| `-b` | `--batch-size` | `10` | Number of jobs to fetch from the server per API request. |
| `-a` | `--aria2c-connections` | `1` | Connections per file. *Keep at 1 to prevent server bans.* |
| N/A | `--temp-dir` | `~/.minerva-dpn/tmp` | Local directory for temporary file storage during transit. |
| N/A | `--keep-files` | `False` | Pass this flag to prevent files from being deleted after upload. |

**Example: High-Concurrency Run**
If you want to pull 8 files at once while maintaining the safe 1-connection-per-file limit:

```cmd
python minerva.py run -c 8 -a 1

```

## Architecture Notes

* **Upload Path:** `gate.minerva-archive.org`
* **Manager Path:** `api.minerva-archive.org`
* **File Integrity:** Files are hashed via SHA-256 locally before the final upload sequence finishes to guarantee 1:1 parity with the source.
