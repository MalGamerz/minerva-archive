# Minerva Archive — DPN Worker (GUI Edition)

A high-performance, graphical worker client for the Minerva Distributed Preservation Network. This tool allows volunteers to help archive the internet by efficiently downloading files from source servers and uploading them safely to the Minerva archive.

This version features a modern, dark-themed UI for real-time tracking of high-bandwidth transfers.

## Features

* **Modern Graphical Interface:** Built with `customtkinter` for a sleek, responsive dark-mode experience.
* **Real-Time Telemetry:** Live UI progress bars tracking download speeds (KB/s), exact file sizes (MiB/GiB), and dynamic ETAs directly from `aria2c`.
* **High-Speed Downloads:** Seamless integration with `aria2c` for accelerated downloading.
* **Network Saturation:** Process multiple files concurrently via UI Spinboxes to maximize high-bandwidth connections.
* **Intelligent Rate Limiting:** Adjustable connection limits and staggered task execution to prevent HTTP 429 (Too Many Requests) IP bans from aggressive source servers.
* **Chunked Uploads:** Safely uploads massive files by breaking them into 8MB chunks, bypassing standard reverse-proxy (e.g., Cloudflare) file size limits.
* **Auto-Cleanup:** Toggleable UI option to automatically purge temporary files immediately after a successful upload to preserve local disk space.

## Prerequisites

* **Python:** Version 3.10 or higher.
* **aria2c:** Highly recommended. The app will fall back to standard Python downloads (`httpx`) if not found, but `aria2c` is required for maximum stability, speed, and real-time ETA tracking.

## Installation

**1. Install Python Dependencies**
The application requires the following libraries to handle the UI and network requests:

```cmd
pip install customtkinter httpx

```

**2. Install aria2c**
Ensure `aria2c` is installed and added to your system's PATH.

* **Windows:** `winget install aria2.aria2`
* **macOS:** `brew install aria2`
* **Linux (Debian/Ubuntu):** `sudo apt install aria2`

*(Note: On Windows, you must restart your terminal after installing for the PATH to update).*

## Usage

To launch the application, run the Python file from your terminal:

```cmd
python minerva_app.py

```

### 1. Authentication

Before starting the worker engine, click **Login with Discord** in the left sidebar. This will open a browser window to authenticate your client. Once authorized, you can close the browser tab. The app will generate a secure token saved to `~/.minerva-dpn/token`.

### 2. Configuring Performance

Use the settings grid in the main window to optimize the worker for your network:

* **Concurrency:** The number of files to download/upload simultaneously. (Recommended: 4-8 for high-speed connections).
* **aria2c Connections:** The number of streams opened per file. *Warning: Keep at 1-3 to prevent aggressive source servers from issuing IP bans.*
* **Temp Directory:** The local path where files are staged during transit.
* **Keep files after upload:** Check this box if you want to retain the files locally instead of auto-deleting them.

### 3. Running the Worker

Click **▶ Start Worker** to begin pulling jobs from the API. You can monitor the live progress, speeds, and ETAs in the **Active Jobs** tab, and view detailed backend events in the **Log** tab.

## Architecture Notes

* **Upload Path:** `gate.minerva-archive.org`
* **Manager Path:** `api.minerva-archive.org`
* **File Integrity:** Files are hashed via SHA-256 locally before the final upload sequence finishes to guarantee 1:1 parity with the source.
