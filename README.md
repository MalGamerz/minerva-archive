# Minerva Archive — DPN Worker (Unofficial GUI)

> **⚠️ Disclaimer:** This is an unofficial, community-developed graphical interface for the Minerva DPN Worker. It is not affiliated with, maintained by, or officially endorsed by the Minerva Archive team. **Please do not direct support requests or bug reports for this GUI to the official Minerva maintainers.** If you encounter an issue, please open an Issue in this repository.

A high-performance, graphical worker client for the Minerva Distributed Preservation Network. This tool allows volunteers to help archive the internet by efficiently downloading files from source servers and uploading them safely to the Minerva archive.

Built from the ground up for high-bandwidth connections, this version features a modern, CustomTkinter dark-mode UI, real-time speed telemetry, and an aggressive multi-threaded backend that pushes your network to its absolute limits.

---

## 🚀 Key Features

* **Modern Graphical Interface:** A sleek, responsive dark-mode experience that completely replaces the terminal worker.
* **Real-Time Telemetry:** Live UI progress bars tracking exact download/upload speeds (MB/s), file sizes, and dynamic ETAs.
* **Smart State Recovery:** Power went out? App closed mid-download? The worker writes a local `.job.json` cache. On your next launch, it instantly sweeps your temp folder and resumes interrupted jobs exactly where they left off without re-downloading a single byte.
* **Auto-Conflict Bypass (409 Aware):** If another volunteer finishes archiving a file milliseconds before you do, the app instantly catches the `409 Conflict`, silently trashes the duplicate, and grabs a fresh job to keep your network redlined.
* **Weaponized Performance:** Utilizes a dedicated CPU ThreadPool for background disk I/O, NVMe `falloc` instant space allocation, and a massive 2GB RAM buffer to swallow gigabit network streams without dropping a frame.
* **Secure Local Auth:** 1-click Discord authentication utilizing a CSRF-protected, auto-terminating local server. Features a 1-click UI button to safely revoke and wipe your credentials from disk.

---

## ⚙️ Installation

You have two ways to run the Minerva Unofficial GUI: downloading the standalone executable, or running it from source via Python.

### Option 1: The Standalone App (Windows Only)

You do not need to install Python to run the app using this method.

1. Download the latest `MinervaWorker.exe` from the **Releases** tab.
2. Ensure `aria2c` is installed on your system (see Step 3 below).
3. Double-click the `.exe` to launch.

### Option 2: Running from Source

If you are on macOS/Linux or want to run the raw Python code:

1. Ensure you have **Python 3.10+** installed.
2. Clone this repository and install the dependencies:
```cmd
pip install -r requirements.txt

```


3. Launch the app:
```cmd
python main.py

```



### Step 3: Install `aria2c` (REQUIRED for maximum speed)

While the app has a basic Python fallback downloader, **`aria2c` is strictly required** for high-speed, multi-connection downloading and state recovery. Ensure it is installed and added to your system's PATH.

* **Windows:** `winget install aria2.aria2` *(Restart your PC/Terminal after installing)*
* **macOS:** `brew install aria2`
* **Linux (Debian/Ubuntu):** `sudo apt install aria2`

---

## 🏎️ Network Tuning Guide (Avoid IP Bans)

The source archive server (Myrient) has strict anti-leech protections. **Your single IP address cannot open more than 15 total connections at the same time.** If you exceed this, your IP will be temporarily banned and your speeds will flatline to 0 KB/s.

Use the settings grid in the UI to optimize your worker. **Ensure that `[Concurrency] × [aria2c Connections] <= 15`.**

**The Heavyweight Setup (For large 1GB+ ISOs):**
* Concurrency: `3` | aria2c Connections: `5`
* *Result:* You download 3 massive games at once, with 5 streams dedicated to each.

**The Machine Gun Setup (For tiny retro ROMs):**
* Concurrency: `15` | aria2c Connections: `1`
* *Result:* You pull 15 different small games simultaneously.

---

## 🏗️ Architecture & Security Notes

* **Upload Path:** `gate.minerva-archive.org`
* **Manager Path:** `api.minerva-archive.org`
* **File Integrity:** Files are hashed via SHA-256 locally before the final upload sequence finishes to guarantee 1:1 parity with the source.
* **Path Traversal Protection:** All incoming file paths from the API are aggressively sanitized, stripping illegal characters and preventing `../../` directory traversal attacks before files are written to your local temp directory.
