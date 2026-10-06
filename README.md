# Tenun Download Manager

<p align="center">
  <img src="assets/icon.png" width="128" height="128" alt="Tenun Download Manager Logo" />
</p>

<p align="center">
  <strong>Fast, reliable, resilient, and non-intimidating desktop download manager for Linux and Windows.</strong>
</p>

<p align="center">
  <a href="#features">Features</a> •
  <a href="#installation">Installation</a> •
  <a href="#usage">Usage</a> •
  <a href="#portable-mode">Portable Mode</a> •
  <a href="#storage--logs">Storage & Logs</a> •
  <a href="#building-from-source">Building from Source</a> •
  <a href="#architecture">Architecture</a>
</p>

---

## Features

- ⚡ **Multi-Segment Parallel Acceleration**: Dynamically splits files into multiple concurrent byte-range HTTP segments with automatic work stealing to maximize bandwidth utilization.
- 🛡️ **Resilient Crash Recovery & Checkpointing**: Atomic `.tenun` and `.part` state persistence; power outages or sudden shutdowns resume seamlessly from the exact byte offset without restarting.
- 🎨 **Modern, Calm & Minimalist Desktop UI**: Clean single-column desktop interface built in **Slint** (max-width 760px), clipboard link suggestion, light/dark theme toggle, and zero clutter.
- 🔍 **Real-Time Probing & Automatic Routing**: Probes remote HTTP headers for size, resumability, and redirects; categorizes downloads automatically into `Documents`, `Compressed`, `Video`, `Audio`, `Images`, or `Programs`.
- 🔐 **Cryptographic Integrity Verification**: Validates file integrity automatically using HTTP `ETag`, `Digest`, `Content-MD5`, or user-specified hashes (SHA-256, SHA-1, MD5).
- 🚦 **Prioritized Queue & Rate Limiting**: Global bandwidth throttling, token bucket rate limiting per download, and priority-ranked concurrency management backed by embedded SQLite.
- 📦 **Single Binary Portability**: Self-contained executable with zero required runtime installers or external DLLs (statically-linked CRT on Windows, pure rustls TLS, bundled SQLite).

---

## Installation

Download the latest pre-compiled release from [GitHub Releases](https://github.com/ExiroStudio/tenun-download-manager/releases).

### Windows (x86_64)
- **Standalone Executable**: Download `tenun.exe` and double-click to run. No Visual C++ Redistributable or installer needed.
- **Portable Zip**: Download `tenun-v0.1.0-windows-x86_64.zip` which includes both GUI (`tenun.exe`) and CLI (`tenun-cli.exe`).

### Linux (x86_64)
- **Debian / Ubuntu**: Download and install the `.deb` package:
  ```bash
  sudo dpkg -i tenun_0.1.0_amd64.deb
  ```
- **Standalone Tarball**: Extract and run immediately:
  ```bash
  tar -xzf tenun-v0.1.0-linux-x86_64.tar.gz
  cd tenun-0.1.0-linux-x86_64
  ./tenun-ui
  ```

---

## Usage

### Desktop Graphical Interface (GUI)
Launch `tenun-ui` (Linux) or `tenun.exe` (Windows):
1. **Add Download**: Paste any HTTP/HTTPS URL into the top add bar and click **Download** (or click the automatic clipboard suggestion chip).
2. **Review Dialog**: Inspect file details, customize output directory, configure connection count, speed limit, or expected checksum in the advanced accordion.
3. **Control**: Pause, resume, retry, or cancel downloads individually. Use `⋯` to open the downloaded file or parent folder.
4. **Settings**: Click the gear icon `⚙️` in the top right to configure global download directories, maximum concurrency, speed limit, and dark mode.

### Command-Line Interface (CLI)
Tenun also provides a full-featured CLI tool `tenun`:

```bash
# Direct fast multi-segment download
tenun get "https://example.com/large-archive.zip" --connections 16 --output ~/Downloads

# Add download to persistent queue
tenun add "https://example.com/video.mp4" --priority 10 --start-now

# List downloads in queue
tenun list

# Pause or resume downloads
tenun pause 1
tenun resume 1

# Verify file checksum
tenun verify path/to/file.iso --expected sha256:abc12345...

# Run background queue daemon
tenun run --concurrent 4 --limit-rate 5MB
```

---

## Portable Mode

To run Tenun completely portably (e.g. from a USB drive) without leaving traces in system directories:
1. Place an empty file named `tenun.portable` directly alongside `tenun.exe` / `tenun-ui`.
2. Tenun will automatically store the SQLite database, temporary part files, configuration, and logs inside a local `./data/` subfolder.

---

## Storage & Logs

When running in standard mode, Tenun saves data in OS-native directories:

| Platform | Database & Data Directory | Configuration Directory | Logs Directory |
| :--- | :--- | :--- | :--- |
| **Linux** | `~/.local/share/tenun/` | `~/.config/tenun/` | `~/.local/share/tenun/logs/` |
| **Windows** | `%LOCALAPPDATA%\Tenun\` | `%APPDATA%\Tenun\` | `%LOCALAPPDATA%\Tenun\logs\` |

---

## Building from Source

### Prerequisites
- **Rust Toolchain**: Stable 1.85+ (`rustup default stable`)
- **Linux Packages**: `libfontconfig1-dev`, `libxkbcommon-dev`
  ```bash
  sudo apt-get install -y libfontconfig1-dev libxkbcommon-dev
  ```

### Build Commands
```bash
# Clone the repository
git clone https://github.com/ExiroStudio/tenun-download-manager.git
cd tenun-download-manager

# Run all test suites
cargo test --all-targets --all-features

# Build optimized release binaries
cargo build --release --workspace

# Package distribution artifacts for Linux (tar.gz, .deb, AppDir)
bash scripts/package-linux.sh
```

The compiled executables will be available at:
- GUI: `target/release/tenun-ui` (or `target/release/tenun.exe` on Windows)
- CLI: `target/release/tenun` (or `target/release/tenun-cli.exe` on Windows)

---

## Architecture

Tenun is structured as a modular Rust workspace:

```
tenun-download-manager/
├── crates/
│   ├── tenun-core/   # Engine: probing, segmentation, actor, state, SQLite DB, integrity
│   ├── tenun-cli/    # Terminal interface with progress spinners & subcommands
│   └── tenun-ui/     # Slint declarative GUI desktop application
├── tests/            # Integration tests & configurable local test server
├── scripts/          # Packaging automation (Linux .deb / Windows MSVC)
└── docs/decisions/   # Architectural Decision Records (ADR 0001 - 0009)
```

- **Thread Separation**: Engine actor runs in a dedicated Tokio background thread; UI runs on the main thread communicating via non-blocking channels.
- **Throttling (10 Hz)**: UI model updates are batched and throttled to 100 ms to guarantee responsive rendering even during intense 32-segment transfers.
- **Zero OpenSSL**: Uses `rustls` with native root certificates for fast, reproducible, and cross-platform TLS.
