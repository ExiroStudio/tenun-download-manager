# Changelog

All notable changes to the **Tenun Download Manager** project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0] - 2026-10-06

Initial production-ready release of Tenun Download Manager for Linux and Windows.

### Added
- **Core Engine (`tenun-core`)**:
  - Remote resource probing discovering Content-Length, HTTP Range support, ETag, Last-Modified, and redirected canonical URLs.
  - Robust Content-Disposition header filename extraction adhering to RFC 6266 and RFC 5987 UTF-8 encoding.
  - Multi-connection segmented downloading dividing files into up to 32 parallel segments with work-stealing algorithms.
  - Sparse positional disk writing writing chunks directly into target files without temporary multi-gigabyte memory buffering.
  - Crash-resilient `.tenun` JSON checkpointing and `.part` sparse files for lossless resume across power cuts and app restarts.
  - Network resilience with exponential backoff, jitter, Retry-After header parsing, stall detection, and dynamic slow segment re-splitting.
  - Token bucket bandwidth limiter for global and per-download rate throttling.
  - Cryptographic integrity verification supporting SHA-256, SHA-1, and MD5, with automatic detection from `Digest`, `Content-MD5`, and `ETag` headers.
  - Collision resolution policies: `Rename`, `Overwrite`, and `Deduplicate`.
  - Actor-based `DownloadManager` running on Tokio with prioritized queue scheduling, crash recovery, and broadcast event streaming.
  - SQLite persistence layer using embedded `rusqlite` in WAL mode.
  - Automatic file categorization into `Documents`, `Compressed`, `Video`, `Audio`, `Images`, and `Programs`.
  - Disk space pre-allocation checks on POSIX (`statvfs`) and Windows (`GetDiskFreeSpaceExW`).
  - Portable mode detection via `tenun.portable` marker file.

- **Desktop Graphical Interface (`tenun-ui`)**:
  - Minimalist, calm, single-column interface built with Slint (max-width 760px).
  - Add bar with instant URL probing and background clipboard suggestion chip.
  - Real-time download rows featuring thin 4px progress bars, transfer speed, and ETA calculation.
  - Action menu for opening files, opening destination folders, copying links, retrying, and deleting records/files.
  - Detailed modal views: Add Download Dialog, Details Dialog, and Settings Dialog.
  - 10 Hz throttled batch refresh loop preventing UI event loop starvation under high concurrency.
  - Inter-Process Communication (IPC) via `interprocess` ensuring single-instance execution and CLI URL forwarding.
  - Native desktop notifications via `notify-rust`.
  - Native folder pickers via `rfd`.
  - Automatic software rendering fallback (`SLINT_BACKEND = "software"`) on headless or unsupported GPU environments.
  - Windows release subsystem configuration suppressing console window popup.

- **Command-Line Interface (`tenun-cli`)**:
  - Standalone `tenun` CLI binary with subcommands: `get`, `add`, `list`, `pause`, `resume`, `cancel`, `remove`, `run`, and `verify`.
  - Real-time multi-progress bars powered by `indicatif`.

- **Packaging & Distribution**:
  - Optimized release profile with Link-Time Optimization (`lto = true`), `opt-level = 3`, single codegen unit, and binary stripping.
  - Static C-Runtime linking (`-C target-feature=+crt-static`) on Windows MSVC eliminating VC++ Redistributable requirements.
  - Embedded Windows resources (icon and manifest) via `winres`.
  - Linux standalone tarball, Debian `.deb` package, and `AppDir` packaging scripts.
  - Automated GitHub Actions CI/CD release workflow generating signed checksums (`SHA256SUMS.txt`).
