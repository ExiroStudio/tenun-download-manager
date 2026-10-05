# 1. Struktur Workspace dan Arsitektur Multi-Crate

Date: 2026-10-05
Status: Accepted

## Konteks
Tenun Download Manager dirancang sebagai download manager desktop berperforma tinggi bergaya IDM untuk Linux dan Windows. Sistem ini membutuhkan engine pengunduhan yang cepat, andal (resume, retry, stall-detection), dan teruji, serta antarmuka desktop modern menggunakan toolkit UI Slint dan antarmuka CLI.

Diperlukan struktur proyek yang memastikan:
1. Engine (`tenun-core`) terisolasi dari UI Slint, baik secara arsitektur maupun dependensi (menjaga independensi lisensi dan portabilitas).
2. Kode CLI (`tenun-cli`) dan UI desktop (`tenun-ui`) hanya bertindak sebagai consumer tipis terhadap API async dan event stream dari `tenun-core`.
3. Pengujian integrasi dan mock test server (`tests/test-server`) dapat dijalankan secara modular dan deterministik di CI Linux dan Windows.

## Keputusan
1. Menggunakan Cargo Workspace dengan susunan berikut:
   - `crates/tenun-core`: Library mesin unduh (probe, segmentasi, resume, queue, integritas data, dan abstraksi platform).
   - `crates/tenun-cli`: Binary CLI `tenun`.
   - `crates/tenun-ui`: Aplikasi desktop berbasis Slint (Rust API).
   - `tests/test-server`: Test server berbasis Axum untuk pengujian HTTP Range, throttling, dan skenario kegagalan jaringan.
2. Abstraksi platform (`tenun-core::platform`):
   - Unix menggunakan `std::os::unix::fs::FileExt::write_at`.
   - Windows menggunakan `std::os::windows::fs::FileExt::seek_write`.
   - Jalur penulisan posisional dibungkus dalam trait `PositionalWrite`.
3. Isolasi Lisensi:
   - `tenun-core` tidak bergantung pada `slint`. Lisensi proyek dapat diputuskan terpisah kelak.
4. Error Handling & Tracing:
   - `thiserror` digunakan pada library (`tenun-core`).
   - `anyhow` hanya diizinkan pada aplikasi binary (`tenun-cli` dan `tenun-ui`).
   - Instrumentasi menggunakan crate `tracing`.

## Konsekuensi
- Memisahkan crate memerlukan pengelolaan dependensi workspace terpusat di `Cargo.toml`.
- Klien UI berkomunikasi dengan engine melalui async message passing / channel, mencegah UI thread terblokir oleh operasi I/O jaringan.
