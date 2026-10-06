# 9. Strategi Pengemasan, Distribusi Single Binary, dan Otomasi Rilis CI/CD

Date: 2026-10-06
Status: Accepted

## Konteks
Setelah penyelesaian antarmuka desktop Slint (Fase 7) dan core engine unduhan yang stabil, Tenun memerlukan mekanisme pengemasan dan rilis yang efisien untuk didistribusikan kepada pengguna di Linux dan Windows.
Tantangan umum dalam distribusi aplikasi desktop adalah:
1. Kebutuhan runtime pihak ketiga (seperti Visual C++ Redistributable pada Windows, atau OpenSSL/SQLite eksternal pada Linux) yang sering menyebabkan error *missing DLL/so* pada sistem operasi pengguna.
2. Pengguna sering kali lebih menyukai berkas portabel mandiri (*single executable*) yang dapat langsung dijalankan tanpa prosedur instalasi yang rumit.
3. Di sisi lain, pengguna Linux desktop memerlukan integrasi menu aplikasi dan ikon sistem melalui format paket standar seperti `.deb` atau AppImage.
4. Ukuran biner kompilasi rilis harus sekecil dan secepat mungkin tanpa mengorbankan fungsionalitas grafis dan kriptografi.

## Keputusan

1. **Prioritas Distribusi Single Binary**:
   - Menargetkan satu berkas eksekutabel mandiri per sistem operasi:
     - **Windows**: Satu berkas `tenun.exe` mandiri yang berisi GUI desktop, SQLite bundled, TLS rustls, dan aset tampilan Slint terbenam.
     - **Linux**: Satu berkas ELF `tenun-ui` mandiri yang hanya bergantung pada library sistem standar (glibc, fontconfig, freetype), tanpa dependensi non-standar.
   - Mode portabel didukung secara bawaan: jika berkas penanda `tenun.portable` berada di samping executable, data dan riwayat unduhan disimpan di direktori lokal di samping biner, bukan di folder data sistem.

2. **Optimasi Profil Rilis Workspace**:
   - Mengonfigurasi profil rilis di `Cargo.toml`:
     ```toml
     [profile.release]
     opt-level = 3
     lto = true
     codegen-units = 1
     panic = "abort"
     strip = true
     ```
   - Penggunaan Link-Time Optimization (LTO) dan penghapusan simbol debug (`strip = true`) memangkas ukuran biner GUI dari ~115 MB menjadi 24 MB dan CLI menjadi 6.3 MB.

3. **Static C-Runtime Linking pada Windows (MSVC)**:
   - Mengonfigurasi `.cargo/config.toml` dengan `rustflags = ["-C", "target-feature=+crt-static"]` untuk target `x86_64-pc-windows-msvc`.
   - Hal ini menghilangkan ketergantungan pada `vcruntime140.dll` atau paket redistributable Microsoft Visual C++.

4. **Penyertaan Manifes dan Ikon Windows**:
   - Memanfaatkan crate `winres` pada `crates/tenun-ui/build.rs` untuk menyematkan ikon resolusi ganda (`assets/icon.ico`), informasi metadata versi, dan manifes DPI awareness ke dalam biner PE Windows.
   - Menyertakan `#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]` agar jendela terminal tidak muncul saat aplikasi diluncurkan.

5. **Format Distribusi Linux & Baseline Kompatibilitas**:
   - Kompilasi rilis Linux dijalankan pada baseline runner Ubuntu 22.04 LTS (glibc 2.35) untuk memastikan biner kompatibel dengan seluruh distribusi Linux modern (Debian 12, Ubuntu 22.04+, Fedora, Arch).
   - Menyediakan paket Debian (`.deb`) dengan struktur hierarki `/usr/bin/tenun`, `/usr/bin/tenun-ui`, berkas `tenun.desktop`, dan ikon hicolor (`256x256` dan `scalable`).
   - Menyediakan struktur `AppDir` untuk bundling AppImage mandiri.

6. **Pipeline Rilis CI/CD GitHub Actions**:
   - Menambahkan `.github/workflows/release.yml` yang terpicu secara otomatis saat tag Git versi `v*` didorong.
   - Pipeline mengompilasi biner untuk kedua OS, membuat paket tarball, zip, dan deb, menghasilkan berkas checksum `SHA256SUMS.txt`, dan mempublikasikan artefak ke GitHub Releases.

## Konsekuensi
- Pengguna Windows dapat langsung mengunduh dan menjalankan `tenun.exe` tanpa instalasi atau error missing DLL.
- Pengguna Linux memiliki pilihan antara biner portabel mandiri atau instalasi terintegrasi via paket `.deb`.
- Proses rilis aplikasi sepenuhnya terotomasi dan dapat diverifikasi integritasnya melalui hash SHA-256.
EOF
