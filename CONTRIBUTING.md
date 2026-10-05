# Panduan Kontribusi Tenun Download Manager

Dokumen ini memuat standar kerja dan aturan pengembangan untuk proyek **Tenun Download Manager**.

## 1. Aturan Git dan Alur Kerja

Setiap perubahan harus tercatat di git agar riwayatnya dapat ditelusuri dengan jelas dan akurat:

- **Satu perubahan logis = satu commit**:
  - Pisahkan per domain: perubahan di `tenun-core`, `tenun-cli`, `tenun-ui`, `platform`, `CI`, dan dokumentasi masuk ke commit terpisah.
  - Jangan menggabungkan domain berbeda dalam satu commit kecuali tidak dapat dipisahkan (jelaskan alasannya di pesan commit).
- **Commit sering dan kecil**:
  - Hindari menumpuk perubahan besar dalam satu commit di akhir pekerjaan.
- **Format Pesan: Conventional Commits**:
  - Format: `<type>(<scope>): <ringkasan singkat dalam bentuk imperatif, <= 72 karakter>`
  - Pilihan `<type>`: `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `build`, `ci`, `chore`.
  - Pilihan `<scope>`: `core`, `cli`, `ui`, `platform`, `probe`, `segment`, `resume`, `queue`, `integrity`, `ci`, `build`, `docs`.
  - Contoh: `feat(segment): bagi file ke segmen dengan ukuran minimum 1 MiB`
  - Sertakan deskripsi *alasan* perubahan pada body commit (bila tidak sepele), beserta footer `Phase: <N>`.
- **Setiap commit harus dapat dibangun dan lolos test**:
  - `cargo fmt --all -- --check`
  - `cargo clippy --all-targets --all-features -- -D warnings`
  - `cargo test --all-targets --all-features`
  - Commit tidak boleh merusak fungsionalitas `git bisect`.
- **Branch per fase**:
  - Kerjakan pada branch `phase/<N>-<nama>` (misal `phase/0-foundation`, `phase/1-probe`).
  - Setelah seluruh kriteria selesai terpenuhi, lakukan merge ke `main` dengan `git merge --no-ff`.
  - Berikan annotated tag `phase-<N>-done` pada commit hasil merge.
- **Arsitektur dan Keputusan Desain (ADR)**:
  - Keputusan desain penting dicatat singkat di `docs/decisions/NNNN-judul.md`.

## 2. Aturan Kualitas Kode

- Rust edition 2021+ dengan async runtime `tokio`.
- Tidak ada `unwrap()` / `expect()` pada kode non-test tanpa komentar penjelasan yang jelas.
- Dokumentasi modul publik (`///` dan `//!`) wajib disediakan.
- Sensitivitas data: Jangan pernah mencatat cookie, header `Authorization`, atau credential token ke dalam log.
- Kode spesifik platform OS hanya boleh berada di modul `platform` (`cfg(unix)` / `cfg(windows)`).
