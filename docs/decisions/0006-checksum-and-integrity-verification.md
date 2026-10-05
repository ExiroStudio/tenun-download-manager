# 6. Integritas Data, Ekstraksi Checksum Otomatis, dan Resolusi Clash Berkas

Date: 2026-10-05
Status: Accepted

## Konteks
Dalam unduhan berkecepatan tinggi multi-koneksi, korupsi data dapat terjadi akibat kerusakan perantara jaringan (*bit rot*, proxy transparan yang korup, transmisi terpotong), atau gangguan sistem penyimpanan lokal. Selain itu, pengguna sering mengunduh berkas yang sama berulang kali atau mengalami benturan nama berkas (*file clash*) di direktori tujuan unduhan.

Tantangan utama yang dihadapi:
1. **Verifikasi Kriptografis Streaming ($O(1)$ Memori)**: Memverifikasi berkas puluhan atau ratusan gigabyte tanpa pernah memuat seluruh isi berkas ke RAM.
2. **Ekstraksi Checksum Otomatis**: Mendeteksi dan memanfaatkan metadata integritas standar industri dari HTTP response headers tanpa mewajibkan pengguna mengetik hash secara manual setiap saat.
3. **Pencegahan Korupsi Target File**: Memastikan berkas `.part` yang tidak lolos verifikasi integritas tidak pernah menimpa atau merusak berkas target final.
4. **Strategi Resolusi Konflik (File Clash)**: Memberikan fleksibilitas bagi pengguna dalam menangani tabrakan nama berkas (Rename, Overwrite, Deduplicate).
5. **Deduplikasi Cerdas**: Melewatkan (*skip*) unduhan secara aman jika berkas lokal yang sudah ada memiliki checksum yang identik dengan remote file.

## Keputusan
1. **Streaming Hashing dengan Pustaka Kriptografis Murni**:
   - Mendukung algoritma SHA-256 (`sha2`), SHA-1 (`sha1`), dan MD5 (`md-5`).
   - Komputasi checksum menggunakan buffer chunk sekuensial sebesar 64 KiB, menjamin penggunaan memori konstan $O(1)$ dan kecepatan throughput maksimal.
2. **Ekstraksi Checksum Otomatis dari Header HTTP**:
   - Mendukung header RFC 3230 / RFC 5843 / RFC 9530 `Digest` (misal `SHA-256=<base64>` atau `MD5=<hex>`).
   - Mendukung header RFC 1864 `Content-MD5` (digest MD5 16-byte base64-encoded).
   - Mendukung header `ETag` standar Amazon S3 / web server Apache/Nginx (deteksi string 32-karakter heksadesimal). ETag multipart S3 (yang mengandung tanda hubung `-`) secara aman diabaikan karena bukan raw MD5.
3. **Pencegahan Korupsi Berkas & Forensik**:
   - Verifikasi checksum dijalankan pada berkas `.part` sebelum operasi `atomic_rename_file` ke target final.
   - Jika terjadi ketidakcocokan hash, unduhan dibatalkan dengan error `TenunError::IntegrityMismatch`. Berkas `.part` dipertahankan di disk untuk analisis forensik, dan berkas target final tidak dibuat/ditimpa.
4. **Strategi `ClashResolution`**:
   - `Rename` (default): Menambahkan penomoran unik seperti `file (1).ext`, `file (2).ext`.
   - `Overwrite`: Menggantikan berkas lama di lokasi tujuan secara atomik.
   - `Deduplicate`: Jika berkas tujuan sudah ada dan memiliki checksum yang cocok dengan remote/expected checksum, proses langsung selesai sukses (`Ok(path)`) dengan melewatkan transfer data. Jika checksum berbeda atau tidak ada checksum pembanding, sistem secara aman beralih ke strategi `Rename` untuk mencegah kehilangan data yang tidak disengaja.
5. **CLI Subperintah `verify` & Opsi `get`**:
   - Subperintah `tenun verify <file>` mendukung `--sha256`, `--sha1`, `--md5`, dan `--checksum-file` (format GNU `SHA256SUMS`).
   - Perintah `tenun get` mendukung `--sha256`, `--sha1`, `--md5`, dan `--clash <rename|overwrite|deduplicate>`.

## Konsekuensi
- Berkas unduhan dijamin 100% utuh dan bebas dari korupsi data.
- Ekstraksi otomatis dari server modern (S3, CDN, mirror server GNU) mengeliminasi langkah manual verifikasi bagi pengguna.
- Penghematan bandwidth dan waktu secara masif pada pengunduhan file duplikat berkat deduplikasi lokal.
