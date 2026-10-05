# 2. Mekanisme Probe dan Unduhan Aliran Tunggal

Date: 2026-10-05
Status: Accepted

## Konteks
Download manager memerlukan cara yang andal untuk mendeteksi karakteristik file remote (ukuran, dukungan HTTP Range, ETag, timestamp, nama file) sebelum memulai proses download, serta mekanisme unduhan aliran tunggal yang tahan terhadap crash dan lock file di Windows.

Banyak server HTTP atau CDN di internet memiliki implementasi `HEAD` yang rusak (mengembalikan ukuran berbeda, memblokir request, atau tidak menyertakan header Range), sedangkan request `GET` dengan `Range: bytes=0-0` dieksekusi secara konsisten oleh stack HTTP server.

Selain itu, kompresi otomatis (gzip/deflate/brotli/zstd) yang dilakukan oleh klien HTTP standar merusak korespondensi antara offset byte fisik dan aliran stream, yang sangat berbahaya bagi download manager segmentasi.

## Keputusan
1. **Probe menggunakan GET `Range: bytes=0-0`**:
   - `206 Partial Content`: Server mendukung Range, ukuran total diekstrak dari header `Content-Range: bytes 0-0/TOTAL`.
   - `200 OK`: Server tidak mendukung Range, ukuran total diambil dari `Content-Length`.
   - `416 Range Not Satisfiable`: Dideteksi khusus untuk berkas kosong (0 byte) dengan header `Content-Range: bytes */0`.
2. **Deaktivasi Kompresi & Enforce `identity`**:
   - Menonaktifkan auto-decompression pada `reqwest::ClientBuilder` (`no_gzip`, `no_brotli`, `no_deflate`, `no_zstd`).
   - Selalu mengirim `Accept-Encoding: identity`.
3. **Prioritas Nama File**:
   - `Content-Disposition` diprioritaskan: `filename*` (RFC 5987 / RFC 6266 dengan percent-encoding) diutamakan di atas `filename="..."`.
   - Fallback ke segmen terakhir path URL yang didecode.
   - Fallback akhir ke `"download"`.
   - Seluruh nama disanitasi menggunakan `sanitize_filename` (menghapus karakter ilegal, reserved names Windows, trailing spaces/dots).
   - Resolusi bentrok otomatis ke pola `nama (1).ext`.
4. **Lifecycle Berkas `.part` dan Windows Lock**:
   - Seluruh data streaming ditulis ke `<nama>.part`.
   - Handle file ditutup (`drop`) sebelum memanggil `atomic_rename_file` untuk mencegah error permission di Windows.
5. **Decoupled Event Stream**:
   - Progres dilaporkan via `tokio::sync::mpsc::UnboundedSender<DownloadEvent>`.

## Konsekuensi
- Arsitektur event stream ini siap diintegrasikan langsung ke UI desktop Slint dan antrean download tanpa modifikasi engine.
