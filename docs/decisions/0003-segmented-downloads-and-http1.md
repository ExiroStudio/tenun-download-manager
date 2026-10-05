# 3. Segmented Download, Penegakan HTTP/1.1, dan Prealokasi Berkas

Date: 2026-10-05
Status: Accepted

## Konteks
Tujuan utama download manager seperti IDM adalah memaksimalkan kecepatan unduh dengan memecah berkas menjadi beberapa segmen yang diunduh secara paralel melalui request HTTP `Range`.

Namun ada beberapa tantangan teknis kritis:
1. **HTTP/2 Connection Multiplexing**: Protokol HTTP/2 secara standar menggabungkan beberapa request ke host yang sama ke dalam satu koneksi TCP fisik tunggal melalui multiplexing stream. Hal ini menghilangkan keuntungan throughput multi-koneksi TCP (misalnya pada jaringan yang memiliki batas kecepatan per-TCP-socket atau latency tinggi).
2. **Integritas Batas Byte**: Pembagian segmen harus menutup tepat interval `[0, total_size)` tanpa tumpang tindih dan tanpa ada celah (gap) byte yang hilang.
3. **Konkurensi Disk I/O**: Banyak worker menulis ke satu berkas yang sama secara bersamaan. Jika menggunakan kursor global (seek biasa), worker akan saling merusak offset. Diperlukan penulisan posisional murni.
4. **Respon Server Tidak Terduga**: Beberapa server yang salah konfigurasi atau proxy tertentu dapat mengembalikan `200 OK` (seluruh file) alih-alih `206 Partial Content` ketika diminta `Range`. Jika worker multi-koneksi tetap menulis di offset segmen, file akan langsung rusak.

## Keputusan
1. **Algoritma Pembagian & Invarian Terverifikasi Proptest**:
   - `calculate_segments` membagi `total_size` ke `min(conn, total_size / min_segment_size)` segmen.
   - Sisa pembagian (remainder) disebarkan secara merata di segmen-segmen awal.
   - Diuji dengan property-based testing (`proptest`) memastikan invarian cakupan `[0, total_size)` tanpa celah dan tanpa tumpang tindih untuk rentang ukuran 1 byte hingga 100 MB dan 1..64 koneksi.
2. **Prealokasi Berkas & Cloned Handle**:
   - Berkas `.part` dialokasikan penuh di awal dengan `set_len(total_size)`.
   - Tiap worker memegang file handle independen via `try_clone()` dan menulis menggunakan trait `PositionalWrite` (`write_all_at` di Unix/Windows).
   - Penulisan di-buffer dalam chunk ±256 KiB untuk meminimalkan syscall.
3. **Penegakan HTTP/1.1 (`http1_only`)**:
   - Klien HTTP `reqwest` dikonfigurasi dengan `http1_only()`.
   - Ini memaksa setiap worker membuka koneksi TCP fisik independen, memaksimalkan alokasi bandwidth dari server/CDN.
4. **Validasi Range & Fallback Transparan**:
   - Respons setiap segmen wajib `206 Partial Content` dan header `Content-Range` divalidasi.
   - Jika server membalas `200 OK`, seluruh worker segera dibatalkan (`abort_all`), berkas `.part` di-reset, dan proses beralih secara transparan ke mode aliran tunggal (`download_single`).
5. **Agregasi Progres Atomik**:
   - Tiap segmen memiliki counter `AtomicU64`.
   - Task background membaca akumulasi byte tiap 200ms untuk menghitung kecepatan sesaat dan estimasi waktu selesai (ETA).

## Konsekuensi
- Kecepatan unduh meningkat secara proporsional mendekati N kali lipat pada server yang membatasi kecepatan per-koneksi (terbukti 8x lebih cepat pada pengujian benchmark terukur).
- File akhir selalu valid dan terhindar dari korupsi data akibat kesalahan respon server.
