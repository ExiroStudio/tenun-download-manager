# 5. Ketahanan Jaringan, Dynamic Re-Split (Work Stealing), dan Token Bucket Speed Limiter

Date: 2026-10-05
Status: Accepted

## Konteks
Unduhan file berukuran besar di jaringan nyata sering kali berhadapan dengan koneksi yang tidak stabil, gangguan sementara (*packet loss*, *connection reset*), server yang membatasi laju permintaan (*rate limiting* / HTTP 429), server yang mengalami beban berlebih (*server error* 5xx), serta koneksi worker yang macet (*stall*).

Selain itu, dalam unduhan multi-segmen paralel, jika salah satu segmen selesai jauh lebih cepat daripada segmen lain, koneksi worker tersebut menjadi menganggur sementara worker lain masih memproses sisa data yang besar. Pengguna juga memerlukan kemampuan untuk membatasi konsumsi bandwidth (*speed limiting*) secara presisi agar tidak mengganggu aktivitas jaringan lainnya di komputer.

Tantangan utama yang dihadapi:
1. **Klasifikasi Error**: Membedakan antara kesalahan sementara (*transient* / retryable), resource yang kedaluwarsa (*needs refresh*), dan error fatal.
2. **Thundering Herd & Backoff**: Menghindari pengulangan koneksi serentak yang membebani server saat terjadi gangguan.
3. **Deteksi Segmen Macet (Stall Detection)**: Menangani koneksi TCP *half-open* atau *hang* di mana socket tidak ditutup tetapi tidak ada byte baru yang mengalir.
4. **Sinkronisasi Dynamic Re-Split (Work Stealing)**: Membagi sisa beban segmen yang aktif ke worker yang menganggur dengan jaminan invarian mutlak: tidak ada byte yang diunduh dua kali dan tidak ada byte yang terlewat.
5. **Akurasi Speed Limiting**: Mengontrol laju pengunduhan per-download atau global dalam toleransi ketat ±10% tanpa membebani CPU.

## Keputusan
1. **Klasifikasi Error & Retry Policy Terukur**:
   - `ErrorKind::Retryable`: Menangani kegagalan soket/I/O, connection reset, timeout, HTTP 500/502/503/504, dan HTTP 429.
   - Header `Retry-After` (baik dalam format detik maupun HTTP Date) dihormati secara ketat.
   - Strategi backoff menggunakan exponential backoff dipadukan dengan random jitter (SplitMix64) untuk memecah konkurensi retry.
   - `ErrorKind::NeedsRefresh`: Mengidentifikasi HTTP 401/403 atau token bertanda tangan kedaluwarsa sehingga antrean/UI dapat memperbarui URL tanpa membuang byte `.part`.
   - `ErrorKind::Fatal`: HTTP 404, 410, dll. langsung menghentikan operasi tanpa retry sia-sia.
2. **Stall Detection Kooperatif**:
   - Setiap worker memantau waktu kedatangan tiap chunk dengan `tokio::time::timeout`.
   - Jika tidak ada byte yang diterima dalam kurun waktu `stall_timeout`, koneksi diputus sepihak dan diulang dari byte offset terakhir yang tersimpan di disk.
3. **Dynamic Re-Split dengan Shared Atomic Boundary & Synchronized Chunk Reservation**:
   - `DynamicSegment` menyimpan batas akhir (`end`) dan progres (`downloaded`) yang disinkronisasi melalui mutex ringan nan cepat.
   - Ketika sebuah worker idle, `SegmentManager::find_and_split_largest` mencari segmen aktif dengan sisa byte terbesar (`remaining >= 2 * min_segment_size`).
   - Batas segmen aktif dipangkas secara atomik menjadi `mid - 1`, dan segmen baru `[mid, old_end]` diserahkan ke worker yang idle.
   - Worker aktif yang sedang mengunduh hanya dapat memesan (`reserve_chunk`) dan menulis data hingga batas `mid - 1`. Segmen baru mengambil alih sisa data dari `mid` sampai `old_end`.
   - Invarian terjaga: cakupan interval `[0, total_size)` tetap gapless dan non-overlapping 100%.
4. **Token Bucket Speed Limiter Berbasis Virtual-Time**:
   - Menggunakan model reservasi waktu virtual (*next allowed instant*) dengan kapasitas burst terukur.
   - Semua worker berbagi instans `TokenBucketLimiter` yang sama untuk per-download throttle.
   - Memberikan pembagian bandwidth adil (FIFO) antar worker dan akurasi tinggi (±10% pada jaringan aktual).
5. **Opsi CLI `--limit-rate`**:
   - Parsing format manusiawi: `500K`, `1M`, `2MB`, `1048576`.

## Konsekuensi
- Unduhan tetap tangguh dan berhasil menyelesaikan file secara utuh meskipun jaringan putus-nyambung atau server mengembalikan respons 503/429.
- Dynamic re-split memaksimalkan utilisasi koneksi sehingga total waktu unduh menjadi optimal ala IDM.
- Batas kecepatan terukur dan stabil tanpa fluktuasi liar.
