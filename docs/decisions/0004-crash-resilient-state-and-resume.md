# 4. State Tahan Crash, Format Sidecar `.tenun`, dan Validasi Resume

Date: 2026-10-05
Status: Accepted

## Konteks
Download manager desktop harus mampu menangani interupsi mendadak seperti crash aplikasi, pembatalan proses oleh pengguna, terputusnya koneksi internet, hingga mati lampu tanpa merusak file yang sedang diunduh.

Tantangan utama yang dihadapi:
1. **Inkonsistensi State vs Disk**: Jika counter progres mencatat byte yang baru diterima di memori (buffer) sebelum benar-benar di-flush ke disk, proses resume kelak akan mengasumsikan rentang byte tersebut sudah ada di berkas `.part`, menyebabkan blok data kosong/korup di dalam berkas akhir.
2. **Korupsi File State saat Crash**: Jika listrik mati saat aplikasi sedang menulis file state, berkas state bisa terpotong (truncated) atau korup jika ditulis langsung di tempat (*in-place write*).
3. **Perubahan Berkas di Server**: Jika berkas di server diperbarui saat download sedang dijeda, melanjutkan download dengan asumsi segmen lama valid akan menggabungkan potongan file versi lama dan versi baru (korupsi diam-diam).
4. **Privasi Data Sensitif**: File state tersimpan di disk pengguna, sehingga header otentikasi (Authorization, token, cookie rahasia) tidak boleh bocor ke file state.

## Keputusan
1. **Format Sidecar `.tenun` (JSON Berversi)**:
   - Disimpan berdampingan dengan berkas unduhan sebagai `<nama>.tenun`.
   - Menggunakan skema JSON eksplisit dengan field `version: 1` agar mudah dimigrasikan dan diinspeksi.
   - Header sensitif (`Authorization`, `Cookie`) difilter secara otomatis oleh `PersistedOptions`.
2. **Invarian Konsistensi Disk**:
   - Nilai `downloaded` per segmen hanya diperbarui dan dicatat setelah data berhasil ditulis ke disk dan di-sync (`sync_file`).
   - Setiap worker melakukan `flush` dan `sync_file` sebelum task berakhir atau dijeda.
3. **Pola Penyimpanan Atomik (Write-Sync-Rename)**:
   - State ditulis ke berkas sementara `<nama>.tenun.tmp`.
   - Melakukan `sync_file` (fsync).
   - Memanggil `atomic_rename_file` menimpa berkas `.tenun` lama. Pola ini menjamin berkas `.tenun` selalu dalam kondisi utuh dan valid di disk setiap saat.
4. **Validasi Remote dengan `If-Range`**:
   - Saat resume, klien mengirim `If-Range` dengan nilai `ETag` kuat (atau `Last-Modified`).
   - Jika server membalas `206 Partial Content`, segmen dilanjutkan dari offset `start + downloaded`.
   - Jika server membalas `200 OK` (atau validator berubah), resume ditolak tegas (`TenunError::RemoteFileChanged`) demi melindungi integritas data.
5. **API Kontrol & `CancellationToken`**:
   - Operasi `pause()` menggunakan `CancellationToken`. Worker menghentikan pembacaan jaringan secara kooperatif, menulis sisa buffer ke disk, melakukan fsync, dan menyimpan checkpoint final sebelum keluar.

## Konsekuensi
- Unduhan dapat dihentikan dan dilanjutkan kapan saja dengan keamanan data penuh.
- Proses resume tahan terhadap crash dan melindungi pengguna dari berkas korup jika server mengubah file.
