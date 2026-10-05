# 7. Download Manager, Penjadwalan Antrean Berprioritas, dan Persistensi SQLite

Date: 2026-10-05
Status: Accepted

## Konteks
Pada fase-fase sebelumnya, Tenun telah memiliki kemampuan unduhan tunggal dan multisegmen yang tangguh, tahan gangguan jaringan, dan tervalidasi integritasnya. Namun, aplikasi download manager desktop modern (seperti IDM) membutuhkan orchestrator pusat untuk:
1. **Pengelolaan Multi-Unduhan Paralel**: Mengatur banyak unduhan secara bersamaan dengan membatasi beban jaringan dan CPU melalui kuota konkurensi (default 3 paralel).
2. **Penjadwalan Antrean dengan Prioritas**: Memastikan unduhan penting dapat diprioritaskan di atas unduhan lainnya, dengan urutan FIFO untuk item berprioritas setara.
3. **Persistensi Data & Crash Recovery**: Menyimpan daftar unduhan, riwayat, dan metadata secara persisten. Jika sistem mati mendadak atau aplikasi ditutup paksa, proses unduhan aktif harus dapat dipulihkan secara aman tanpa kehilangan data.
4. **Klasifikasi Kategori Berkas & Tata Letak Folder**: Mengelompokkan berkas unduhan ke subfolder yang sesuai (Documents, Compressed, Video, Audio, Images, Programs) berdasarkan ekstensi atau MIME type secara otomatis.
5. **Verifikasi Ruang Penyimpanan**: Memeriksa kapasitas ruang disk yang tersedia sebelum memulai prealokasi berkas besar agar tidak menyebabkan error kepenuhan disk di tengah transfer.
6. **API Stabil & Decoupled untuk UI dan CLI**: Menyediakan interface yang bersih dan aman untuk dikonsumsi oleh UI Slint (Fase 7) dan CLI tanpa mengakses struktur internal secara langsung.

## Keputusan
1. **Arsitektur Actor Model (`DownloadManager` & `ManagerActor`)**:
   - `DownloadManager` berperan sebagai handle ringan yang aman di-clone (`Clone`), thread-safe, dan mengekspos API publik: `add`, `pause`, `resume`, `cancel`, `remove`, `list`, `set_max_concurrent`, `set_global_speed_limit`, `shutdown`, serta `subscribe`.
   - Menggunakan `tokio::sync::mpsc` untuk pengiriman perintah (`ManagerCommand`) ke loop aktor latar belakang (`ManagerActor`).
   - Setiap pekerja unduhan dijalankan dalam task tokio terpisah dengan token pembatalan kooperatif (`CancellationToken`).
   - Event status, progres, penyelesaian, dan kegagalan disiarkan ke semua observer (UI, CLI daemon, logging) melalui channel broadcast berkecepatan tinggi `tokio::sync::broadcast`.

2. **Persistensi SQLite Tertanam (`rusqlite` dengan fitur `bundled`)**:
   - Memilih SQLite tertanam via `rusqlite` dengan fitur `bundled`, sehingga aplikasi tidak bergantung pada keberadaan dynamic library SQLite di sistem operasi host pengguna.
   - Mengaktifkan mode Write-Ahead Logging (WAL) dan penanganan timeout konkurensi:
     `PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000;`
   - Menyimpan atribut unduhan lengkap pada tabel `downloads`: id, uuid, url, final_url, filename, dest_dir, total_bytes, downloaded_bytes, status, category, priority, etag, last_modified, timestamp, options_json, dan pesan error.

3. **Pemulihan Crash Otomatis (*Crash Recovery*)**:
   - Saat inisialisasi aplikasi / manager (`Database::open` atau `recover_interrupted_downloads`), semua rekaman yang tertinggal dalam status aktif (`Downloading`, `Probing`, `Verifying`) secara atomik dialihkan kembali ke status `Paused`.
   - Pengguna dapat melanjutkan unduhan tersebut kapan saja, di mana sistem akan menyambung transfer menggunakan berkas checkpoint `.tenun` dan `.part` tanpa mengunduh ulang dari nol.

4. **Klasifikasi Kategori & Perutean Otomatis (`FileCategory`)**:
   - Ekstensi berkas dan MIME type dipetakan ke enum `FileCategory`: `Documents`, `Compressed`, `Video`, `Audio`, `Images`, `Programs`, atau `General`.
   - Jika pengguna tidak menentukan direktori keluaran kustom, berkas otomatis diarahkan ke `Downloads/<Category>` (misal `Downloads/Compressed` untuk `.zip`, `.tar.gz`, `.iso`).

5. **Pengecekan Ruang Disk Platform**:
   - Mengimplementasikan `available_disk_space` dan `ensure_disk_space` di modul platform:
     - Linux/Unix: Menggunakan syscall POSIX `libc::statvfs`.
     - Windows: Menggunakan API Win32 `GetDiskFreeSpaceExW`.
   - Prealokasi berkas segera dibatalkan dengan error `TenunError::InsufficientDiskSpace` jika kapasitas disk tidak mencukupi ukuran total berkas ditambah margin aman 10 MiB.

6. **Ekstensi Antrean CLI**:
   - Menambahkan subperintah CLI:
     - `tenun add <url> [--output <dir>] [--connections <n>] [--priority <p>] [--category <cat>] [--start-now]`
     - `tenun list [--status <status>] [--category <cat>]`
     - `tenun pause <id>`
     - `tenun resume <id>`
     - `tenun cancel <id>`
     - `tenun remove <id> [--delete-file]`
     - `tenun run [--concurrent <n>] [--limit-rate <rate>]` (daemon pemroses antrean hingga Ctrl+C)

## Konsekuensi
- Manajemen download menjadi terpusat, modular, dan sepenuhnya siap dihubungkan dengan antarmuka desktop Slint pada Fase 7.
- Sistem kebal terhadap restart mendadak atau crash; tidak ada unduhan aktif yang hilang dari database atau berkas rusak tanpa status yang jelas.
- Pengguna mendapatkan pengalaman otomatisasi folder unduhan yang bersih dan terstruktur rapi.
