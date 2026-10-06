# 8. Antarmuka Pengguna Desktop dengan Slint dan Arsitektur Single-Instance

Date: 2026-10-06
Status: Accepted

## Konteks
Setelah core engine `DownloadManager` dan persistensi SQLite selesai di Fase 6, Tenun memerlukan antarmuka grafis desktop lengkap yang:
1. **Prinsip Desain Modern, Tenang, dan Minimalis**: Berbeda dengan aplikasi download manager tradisional yang penuh toolbar dan tabel padat membingungkan, Tenun mengusung tampilan satu kolom yang bersih (maksimal ±760 px di tengah jendela) dengan hanya satu aksi utama yang selalu tampak (tempel tautan, lalu Unduh). Seluruh string UI menggunakan bahasa Inggris untuk konsistensi internasional.
2. **Pemisahan Threading & Event Loop**: Antarmuka grafis tidak boleh memblokir transfer jaringan atau I/O disk. Sebaliknya, worker unduhan yang sangat sibuk tidak boleh membanjiri event loop UI dengan ribuan event per detik yang dapat membekukan rendering antarmuka pengguna.
3. **Penyegaran Progres Terkendali (*Throttling*)**: Pada saat puluhan unduhan berjalan paralel, pembaruan model data antarmuka harus dibatasi (±10 Hz / 100 ms) agar konsumsi CPU dan render frame tetap efisien.
4. **Integrasi Ekosistem Desktop Modern**:
   - Deteksi tautan clipboard otomatis (`arboard`).
   - Dialog pemilihan folder direktori native (`rfd`).
   - Pembukaan berkas dan folder hasil unduhan melalui default system explorer (`open`).
   - Notifikasi penyelesaian unduhan desktop (`notify-rust`).
   - Proteksi instans tunggal (*Single Instance IPC*) menggunakan soket lokal / named pipe (`interprocess`).
   - Fallback rendering perangkat lunak jika inisialisasi GPU/OpenGL gagal.
   - Penonaktifan konsol terminal pada Windows rilis (`windows_subsystem = "windows"`).

## Keputusan
1. **Slint Declarative UI Framework**:
   - Memilih framework UI Slint dengan compiler `slint-build` dan arsitektur komponen terisolasi dalam folder `crates/tenun-ui/ui/`:
     - `theme.slint`: Definisi token warna (light & dark mode), tipografi, dan metrik sudut membulat.
     - `common.slint`: Komponen tombol (`PrimaryButton`, `SecondaryButton`, `IconButton`, `TextLinkButton`), input kustom (`CustomInput`), dan indikator progres tipis 4px (`ThinProgressBar`).
     - `add_bar.slint`: Bilah penambahan unduhan dengan input URL, tombol "Download", dan chip saran clipboard.
     - `tab_bar.slint`: Tab navigasi berbasis teks ("All", "Downloading", "Completed") dengan badge jumlah dan garis aksen `#3B5BDB`.
     - `download_row.slint`: Baris item unduhan dengan judul berkas, ukuran, kecepatan, ETA, tombol jeda/lanjut, "Open", dan menu aksi `⋯`.
     - `add_dialog.slint`, `detail_dialog.slint`, `settings_dialog.slint`, `action_menu.slint`: Dialog modal yang rapi dan elegan.
     - `app.slint`: Kontainer root `MainWindow` dengan lebar maksimal 760 px yang responsif.

2. **Isolasi Runtime Tokio dan Engine Actor**:
   - Tokio runtime dijalankan pada thread OS terpisah di latar belakang (`std::thread::spawn`).
   - Thread antarmuka pengguna (Slint) hanya berkomunikasi dengan aktor `DownloadManager` melalui perintah non-blocking dan membaca event dari broadcast receiver.
   - Menghindari kepanikan reactor dengan memasuki guard runtime Tokio (`rt_handle.enter()`) saat inisialisasi manager.

3. **Throttled Batch UI Refresh (~10 Hz)**:
   - State unduhan di-cache di memori (`AppState`) dan ditandai kotor (`dirty = true`) saat ada event baru dari worker unduhan.
   - Timer periodik Slint pada UI thread (`slint::Timer`) berjalan setiap 100 ms (10 Hz). Jika state kotor, pembaruan model data `VecModel<DownloadItemData>`, badge tab, dan ringkasan footer diproses sekaligus dalam satu siklus render.

4. **Single-Instance IPC Menggunakan `interprocess`**:
   - Tenun mendengarkan soket domain Unix lokal (`tenun-download-manager.sock`) pada Linux atau named pipe pada Windows.
   - Jika pengguna meluncurkan proses Tenun kedua (misalnya dari terminal atau integrasi browser dengan argumen URL), proses kedua mendeteksi instans pertama, mengirimkan URL lewat stream IPC, dan langsung keluar dengan aman. Instans utama menangkap URL tersebut dan membuka dialog tambah unduhan.

5. **Desktop Integrations & Fallbacks**:
   - `arboard`: Melakukan polling berkala (1,5 detik) pada clipboard pengguna. Jika ditemukan URL HTTP/HTTPS baru, chip saran ditampilkan tanpa mengganggu alur kerja pengguna.
   - `rfd`: Dialog folder asli sistem operasi untuk pemilihan folder simpan dan folder unduhan default di Pengaturan.
   - `open`: Membuka berkas hasil unduhan atau membuka folder tempat berkas disimpan.
   - `notify-rust`: Mengirim notifikasi penyelesaian unduhan desktop secara elegan.
   - **Software Renderer Fallback**: Jika pembuatan jendela gagal akibat keterbatasan OpenGL atau driver display, aplikasi otomatis beralih ke backend perangkat lunak (`SLINT_BACKEND = "software"`).

## Konsekuensi
- Pengguna mendapatkan antarmuka grafis desktop modern, hening, dan sangat responsif.
- Konsumsi CPU tetap rendah bahkan saat mengunduh berkas dengan puluhan koneksi segmen secara simultan.
- Seluruh rantai unduhan dari input URL, pemindaian metadata (*probe*), pembagian segmen, hingga notifikasi penyelesaian terintegrasi penuh secara native.
