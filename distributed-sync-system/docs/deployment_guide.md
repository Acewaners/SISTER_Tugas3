# Panduan Deployment & Troubleshooting

Panduan ini berisi langkah-langkah untuk melakukan *deployment* dan pemeliharaan Sistem Sinkronisasi Terdistribusi.

## Prasyarat (Prerequisites)
- **Python 3.12+**
- **Docker** dan **Docker Compose**
- **Git**

---

## 1. Deployment Lokal (Native Python)
Untuk *debugging* dan pengujian manual, Anda bisa menjalankan setiap node secara langsung di komputer Anda menggunakan beberapa jendela terminal.

### Instalasi
```bash
git clone <repository_url>
cd distributed-sync-system
pip install -r requirements.txt
```

### Menjalankan Kluster
Buka tiga jendela terminal secara terpisah dan jalankan perintah berikut:
- **Terminal 1:** `python -m src.nodes.__main__ --node-id node_1 --port 8001`
- **Terminal 2:** `python -m src.nodes.__main__ --node-id node_2 --port 8002`
- **Terminal 3:** `python -m src.nodes.__main__ --node-id node_3 --port 8003`

### Menjalankan Rangkaian Tes (Test Suite)
Buka terminal ke-4 dan jalankan urutan tes otomatis dengan jeda *step-by-step*:
```bash
python test_sequence.py
```

---

## 2. Containerized Deployment (Menggunakan Docker)
Untuk menyerupai lingkungan *Production*, sistem ini sepenuhnya sudah di-containerize.

### Menjalankan Kluster
Masuk ke direktori `docker/` dan jalankan perintah ini:
```bash
cd docker
docker-compose up --build
```
Perintah ini akan secara otomatis mem-*build* image dari `Dockerfile.node`, menyalakan 3 container yang saling terhubung, dan menyuntikkan (inject) variabel `PEERS` untuk memastikan jaringan virtual berjalan dinamis.

### Mematikan Kluster
```bash
docker-compose down
```

---

## 3. Konfigurasi (`.env`)
Perilaku sistem bisa diubah dan dikonfigurasi melalui file `.env`. Variabel-variabel penting di antaranya:
- `ELECTION_TIMEOUT_MIN` & `ELECTION_TIMEOUT_MAX`: Mengatur seberapa cepat pemilihan Leader Raft dilakukan.
- `VIRTUAL_NODES`: Menentukan jumlah cincin virtual per node fisik untuk algoritma *Consistent Hashing* (default: 150).
- `CACHE_MAX_SIZE`: Ambang batas sebelum cache lama dihapus oleh kebijakan *LRU*.

---

## 4. Troubleshooting (Penyelesaian Masalah) & Kendala Umum

### Kendala: Terjebak di status "WON ELECTION" tiada henti (Split Vote / Failover loop)
- **Penyebab:** Ada masalah jaringan yang menghalangi respons voting, atau tabrakan pesan *heartbeat* antar komponen.
- **Solusi:** Pastikan Anda menggunakan `127.0.0.1` saat di lokal daripada `localhost` untuk menghindari error resolusi IPv6 (`::1`) di Windows. Jika menggunakan Docker, pastikan variabel `PEERS` sudah disetel dengan format IP:Port yang benar di `docker-compose.yml`.

### Kendala: Error "Address already in use"
- **Penyebab:** Proses Python sebelumnya belum ditutup secara sempurna, atau port 8001/8002/8003 masih terpakai oleh aplikasi lain.
- **Solusi:** Matikan (Kill) proses Python yang masih tersangkut (zombie) atau *restart* container Docker (`docker-compose restart`).

### Kendala: Terjadi Deadlock di log
- **Penyebab:** Jaringan terputus (Network partition) atau beberapa *client* meminta akses *EXCLUSIVE* pada waktu yang bersamaan persis.
- **Solusi:** *Lock Manager* sudah dilengkapi sistem auto-expired (`LOCK_DEFAULT_TTL=30`). Cukup tunggu selama 30 detik agar kunci yang tersangkut dilepas secara otomatis.
