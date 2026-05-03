# 📋 Project Plan — Tugas 2: Distributed Synchronization System
> **Mata Kuliah:** Sistem Parallel dan Terdistribusi  
> **Deadline:** 3 Mei 2026, pukul 18.00 WITA  
> **Bobot:** 30% dari total nilai akhir  
> **Estimasi Waktu:** ~5 jam kerja

---

## ✅ Checklist Pengumpulan

- [ ] Link GitHub repository (public)
- [ ] Link YouTube video (publik, 10–15 menit)
- [ ] PDF report → `report_[NIM]_[Nama].pdf`
- [ ] Screenshots hasil testing
- [ ] File `.env` (tanpa data sensitif)

---

## 🗂️ Struktur Project

```
distributed-sync-system/
├── src/
│   ├── nodes/
│   │   ├── __init__.py
│   │   ├── base_node.py
│   │   ├── lock_manager.py
│   │   ├── queue_node.py
│   │   └── cache_node.py
│   ├── consensus/
│   │   ├── __init__.py
│   │   ├── raft.py
│   │   └── pbft.py          # opsional (bonus)
│   ├── communication/
│   │   ├── __init__.py
│   │   ├── message_passing.py
│   │   └── failure_detector.py
│   └── utils/
│       ├── __init__.py
│       ├── config.py
│       └── metrics.py
├── tests/
│   ├── unit/
│   ├── integration/
│   └── performance/
├── docker/
│   ├── Dockerfile.node
│   └── docker-compose.yml
├── docs/
│   ├── architecture.md
│   ├── api_spec.yaml
│   └── deployment_guide.md
├── benchmarks/
│   └── load_test_scenarios.py
├── requirements.txt
├── .env.example
├── README.md
└── report.pdf
```

---

## 🎯 Rincian Poin & Target

### Core Requirements — 70 poin (WAJIB)

#### A. Distributed Lock Manager `(25 poin)`
- [ ] Implementasi distributed lock → algoritma **Raft Consensus**
- [ ] Minimum **3 nodes** yang saling berkomunikasi
- [ ] Support **shared** dan **exclusive** locks
- [ ] Handle **network partition** scenarios
- [ ] Implementasi **deadlock detection** untuk distributed environment

#### B. Distributed Queue System `(20 poin)`
- [ ] Implementasi distributed queue → **consistent hashing**
- [ ] Support multiple **producers** dan **consumers**
- [ ] Implementasi **message persistence** dan recovery
- [ ] Handle **node failure** tanpa kehilangan data
- [ ] Support **at-least-once delivery** guarantee

#### C. Distributed Cache Coherence `(15 poin)`
- [ ] Implementasi cache coherence protocol → pilih salah satu: **MESI / MOSI / MOESI**
- [ ] Support multiple cache nodes
- [ ] Handle **cache invalidation** dan update propagation
- [ ] Implementasi cache replacement policy → **LRU** atau **LFU**
- [ ] **Performance monitoring** dan metrics collection

#### D. Containerization `(10 poin)`
- [ ] Buat **Dockerfile** untuk setiap komponen
- [ ] Implementasi **docker-compose** untuk orchestration
- [ ] Support **scaling nodes** secara dinamis
- [ ] Konfigurasi environment via file **`.env`**

---

### Documentation & Reporting — 20 poin (WAJIB)

#### A. Technical Documentation `(10 poin)`
- [ ] Arsitektur sistem lengkap + **diagram**
- [ ] Penjelasan algoritma yang digunakan
- [ ] **API documentation** dengan OpenAPI/Swagger spec (`api_spec.yaml`)
- [ ] **Deployment guide** dan troubleshooting (`deployment_guide.md`)

#### B. Performance Analysis Report `(10 poin)`
- [ ] Benchmarking dengan berbagai skenario
- [ ] Analisis **throughput**, **latency**, dan **scalability**
- [ ] Comparison **single-node vs distributed**
- [ ] **Grafik dan visualisasi** performa

---

### Video Demonstration — 10 poin (WAJIB)

- [ ] Upload ke **YouTube** (publik)
- [ ] Durasi: **10–15 menit**
- [ ] Bahasa Indonesia, jelas dan profesional
- [ ] Struktur video:

| Segmen | Durasi |
|---|---|
| Pendahuluan & tujuan | 1–2 menit |
| Penjelasan arsitektur sistem | 2–3 menit |
| Live demo semua fitur | 5–7 menit |
| Performance testing | 2–3 menit |
| Kesimpulan & tantangan | 1–2 menit |

- [ ] Cantumkan link video di README dan report

---

## ⭐ Bonus Features (Maks +15 poin)

| Pilihan | Poin | Target |
|---|---|---|
| **D. Security & Encryption** (E2E + RBAC + audit log) | +5 | [ ] |

> 💡 Rekomendasi: fokus dulu ke core, lalu kejar **Bonus D (Security)** karena relatif modular.

---

## 🛠️ Tech Stack

| Kategori | Tools |
|---|---|
| **Bahasa** | Python 3.8+ (asyncio) |
| **Container** | Docker, Docker Compose |
| **State** | Redis |
| **Networking** | asyncio, aiohttp, atau ZeroMQ |
| **Testing** | pytest, Locust (load testing) |
| **Optional** | gRPC, Prometheus + Grafana, Kubernetes, Apache Kafka |

---

## 📅 Rencana Pengerjaan

> Deadline: **3 Mei 2026, 18.00 WITA** — sisa waktu sangat mepet, prioritaskan core dulu.

| Urutan | Komponen | Estimasi | Status |
|---|---|---|---|
| 1 | Setup project structure + Docker skeleton | 30 menit | [ ] |
| 2 | Raft Consensus + Distributed Lock Manager | 90 menit | [ ] |
| 3 | Distributed Queue (consistent hashing) | 60 menit | [ ] |
| 4 | Cache Coherence (MESI/LRU) | 45 menit | [ ] |
| 5 | Docker Compose + `.env` config | 30 menit | [ ] |
| 6 | Benchmarking + performance metrics | 30 menit | [ ] |
| 7 | Dokumentasi + PDF report | 30 menit | [ ] |
| 8 | Rekam & upload video YouTube | 45 menit | [ ] |
| **Total** | | **~6 jam** | |

---

## 📎 Referensi

- [Raft Consensus Algorithm Paper](https://raft.github.io/raft.pdf)
- [Redis Distributed Lock Docs](https://redis.io/docs/manual/patterns/distributed-locks/)
- [Distributed Systems: Principles and Paradigms — Tanenbaum]
- [Designing Data-Intensive Applications — Kleppmann]
- [MIT Distributed Systems Course (YouTube)](https://www.youtube.com/watch?v=HJB3Q5xb8U8)
- [PBFT Paper](https://pmg.csail.mit.edu/papers/osdi99.pdf)

---

> ⚠️ **Catatan:** Late submission dikenai penalti **-10% per hari**. Pastikan semua link (GitHub + YouTube) aktif dan dapat diakses sebelum deadline.