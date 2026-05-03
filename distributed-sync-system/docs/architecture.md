# Sistem Sinkronisasi Terdistribusi - Arsitektur

## 1. Gambaran Umum Sistem
Sistem Sinkronisasi Terdistribusi adalah platform multi-node yang kuat, dirancang untuk menyediakan primitif terdistribusi esensial: **Manajemen Kunci (Locking) berbasis Konsensus**, **Antrean Pesan Terpartisi (Message Queue)**, **Koherensi Cache**, dan **Deteksi Kegagalan (Failure Detection)**.

Sistem ini berjalan sebagai kluster node peer-to-peer, di mana setiap node menjalankan server Python `asyncio` yang melayani permintaan TCP berbasis JSON.

```mermaid
graph TD
    subgraph External_Layer [Client Interface]
        Client((Client Application))
    end

    subgraph Cluster_Network [Docker Overlay Network]
        direction TB
        
        subgraph Node_N [Distributed Node Instance]
            direction TB
            Base[Base Node API]
            
            subgraph Services [Synchronization Services]
                LM[Lock Manager]
                QN[Queue Node]
                CN[Cache Node]
            end
            
            subgraph Consensus_Layer [Consistency Engine]
                Raft[Raft Consensus]
                State[(State Machine)]
            end
            
            subgraph Network_Layer [Communication Layer]
                MP[Message Passing]
                FD[Failure Detector]
            end
        end
    end

    Client -->|HTTP/JSON| Base
    Base --> Services
    
    LM <--> Raft
    QN <--> Raft
    CN <--> FD
    
    Raft <--> State
    Raft <--> MP
    FD <--> MP
    
    MP <==>|Internal RPC / TCP| Peer_Nodes[...]

    style Node_N fill:#fdfdfd,stroke:#333,stroke-width:2px
    style Services fill:#fff3e0,stroke:#f57c00
    style Consensus_Layer fill:#e1f5fe,stroke:#0288d1

## 2. Komponen Utama

### A. Konsensus Raft & Distributed Lock Manager
**Tujuan:** Mencegah masalah *split-brain* dan memastikan hak akses eksklusif ke sumber daya (resource) yang tersebar di seluruh kluster.
- **Pemilihan Leader (Election):** Node menggunakan algoritma Raft untuk memilih satu Leader. Follower memiliki waktu tunggu acak (3.0 detik - 6.0 detik) dan akan memulai pemilihan baru jika tidak menerima sinyal dari Leader.
- **Replikasi Log:** Semua perubahan status kunci (acquire/release) dirutekan melalui Leader.
- **Manajemen Kunci (Locking):** Mendukung dua mode kunci: `SHARED` (banyak pembaca) dan `EXCLUSIVE` (satu penulis). Deteksi *deadlock* diimplementasikan menggunakan batas waktu (timeout).

```mermaid
sequenceDiagram
    participant Client
    participant Leader
    participant Follower
    
    Client->>Leader: lock_acquire (resource_A, EXCLUSIVE)
    Leader->>Leader: Tambahkan ke Log Lokal
    Leader->>Follower: AppendEntries (Replikasi Log)
    Follower-->>Leader: Ack (Konfirmasi)
    Leader->>Leader: Commit Log & Berikan Kunci
    Leader-->>Client: Success = True
```

### B. Distributed Queue (Antrean Terdistribusi - Consistent Hashing)
**Tujuan:** Mendistribusikan beban pesan secara merata ke seluruh node.
- **Cincin Consistent Hashing:** Menggunakan mekanisme hash node virtual (150 node virtual per node fisik) untuk memetakan partisi/topik antrean ke node fisik tertentu.
- **Garansi At-Least-Once Delivery:** Pesan akan tetap berada di dalam antrean sampai konsumen secara eksplisit mengirimkan `ACK` (konfirmasi terima).
- **Rebalancing:** Jika sebuah node mati (crash), node virtual miliknya akan dihapus dari cincin, dan topik antrean akan dipindahkan dengan mulus ke node terdekat yang tersedia.

### C. Distributed Cache Coherence (Protokol MESI)
**Tujuan:** Menjaga konsistensi data saat menyimpan kunci yang identik (cache) di beberapa node sekaligus.
- **Status MESI:**
  - **M**odified: Hanya cache ini yang memiliki salinan valid (dirty).
  - **E**xclusive: Hanya cache ini yang memiliki data tersebut (clean).
  - **S**hared: Beberapa cache memiliki data yang sama.
  - **I**nvalid: Data pada cache ini sudah usang (tidak valid).
- **Kebijakan LRU:** Ketika kapasitas cache mencapai `CACHE_MAX_SIZE` (default 1000), data yang Paling Lama Tidak Digunakan (Least Recently Used) akan dihapus secara otomatis.

### D. Failure Detector (Protokol SWIM-like)
**Tujuan:** Mengidentifikasi kerusakan node dan memicu konfigurasi ulang kluster.
- **Heartbeats:** Ada proses berjalan di latar belakang yang saling menyapa peer node setiap beberapa detik (`fd_heartbeat`).
- **State Machine:** Status node dilacak sebagai `Alive` (Hidup), `Suspected` (Dicurigai), atau `Dead` (Mati). Jika sebuah node melewatkan beberapa heartbeat berturut-turut, statusnya berubah menjadi dead, lalu memicu pergantian Leader (Failover) dan penyesuaian (Rebalancing) antrean.

---

## 3. Teknologi yang Digunakan
- **Inti Program:** Python 3.12 (Asyncio, Socket)
- **Containerization:** Docker & Docker Compose
- **Penyimpanan State:** In-Memory (Simulasi) / Redis (Opsional)
- **Jaringan:** Protokol TCP JSON Kustom