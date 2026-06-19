# 📖 Panduan Lengkap: Memahami Big Data Pipeline dari Nol

**Untuk:** Siapapun yang ingin memahami apa yang telah kita bangun dan kenapa  
**Proyek:** Flight Delay Analysis Pipeline  
**Dibuat:** 2026-06-18  

---

## 📌 Daftar Isi

1. [Gambaran Besar: Apa yang Kita Bangun?](#1-gambaran-besar)
2. [Docker: Kenapa Tidak Install Langsung?](#2-docker--kenapa-tidak-install-langsung)
3. [Kafka: Pintu Masuk Data](#3-kafka--pintu-masuk-data)
4. [ClickHouse: Gudang Analitik](#4-clickhouse--gudang-analitik)
5. [Spark: Mesin Pengolah](#5-spark--mesin-pengolah)
6. [Grafana: Papan Dashboard](#6-grafana--papan-dashboard)
7. [Alur Data: Dari CSV ke Dashboard](#7-alur-data-dari-csv-ke-dashboard)
8. [Preprocessing: Apa yang Spark Lakukan?](#8-preprocessing-apa-yang-spark-lakukan)
9. [Tabel-Tabel ClickHouse: Kenapa Ada Banyak?](#9-tabel-tabel-clickhouse-kenapa-ada-banyak)
10. [AWS EC2: Kenapa Perlu Cloud?](#10-aws-ec2-kenapa-perlu-cloud)
11. [Strategi Arsitektur: Lokal vs Cloud](#11-strategi-arsitektur-lokal-vs-cloud)
12. [Pertanyaan Umum (FAQ)](#12-pertanyaan-umum-faq)

---

## 1. Gambaran Besar

### Apa masalah yang kita selesaikan?

Pemerintah AS melalui BTS (Bureau of Transportation Statistics) mempublikasikan data keterlambatan seluruh penerbangan domestik dari 1987 hingga sekarang. Satu bulan data saja bisa berisi **500.000 hingga 600.000 baris penerbangan**.

Tantangannya:
- Data mentahnya sangat besar (5 tahun = ~30 juta baris)
- Data mentahnya "kotor" — ada nilai kosong, duplikat, format tidak konsisten
- Tim yang berbeda butuh data dalam format yang berbeda (DS butuh fitur model, Dashboard butuh agregasi)
- Tidak bisa analisis data sebesar itu di Excel atau laptop biasa

**Solusinya: Pipeline Data Bertahap**

Ibarat sebuah pabrik modern:

```
📦 BAHAN BAKU        🏭 PABRIK            📊 PRODUK JADI
Data CSV Mentah  →   Kafka + Spark    →   ClickHouse + Grafana
(kotor, besar)       (cuci, olah)         (bersih, siap analisis)
```

Inilah yang kita bangun. Setiap bagian punya peran spesifik dan tidak bisa saling menggantikan.

---

## 2. Docker: Kenapa Tidak Install Langsung?

### Masalahnya

Bayangkan Anda harus menginstall 5 aplikasi besar di laptop Anda:
- Kafka (perlu Java 11)
- ClickHouse (perlu library sistem tertentu)
- Spark (perlu Java 17, berbeda dari Kafka!)
- Grafana
- Zookeeper (dulu dibutuhkan Kafka)

Setiap aplikasi punya **ketergantungan berbeda**, bisa saling bertabrakan, dan sangat susah di-uninstall kalau ada masalah.

### Solusinya: Docker Container

Docker itu seperti **"kotak plastik kedap udara"** untuk setiap aplikasi. Masing-masing aplikasi hidup di dalam kotaknya sendiri, terisolasi satu sama lain, tidak bisa saling ganggu. Tapi mereka bisa berkomunikasi lewat "lubang" yang kita tentukan (Port).

```
┌─────────────────────────────────────────────┐
│              LAPTOP ANDA                    │
│                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │  Kafka   │  │ClickHouse│  │  Grafana │  │
│  │ Container│  │ Container│  │ Container│  │
│  └──────────┘  └──────────┘  └──────────┘  │
│                                             │
│  ┌──────────┐  ┌──────────┐                 │
│  │  Spark   │  │  Spark   │                 │
│  │  Master  │  │  Worker  │                 │
│  └──────────┘  └──────────┘                 │
└─────────────────────────────────────────────┘
```

### Docker Compose: Manajer Kotak Plastik

File `docker-compose.yml` adalah "resep" yang memberitahu Docker:
- Container apa saja yang perlu dibuat
- Bagaimana cara mereka saling berkomunikasi
- Port mana yang dibuka ke luar
- Data mana yang harus disimpan permanen

Saat Anda menjalankan `docker compose up -d`, Docker membaca resep itu dan menghidupkan semua container sekaligus dalam urutan yang benar (Kafka dulu, baru ClickHouse, baru yang lain).

### Volume: Agar Data Tidak Hilang

Secara default, kalau container dihapus, datanya ikut hilang. Untuk mencegah ini, kita pakai **Volume**. Volume seperti "hard disk eksternal" yang tetap ada meski containernya dihapus lalu dibuat ulang.

```yaml
volumes:
  clickhouse_data:   # Data ClickHouse disimpan di sini
  grafana_data:      # Konfigurasi Grafana disimpan di sini
```

---

## 3. Kafka: Pintu Masuk Data

### Analogi Sederhana

Bayangkan Kafka adalah **ban berjalan (conveyor belt) di pabrik**. Tugasnya cuma satu: mengangkut barang dari titik A ke titik B dengan cepat dan andal. Kafka tidak tahu isi barangnya, tidak peduli, dan tidak mengubahnya.

### Kenapa Tidak Langsung Masukkan ke ClickHouse?

Pertanyaan bagus! Jawabannya ada beberapa:

1. **Kecepatan masuk tidak sama dengan kecepatan proses.** Data bisa datang dalam gelombang besar (spike), sedangkan ClickHouse perlu waktu untuk menyimpan dengan benar. Kafka bertindak sebagai "buffer" — menampung dulu, lalu dikonsumsi perlahan.

2. **Bisa ada banyak konsumen sekaligus.** Kafka memungkinkan banyak pihak membaca data yang sama secara bersamaan — ClickHouse membaca untuk disimpan, Spark membaca untuk diproses, sistem monitoring membaca untuk alert. Kalau langsung ke ClickHouse, tidak bisa seperti itu.

3. **Toleransi kesalahan.** Kalau ClickHouse sementara mati (crash, restart), data di Kafka tidak hilang. ClickHouse bisa melanjutkan membaca dari titik terakhir ketika sudah hidup kembali.

### Topic: Folder di Kafka

Di Kafka, data dikelompokkan dalam "**Topic**". Ibarat folder di Google Drive. Kita punya satu topic utama: `ontime.raw` — tempat semua data penerbangan mentah dikirim.

### Bagaimana Kafka Bekerja di Proyek Ini

```
Script Python           Kafka             ClickHouse
(stream_ontime.py)   (ontime.raw)      (ontime_kafka_raw)
      │                   │                   │
      ├── Baca CSV ──────►│                   │
      │   1000 baris      │                   │
      ├── Baca CSV ──────►│                   │
      │   1000 baris      │   ClickHouse ────►│
      ├── ...             │   otomatis         │
                          │   membaca          │ (disimpan permanen
                          │   pesan dari       │  ke ontime_raw)
                          │   Kafka            │
```

### Port yang Dipakai Kafka

- **Port 9092**: Komunikasi di dalam jaringan Docker (antar container)
- **Port 9094**: Akses dari luar Docker (dari laptop/skrip Python Anda)

Itulah kenapa di `config.py` ada `KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"` — Anda mengakses Kafka dari luar Docker melalui port 9094.

---

## 4. ClickHouse: Gudang Analitik

### Apa Bedanya Dengan MySQL/PostgreSQL Biasa?

Database biasa (MySQL, PostgreSQL) dirancang untuk **transaksi** — baca/tulis satu baris, cepat, sering. Cocok untuk toko online (satu order = satu transaksi).

ClickHouse dirancang untuk **analitik** — baca jutaan baris sekaligus, hitung rata-rata, jumlah, tanpa peduli tulis satu per satu. Cocok sekali untuk proyek kita.

Perbandingan:

| Aspek | MySQL | ClickHouse |
|-------|-------|------------|
| Simpan 1 baris | ✅ Sangat cepat | ⚡ Oke |
| Hitung rata-rata 30 juta baris | ⚠️ Lambat | ✅ Sangat cepat |
| Cocok untuk | Aplikasi web, e-commerce | Analitik, dashboard, BI |

### Kenapa Bisa Secepat Itu?

ClickHouse menyimpan data secara **kolom demi kolom** (columnar storage), bukan baris demi baris. 

Bayangkan Anda punya tabel dengan 100 kolom dan 30 juta baris. Kalau Anda hanya butuh kolom `ArrDelay` untuk dihitung rata-ratanya, ClickHouse cukup membaca 1 dari 100 kolom itu — dan melewatkan 99 kolom lainnya. Database biasa harus baca semua 100 kolom untuk setiap baris.

### Materialized View: Jembatan Otomatis

Di proyek ini ada komponen penting bernama **Materialized View** (`mv_ontime_kafka_to_raw`). Ini adalah "jembatan pintar" yang bekerja otomatis:

```
ontime_kafka_raw  ──(Materialized View)──►  ontime_raw
(tabel Kafka Engine,                         (tabel permanen,
 hanya bisa dibaca                            bisa di-query
 sekali — seperti                             berkali-kali)
 conveyor belt)
```

Tanpa Materialized View, data dari Kafka yang sudah dibaca akan "hilang" (tidak bisa dibaca lagi). Dengan Materialized View, setiap data yang masuk dari Kafka otomatis disalin ke tabel permanen `ontime_raw`.

### Web ClickHouse Play

ClickHouse punya antarmuka web di `http://localhost:8123/play`. Di sana Anda bisa menulis query SQL langsung di browser dan melihat hasilnya. Persis seperti phpMyAdmin untuk MySQL, tapi lebih modern.

---

## 5. Spark: Mesin Pengolah

### Analogi Sederhana

Spark adalah **mesin raksasa di pabrik**. Tugasnya: mengambil bahan baku kotor (dari `ontime_raw`), membersihkannya, menggilingnya, dan menghasilkan produk jadi berkualitas (ke `ontime_curated` dan `ontime_features`).

### Kenapa Tidak Cukup Pakai SQL Biasa?

Logika pembersihan data kadang terlalu kompleks untuk SQL biasa:
- Menghitung percentile ke-99.5 dari 30 juta baris lalu menggunakan hasilnya untuk cap nilai lain
- Mendeteksi duplikat berdasarkan 6 kolom sekaligus
- Membangun fitur historis (rata-rata delay per rute berdasarkan data masa lalu)

Spark memungkinkan kita menulis logika ini dalam bahasa Python (PySpark) yang jauh lebih fleksibel, lalu menjalankannya secara paralel di banyak mesin/core sekaligus.

### Arsitektur Master-Worker

Spark di proyek ini punya dua container:

```
┌─────────────────┐       ┌─────────────────┐
│  spark-master   │       │  spark-worker   │
│                 │       │                 │
│  - Koordinator  │──────►│  - Pelaksana    │
│  - Bagi tugas   │       │  - 1 CPU, 1GB   │
│  - Port 8080    │       │  RAM            │
│    (Web UI)     │       └─────────────────┘
└─────────────────┘
```

**Master**: Seperti manajer pabrik. Menerima perintah, membagi pekerjaan ke worker.  
**Worker**: Seperti karyawan pabrik. Mengerjakan bagian kecil data yang ditugaskan master.

Untuk menjalankan Spark job, Anda tidak menjalankannya langsung — tapi mengirim "surat perintah" ke Master lewat perintah `spark-submit`.

---

## 6. Grafana: Papan Dashboard

### Grafana Adalah "Pembaca Laporan", Bukan "Penyimpan Data"

Kesalahpahaman umum: Grafana bukan database. Grafana tidak menyimpan data penerbangan. Grafana hanya **membaca** data dari ClickHouse dan **menampilkannya** dalam bentuk grafik cantik.

Cara kerjanya:
1. Anda buka dashboard Grafana di browser (`http://localhost:3000`)
2. Grafana diam-diam mengirim query SQL ke ClickHouse
3. ClickHouse membalas dengan data
4. Grafana mengubah data itu menjadi grafik

Jadi kalau ClickHouse mati, Grafana tidak bisa menampilkan apa-apa.

### Plugin ClickHouse di Grafana

Grafana tidak bisa bicara langsung ke ClickHouse tanpa "penerjemah". Kita install plugin khusus: `grafana-clickhouse-datasource`. Plugin ini yang tahu cara "bahasa" ClickHouse.

---

## 7. Alur Data: Dari CSV ke Dashboard

Ini adalah perjalanan lengkap sebutir data penerbangan, dari file CSV mentah hingga muncul di dashboard:

```
TAHAP 1: DOWNLOAD
BTS Website ──► download_ontime.py ──► data/raw/ontime/year=2021/month=01/*.csv

        "Script Python mendownload file ZIP dari website pemerintah AS,
         mengekstrak, dan menyimpannya di folder lokal terstruktur"

TAHAP 2: STREAMING KE KAFKA  
data/raw/ontime/*.csv ──► stream_ontime.py ──► Kafka (ontime.raw)

        "Script Python membaca CSV baris per baris, mengubahnya ke
         format JSON, dan mengirim 1000 baris per batch ke Kafka"

TAHAP 3: KAFKA → CLICKHOUSE (OTOMATIS)
Kafka (ontime.raw) ──► [Materialized View] ──► flight_delay.ontime_raw

        "ClickHouse secara otomatis dan terus-menerus mengkonsumsi pesan
         dari Kafka dan menyimpannya ke tabel ontime_raw"

TAHAP 4: SPARK PREPROCESSING (MANUAL)
flight_delay.ontime_raw ──► Spark (preprocess_ontime.py) ──► 3 Tabel Output

        "Spark membaca jutaan baris dari ontime_raw, membersihkan,
         memvalidasi, dan menulis hasilnya ke tabel curated"

TAHAP 5: SPARK AGGREGATION (MANUAL)
flight_delay.ontime_curated ──► Spark (aggregate.py) ──► agg_* tables

        "Spark menghitung ringkasan statistik (rata-rata delay per bulan,
         per maskapai, dll) dan menyimpannya ke tabel agregasi"

TAHAP 6: VISUALISASI (OTOMATIS)
agg_* tables ──► Grafana Dashboard ──► Browser Anda

        "Grafana mengquery tabel agregasi setiap panel dibuka dan
         menampilkan hasilnya sebagai grafik dan tabel"
```

---

## 8. Preprocessing: Apa yang Spark Lakukan?

Ini adalah inti dari seluruh pipeline. Mari kita bedah step by step apa yang dilakukan `preprocess_ontime.py`.

### Step 1: Baca Data Mentah

Spark terhubung ke ClickHouse lewat JDBC (Java Database Connectivity) dan membaca seluruh isi tabel `ontime_raw` ke dalam memorinya.

### Step 2: Type Casting — Paksa Format yang Benar

Data yang masuk dari CSV semuanya berupa **teks (String)**. Angka seperti `"2021"` secara teknis adalah teks, bukan angka. Kita perlu "mengconvert" ke tipe yang tepat.

Contoh:
```python
# Sebelum casting:  FlightDate = "2021-01-15" (teks)
# Sesudah casting:  FlightDate = 2021-01-15   (Date, bisa dihitung selisih harinya)

# Sebelum casting:  ArrDelay = "25.0" (teks)
# Sesudah casting:  ArrDelay = 25.0   (Float, bisa dihitung rata-ratanya)
```

Kalau nilai tidak bisa diconvert (misalnya `"N/A"` dipaksa jadi angka), hasilnya otomatis jadi `NULL`.

### Step 3: Filter & Validasi — Tolak Data Sampah

Tidak semua baris itu valid. Ada yang harus ditolak:

| Kondisi | Alasan Ditolak |
|---------|---------------|
| `FlightDate` kosong/null | Kita tidak tahu tanggal penerbangan kapan |
| `Origin` atau `Dest` kosong | Kita tidak tahu rute penerbangannya |
| Maskapai tidak ada | Data orphan — tidak bisa dianalisis per maskapai |
| Tahun di luar 2021-2025 | Di luar scope proyek |
| Jarak ≤ 0 | Tidak logis, kemungkinan data corrupt |
| Jam keberangkatan < 0 atau > 2359 | Tidak ada jam seperti itu |

**Penting:** Baris yang ditolak **tidak dihapus diam-diam**. Mereka disimpan ke tabel `pipeline_rejected_rows` lengkap dengan alasan penolakannya. Ini penting untuk audit dan debugging.

### Step 4: Deduplikasi — Hapus Duplikat

Bisa terjadi penerbangan yang sama muncul dua kali (misalnya kalau streaming dijalankan ulang). Sebuah penerbangan dianggap unik berdasarkan 6 kombinasi kolom:

```
FlightDate + Maskapai + Nomor Penerbangan + Origin + Dest + Jam Jadwal Berangkat
```

Kalau ada dua baris dengan kombinasi yang sama, yang paling baru (berdasarkan `ingest_ts`) yang dipertahankan. Yang lama dihapus.

### Step 5: Outlier Handling — Atasi Nilai Ekstrem

Ada penerbangan yang terlambat 1500 menit (25 jam!) — biasanya karena cancel dan dijadwalkan ulang keesokan harinya. Nilai ekstrem seperti ini bisa merusak model ML.

Strategi yang kita pakai: **bukan menghapus outlier, tapi membuat dua versi**:
- `dep_delay_minutes_original` — nilai asli, untuk audit
- `dep_delay_minutes_capped` — nilai yang sudah "dipotong" di batas percentile ke-99.5

Ini berarti model bisa memilih mana yang dipakai, dan data aslinya tidak hilang.

### Step 6: Feature Engineering — Membuat Fitur Baru

Dari kolom yang ada, Spark membuat kolom-kolom baru yang berguna untuk model prediksi:

**Fitur Waktu** (dari jadwal/CRS, bukan waktu aktual):
```
CRSDepTime = 1430  →  dep_hour = 14
                   →  dep_time_bucket = "Afternoon"
FlightDate = 2021-07-15  →  season = "Summer"
                         →  is_weekend = 0 (kamis)
```

**Fitur Rute:**
```
Origin = "LAX", Dest = "JFK"  →  route = "LAX-JFK"
OriginState = "CA", DestState = "NY"  →  same_state_route = 0
```

**Fitur Historis** (rata-rata performa masa lalu):
```
Untuk setiap baris:
route_avg_arr_delay_prev = rata-rata ArrDelay semua penerbangan di rute ini sebelumnya
carrier_arr_delay_rate_prev = rasio keterlambatan maskapai ini secara historis
```

### Output: 3 Tabel Berbeda

Dari satu proses Spark, dihasilkan 3 tabel berbeda sesuai kebutuhan masing-masing tim:

| Tabel | Isi | Untuk Siapa |
|-------|-----|-------------|
| `ontime_curated` | Semua kolom setelah dibersihkan | Semua tim (data bersih umum) |
| `ontime_features` | Hanya fitur model + label | Tim Data Science |
| `ontime_post_event_analysis` | Kolom yang tahu setelah penerbangan selesai | Tim Analisis Operasional |

---

## 9. Tabel-Tabel ClickHouse: Kenapa Ada Banyak?

Mungkin Anda bertanya: kenapa ada begitu banyak tabel? Kenapa tidak satu tabel saja?

### Prinsip: Separation of Concerns

Setiap tabel punya "kontrak" yang jelas:

```
ontime_kafka_raw      → Tabel Kafka Engine: jembatan sementara dari Kafka
ontime_raw            → Data mentah, apa adanya dari CSV (audit trail)
ontime_curated        → Data bersih, siap analisis umum
ontime_features       → Siap untuk model ML (sudah direkayasa fiturnya)
ontime_post_event     → Data pasca-penerbangan (untuk analisis sebab-akibat)

pipeline_run_log      → Catatan setiap pipeline yang dijalankan
pipeline_quality_*    → Metrik kualitas data per run
pipeline_rejected_*   → Baris yang ditolak (untuk debugging)
eda_quality_summary   → Ringkasan analisis data eksplorasi

agg_monthly_delay     → Ringkasan bulanan (untuk grafik tren)
agg_carrier_perf      → Performa per maskapai
agg_airport_perf      → Performa per bandara
agg_route_perf        → Performa per rute
agg_hourly_delay      → Pola delay per jam
agg_delay_reason      → Kontribusi penyebab delay
```

### Kenapa Ada Tabel Agregasi?

Bayangkan Grafana menampilkan grafik "Rata-rata Delay per Bulan 2021-2025". Kalau Grafana langsung query ke `ontime_curated` (30 juta baris), setiap kali dashboard dibuka, ClickHouse harus menghitung ulang rata-rata dari 30 juta baris. Ini lambat dan membuang resource.

Dengan tabel `agg_monthly_delay`, hasilnya sudah dihitung oleh Spark dan disimpan. Grafana tinggal baca 60 baris (5 tahun × 12 bulan) — jauh lebih cepat!

### ReplacingMergeTree: Tabel yang Bisa "Update"

Tabel agregasi menggunakan Engine `ReplacingMergeTree`, bukan `MergeTree` biasa. Kenapa? Karena kalau Spark menghitung ulang agregasi (misal ada data baru), kita ingin nilai lama **diganti** oleh nilai baru, bukan ditambah.

`ReplacingMergeTree` secara otomatis menghapus baris duplikat (berdasarkan ORDER BY key) dan menyimpan yang terbaru saja.

---

## 10. AWS EC2: Kenapa Perlu Cloud?

### Masalah dengan Laptop Lokal

Saat ini, seluruh pipeline berjalan di laptop Anda. Masalahnya:

1. **Laptop harus terus menyala.** Kalau laptop mati, teman tim tidak bisa akses data.
2. **IP tidak tetap.** Setiap kali Anda connect ke WiFi baru, IP berubah. Teman tim tidak bisa connect.
3. **Tidak bisa diakses dari luar jaringan Anda.** Teman tim yang di rumah masing-masing tidak bisa terhubung.

### Solusi: EC2 = Komputer Sewaan 24/7

EC2 (Elastic Compute Cloud) adalah layanan AWS yang memungkinkan Anda "menyewa" komputer virtual yang:
- Menyala 24 jam sehari, 7 hari seminggu
- Punya IP Publik tetap
- Bisa diakses dari mana saja di dunia

### Apa yang Kita Install di EC2?

**Hanya ClickHouse saja** — bukan Kafka, bukan Spark, bukan seluruh pipeline. Kenapa?

Karena EC2 gratis tier hanya punya **1 GB RAM**. Untuk menjalankan Kafka + Spark + ClickHouse sekaligus, dibutuhkan minimal 8-16 GB RAM. Kita tidak mau bayar.

Strateginya: **Proses berat di laptop (gratis), hasil akhir dipajang di EC2 (murah).**

```
LAPTOP ANDA (Pabrik):           AWS EC2 (Etalase):
Kafka + Spark + ClickHouse  →   ClickHouse saja
(proses data berat)             (simpan + sajikan data bersih)
```

### Cara Akses EC2: SSH

SSH (Secure Shell) adalah cara yang aman untuk mengendalikan komputer lain dari jarak jauh melalui terminal. Ibarat remote desktop, tapi berbasis teks.

```
Laptop Anda              Internet              EC2 AWS
(PowerShell)    ──SSH──►  (terenkripsi)  ──►  (Linux Terminal)
```

### File .pem: Kunci Rumah

Saat membuat EC2, AWS menghasilkan file `.pem` (Private Key). File ini adalah **kunci digital** untuk membuka pintu EC2. Siapapun yang punya file `.pem` ini bisa masuk ke server Anda, jadi simpan baik-baik dan jangan di-share!

Perintah SSH:
```bash
ssh -i "nama-kunci.pem" ubuntu@IP_PUBLIC_EC2
#     ↑ gunakan kunci ini  ↑ user di EC2  ↑ alamat server
```

### Security Group: Firewall AWS

Security Group adalah "daftar tamu yang boleh masuk" ke EC2 Anda. Setiap koneksi yang masuk ke EC2 dicek dulu — apakah port dan IP-nya ada di daftar tamu?

Port yang kita buka:
| Port | Untuk Apa | Source |
|------|-----------|--------|
| 22 | SSH (akses terminal) | My IP saja (hanya Anda) |
| 8123 | ClickHouse HTTP | My IP saja (keamanan data) |
| 3000 | Grafana dashboard | Anywhere (semua orang bisa lihat dashboard) |

---

## 11. Strategi Arsitektur: Lokal vs Cloud

### Mengapa Arsitektur Kita Dibagi Dua?

Inilah arsitektur akhir proyek:

```
                        LAPTOP ANDA
          ┌─────────────────────────────────────┐
          │                                     │
          │  CSV   →  Kafka  →  ClickHouse      │
          │                        ↓            │
          │                      Spark          │
          │                    (Proses)         │
          └─────────────────────────────────────┘
                              │
                      Export data bersih
                     (CSV atau direct import)
                              │
                              ▼
                       AWS EC2
          ┌─────────────────────────────────────┐
          │                                     │
          │          ClickHouse                 │
          │    (Data bersih yang sudah siap)    │
          │                                     │
          │          ← Tim DS bisa akses        │
          │          ← Grafana cloud bisa       │
          │            connect ke sini          │
          └─────────────────────────────────────┘
```

### Kapan Data Dipindahkan ke EC2?

Tidak sekarang. Data dipindahkan ke EC2 nanti setelah:
1. Seluruh pipeline lokal selesai (streaming + Spark)
2. Tabel `ontime_curated` dan `ontime_features` sudah terisi penuh
3. Menjelang presentasi (H-3 sampai H-5)

Cara memindahkan:
```bash
# Di laptop: export ke CSV
clickhouse-client --query "SELECT * FROM flight_delay.ontime_curated" > curated.csv

# Upload ke EC2
scp -i "kunci.pem" curated.csv ubuntu@IP_EC2:~/

# Di EC2: import CSV ke ClickHouse
cat curated.csv | docker exec -i clickhouse clickhouse-client \
  --query "INSERT INTO flight_delay.ontime_curated FORMAT CSV"
```

---

## 12. Pertanyaan Umum (FAQ)

### ❓ Kenapa streaming lambat sekali?

`stream_ontime.py` menggunakan setting:
- `STREAM_BATCH_SIZE = 1000` → kirim 1000 baris per batch
- `STREAM_SLEEP_SECONDS = 0.2` → tunggu 0.2 detik antar batch

Artinya per detik dikirim 5000 baris. Untuk 5 tahun data (~30 juta baris):
```
30.000.000 ÷ 5000 = 6000 detik ≈ 100 menit ≈ 1.7 jam
```
Itu **normal** dan sengaja dibuat lambat agar tidak membebani Kafka.

### ❓ Kenapa ada kolom NULL di data?

NULL artinya "tidak ada data" atau "tidak diketahui". Misalnya `ArrDelay` NULL pada penerbangan yang dibatalkan — penerbangan itu tidak pernah mendarat, jadi tidak ada data delay kedatangan. NULL di sini bermakna secara logis.

### ❓ Apa itu `pipeline_run_id`?

Setiap kali Spark dijalankan, dihasilkan ID unik (misalnya `3f8a-9b21-...`). Ini seperti "nomor batch produksi" di pabrik. Manfaatnya:
- Kalau ada masalah, bisa lacak data dari run mana yang bermasalah
- Bisa bandingkan kualitas antar run
- Bisa hapus hasil run tertentu tanpa hapus seluruh data

### ❓ Kenapa ada kolom `dep_delay_minutes_capped` dan `original`?

Karena ada penerbangan yang terlambat ekstrem (1000+ menit). Kalau nilai ini dimasukkan ke model ML begitu saja, model akan "terobsesi" pada kasus ekstrem ini dan performanya di kasus normal jadi buruk.

Dengan membuat versi `capped` (nilai dipotong di batas atas), model bisa dilatih dengan distribusi yang lebih normal. Tapi nilai aslinya tetap disimpan untuk keperluan audit.

### ❓ Apa itu `leakage` yang sering disebut?

Dalam konteks model prediksi, **leakage** (kebocoran) adalah ketika model "mengetahui masa depan" saat training — yang tidak mungkin terjadi di dunia nyata.

Contoh di proyek ini: `CarrierDelay` (berapa menit delay karena maskapai) hanya diketahui **setelah** penerbangan mendarat. Kalau kita masukkan ini sebagai input fitur model, model akan "curang" — ia bisa memprediksi delay karena sudah tahu penyebabnya. Tapi di dunia nyata, sebelum pesawat berangkat kita tidak tahu ini.

Itulah kenapa kolom ini dipisahkan ke `ontime_post_event_analysis` dan **tidak boleh masuk** ke `ontime_features`.

### ❓ Kenapa Grafana perlu password?

Karena EC2 terhubung ke internet publik. Tanpa password, siapapun bisa buka dashboard Grafana Anda dan melihat data. Dengan password `rahasia123`, minimal ada satu lapisan keamanan.

### ❓ Kenapa SQL di AWS tidak bisa multi-statement?

Web ClickHouse Play memiliki batasan keamanan — hanya bisa menjalankan satu perintah SQL per klik "Run". Ini mencegah serangan SQL Injection. Solusinya: gunakan terminal Docker (`docker exec clickhouse clickhouse-client -n -q "..."`), yang mengizinkan banyak perintah sekaligus dengan flag `-n`.

---

## 🗺️ Peta Jalan: Apa yang Sudah Selesai, Apa yang Belum

### ✅ Sudah Selesai
- [x] Setup Docker Compose (5 service: Kafka, ClickHouse, Spark Master, Spark Worker, Grafana)
- [x] Download dataset 5 tahun (2021-2025) dari BTS
- [x] Streaming data ke Kafka (sedang berjalan)
- [x] Schema ClickHouse lokal (semua tabel sudah ada)
- [x] Schema ClickHouse AWS (semua tabel sudah dibuat manual)
- [x] EC2 AWS hidup dengan ClickHouse berjalan
- [x] Script preprocessing (`preprocess_ontime.py`)
- [x] Script EDA (`eda_profile.py`)

### ⏳ Menunggu
- [ ] Streaming selesai → ClickHouse lokal terisi data bersih
- [ ] Jalankan Spark preprocessing
- [ ] Jalankan Spark aggregation
- [ ] Setup Grafana dashboard

### 🔜 Langkah Selanjutnya (Setelah Streaming Selesai)
1. Jalankan Spark preprocessing
2. Jalankan Spark aggregation
3. Buat dashboard Grafana dari tabel agregasi
4. Export data ke EC2 (H-3 sebelum presentasi)
5. Setup Grafana di EC2 (opsional — atau bisa pakai Grafana lokal untuk presentasi)

---

*Dokumen ini dibuat berdasarkan kode aktual proyek. Jika ada pertanyaan spesifik tentang salah satu bagian, silakan tanyakan.*
