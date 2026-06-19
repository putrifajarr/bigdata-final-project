# 📚 Panduan Lengkap: Konsep Teknologi, Alur Pipeline & Strategi Tim

**Untuk:** Seluruh anggota tim yang baru bergabung atau ingin memahami lebih dalam  
**Dibuat:** 2026-06-18

---

## 🧠 Bagian 1: Apa Fungsi Setiap Tools? (Konsep dari Nol)

Bayangkan proyek ini seperti sebuah **pabrik pengolahan data penerbangan.** Setiap tools adalah satu bagian dari jalur produksi pabrik tersebut.

---

### 🚰 Kafka — "Ban Berjalan / Conveyor Belt"

**Analogi:** Bayangkan sebuah pabrik makanan. Ada ban berjalan yang terus mengalirkan bahan baku dari gudang ke mesin pengolah. Kafka adalah ban berjalan itu.

**Fungsi teknis:**
- Kafka adalah sistem **message broker** — ia menerima data yang "dikirim" (di-*produce*) dari satu sisi, lalu menyalurkannya ke pihak lain yang "membutuhkan" (men-*consume*) secara real-time atau near-real-time.
- Dalam proyek ini: Script Python (`stream_ontime.py`) membaca file CSV → mengirim baris demi baris ke Kafka → ClickHouse mendengarkan (listen) Kafka dan langsung menyedot data masuk ke tabel `ontime_raw`.

**Kenapa tidak langsung dari CSV ke ClickHouse?**  
Karena Kafka memberikan **buffer dan ketahanan (resilience)**. Jika ClickHouse sedang sibuk, Kafka menampung dulu. Jika terjadi error, data tidak hilang karena masih ada di Kafka (sesuai konfigurasi `retention`). Ini menyimulasikan skenario dunia nyata dimana data datang dari berbagai sumber secara terus-menerus (pesawat landing, departure, delay report, dll).

**Di proyek ini Kafka digunakan untuk:**
- Menerima stream data dari file CSV (simulasi data real-time)
- ClickHouse secara otomatis menyedot data dari Kafka via **Kafka Engine Table**

---

### ⚡ Spark — "Mesin Pengolah / Pabrik Utama"

**Analogi:** Mesin besar di pabrik yang menerima bahan baku (data mentah), membersihkannya, memotongnya, dan menghasilkan produk jadi.

**Fungsi teknis:**
- Apache Spark adalah **distributed data processing engine** — didesain untuk memproses data dalam jumlah sangat besar secara paralel (dibagi ke banyak "worker").
- Dalam proyek ini Spark menjalankan 4 job secara berurutan:

```
[eda_profile.py]        → Analisis profil data mentah (statistik dasar, distribusi)
      ↓
[preprocess_ontime.py]  → Cleaning, type casting, deduplikasi, feature engineering
      ↓
[validate_quality.py]   → Quality gate: apakah data hasil preprocessing layak?
      ↓
[aggregate_ontime.py]   → Hitung agregasi (delay per maskapai, per rute, per bulan)
```

**Kenapa Spark dan bukan pandas biasa?**  
Dataset penerbangan 2021-2025 bisa mencapai 30-50 juta baris. pandas akan kehabisan RAM. Spark memecah data menjadi partisi kecil dan memproses secara paralel menggunakan master + worker.

---

### 🗄️ ClickHouse — "Gudang Produk Jadi / Database Analitik"

**Analogi:** Gudang modern yang super cepat untuk mencari barang. Anda bisa bertanya "berapa total delay maskapai X di bulan Januari?" dan jawaban datang dalam hitungan milidetik.

**Fungsi teknis:**
- ClickHouse adalah **columnar OLAP database** — dioptimalkan untuk query analitik yang membaca jutaan baris dengan sangat cepat.
- Berbeda dengan MySQL/PostgreSQL yang adalah *row-based* (cocok untuk transaksi), ClickHouse menyimpan data per kolom sehingga query agregasi jauh lebih cepat.

**Tabel-tabel di ClickHouse proyek ini:**

| Tabel | Diisi oleh | Isi |
|---|---|---|
| `ontime_raw` | Kafka (otomatis) | Data mentah langsung dari CSV |
| `ontime_curated` | Spark preprocess | Data setelah dibersihkan |
| `ontime_features` | Spark preprocess | Fitur-fitur untuk ML |
| `pipeline_run_log` | Semua Spark job | Log status setiap tahap |
| `agg_monthly_delay` | Spark aggregate | Agregasi delay per bulan |
| `agg_carrier_performance` | Spark aggregate | Performa per maskapai |

---

### 📊 Grafana — "Papan Dashboard / Display Room"

**Analogi:** Layar besar di ruang kontrol pabrik yang menampilkan semua statistik secara visual — grafik, chart, tabel — secara real-time.

**Fungsi teknis:**
- Grafana adalah tools visualisasi data. Ia terhubung ke ClickHouse sebagai *datasource* dan menampilkan data dalam bentuk dashboard interaktif.
- Tidak menyimpan data — hanya membaca dari ClickHouse.

---

### 📦 AWS S3 — "Gudang Bahan Baku / Cold Storage"

**Analogi:** Gudang besar di luar kota yang menyimpan bahan baku dengan harga murah. Tidak bisa diakses secepat gudang utama, tapi kapasitasnya sangat besar dan biaya per GB sangat murah.

**Fungsi teknis:**
- S3 (*Simple Storage Service*) adalah object storage dari AWS — pada dasarnya tempat menyimpan file di cloud dengan harga sangat murah (~$0.023/GB/bulan).
- Cocok untuk menyimpan file statis: CSV, Parquet, JSON, gambar, dll.
- **Bukan database** — tidak bisa di-query langsung layaknya SQL.

---

### 💻 AWS EC2 — "Komputer Sewaan di Cloud"

**Analogi:** Menyewa sebuah komputer/server di pusat data Amazon. Komputer itu menyala 24/7 tanpa Anda perlu khawatir listrik atau hardware.

**Fungsi teknis:**
- EC2 (*Elastic Compute Cloud*) adalah virtual machine (VM) yang berjalan di server AWS.
- Anda bisa install apapun di sana — Docker, database, web server, dll.
- Berbeda dengan S3 yang hanya menyimpan file, EC2 adalah komputer sungguhan yang bisa menjalankan program.

---

## 🔄 Bagian 2: Alur Lengkap Pipeline (Dari Data Mentah → Dashboard)

```
FASE 1: INGEST (Memasukkan Data)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[BTS.gov Website]
      │ download_ontime.py (Python di lokal)
      │ ↓ Download file ZIP bulanan
[CSV Files di data/raw/] (~15GB, 60 file)
      │ stream_ontime.py (Python di lokal)
      │ ↓ Baca CSV baris per baris, kirim ke Kafka
[Kafka Broker] (Docker container, port 9092/9094)
      │ ↓ ClickHouse Engine Table otomatis membaca
[ClickHouse: ontime_raw] ← Data mentah tersimpan di sini


FASE 2: PROCESS (Mengolah Data)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[ClickHouse: ontime_raw]
      │ Spark membaca via JDBC
      │ ↓ eda_profile.py → statistik profil → disimpan ke ClickHouse
      │ ↓ preprocess_ontime.py → cleaning, feature engineering
[ClickHouse: ontime_curated + ontime_features]
      │ ↓ validate_quality.py → cek kualitas data
      │   Jika PASSED ↓
      │ ↓ aggregate_ontime.py → hitung agregasi
[ClickHouse: agg_* tables]


FASE 3: VISUALIZE & MODEL (Menggunakan Data)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[ClickHouse: semua tabel]
      │ ↓ Grafana baca via plugin         ↓ Data Scientist baca via Python
[Grafana Dashboard]                    [Jupyter Notebook / ML Model]
```

---

## ☁️ Bagian 3: Kalau ClickHouse di EC2 — Berapa Biaya & Apa Dampaknya?

### Biaya EC2 yang Perlu Dipahami

| Instance Type | RAM | vCPU | Harga/Bulan | Free Tier? |
|---|---|---|---|---|
| **t2.micro** | 1 GB | 1 | **Gratis** (750 jam/bulan) | ✅ Ya, 12 bulan |
| t3.micro | 1 GB | 2 | ~$8/bulan | ✅ Ya (region tertentu) |
| t3.small | 2 GB | 2 | ~$15/bulan | ❌ Tidak |
| t3.medium | 4 GB | 2 | ~$30/bulan | ❌ Tidak |

**ClickHouse di t2.micro (Free Tier) — Apakah bisa?**

> [!WARNING]
> **ClickHouse membutuhkan minimal 2GB RAM untuk berjalan stabil dengan dataset jutaan baris.** t2.micro hanya punya 1GB. Artinya:
> - Bisa start, tapi akan crash saat ada query besar
> - Tidak disarankan untuk dataset 2021-2025 (30-50 juta baris)
> - **Masih bisa dipakai** jika Anda hanya upload *processed data* berukuran lebih kecil (hasil preprocessing yang sudah terfilter)

**Estimasi biaya jika keluar dari Free Tier:**
- t3.small (2GB): ~$15/bulan
- Storage EBS 30GB: ~$2.4/bulan
- Data transfer keluar: 100GB pertama gratis, setelahnya $0.09/GB
- **Total perkiraan: ~$17-20/bulan** (jika keluar Free Tier)

> [!TIP]
> **Strategi terbaik untuk Free Tier:** Gunakan t2.micro *hanya* untuk ClickHouse dengan data yang sudah di-aggregate (tabel `agg_*` yang ukurannya kecil ~10-50MB). Data Scientist dan Dashboard tim bisa query tabel agregasi ini tanpa perlu akses raw data.

---

## ❓ Bagian 4: Jawaban Pertanyaan Spesifik Anda

### "Kalau ClickHouse di EC2, apakah saya masih perlu menyalakan komputer?"

**TIDAK.** Inilah keuntungan utama EC2.

```
Skenario A: ClickHouse di mesin LOKAL Anda
┌─────────────────────────────────────────────┐
│  Komputer Anda HARUS MENYALA                │
│  Ketika Anda tidur → tim tidak bisa akses   │
│  Listrik mati → tim tidak bisa akses        │
└─────────────────────────────────────────────┘

Skenario B: ClickHouse di EC2
┌─────────────────────────────────────────────┐
│  EC2 berjalan 24/7 di server Amazon         │
│  Komputer Anda boleh mati → tim tetap akses │
│  Anda liburan → tim tetap bisa query        │
│  Ngrok/Tailscale TIDAK DIPERLUKAN           │
└─────────────────────────────────────────────┘
```

**Tapi ada yang perlu diperhatikan:**  
- Pipeline (Kafka + Spark) tetap harus dijalankan di mesin Anda (karena butuh RAM besar)
- Setelah pipeline selesai, Anda perlu **export data dari ClickHouse lokal → import ke ClickHouse EC2**
- Atau alternatifnya: Spark langsung menulis ke ClickHouse EC2 (butuh konfigurasi tambahan)

---

### "Kalau S3 hanya menyimpan hasil preprocessing, apakah tim DS masih perlu setup banyak hal?"

**Hampir tidak ada setup.** Ini yang perlu dilakukan Data Scientist:

```python
# 1. Install satu library saja (5 detik)
# pip install clickhouse-connect pandas

# 2. Konek ke ClickHouse (lokal Anda atau EC2)
import clickhouse_connect

client = clickhouse_connect.get_client(
    host='<IP_EC2_atau_IP_TAILSCALE>',
    port=8123,
    database='flight_delay'
)

# 3. Langsung ambil data — SELESAI
import pandas as pd
df = client.query_df("SELECT * FROM ontime_features LIMIT 100000")
print(df.head())
```

**Yang perlu Anda (DE Lead) siapkan untuk tim DS:**
1. IP address ClickHouse (EC2 Public IP atau IP Tailscale)
2. `PIPELINE_RUN_ID` terbaru yang sudah PASSED quality gate
3. Penjelasan kolom tersedia di `docs/data_dictionary.md`

**Yang TIDAK perlu tim DS lakukan:**
- Tidak perlu install Docker
- Tidak perlu download data raw
- Tidak perlu setup Kafka/Spark
- Tidak perlu clone repo (kecuali mau lihat kode)

---

### "Kafka tidak di-publish ke AWS — apakah bermasalah?"

**Sama sekali tidak masalah.** Berikut penjelasannya:

Kafka hanya dibutuhkan pada **Fase Ingest** (memasukkan data mentah). Setelah data sudah masuk ke tabel `ontime_raw` di ClickHouse, Kafka **sudah selesai tugasnya**.

```
Timeline penggunaan Kafka:
─────────────────────────────────────────────────
Fase Ingest   │ Kafka AKTIF dipakai
              │ (stream_ontime.py → Kafka → ClickHouse)
              ▼
Data sudah    │ Kafka TIDAK dipakai lagi
di ontime_raw │
              ▼
Spark Process │ Spark baca langsung dari ClickHouse
              │ (bukan dari Kafka)
              ▼
ClickHouse    │ Tim DS query langsung ke ClickHouse
populated     │ (bukan dari Kafka)
─────────────────────────────────────────────────
```

**Kesimpulan:** Kafka adalah tools sementara untuk "menyalurkan" data, bukan untuk menyimpan data jangka panjang. Tim DS, Dashboard, dan DevOps tidak pernah berinteraksi langsung dengan Kafka.

---

## 🏗️ Bagian 5: Arsitektur yang Direkomendasikan (Berdasarkan Budget Free Tier)

### Pilihan 1: Full Lokal + Tailscale (Paling Murah = GRATIS)

```
MESIN ANDA (DE Lead)
├── Docker:
│   ├── Kafka          → hanya saat ingest
│   ├── Spark          → hanya saat processing
│   ├── ClickHouse     → selalu menyala (butuh komputer Anda aktif)
│   └── Grafana        → selalu menyala (butuh komputer Anda aktif)
│
├── Tailscale:
│   └── Tim bisa akses ClickHouse & Grafana via IP Tailscale
│
└── GitHub:
    └── Semua kode tersimpan di sini

BIAYA: $0/bulan
KEKURANGAN: Tim tidak bisa akses saat komputer Anda mati
```

### Pilihan 2: ClickHouse + Grafana di EC2 (Free Tier, Terbatas)

```
AWS EC2 t2.micro (GRATIS 12 bulan)
├── ClickHouse  ← Data processed (tabel agg_* dan curated)
└── Grafana     ← Dashboard online 24/7
               ← CATATAN: Butuh hati-hati RAM 1GB, jangan load data terlalu besar

AWS S3 (GRATIS hingga 5GB)
└── Backup data processed (CSV/Parquet dari hasil Spark)

MESIN ANDA (hanya saat pipeline berjalan)
├── Kafka + Spark (proses data raw)
└── Setelah selesai: export ke EC2, boleh mati

GitHub
└── Semua kode

BIAYA: $0/bulan (selama dalam Free Tier limit)
KELEBIHAN: Tim bisa akses 24/7 tanpa komputer Anda aktif
KEKURANGAN: RAM terbatas, perlu export manual setiap ada run baru
```

### Pilihan 3: ClickHouse di EC2 t3.small (Paling Stabil, Ada Biaya)

```
AWS EC2 t3.small (2GB RAM) ← ~$15/bulan
└── ClickHouse + Grafana    ← Stabil untuk jutaan baris

AWS S3
└── Backup semua data processed

MESIN ANDA (hanya saat pipeline)
└── Kafka + Spark

BIAYA: ~$17-20/bulan
KELEBIHAN: Stabil, tim bisa akses kapan saja
```

---

## 📋 Bagian 6: Checklist Setup EC2 (Langkah demi Langkah)

Jika Anda memutuskan untuk memakai EC2:

### A. Persiapan di AWS Console

```
1. Buka https://console.aws.amazon.com
2. Buat account AWS (butuh kartu kredit untuk verifikasi, tapi Free Tier tidak ditagih)
3. Pergi ke EC2 → Launch Instance
4. Pilih:
   - AMI: Amazon Linux 2023 (atau Ubuntu 22.04)
   - Instance type: t2.micro (Free Tier)
   - Storage: 30GB (maksimum Free Tier)
5. Buat Key Pair (.pem file) — SIMPAN BAIK-BAIK, tidak bisa diunduh ulang
6. Security Group — buka port:
   - 22   (SSH)
   - 8123 (ClickHouse HTTP)
   - 3000 (Grafana)
7. Launch instance
8. Catat Public IP Address EC2 Anda
```

### B. Setup Docker di EC2

```bash
# Masuk ke EC2 via SSH (dari PowerShell di mesin Anda)
ssh -i "nama-key.pem" ec2-user@<PUBLIC_IP_EC2>

# Install Docker
sudo yum update -y
sudo yum install docker -y
sudo service docker start
sudo usermod -a -G docker ec2-user
# Logout dan login ulang agar perubahan group berlaku
exit
ssh -i "nama-key.pem" ec2-user@<PUBLIC_IP_EC2>

# Verifikasi Docker
docker --version
```

### C. Jalankan ClickHouse di EC2

```bash
# Di dalam EC2:
docker run -d \
  --name clickhouse \
  --restart always \
  -p 8123:8123 \
  -p 9000:9000 \
  -e CLICKHOUSE_DB=flight_delay \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=rahasia123 \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  -v clickhouse_data:/var/lib/clickhouse \
  clickhouse/clickhouse-server:24.3

# Inisialisasi schema (jalankan file SQL)
# Upload dulu file SQL ke EC2:
# (dari PowerShell lokal)
scp -i "nama-key.pem" -r clickhouse/init/ ec2-user@<PUBLIC_IP>:~/

# Kembali di EC2:
for f in ~/init/*.sql; do
  docker exec -i clickhouse clickhouse-client < "$f"
done

echo "ClickHouse siap!"
```

### D. Export Data dari ClickHouse Lokal → EC2

```powershell
# Di PowerShell lokal Anda, setelah pipeline selesai:

# Export tabel curated
Invoke-WebRequest -Uri "http://localhost:8123/?query=SELECT+*+FROM+flight_delay.ontime_curated+FORMAT+CSVWithNames" `
  -OutFile "data/export/ontime_curated.csv"

# Export tabel features
Invoke-WebRequest -Uri "http://localhost:8123/?query=SELECT+*+FROM+flight_delay.ontime_features+FORMAT+CSVWithNames" `
  -OutFile "data/export/ontime_features.csv"

# Upload ke EC2 via SCP
scp -i "nama-key.pem" data/export/ontime_curated.csv ec2-user@<PUBLIC_IP>:~/
scp -i "nama-key.pem" data/export/ontime_features.csv ec2-user@<PUBLIC_IP>:~/

# Di EC2: Import ke ClickHouse
# ssh ke EC2 dulu
cat ~/ontime_curated.csv | docker exec -i clickhouse clickhouse-client \
  --query "INSERT INTO flight_delay.ontime_curated FORMAT CSVWithNames"
```

---

## 🎯 Bagian 7: Rekomendasi Final untuk Proyek Ini

### Skenario Terbaik (Gratis + Praktis untuk Akademik)

```
FASE PENGERJAAN AKTIF
├── Infrastruktur: Full di mesin Anda (Docker)
├── Sharing akses: Tailscale (gratis, stabil)
└── Kode: GitHub

FASE PRESENTASI/DEMO
├── ClickHouse di EC2 t2.micro (gratis)
│   └── Upload hanya tabel agregasi (kecil, aman di 1GB RAM)
├── Grafana di EC2 t2.micro (tapi ingat, RAM hanya 1GB total)
│   └── Alternatif: Screenshot/export PDF dari dashboard lokal
└── Data Science: query EC2 atau terima CSV dari S3

GITHUB
└── Upload: semua kode + sample 500 baris + dokumentasi + screenshot
    JANGAN upload: data raw 15GB
```

### Tabel Keputusan Cepat

| Pertanyaan | Jawaban |
|---|---|
| Kafka perlu di AWS? | ❌ Tidak. Kafka selesai tugasnya setelah data masuk ClickHouse. |
| Spark perlu di AWS? | ❌ Tidak untuk Free Tier. Terlalu rakus RAM. |
| ClickHouse perlu di EC2? | ✅ Opsional tapi direkomendasikan untuk tim bisa akses 24/7 |
| S3 untuk apa? | ✅ Backup data processed dan sharing ke tim |
| Perlu Ngrok jika ClickHouse di EC2? | ❌ Tidak. EC2 punya Public IP sendiri |
| Tim DS perlu setup banyak hal? | ❌ Tidak. Cukup `pip install clickhouse-connect` |
| Komputer DE harus menyala? | Hanya saat jalankan pipeline (Kafka+Spark). Tidak jika ClickHouse di EC2. |
| Biaya EC2 Free Tier? | $0 untuk 750 jam/bulan selama 12 bulan pertama |
| Biaya setelah Free Tier habis? | t2.micro ~$8/bulan, t3.small ~$15/bulan |

---

*Dokumen ini akan terus diperbarui sesuai perkembangan proyek.*
