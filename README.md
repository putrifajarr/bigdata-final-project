# ✈️ Flight Delay Analysis & Prediction Pipeline

> **Big Data Final Project** — Analisis dan prediksi keterlambatan penerbangan domestik AS menggunakan pipeline data end-to-end berbasis Kafka, Spark, ClickHouse, dan Grafana.

---

## 🗂️ Struktur Proyek

```
final-project/
├── clickhouse/init/          # Schema SQL ClickHouse (semua tabel)
├── data/
│   ├── raw/ontime/           # Data BTS per tahun/bulan (tidak di-push, lihat .gitignore)
│   ├── sample/               # Data sampel kecil untuk testing
│   └── rejected/             # Row yang ditolak pipeline (diisi oleh Spark)
├── docs/
│   ├── PANDUAN_ANGGOTA_2_SAMPAI_5.md   # Workflow lengkap untuk tim DS, BI, BA
│   ├── panduan_lengkap_pipeline.md      # Penjelasan arsitektur dari nol
│   ├── pembagian_tugas_tim.md           # Pembagian tugas & kode contoh per anggota
│   ├── team_handoff.md                  # Handoff DE → DS & BI (tabel, split, query)
│   └── cloud_ec2_s3.md                  # Panduan deploy ke AWS EC2
├── grafana/provisioning/     # Konfigurasi datasource & dashboard Grafana
├── producer/
│   ├── config.py             # Konfigurasi Kafka & path data
│   ├── download_ontime.py    # Script download dataset BTS
│   ├── stream_ontime.py      # Script streaming CSV → Kafka
│   └── requirements.txt
├── spark/
│   ├── Dockerfile
│   ├── jobs/
│   │   ├── eda_profile.py        # Job: EDA & column profiling
│   │   ├── preprocess_ontime.py  # Job: Cleaning, feature engineering
│   │   ├── aggregate_ontime.py   # Job: Agregasi untuk dashboard
│   │   └── validate_quality.py   # Job: Quality check
│   └── checkpoints/          # Spark streaming checkpoint (tidak di-push)
├── docker-compose.yml        # Definisi seluruh stack (Kafka, ClickHouse, Spark, Grafana)
├── .env.example              # Template variabel environment
├── DE_SETUP.md               # Panduan lengkap setup & menjalankan pipeline
├── IMPLEMENTATION_PLAN.md    # Rencana implementasi teknis detail
└── TEAM_NOTES.md             # Catatan bug & troubleshooting tim
```

---

## 🏗️ Arsitektur Pipeline

```
BTS CSV Files (2021–2025)
        │
        ▼ stream_ontime.py
   Kafka Topic: ontime.raw
        │
        ▼ Materialized View (otomatis)
   ClickHouse: ontime_raw  (raw landing)
        │
        ▼ spark-submit preprocess_ontime.py
   ClickHouse: ontime_curated        (data bersih)
               ontime_features       (fitur model, bebas leakage)
               ontime_post_event_*   (analisis pasca penerbangan)
        │
        ▼ spark-submit aggregate_ontime.py
   ClickHouse: agg_monthly_delay, agg_carrier_performance,
               agg_airport_performance, agg_route_performance,
               agg_hourly_delay, agg_delay_reason
        │
        ▼
   Grafana Dashboard ──► AWS EC2 (47.129.195.124:3000)
```

---

## 🚀 Cara Menjalankan Pipeline (Lokal)

### 1. Siapkan Environment

```bash
cp .env.example .env
# Edit .env sesuai kebutuhan (password, path, dll.)
```

### 2. Jalankan Stack Docker

```bash
docker compose up -d
# Tunggu semua service healthy (~30 detik)
docker compose ps
```

### 3. Download Dataset BTS

```bash
pip install -r producer/requirements.txt
python producer/download_ontime.py
```

### 4. Streaming ke Kafka

```bash
python producer/stream_ontime.py
# Proses ~1.5-2 jam untuk 30 juta baris
```

### 5. Jalankan Spark Jobs (Setelah Streaming Selesai)

```bash
# Lihat DE_SETUP.md untuk perintah lengkap dengan --packages dan --conf
docker exec -u root spark-master spark-submit /opt/spark-apps/eda_profile.py
docker exec -u root spark-master spark-submit /opt/spark-apps/preprocess_ontime.py
docker exec -u root spark-master spark-submit /opt/spark-apps/aggregate_ontime.py
```

### 6. Buka Dashboard Grafana

- Lokal: `http://localhost:3000` (admin/admin)
- AWS: `http://47.129.195.124:3000` (admin/admin123)

> ⚠️ Untuk perintah lengkap beserta flags Spark dan troubleshooting, lihat [`DE_SETUP.md`](DE_SETUP.md).

---

## 📊 Stack Teknologi

| Komponen | Teknologi | Versi |
|---|---|---|
| Message Broker | Apache Kafka (Bitnami) | 3.3.2 |
| Data Warehouse | ClickHouse | Latest |
| Processing Engine | Apache Spark | 3.x |
| Dashboard | Grafana | 10.4.0 |
| Cloud | AWS EC2 + ClickHouse | - |
| Language | Python (PySpark) | 3.x |

---

## 📁 Data

Dataset berasal dari **Bureau of Transportation Statistics (BTS)** — On-Time Performance:
- **Cakupan:** 2021–2025 (5 tahun, ~29–33 juta penerbangan)
- **Sumber:** https://www.transtats.bts.gov/
- **Ukuran mentah:** ~10 GB (ZIP + CSV)

> Data mentah **tidak disimpan di repository** (lihat `.gitignore`). Data sudah diproses dan tersedia di server AWS.

---

## 👥 Tim

| Anggota | Peran |
|---|---|
| Anggota 1 | Data Engineer — Infrastructure & Pipeline |
| Anggota 2 | Data Scientist — Model Builder |
| Anggota 3 | Data Scientist — Evaluator & EDA |
| Anggota 4 | Data Analyst — Dashboard Builder |
| Anggota 5 | Business Analyst — Storyteller & Presenter |

> Panduan kerja lengkap untuk Anggota 2–5: [`docs/PANDUAN_ANGGOTA_2_SAMPAI_5.md`](docs/PANDUAN_ANGGOTA_2_SAMPAI_5.md)
