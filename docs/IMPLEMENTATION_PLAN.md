# Implementation Plan: OnTime Flight Delay Pipeline 2021–2025

## 1. Ringkasan Tujuan

Project ini bertujuan membangun pipeline data end-to-end untuk dataset **OnTime Airline Performance** periode **2021–2025**. Pipeline berjalan secara lokal menggunakan **Docker Compose** dengan empat layanan utama:

1. **Kafka** untuk simulasi streaming data penerbangan.
2. **ClickHouse** sebagai storage utama dan analytics database.
3. **Spark** untuk preprocessing, EDA, data quality, feature engineering, dan agregasi.
4. **Grafana** untuk visualisasi dashboard langsung dari ClickHouse.

Pipeline dibuat agar:

- Dataset 5 tahun terakhir kalender lengkap, yaitu **2021–2025**, bisa masuk ke sistem.
- Streaming Kafka terlihat berjalan saat demo.
- Data mentah tetap tersimpan untuk audit dan replay.
- Data bersih dan feature table siap dipakai oleh tim data science.
- Dashboard Grafana bisa menampilkan ingestion health, data quality, delay trend, carrier performance, airport performance, route analysis, dan delay causes.
- Arsitektur lokal bisa diperluas ke cloud menggunakan **EC2** dan **S3** sebagai nilai tambah.

Dataset referensi:

- ClickHouse OnTime dataset documentation: <https://clickhouse.com/docs/getting-started/example-datasets/ontime>
- Sumber data asli: Bureau of Transportation Statistics, On-Time Performance.

---

## 2. Target Output Akhir

Output akhir yang harus tersedia:

1. `docker-compose.yml` untuk menjalankan seluruh stack lokal.
2. Struktur folder project yang jelas untuk data, schema, jobs, producer, dashboard, dan docs.
3. ClickHouse database `flight_delay`.
4. Kafka topic `ontime.raw`.
5. Raw landing table di ClickHouse.
6. Curated table hasil cleaning.
7. Feature table tanpa data leakage.
8. Post-event analysis table untuk analisis penyebab delay.
9. EDA summary table.
10. Data quality metrics table.
11. Aggregate tables untuk dashboard.
12. Grafana dashboard provisioning.
13. Dokumentasi alur kerja untuk anggota tim.
14. Instruksi menjalankan pipeline lokal.
15. Opsional cloud deployment note untuk EC2 dan S3.

---

## 3. Batasan dan Keputusan Utama

### 3.1 Periode Dataset

Dataset yang digunakan hanya:

- 2021
- 2022
- 2023
- 2024
- 2025

Alasan:

- Lima tahun kalender lengkap lebih stabil untuk EDA dan training.
- Menghindari data parsial tahun berjalan.
- Cocok untuk pembagian train/test berbasis waktu.

Rekomendasi split model:

- Train: 2021–2024
- Test: 2025

### 3.2 Stack Wajib

Project wajib menggunakan:

- Kafka
- ClickHouse
- Spark
- Grafana

Catatan:

- Producer boleh dibuat sebagai script yang dijalankan manual dari host atau container Spark.
- Tidak perlu membuat service producer permanen di Docker Compose agar arsitektur tetap sesuai instruksi empat layanan utama.
- Streamlit tidak digunakan pada implementasi utama karena Grafana dipilih sebagai visualisasi utama.

### 3.3 Prinsip Preprocessing

Preprocessing harus:

- Reproducible.
- Auditable.
- Memisahkan raw, curated, feature, dan aggregate layer.
- Menghindari data leakage.
- Menyimpan rejected rows dan quality metrics.
- Mempertahankan nilai asli untuk analisis audit.
- Menyediakan versi fitur yang aman untuk model prediksi sebelum departure.

---

## 4. Arsitektur Sistem

## 4.1 Alur Data Utama

```text
BTS OnTime ZIP/CSV
        |
        v
Local raw storage / optional S3
        |
        v
Producer script
        |
        v
Kafka topic: ontime.raw
        |
        v
ClickHouse Kafka Engine table
        |
        v
ClickHouse raw MergeTree table
        |
        v
Spark preprocessing + EDA + aggregation
        |
        v
ClickHouse curated / features / aggregates
        |
        v
Grafana dashboard
```

## 4.2 Layer Data

### Raw Layer

Tujuan:

- Menyimpan data sebagaimana diterima dari Kafka.
- Menjadi sumber audit dan replay.
- Tidak melakukan transformasi kompleks.

Tabel:

- `ontime_kafka_raw`
- `ontime_raw`

### Curated Layer

Tujuan:

- Menyimpan data yang sudah dibersihkan.
- Type casting sudah benar.
- Row invalid sudah dipisahkan.
- Kolom penting sudah distandardisasi.

Tabel:

- `ontime_curated`

### Feature Layer

Tujuan:

- Menyediakan fitur untuk model regresi dan klasifikasi.
- Tidak mengandung leakage.
- Siap digunakan oleh tim data science.

Tabel:

- `ontime_features`

### Post-Event Analysis Layer

Tujuan:

- Menyimpan kolom yang hanya diketahui setelah penerbangan terjadi.
- Dipakai untuk analisis penyebab delay, bukan untuk prediksi awal.

Tabel:

- `ontime_post_event_analysis`

### Aggregate Layer

Tujuan:

- Menyediakan tabel cepat untuk Grafana.
- Menghindari dashboard query langsung ke raw table.

Tabel:

- `agg_monthly_delay`
- `agg_carrier_performance`
- `agg_airport_performance`
- `agg_route_performance`
- `agg_hourly_delay`
- `agg_delay_reason`

---

## 5. Struktur Folder Project

Struktur folder yang direkomendasikan:

```text
final-project/
├── docker-compose.yml
├── .env.example
├── README.md
├── IMPLEMENTATION_PLAN.md
├── clickhouse/
│   └── init/
│       ├── 00_create_database.sql
│       ├── 01_raw_tables.sql
│       ├── 02_curated_tables.sql
│       ├── 03_feature_tables.sql
│       ├── 04_quality_tables.sql
│       └── 05_aggregate_tables.sql
├── data/
│   ├── raw/
│   │   └── ontime/
│   │       └── year=YYYY/
│   │           └── month=MM/
│   ├── sample/
│   ├── manifest.csv
│   └── rejected/
├── producer/
│   ├── requirements.txt
│   ├── download_ontime.py
│   ├── stream_ontime.py
│   └── config.py
├── spark/
│   ├── jobs/
│   │   ├── eda_profile.py
│   │   ├── preprocess_ontime.py
│   │   ├── aggregate_ontime.py
│   │   └── validate_quality.py
│   ├── conf/
│   └── checkpoints/
├── grafana/
│   └── provisioning/
│       ├── datasources/
│       │   └── clickhouse.yml
│       └── dashboards/
│           ├── dashboards.yml
│           └── flight_delay_dashboard.json
└── docs/
    ├── data_dictionary.md
    ├── eda_findings.md
    ├── team_handoff.md
    └── cloud_ec2_s3.md
```

---

## 6. Docker Compose Design

## 6.1 Services

Compose harus berisi empat service utama:

### Kafka

Fungsi:

- Menjadi message broker untuk simulasi streaming.
- Menyediakan topic `ontime.raw`.

Rekomendasi image:

- `bitnami/kafka`

Konfigurasi lokal:

- Single-node Kafka.
- KRaft mode agar tidak perlu Zookeeper.
- Topic partitions: `3`.
- Replication factor: `1`.
- Retention minimal 7 hari.

### ClickHouse

Fungsi:

- Menyimpan raw, curated, features, quality metrics, dan aggregate tables.
- Membaca Kafka topic melalui Kafka Engine.

Rekomendasi image:

- `clickhouse/clickhouse-server`

Port:

- `8123` untuk HTTP interface.
- `9000` untuk native protocol.

Volume:

- `clickhouse_data:/var/lib/clickhouse`
- `./clickhouse/init:/docker-entrypoint-initdb.d`

### Spark

Fungsi:

- Menjalankan preprocessing.
- Menjalankan EDA.
- Menjalankan agregasi.
- Menulis output ke ClickHouse.

Rekomendasi image:

- `bitnami/spark`

Mode:

- Single Spark master/worker sederhana.
- Untuk project lokal, satu container Spark cukup jika job dijalankan via `spark-submit` di dalam container.

Dependency:

- Spark Kafka connector.
- ClickHouse JDBC driver atau ClickHouse Spark connector.

### Grafana

Fungsi:

- Visualisasi dashboard.
- Query langsung ke ClickHouse.

Rekomendasi image:

- `grafana/grafana`

Plugin:

- ClickHouse datasource plugin.

Port:

- `3000`

---

## 7. Environment Configuration

Buat `.env.example`:

```env
COMPOSE_PROJECT_NAME=flight-delay-pipeline

DATA_START_YEAR=2021
DATA_END_YEAR=2025

CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_NATIVE_PORT=9000
CLICKHOUSE_DB=flight_delay
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=

KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC_RAW=ontime.raw
KAFKA_TOPIC_PARTITIONS=3
KAFKA_RETENTION_MS=604800000

STREAM_BATCH_SIZE=1000
STREAM_SLEEP_SECONDS=0.2

GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=admin

STORAGE_MODE=local
S3_BUCKET=
AWS_REGION=
```

---

## 8. Data Acquisition Plan

## 8.1 Download Strategy

Script `producer/download_ontime.py` bertugas:

1. Iterasi tahun 2021–2025.
2. Iterasi bulan 1–12.
3. Download file ZIP bulanan BTS.
4. Simpan ke:

```text
data/raw/ontime/year=YYYY/month=MM/
```

5. Extract CSV ke folder yang sama.
6. Hitung checksum file.
7. Hitung estimasi jumlah row.
8. Update `data/manifest.csv`.

## 8.2 Manifest Schema

`data/manifest.csv` berisi:

| Column | Description |
|---|---|
| `year` | Tahun data |
| `month` | Bulan data |
| `file_name` | Nama file |
| `file_path` | Path lokal |
| `file_size_bytes` | Ukuran file |
| `checksum_sha256` | Hash file |
| `download_timestamp` | Waktu download |
| `row_count_estimate` | Estimasi row |
| `status` | `downloaded`, `missing`, `invalid`, `extracted` |
| `error_message` | Error jika ada |

## 8.3 Validasi File

Setiap file harus dicek:

- File ZIP berhasil diunduh.
- File tidak kosong.
- File bisa diekstrak.
- CSV memiliki header.
- Header mengandung kolom minimum yang dibutuhkan.
- Row count lebih dari 0.

Jika gagal:

- Tulis status `invalid` atau `missing`.
- Simpan error message.
- Pipeline tetap lanjut untuk file lain.

---

## 9. Kolom Yang Digunakan

## 9.1 Kolom Yang Diambil dari Raw Source

### Waktu

- `Year`
- `Quarter`
- `Month`
- `DayofMonth`
- `DayOfWeek`
- `FlightDate`

### Maskapai

- `Reporting_Airline`
- `DOT_ID_Reporting_Airline`
- `IATA_CODE_Reporting_Airline`
- `Tail_Number`
- `Flight_Number_Reporting_Airline`

### Origin

- `OriginAirportID`
- `Origin`
- `OriginCityName`
- `OriginState`
- `OriginStateName`

### Destination

- `DestAirportID`
- `Dest`
- `DestCityName`
- `DestState`
- `DestStateName`

### Jadwal Aman untuk Prediksi

- `CRSDepTime`
- `CRSArrTime`
- `CRSElapsedTime`
- `DepTimeBlk`
- `ArrTimeBlk`

### Target dan Label

- `DepDelay`
- `DepDelayMinutes`
- `DepDel15`
- `ArrDelay`
- `ArrDelayMinutes`
- `ArrDel15`
- `Cancelled`
- `CancellationCode`
- `Diverted`

### Jarak

- `Distance`
- `DistanceGroup`

### Delay Reason

- `CarrierDelay`
- `WeatherDelay`
- `NASDelay`
- `SecurityDelay`
- `LateAircraftDelay`

### Metadata Ingestion

- `ingest_ts`
- `source_file`
- `source_year`
- `source_month`

## 9.2 Kolom Yang Tidak Diambil untuk Pipeline Awal

Kolom berikut tidak perlu diambil pada pipeline awal:

- `OriginAirportSeqID`
- `OriginCityMarketID`
- `OriginStateFips`
- `OriginWac`
- `DestAirportSeqID`
- `DestCityMarketID`
- `DestStateFips`
- `DestWac`
- `DepTime`
- `ArrTime`
- `WheelsOff`
- `WheelsOn`
- `TaxiOut`
- `TaxiIn`
- `ActualElapsedTime`
- `AirTime`
- Semua kolom detail diversion `Div1*` sampai `Div5*`

Alasan:

- Redundan.
- Berpotensi menjadi leakage.
- Terlalu sparse.
- Kurang relevan untuk prediksi awal.
- Bisa ditambahkan nanti jika EDA membuktikan nilainya signifikan.

---

## 10. Kafka Streaming Plan

## 10.1 Topic

Topic utama:

```text
ontime.raw
```

Konfigurasi:

- partitions: `3`
- replication factor: `1`
- retention: `604800000 ms`

## 10.2 Producer Behavior

Script `producer/stream_ontime.py`:

1. Membaca file CSV hasil extract.
2. Memilih hanya kolom yang disepakati.
3. Melakukan trim string.
4. Mengubah empty string menjadi null.
5. Menambahkan metadata ingestion.
6. Serialize row sebagai JSON.
7. Publish ke Kafka topic `ontime.raw`.
8. Mengirim data per batch.
9. Memberikan jeda antar batch agar streaming terlihat.

## 10.3 JSON Message Shape

Setiap Kafka message berbentuk JSON flat:

```json
{
  "Year": 2025,
  "Month": 1,
  "FlightDate": "2025-01-01",
  "IATA_CODE_Reporting_Airline": "AA",
  "Origin": "JFK",
  "Dest": "LAX",
  "CRSDepTime": 800,
  "CRSArrTime": 1130,
  "Distance": 2475,
  "ArrDelayMinutes": 12,
  "ArrDel15": 0,
  "Cancelled": 0,
  "ingest_ts": "2026-06-17T10:00:00Z",
  "source_file": "On_Time_Reporting_Carrier_On_Time_Performance_2025_1.csv",
  "source_year": 2025,
  "source_month": 1
}
```

---

## 11. ClickHouse Schema Plan

## 11.1 Database

Database:

```sql
CREATE DATABASE IF NOT EXISTS flight_delay;
```

## 11.2 Kafka Engine Table

Table:

```text
flight_delay.ontime_kafka_raw
```

Fungsi:

- Membaca data JSON dari Kafka.
- Tidak dipakai langsung untuk analisis.

Engine:

```text
Kafka
```

Format:

```text
JSONEachRow
```

## 11.3 Raw Landing Table

Table:

```text
flight_delay.ontime_raw
```

Engine:

```text
MergeTree
```

Partition:

```text
toYYYYMM(FlightDate)
```

Order:

```text
(FlightDate, IATA_CODE_Reporting_Airline, Origin, Dest)
```

Tujuan:

- Audit.
- Replay.
- Baseline source untuk Spark batch jobs.

## 11.4 Materialized View

Materialized view:

```text
flight_delay.mv_ontime_kafka_to_raw
```

Fungsi:

- Otomatis insert Kafka messages ke `ontime_raw`.

## 11.5 Curated Table

Table:

```text
flight_delay.ontime_curated
```

Isi:

- Data cleaned.
- Tipe data sudah distandardisasi.
- Row invalid sudah dikeluarkan.
- Delay reason masih boleh ada untuk analisis, tetapi ditandai post-event.

## 11.6 Feature Table

Table:

```text
flight_delay.ontime_features
```

Isi:

- Fitur aman untuk prediksi sebelum penerbangan.
- Tidak mengandung kolom leakage.

Tidak boleh berisi:

- `DepTime`
- `ArrTime`
- `TaxiOut`
- `TaxiIn`
- `WheelsOff`
- `WheelsOn`
- `ActualElapsedTime`
- `AirTime`
- `CarrierDelay`
- `WeatherDelay`
- `NASDelay`
- `SecurityDelay`
- `LateAircraftDelay`

Target boleh tersedia sebagai label:

- `dep_delay_minutes_label`
- `arr_delay_minutes_label`
- `dep_del15_label`
- `arr_del15_label`
- `cancelled_label`

## 11.7 Quality Tables

Tables:

- `pipeline_run_log`
- `pipeline_quality_metrics`
- `pipeline_rejected_rows`
- `eda_quality_summary`
- `eda_column_profile`

Tujuan:

- Audit pipeline.
- Debug failure.
- Dashboard quality.
- Menentukan apakah curated/features valid.

---

## 12. EDA Plan

EDA dilakukan sebelum preprocessing final agar keputusan cleaning berdasarkan fakta data, bukan asumsi.

Job:

```text
spark/jobs/eda_profile.py
```

## 12.1 EDA Output

EDA menghasilkan:

1. Total row per year.
2. Total row per month.
3. Missing value ratio per column.
4. Invalid date count.
5. Invalid airport code count.
6. Invalid carrier code count.
7. Duplicate estimate.
8. Distribution summary untuk:
   - `DepDelay`
   - `DepDelayMinutes`
   - `ArrDelay`
   - `ArrDelayMinutes`
   - `Cancelled`
   - `Diverted`
9. Delay class imbalance:
   - ratio `DepDel15 = 1`
   - ratio `ArrDel15 = 1`
   - ratio `Cancelled = 1`
10. Top carrier.
11. Top origin airport.
12. Top destination airport.
13. Top route.
14. Outlier delay extreme.
15. Volume drift antar tahun.
16. Delay drift antar tahun.

## 12.2 EDA Tables

### `eda_quality_summary`

Kolom:

- `run_id`
- `metric_name`
- `metric_value`
- `year`
- `month`
- `created_at`

### `eda_column_profile`

Kolom:

- `run_id`
- `column_name`
- `data_type`
- `row_count`
- `null_count`
- `null_ratio`
- `distinct_count`
- `min_value`
- `max_value`
- `created_at`

## 12.3 EDA Document

File:

```text
docs/eda_findings.md
```

Isi:

- Ringkasan dataset.
- Masalah kualitas data.
- Rekomendasi preprocessing.
- Kolom yang dianggap leakage.
- Kolom yang aman untuk modeling.
- Catatan imbalance.
- Catatan drift.

---

## 13. Preprocessing Plan

Job:

```text
spark/jobs/preprocess_ontime.py
```

## 13.1 Input

Input utama:

```text
flight_delay.ontime_raw
```

Input tambahan:

- `DATA_START_YEAR`
- `DATA_END_YEAR`
- `run_id`

## 13.2 Type Casting

Transformasi tipe data:

- `FlightDate` → Date.
- `Year` → Integer.
- `Quarter` → Integer.
- `Month` → Integer.
- `DayofMonth` → Integer.
- `DayOfWeek` → Integer.
- `CRSDepTime` → Integer.
- `CRSArrTime` → Integer.
- `CRSElapsedTime` → Double.
- `Distance` → Double.
- `DistanceGroup` → Integer.
- `Cancelled` → Integer 0/1.
- `Diverted` → Integer 0/1.
- `DepDel15` → Integer 0/1/null.
- `ArrDel15` → Integer 0/1/null.
- Delay minute columns → Double.

## 13.3 Cleaning Rules

Drop row jika:

- `FlightDate` null.
- `Origin` null.
- `Dest` null.
- `IATA_CODE_Reporting_Airline` null.
- `Year` di luar 2021–2025.
- `Distance <= 0`.
- `CRSDepTime` invalid.
- `CRSArrTime` invalid.

Standardisasi:

- Trim semua string.
- Empty string menjadi null.
- Kategorikal penting null menjadi `UNKNOWN`.
- Delay reason null menjadi `0`.

## 13.4 Cancelled dan Diverted Handling

Untuk row cancelled:

- `Cancelled = 1`.
- `ArrDelayMinutes` bisa null.
- `DepDelayMinutes` bisa null.
- Jangan paksa delay aktual menjadi 0 untuk label regresi utama.
- Buat flag:
  - `is_cancelled`
  - `has_arr_delay_label`
  - `has_dep_delay_label`

Untuk row diverted:

- `Diverted = 1`.
- Target arrival delay bisa tidak reliable.
- Pisahkan untuk analisis operasional.

Regresi delay sebaiknya memakai row:

- `Cancelled = 0`
- `Diverted = 0`
- label delay tidak null

Klasifikasi cancellation memakai semua row valid.

## 13.5 Deduplication

Dedup key:

- `FlightDate`
- `IATA_CODE_Reporting_Airline`
- `Flight_Number_Reporting_Airline`
- `Origin`
- `Dest`
- `CRSDepTime`

Jika duplicate ditemukan:

- Simpan satu row berdasarkan `ingest_ts` terbaru.
- Tulis jumlah duplicate ke `pipeline_quality_metrics`.

## 13.6 Outlier Handling

Tidak menghapus outlier secara langsung.

Buat kolom:

- `dep_delay_minutes_original`
- `arr_delay_minutes_original`
- `dep_delay_minutes_capped`
- `arr_delay_minutes_capped`

Rekomendasi capping awal:

- Lower bound: `0` untuk delay minutes.
- Upper bound: percentile 99.5 dari data training, dihitung Spark.

Catatan:

- Nilai asli tetap disimpan di curated.
- Nilai capped digunakan untuk model regresi baseline jika outlier terlalu ekstrem.

## 13.7 Feature Engineering Waktu

Buat kolom:

- `flight_year`
- `flight_quarter`
- `flight_month`
- `flight_day`
- `day_of_week`
- `is_weekend`
- `season`
- `dep_hour`
- `arr_hour`
- `dep_time_bucket`
- `arr_time_bucket`

Rules:

- `dep_hour = floor(CRSDepTime / 100)`
- `arr_hour = floor(CRSArrTime / 100)`
- `is_weekend = DayOfWeek in (6, 7)`
- `season`:
  - Winter: 12, 1, 2
  - Spring: 3, 4, 5
  - Summer: 6, 7, 8
  - Fall: 9, 10, 11

## 13.8 Feature Engineering Rute

Buat kolom:

- `route`
- `same_state_route`
- `distance_bucket`

Rules:

- `route = concat(Origin, '-', Dest)`
- `same_state_route = OriginState == DestState`
- `distance_bucket` berdasarkan `DistanceGroup` jika tersedia.

Historical features:

- `route_avg_arr_delay_prev`
- `route_arr_delay_rate_prev`
- `route_cancel_rate_prev`

Catatan penting:

- Historical features harus dihitung hanya dari data sebelum tanggal row tersebut.
- Jika implementasi awal terlalu berat, gunakan agregasi train-period untuk baseline dan dokumentasikan sebagai approximation.

## 13.9 Feature Engineering Carrier dan Airport

Buat historical features:

- `carrier_arr_delay_rate_prev`
- `carrier_cancel_rate_prev`
- `origin_arr_delay_rate_prev`
- `origin_cancel_rate_prev`
- `dest_arr_delay_rate_prev`
- `dest_cancel_rate_prev`
- `route_carrier_arr_delay_rate_prev`

Fallback untuk kategori baru:

- Gunakan global average dari training period.

## 13.10 Anti-Leakage Rules

Feature table tidak boleh memakai kolom yang baru diketahui setelah flight berjalan atau selesai.

Kolom forbidden:

- `DepTime`
- `ArrTime`
- `TaxiOut`
- `TaxiIn`
- `WheelsOff`
- `WheelsOn`
- `ActualElapsedTime`
- `AirTime`
- `CarrierDelay`
- `WeatherDelay`
- `NASDelay`
- `SecurityDelay`
- `LateAircraftDelay`

Kolom target boleh disimpan sebagai label, tetapi tidak boleh dipakai sebagai input feature:

- `DepDelay`
- `DepDelayMinutes`
- `ArrDelay`
- `ArrDelayMinutes`
- `DepDel15`
- `ArrDel15`
- `Cancelled`

---

## 14. Data Quality Gates

Job:

```text
spark/jobs/validate_quality.py
```

## 14.1 Quality Gate Criteria

Pipeline valid jika:

- Valid row ratio minimal `95%`.
- Tidak ada `FlightDate` null di curated.
- Tidak ada `Origin` null di curated.
- Tidak ada `Dest` null di curated.
- Tidak ada `IATA_CODE_Reporting_Airline` null di curated.
- Tidak ada `Year` di luar 2021–2025.
- Duplicate rate kurang dari `1%`.
- `Cancelled` hanya berisi `0` atau `1`.
- `Diverted` hanya berisi `0` atau `1`.
- `DepDel15` hanya berisi `0`, `1`, atau null.
- `ArrDel15` hanya berisi `0`, `1`, atau null.
- `ontime_features` tidak mengandung forbidden leakage columns.

## 14.2 Run Status

Tabel:

```text
pipeline_run_log
```

Status:

- `STARTED`
- `EDA_COMPLETED`
- `PREPROCESSING_COMPLETED`
- `QUALITY_PASSED`
- `QUALITY_FAILED`
- `AGGREGATION_COMPLETED`
- `FAILED`

Jika quality gagal:

- Raw data tetap aman.
- Curated/features untuk run tersebut tidak dianggap valid.
- Error dicatat di `pipeline_run_log`.
- Detail row invalid masuk ke `pipeline_rejected_rows`.

---

## 15. Aggregation Plan

Job:

```text
spark/jobs/aggregate_ontime.py
```

Input:

- `ontime_curated`
- `ontime_post_event_analysis`

Output:

- Aggregate tables di ClickHouse.

## 15.1 Monthly Delay

Table:

```text
agg_monthly_delay
```

Metrics:

- total flights
- average departure delay
- average arrival delay
- departure delay rate
- arrival delay rate
- cancellation rate
- diverted rate

Group by:

- year
- month

## 15.2 Carrier Performance

Table:

```text
agg_carrier_performance
```

Metrics:

- total flights
- average arrival delay
- average departure delay
- arrival delay rate
- departure delay rate
- cancellation rate

Group by:

- airline code
- airline name
- year
- month

## 15.3 Airport Performance

Table:

```text
agg_airport_performance
```

Metrics:

- origin total flights
- destination total flights
- average origin departure delay
- average destination arrival delay
- origin delay rate
- destination delay rate
- cancellation rate

Group by:

- airport code
- airport role: `origin` atau `destination`
- year
- month

## 15.4 Route Performance

Table:

```text
agg_route_performance
```

Metrics:

- total flights
- average arrival delay
- average departure delay
- arrival delay rate
- cancellation rate
- average distance

Group by:

- route
- origin
- dest
- year
- month

## 15.5 Hourly Delay

Table:

```text
agg_hourly_delay
```

Metrics:

- total flights
- average delay
- delay rate
- cancellation rate

Group by:

- dep_hour
- day_of_week
- month

## 15.6 Delay Reason

Table:

```text
agg_delay_reason
```

Metrics:

- total carrier delay minutes
- total weather delay minutes
- total NAS delay minutes
- total security delay minutes
- total late aircraft delay minutes
- percentage contribution per reason

Group by:

- year
- month
- carrier

---

## 16. Grafana Dashboard Plan

## 16.1 Datasource

Grafana datasource:

- Type: ClickHouse
- Host: `clickhouse`
- Port: `8123`
- Database: `flight_delay`
- User: `default`

## 16.2 Dashboard Sections

### Ingestion Health

Panels:

- Total raw rows.
- Rows ingested per minute.
- Latest ingest timestamp.
- Kafka-to-ClickHouse ingestion trend.

Source tables:

- `ontime_raw`
- `pipeline_run_log`

### Data Quality

Panels:

- Valid row ratio.
- Rejected row count.
- Duplicate rate.
- Missing value ratio.
- Quality status per run.

Source tables:

- `pipeline_quality_metrics`
- `eda_column_profile`

### Delay Overview

Panels:

- Monthly average arrival delay.
- Monthly average departure delay.
- Monthly arrival delay rate.
- Monthly cancellation rate.

Source table:

- `agg_monthly_delay`

### Carrier Analysis

Panels:

- Top delayed carriers.
- Carrier cancellation rate.
- Carrier volume trend.

Source table:

- `agg_carrier_performance`

### Airport Analysis

Panels:

- Top origin airports by delay rate.
- Top destination airports by delay rate.
- Airport volume by month.

Source table:

- `agg_airport_performance`

### Route Analysis

Panels:

- Top delayed routes.
- High-volume routes.
- Average delay by distance group.

Source table:

- `agg_route_performance`

### Delay Causes

Panels:

- Delay reason stacked bar.
- Weather delay trend.
- Carrier delay trend.
- Late aircraft delay trend.

Source table:

- `agg_delay_reason`

---

## 17. Machine Learning Handoff Plan

Preprocessing bukan akhir pipeline. Setelah feature table siap, tim data science melanjutkan ke tahap berikut.

## 17.1 Dataset untuk Modeling

Input utama:

```text
flight_delay.ontime_features
```

Split:

- Train: 2021–2024
- Test: 2025

## 17.2 Use Case Regresi

Tujuan:

- Memprediksi berapa menit keterlambatan.

Target:

- `arr_delay_minutes_label`
- Alternatif: `dep_delay_minutes_label`

Data yang digunakan:

- `Cancelled = 0`
- `Diverted = 0`
- label tidak null

Baseline:

- Linear Regression
- Ridge Regression

Advanced:

- LightGBM Regressor
- XGBoost Regressor

Jika harus murni Spark:

- Spark MLlib Linear Regression untuk baseline.
- Spark GBTRegressor sebagai alternatif tree-based.
- LightGBM via SynapseML jika dependency tersedia.

Metrics:

- MAE
- RMSE
- Median Absolute Error
- Error by carrier
- Error by origin airport
- Error by route

## 17.3 Use Case Klasifikasi

Tujuan:

- Memprediksi apakah penerbangan delay lebih dari 15 menit atau cancelled.

Target:

- `arr_del15_label`
- `dep_del15_label`
- `cancelled_label`

Baseline:

- Logistic Regression

Advanced:

- LightGBM Classifier
- XGBoost Classifier
- Spark GBTClassifier jika harus tetap di Spark MLlib.

Metrics:

- Accuracy
- Precision
- Recall
- F1-score
- ROC-AUC
- PR-AUC
- Confusion matrix

Catatan:

- Karena delay/cancelled bisa imbalance, jangan hanya mengandalkan accuracy.
- PR-AUC dan recall kelas positif wajib dilaporkan.

## 17.4 Model Output

Prediksi batch disimpan ke:

```text
flight_delay.model_predictions
```

Kolom minimal:

- `prediction_run_id`
- `FlightDate`
- `IATA_CODE_Reporting_Airline`
- `Origin`
- `Dest`
- `route`
- `model_name`
- `model_version`
- `prediction_type`
- `actual_value`
- `predicted_value`
- `predicted_probability`
- `created_at`

Grafana dapat menampilkan:

- Actual vs predicted delay.
- Model error trend.
- Classification performance.
- Top routes with highest prediction error.

---

## 18. Cloud Extension Plan: EC2 dan S3

Cloud bersifat opsional setelah lokal stabil.

## 18.1 S3 Usage

S3 digunakan untuk:

- Raw ZIP archive.
- Extracted CSV archive.
- Manifest.
- Optional Spark checkpoint.

Struktur:

```text
s3://bucket-name/ontime/raw/year=YYYY/month=MM/
s3://bucket-name/ontime/manifest/
s3://bucket-name/ontime/checkpoints/
```

## 18.2 EC2 Usage

EC2 menjalankan Docker Compose yang sama.

Rekomendasi minimal:

- Instance: `t3.large` atau lebih tinggi.
- Storage: EBS minimal disesuaikan dengan ukuran raw + ClickHouse.
- Security group:
  - SSH 22 terbatas ke IP tim.
  - Grafana 3000 terbatas ke IP tim.
  - ClickHouse port tidak perlu dibuka publik.

## 18.3 Environment Switch

`.env`:

```env
STORAGE_MODE=s3
S3_BUCKET=your-bucket
AWS_REGION=ap-southeast-1
```

Jika `STORAGE_MODE=local`, script membaca/menulis ke folder lokal.

Jika `STORAGE_MODE=s3`, script mirror raw file ke S3.

---

## 19. Execution Order

Urutan implementasi yang disarankan:

1. Buat struktur folder project.
2. Buat `.env.example`.
3. Buat `docker-compose.yml`.
4. Buat ClickHouse init SQL.
5. Jalankan Docker Compose.
6. Pastikan Kafka, ClickHouse, Spark, dan Grafana healthy.
7. Buat downloader dataset.
8. Download sample satu bulan terlebih dahulu.
9. Buat Kafka producer.
10. Stream sample ke Kafka.
11. Pastikan ClickHouse raw table terisi.
12. Buat Spark EDA job.
13. Jalankan EDA sample.
14. Buat Spark preprocessing job.
15. Jalankan preprocessing sample.
16. Validasi quality gates.
17. Buat aggregate job.
18. Buat Grafana datasource.
19. Buat dashboard awal.
20. Scale test ke satu tahun.
21. Scale test ke lima tahun.
22. Dokumentasikan hasil EDA dan masalah data.
23. Serahkan feature table ke tim data science.
24. Opsional deploy ke EC2 dan mirror ke S3.

---

## 20. Testing Plan

## 20.1 Unit Tests

Test yang perlu dibuat:

- CSV schema validation.
- Empty string to null conversion.
- HHMM to hour conversion.
- Date parsing.
- Dedup key generation.
- Leakage column detection.
- Target label validation.

## 20.2 Integration Tests

Skenario:

1. Producer mengirim sample rows ke Kafka.
2. ClickHouse Kafka Engine membaca topic.
3. Materialized view mengisi `ontime_raw`.
4. Spark membaca raw data.
5. Spark menulis curated data.
6. Spark menulis feature data.
7. Spark menulis aggregate data.
8. Grafana berhasil query ClickHouse.

## 20.3 Data Quality Tests

Validasi:

- Tahun hanya 2021–2025.
- Key fields tidak null.
- Duplicate rate kurang dari threshold.
- Target classification valid.
- Feature table bebas leakage.
- Delay reason tidak masuk feature input.

## 20.4 Performance Tests

Tahapan load test:

1. Sample kecil.
2. Satu bulan data.
3. Satu tahun data.
4. Lima tahun data.

Metrics yang dicatat:

- Producer throughput.
- Kafka lag.
- ClickHouse insert rate.
- Spark processing time.
- Query latency Grafana.

---

## 21. Acceptance Criteria

Project dianggap berhasil jika:

- Docker Compose bisa menjalankan Kafka, ClickHouse, Spark, dan Grafana.
- Dataset 2021–2025 bisa diunduh atau minimal mekanisme download tersedia.
- Producer bisa melakukan streaming sample data ke Kafka.
- ClickHouse raw table otomatis menerima data dari Kafka.
- Spark bisa menghasilkan EDA output.
- Spark bisa menghasilkan curated table.
- Spark bisa menghasilkan feature table bebas leakage.
- Spark bisa menghasilkan aggregate tables.
- Grafana dashboard bisa menampilkan metrik utama.
- Data quality gates berjalan dan mencatat status pipeline.
- Tim data science bisa menggunakan `ontime_features` untuk regresi dan klasifikasi.

---

## 22. Pembagian Tugas Tim

## 22.1 Data Engineer

Tanggung jawab:

- Docker Compose.
- Kafka setup.
- ClickHouse schema.
- Kafka producer.
- Spark preprocessing.
- Spark aggregation.
- Data quality gates.

Deliverables:

- Pipeline lokal berjalan.
- Raw, curated, features, dan aggregates tersedia.
- Run log dan quality metrics tersedia.

## 22.2 Data Scientist

Tanggung jawab:

- Review EDA.
- Feature validation.
- Target definition final.
- Baseline model.
- Advanced model.
- Model evaluation.
- Interpretasi hasil.

Deliverables:

- Notebook atau Spark ML job.
- Metrics model.
- Feature importance.
- Rekomendasi model final.

## 22.3 Dashboard/BI

Tanggung jawab:

- Grafana datasource.
- Dashboard panels.
- Query optimization.
- Narasi insight.

Deliverables:

- Dashboard Grafana siap demo.
- Panel ingestion, quality, delay, carrier, airport, route, dan delay cause.

## 22.4 DevOps/Cloud

Tanggung jawab:

- EC2 provisioning.
- S3 bucket.
- Security group.
- Deployment Docker Compose di EC2.
- Backup raw data.

Deliverables:

- Optional cloud deployment.
- Dokumentasi deploy cloud.

---

## 23. Risiko dan Mitigasi

| Risiko | Dampak | Mitigasi |
|---|---|---|
| Dataset 5 tahun terlalu besar untuk laptop | Pipeline lambat | Mulai dari sample, lalu 1 bulan, 1 tahun, baru 5 tahun |
| Kafka/ClickHouse connector Spark bermasalah | Job gagal | Gunakan JDBC ClickHouse sebagai fallback |
| BTS URL berubah | Download gagal | Siapkan mode manual file drop ke `data/raw` |
| Grafana plugin ClickHouse gagal install | Dashboard gagal | Gunakan plugin preinstall image atau query via HTTP datasource fallback |
| Data imbalance tinggi | Model bias | Gunakan PR-AUC, class weights, threshold tuning |
| Data leakage tidak sengaja masuk fitur | Evaluasi model terlalu optimis | Enforce forbidden columns check |
| EC2 resource kecil | Pipeline lambat | Gunakan sample demo atau upgrade instance |

---

## 24. Catatan Penting untuk Preprocessing Profesional

Preprocessing yang optimal bukan hanya membersihkan null. Prinsip yang harus dijaga:

1. **Pisahkan raw dan curated.**
   - Jangan overwrite raw data.

2. **Jangan hapus outlier tanpa bukti.**
   - Simpan nilai asli dan buat versi capped.

3. **Jangan campur fitur pre-flight dan post-flight.**
   - Kolom seperti delay reason sangat berguna untuk analisis, tetapi berbahaya untuk model prediksi awal.

4. **Gunakan split berbasis waktu.**
   - Random split bisa menyebabkan leakage temporal.

5. **Log semua keputusan kualitas data.**
   - Row rejected harus bisa dilacak.

6. **Dashboard jangan query raw table untuk panel berat.**
   - Gunakan aggregate tables.

7. **Mulai dari sample kecil.**
   - Validasi pipeline dulu sebelum menjalankan lima tahun data penuh.

---

## 25. Roadmap Mingguan

## Pekan 1

Target:

- Docker Compose selesai.
- ClickHouse schema selesai.
- Sample data berhasil masuk Kafka dan ClickHouse.
- Dataset downloader tersedia.

## Pekan 2

Target:

- EDA Spark job selesai.
- Preprocessing Spark job selesai.
- Curated dan feature table tersedia.
- Quality gates berjalan.

## Pekan 3

Target:

- Aggregate job selesai.
- Grafana dashboard selesai.
- Data 2021–2025 diproses.
- Handoff ke tim data science.

## Pekan 4

Target:

- Baseline model selesai.
- Evaluation report selesai.
- Optional EC2/S3 deployment.
- Final demo pipeline end-to-end.

<!-- Untuk ke AWS -->
docker exec clickhouse clickhouse-client -q "TRUNCATE TABLE flight_delay.ontime_curated; TRUNCATE TABLE flight_delay.ontime_features; TRUNCATE TABLE flight_delay.agg_monthly_delay; TRUNCATE TABLE flight_delay.agg_carrier_performance; TRUNCATE TABLE flight_delay.agg_airport_performance; TRUNCATE TABLE flight_delay.agg_route_performance; TRUNCATE TABLE flight_delay.agg_hourly_delay; TRUNCATE TABLE flight_delay.agg_delay_reason;"

docker exec spark-master spark-submit --driver-memory 2g --executor-memory 3g /opt/spark-apps/preprocess_ontime.py

docker exec spark-master spark-submit --driver-memory 2g --executor-memory 3g /opt/spark-apps/aggregate_ontime.py
