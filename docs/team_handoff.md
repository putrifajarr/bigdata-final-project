# Team Handoff — OnTime Flight Delay Pipeline

Dokumen ini ditujukan untuk anggota tim yang menggunakan output dari pipeline
yang sudah dibangun oleh Data Engineer. Pastikan pipeline sudah dalam status
`QUALITY_PASSED` sebelum melanjutkan ke tahap modeling atau dashboard.

---

## Status Pipeline — Cara Cek

Cek status run terakhir di ClickHouse:

```sql
SELECT run_id, job_name, status, message, created_at
FROM flight_delay.pipeline_run_log
ORDER BY created_at DESC
LIMIT 20;
```

Pipeline dianggap siap jika status terakhir adalah `QUALITY_PASSED` atau `AGGREGATION_COMPLETED`.

---

## Untuk Tim Data Scientist

### Tabel Utama

| Tabel | Deskripsi |
|---|---|
| `flight_delay.ontime_features` | Input siap untuk model — bebas leakage |
| `flight_delay.ontime_curated` | Data bersih dengan kolom lengkap termasuk aktual |
| `flight_delay.ontime_post_event_analysis` | Delay reason dan kolom post-flight untuk analisis |

### Split Train/Test

Gunakan split berbasis waktu — **jangan gunakan random split** karena bisa menyebabkan leakage temporal:

```python
df_train = df_features.filter("FlightDate < '2025-01-01'")   # 2021-2024
df_test  = df_features.filter("FlightDate >= '2025-01-01'")  # 2025
```

### Kolom Label

Label target tersedia di `ontime_features` dengan nama:

| Kolom Label | Dipakai untuk |
|---|---|
| `arr_delay_minutes_label` | Regresi (berapa menit delay) |
| `dep_delay_minutes_label` | Regresi alternatif |
| `arr_del15_label` | Klasifikasi biner (delay > 15 menit?) |
| `dep_del15_label` | Klasifikasi biner alternatif |
| `cancelled_label` | Klasifikasi cancellation |

### Data yang Direkomendasikan untuk Regresi

Filter sebelum training regresi:

```python
df_regression = df_features.filter(
    (col("cancelled_label") == 0) &
    (col("arr_delay_minutes_label").isNotNull())
)
```

### Kolom yang TIDAK Boleh Dipakai sebagai Input Fitur

Kolom berikut hanya tersedia sebagai label dan tidak boleh masuk ke feature vector:

- `arr_delay_minutes_label`
- `dep_delay_minutes_label`
- `arr_del15_label`
- `dep_del15_label`
- `cancelled_label`

Kolom leakage berikut **tidak ada** di `ontime_features` (sengaja dipisah ke `ontime_post_event_analysis`):

- `CarrierDelay`, `WeatherDelay`, `NASDelay`, `SecurityDelay`, `LateAircraftDelay`
- `DepTime`, `ArrTime`, `TaxiOut`, `TaxiIn`, `WheelsOff`, `WheelsOn`
- `ActualElapsedTime`, `AirTime`

### Historical Features — Catatan

Kolom berikut menggunakan rata-rata keseluruhan training period sebagai baseline approximation:

- `route_avg_arr_delay_prev`
- `carrier_arr_delay_rate_prev`
- `origin_arr_delay_rate_prev`
- (dan kolom `*_prev` lainnya)

Ini cukup untuk baseline model. Untuk implementasi yang lebih strict, gunakan lookback window per tanggal.
Dokumentasikan metrik model yang menggunakan versi ini vs yang tidak.

### Query dari Python/ClickHouse

```python
import clickhouse_connect

client = clickhouse_connect.get_client(host="localhost", port=8123)
df = client.query_df("""
    SELECT *
    FROM flight_delay.ontime_features
    WHERE FlightDate < '2025-01-01'
    LIMIT 100000
""")
```

---

## Untuk Tim Dashboard/BI

### Tabel Aggregate

| Tabel | Dipakai untuk Panel |
|---|---|
| `agg_monthly_delay` | Delay Overview |
| `agg_carrier_performance` | Carrier Analysis |
| `agg_airport_performance` | Airport Analysis |
| `agg_route_performance` | Route Analysis |
| `agg_hourly_delay` | Pola jam/hari |
| `agg_delay_reason` | Delay Causes |

### Aturan Dashboard

- **Jangan** query langsung ke `ontime_raw` atau `ontime_curated` untuk panel berat.
- Gunakan aggregate table di atas untuk semua panel visualisasi.
- Untuk panel quality monitoring, boleh query `pipeline_quality_metrics` dan `eda_column_profile`.

### Akses Grafana

- URL: `http://localhost:3000`
- Username: `admin`
- Password: lihat `.env`

Dashboard awal sudah tersedia sebagai provisioning. Modifikasi dapat dilakukan langsung dari UI Grafana.

---

## Versi Data — Tracking via run_id

Semua tabel menyimpan `pipeline_run_id`. Gunakan ini untuk memfilter data dari run tertentu
jika ada lebih dari satu pipeline run dalam satu tabel:

```sql
SELECT DISTINCT run_id, count() AS rows
FROM flight_delay.ontime_curated
GROUP BY run_id
ORDER BY run_id;
```

---

## Kontak

Untuk pertanyaan terkait pipeline, schema, atau kualitas data — hubungi Data Engineer.
Jangan langsung modifikasi tabel di ClickHouse tanpa koordinasi.
