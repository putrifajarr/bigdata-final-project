# 👥 Rencana Implementasi & Pembagian Tugas Tim — FINAL

**Proyek:** Flight Delay Analysis & Prediction  
**Total Anggota:** 5 Orang  
**Fondasi Infrastruktur:** ✅ Selesai dikerjakan Data Engineer

> Dokumen ini adalah panduan kerja yang detail untuk setiap anggota tim.
> **Bagikan dokumen ini ke seluruh tim.** Setiap orang wajib membaca bagian miliknya dari awal hingga akhir sebelum mulai bekerja.

---

## 🔑 Prasyarat Semua Anggota (Wajib Dilakukan Pertama)

Sebelum mulai bekerja, semua anggota tim WAJIB menginstal library berikut di laptop masing-masing:

```bash
pip install clickhouse-connect pandas matplotlib seaborn scikit-learn
```

Kemudian, test koneksi ke database terpusat dengan *script* berikut:

```python
import clickhouse_connect

client = clickhouse_connect.get_client(
    host="47.129.195.124",
    port=8123,
    username="default",
    password="rahasia123",
    database="flight_delay"
)

# Jika berhasil, akan muncul angka jumlah baris
result = client.query("SELECT count() FROM ontime_features")
print("✅ Koneksi berhasil! Jumlah data fitur:", result.first_row[0])
```

Jika muncul angka, Anda sudah terhubung. Laporkan ke Data Engineer jika gagal.

---

---

## 👤 ANGGOTA 1: Data Engineer (Putri/DE)

**Peran:** Pemimpin Teknis & Penjaga Infrastruktur. Semua anggota tim bergantung pada keberhasilan pekerjaan ini.

**Status Saat Ini:** 🔄 Sedang Berjalan (Streaming Kafka ~2 jam, data ~12 juta baris)

### Step-by-Step Tugas Data Engineer:

- **[Step 1] Tunggu Streaming Selesai** *(±2-3 jam lagi)*
  - Pantau terminal `stream_ontime.py`. Tunggu sampai muncul pesan selesai atau program berhenti sendiri.
  - Verifikasi: `SELECT source_year, count() FROM ontime_raw GROUP BY source_year ORDER BY source_year`

- **[Step 2] Jalankan Spark EDA**
  ```bash
  docker exec spark-master spark-submit /opt/spark-apps/eda_profile.py
  ```
  - Tunggu hingga selesai (~10-20 menit).

- **[Step 3] Jalankan Spark Preprocessing** *(yang terpenting)*
  ```bash
  docker exec spark-master spark-submit /opt/spark-apps/preprocess_ontime.py
  ```
  - Ini akan mengisi tabel `ontime_curated`, `ontime_features`, dan `ontime_post_event_analysis`.
  - Tunggu hingga selesai (~20-40 menit).

- **[Step 4] Jalankan Spark Aggregation**
  ```bash
  docker exec spark-master spark-submit /opt/spark-apps/aggregate_ontime.py
  ```
  - Ini akan mengisi semua tabel `agg_*` yang dibutuhkan Grafana.

- **[Step 5] Ekspor Data Lokal ke AWS**
  - Ekspor tabel bersih dari ClickHouse lokal ke file CSV.
  - Upload ke EC2, dan impor ke ClickHouse AWS.
  - *(Lihat dokumen `docs/cloud_ec2_s3.md` untuk perintah detailnya)*

- **[Step 6] Install & Konfigurasi Grafana di AWS**
  ```bash
  docker run -d --name grafana --restart always -p 3000:3000 \
    -e GF_SECURITY_ADMIN_USER=admin \
    -e GF_SECURITY_ADMIN_PASSWORD=admin123 \
    -e GF_INSTALL_PLUGINS=grafana-clickhouse-datasource \
    grafana/grafana:10.4.0
  ```
  - Sambungkan Grafana ke ClickHouse AWS (host: `localhost`, port: `9000`).
  - Berikan URL Grafana AWS ke Anggota 4.

- **[Step 7] Buka Akses AWS untuk Tim**
  - Masuk ke AWS Console → EC2 → Security Groups.
  - Pastikan Port `8123` dan `9000` terbuka untuk semua IP (Anywhere 0.0.0.0/0) agar tim bisa mengakses.

- **[Step 8] Pastikan Seluruh Anggota Bisa Konek**
  - Bantu anggota yang gagal koneksi satu per satu.

---

---

## 👤 ANGGOTA 2: Data Scientist — Model Builder (DS1)

**Peran:** Membangun dan melatih model Machine Learning utama untuk memprediksi keterlambatan pesawat.  
**Input Data:** Tabel `ontime_features` di AWS ClickHouse.  
**Output:** File model terlatih (`.pkl` atau `.joblib`) + laporan metrik.

### Apa yang Harus Diprediksi?

Ada **2 target prediksi utama** (pilih keduanya):

| Target | Kolom | Tipe Masalah |
|--------|-------|--------------|
| Apakah penerbangan delay ≥15 menit? | `arr_del15_label` | Klasifikasi Biner |
| Berapa menit penerbangan delay? | `arr_delay_minutes_label` | Regresi |

### Step-by-Step:

**Step 1: Ambil Data dari AWS ClickHouse**
```python
import clickhouse_connect
import pandas as pd

client = clickhouse_connect.get_client(
    host="47.129.195.124", port=8123,
    username="default", password="rahasia123", database="flight_delay"
)

# Ambil semua data fitur (kecuali tahun 2025 untuk testing)
df = client.query_df("""
    SELECT *
    FROM ontime_features
    WHERE FlightDate < '2025-01-01'
""")

print(f"Total data: {len(df):,} baris")
print(df.dtypes)
```

**Step 2: Tentukan Kolom Fitur (Input) dan Target (Output)**
```python
# Kolom yang BOLEH dipakai sebagai input (diketahui SEBELUM pesawat terbang)
FEATURE_COLS = [
    'flight_year', 'flight_quarter', 'flight_month', 'flight_day',
    'day_of_week', 'is_weekend', 'dep_hour', 'arr_hour',
    'same_state_route', 'distance_bucket',
    'CRSElapsedTime', 'Distance', 'DistanceGroup',
    'route_avg_arr_delay_prev', 'route_arr_delay_rate_prev',
    'carrier_arr_delay_rate_prev', 'carrier_cancel_rate_prev',
    'origin_arr_delay_rate_prev', 'dest_arr_delay_rate_prev',
    # Kolom kategorikal (perlu di-encode)
    'IATA_CODE_Reporting_Airline', 'Origin', 'Dest',
    'season', 'dep_time_bucket', 'arr_time_bucket', 'OriginState', 'DestState'
]

TARGET_COL = 'arr_del15_label'  # 1 = delay, 0 = tidak delay
```

**Step 3: Preprocessing untuk Model**
```python
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split

# Pisahkan fitur numerik dan kategorikal
cat_cols = ['IATA_CODE_Reporting_Airline', 'Origin', 'Dest',
            'season', 'dep_time_bucket', 'arr_time_bucket',
            'OriginState', 'DestState']

# Encode kolom kategorikal
le = LabelEncoder()
for col in cat_cols:
    df[col] = le.fit_transform(df[col].astype(str))

X = df[FEATURE_COLS].fillna(0)
y = df[TARGET_COL].fillna(0).astype(int)

# Split: 80% training, 20% testing
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"Training: {len(X_train):,} | Testing: {len(X_test):,}")
```

**Step 4: Latih Model Baseline dulu**
```python
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, roc_auc_score

# Model Baseline: Logistic Regression
model_lr = LogisticRegression(max_iter=1000, random_state=42)
model_lr.fit(X_train, y_train)

y_pred = model_lr.predict(X_test)
print("=== Baseline: Logistic Regression ===")
print(classification_report(y_test, y_pred))
print(f"ROC-AUC: {roc_auc_score(y_test, y_pred):.4f}")
```

**Step 5: Tingkatkan dengan Model yang Lebih Kuat**
```python
from sklearn.ensemble import RandomForestClassifier
import xgboost as xgb

# Model Advanced: Random Forest
model_rf = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
model_rf.fit(X_train, y_train)

# Model Advanced: XGBoost
model_xgb = xgb.XGBClassifier(n_estimators=100, random_state=42, use_label_encoder=False)
model_xgb.fit(X_train, y_train)
```

**Step 6: Simpan Model Terbaik**
```python
import joblib

# Simpan model terbaik
joblib.dump(model_rf, 'model_best.pkl')
print("Model disimpan ke model_best.pkl")
```

**Step 7: Koordinasikan Hasil ke Anggota 3 & 5**
- Kirimkan file `.pkl` model terbaik ke Anggota 3 untuk dievaluasi.
- Kirimkan ringkasan angka metrik (Accuracy, ROC-AUC) ke Anggota 5 untuk dimasukkan ke slide presentasi.

---

---

## 👤 ANGGOTA 3: Data Scientist — Model Evaluator & EDA (DS2)

**Peran:** Memastikan model yang dibuat Anggota 2 itu benar dan tidak "curang". Juga melakukan analisis mendalam tentang KENAPA delay bisa terjadi.  
**Input:** Model dari Anggota 2 (`.pkl`) + Tabel `ontime_features` dan `ontime_post_event_analysis`.  
**Output:** Laporan evaluasi model + grafik Feature Importance + Wawasan EDA.

### Step-by-Step:

**Step 1: Evaluasi Model dari Anggota 2 Secara Menyeluruh**
```python
import joblib
import pandas as pd
from sklearn.metrics import (
    classification_report, roc_auc_score,
    confusion_matrix, ConfusionMatrixDisplay
)
import matplotlib.pyplot as plt

# Load model dari Anggota 2
model = joblib.load('model_best.pkl')

# Evaluasi di data testing
y_pred = model.predict(X_test)
y_proba = model.predict_proba(X_test)[:, 1]

print(classification_report(y_test, y_pred,
      target_names=["Tepat Waktu", "Delay"]))
print(f"ROC-AUC Score: {roc_auc_score(y_test, y_proba):.4f}")

# Plot Confusion Matrix
cm = confusion_matrix(y_test, y_pred)
disp = ConfusionMatrixDisplay(confusion_matrix=cm,
       display_labels=["Tepat Waktu", "Delay"])
disp.plot()
plt.title("Confusion Matrix — Model Prediksi Delay")
plt.savefig("confusion_matrix.png", dpi=150, bbox_inches='tight')
```

**Step 2: Analisis Feature Importance**
```python
import numpy as np

# Ambil feature importance dari Random Forest
importances = model.feature_importances_
feat_names = FEATURE_COLS
indices = np.argsort(importances)[::-1][:15]  # Top 15 fitur

plt.figure(figsize=(10, 7))
plt.barh([feat_names[i] for i in indices[::-1]],
         [importances[i] for i in indices[::-1]])
plt.xlabel("Tingkat Kepentingan Fitur")
plt.title("Top 15 Fitur Paling Berpengaruh terhadap Delay")
plt.tight_layout()
plt.savefig("feature_importance.png", dpi=150)
```

**Step 3: Analisis Eksplorasi Data (EDA) Mendalam**
```python
# Ambil data post-event untuk analisis penyebab delay
df_cause = client.query_df("""
    SELECT
        IATA_CODE_Reporting_Airline   AS maskapai,
        sum(CarrierDelay)             AS total_carrier_delay,
        sum(WeatherDelay)             AS total_weather_delay,
        sum(NASDelay)                 AS total_nas_delay,
        sum(LateAircraftDelay)        AS total_late_aircraft,
        count()                       AS total_penerbangan
    FROM ontime_post_event_analysis
    WHERE arr_del15_label = 1
    GROUP BY maskapai
    ORDER BY total_carrier_delay DESC
    LIMIT 10
""")

# Tampilkan: Maskapai mana yang paling sering delay karena kesalahan sendiri?
print(df_cause)
```

**Step 4: Cek Masalah Imbalanced Data**
```python
# Berapa % penerbangan yang delay vs yang tepat waktu?
delay_rate = y.mean()
print(f"Proporsi penerbangan yang delay: {delay_rate:.1%}")
print(f"Proporsi penerbangan tepat waktu: {1-delay_rate:.1%}")
# Jika imbalance > 80/20, laporkan ke DS1 untuk pakai class_weight='balanced'
```

**Step 5: Buat Laporan Temuan**
- Buat file Jupyter Notebook berisi semua grafik dan analisis.
- Simpan gambar `confusion_matrix.png` dan `feature_importance.png`.
- Kirimkan wawasan paling menarik ke Anggota 5 untuk dimasukkan ke slide.

---

---

## 👤 ANGGOTA 4: Data Analyst — Dashboard Builder (BI)

**Peran:** Merancang dan membangun Dashboard Grafana yang indah dan informatif agar semua orang bisa memahami data secara visual.  
**Input:** Tabel agregasi `agg_*` di Grafana AWS (URL akan diberikan DE setelah setup).  
**Output:** Dashboard Grafana yang sudah selesai dan siap di-demo.

> ⚠️ **JANGAN** query tabel `ontime_raw` atau `ontime_curated` langsung dari Grafana. Gunakan **HANYA** tabel yang berawalan `agg_`.

### Step-by-Step:

**Step 1: Login ke Grafana AWS**
- Buka browser, masuk ke: `http://47.129.195.124:3000`
- Username: `admin`, Password: `admin123`

**Step 2: Tambahkan Data Source ClickHouse**
- Klik menu kiri → `Connections` → `Data Sources` → `Add new data source`
- Cari dan pilih `ClickHouse`
- Isi pengaturan:
  - **Server address:** `localhost`
  - **Server port:** `9000`
  - **Username:** `default`
  - **Password:** `rahasia123`
  - **Default database:** `flight_delay`
- Klik `Save & Test`. Harus muncul pesan hijau sukses.

**Step 3: Buat Dashboard Baru**
- Klik `+` → `New Dashboard` → `Add new panel`

**Step 4: Buat Panel-Panel Berikut (Satu per Satu)**

**Panel 1 — Tren Rata-rata Delay Bulanan (Time Series Chart)**
```sql
SELECT
    makeDate(year, month) AS tanggal,
    avg_arr_delay         AS rata_rata_delay_kedatangan,
    avg_dep_delay         AS rata_rata_delay_keberangkatan
FROM flight_delay.agg_monthly_delay
ORDER BY tanggal
```

**Panel 2 — Peringkat Maskapai Paling Sering Delay (Bar Chart)**
```sql
SELECT
    airline_code,
    round(arr_delay_rate * 100, 1) AS pct_delay
FROM flight_delay.agg_carrier_performance
WHERE year = 2024
GROUP BY airline_code, arr_delay_rate
ORDER BY arr_delay_rate DESC
LIMIT 10
```

**Panel 3 — Pola Delay per Jam Keberangkatan (Heatmap)**
```sql
SELECT
    dep_hour AS jam,
    day_of_week AS hari,
    round(delay_rate * 100, 1) AS pct_delay
FROM flight_delay.agg_hourly_delay
ORDER BY jam, hari
```

**Panel 4 — 10 Rute Paling Sering Delay**
```sql
SELECT
    route,
    total_flights,
    round(arr_delay_rate * 100, 1) AS pct_delay
FROM flight_delay.agg_route_performance
WHERE year = 2024
ORDER BY arr_delay_rate DESC
LIMIT 10
```

**Panel 5 — Kontribusi Penyebab Delay per Maskapai (Stacked Bar)**
```sql
SELECT
    airline_code,
    round(pct_carrier * 100, 1)       AS persen_maskapai,
    round(pct_weather * 100, 1)       AS persen_cuaca,
    round(pct_nas * 100, 1)           AS persen_nas,
    round(pct_late_aircraft * 100, 1) AS persen_pesawat_terlambat
FROM flight_delay.agg_delay_reason
WHERE year = 2024
ORDER BY persen_maskapai DESC
LIMIT 10
```

**Step 5: Atur Layout & Estetika**
- Beri judul setiap panel yang jelas dalam Bahasa Indonesia.
- Gunakan warna merah untuk "delay tinggi" dan hijau untuk "delay rendah".
- Atur ukuran setiap panel agar proporsional.

**Step 6: Simpan Dashboard & Bagikan URL**
- Klik ikon simpan (💾) di pojok kanan atas.
- Beri nama Dashboard: `"Flight Delay Analysis — Final Project"`
- Bagikan URL Dashboard ke seluruh tim (terutama ke Anggota 5 untuk dimasukkan ke slide presentasi).

---

---

## 👤 ANGGOTA 5: Business Analyst — Storyteller & Presenter (BA)

**Peran:** Menjahit semua hasil kerja teknis menjadi sebuah narasi bisnis yang memukau. Anda adalah "wajah" tim saat presentasi.  
**Input:** Temuan dari Anggota 2, 3, grafik dari Anggota 3, dan Dashboard dari Anggota 4.  
**Output:** Slide Presentasi Final + Narasi Demo Live.

### Step-by-Step:

**Step 1: Pahami Konteks Bisnis**
- Baca dokumen `docs/panduan_lengkap_pipeline.md` untuk memahami apa yang telah dibangun.
- Jawab pertanyaan ini secara mandiri sebelum minta bantuan tim:
  - Siapa yang dirugikan oleh delay pesawat? (Penumpang, maskapai, bandara, pemerintah)
  - Apa manfaat ekonomi jika kita bisa memprediksi delay 1 hari sebelumnya?

**Step 2: Kumpulkan Bahan dari Semua Anggota**
- Dari **Anggota 2 (DS1):** Angka akurasi model, ROC-AUC.
- Dari **Anggota 3 (DS2):** Gambar `feature_importance.png`, `confusion_matrix.png`, temuan penyebab delay utama.
- Dari **Anggota 4 (BI):** URL dan tangkapan layar Dashboard Grafana yang sudah jadi.
- Dari **Data Engineer (Anda):** Minta diagram arsitektur (Kafka → Spark → ClickHouse → Grafana).

**Step 3: Susun Struktur Slide Presentasi**

Gunakan struktur narasi "Problem → Solution → Result → Recommendation":

| Slide | Judul | Isi |
|-------|-------|-----|
| 1 | Cover | Nama proyek, nama tim, tanggal |
| 2 | Problem Statement | Fakta mengejutkan tentang kerugian akibat delay pesawat di AS |
| 3 | Tujuan Proyek | 3 tujuan: Analisis historis, prediksi, dan dashboard real-time |
| 4 | Dataset & Sumber Data | Dataset BTS, 5 tahun, 29 juta penerbangan |
| 5 | Arsitektur Pipeline | Diagram: CSV → Kafka → Spark → ClickHouse → Grafana → AWS |
| 6 | Temuan EDA #1 | Grafik: Tren delay per bulan (ambil dari Grafana) |
| 7 | Temuan EDA #2 | Grafik: Maskapai paling sering delay (ambil dari Grafana) |
| 8 | Temuan EDA #3 | Grafik: Pola delay per jam & hari (Heatmap dari Grafana) |
| 9 | Penyebab Utama Delay | Grafik: Apa penyebab terbesar delay? (Cuaca? Maskapai?) |
| 10 | Model Prediktif — Metodologi | Penjelasan singkat fitur yang digunakan, algoritma yang dipilih |
| 11 | Model Prediktif — Hasil | Tabel metrik: Accuracy, Precision, Recall, ROC-AUC (dari DS1 & DS2) |
| 12 | Feature Importance | Gambar `feature_importance.png` dari Anggota 3 |
| 13 | Demo Live | "Sekarang kami akan menunjukkan dashboard secara langsung..." |
| 14 | Rekomendasi Bisnis | 3-5 rekomendasi konkret berdasarkan data |
| 15 | Kesimpulan & Penutup | Ringkasan pencapaian proyek + ucapan terima kasih |

**Step 4: Rekomendasi Bisnis yang Kuat (Minimal 3)**
Berdasarkan data yang ada, susun rekomendasi seperti:
1. *"Maskapai X secara konsisten memiliki tingkat delay 40% lebih tinggi dari rata-rata. Regulator perlu menyelidiki efisiensi operasional mereka."*
2. *"Penerbangan yang berangkat antara pukul 17:00-20:00 (jam sibuk sore) memiliki kemungkinan delay 2x lebih tinggi. Penumpang disarankan memilih penerbangan pagi."*
3. *"Dengan model prediksi kita (ROC-AUC X%), maskapai bisa memprediksi [X]% dari penerbangan yang berpotensi delay sebelum boarding."*

**Step 5: Latihan Presentasi**
- Latihan sendiri minimal 2 kali sebelum gladi resik tim.
- Pastikan demo Grafana bisa dibuka di laptop Anda langsung (bukan screenshot).
- Siapkan jawaban untuk pertanyaan dosen yang mungkin muncul:
  - *"Kenapa pakai Kafka? Tidak bisa langsung ke ClickHouse saja?"*
  - *"Kenapa tidak pakai LSTM atau Deep Learning?"*
  - *"Seberapa akurat modelnya di data dunia nyata?"*

---

## 🗓️ Timeline Kolaborasi

| Hari | Data Engineer | DS1 (Anggota 2) | DS2 (Anggota 3) | BI (Anggota 4) | BA (Anggota 5) |
|------|--------------|-----------------|-----------------|----------------|----------------|
| **Hari 1** | Streaming → Spark → Import AWS | Setup koneksi AWS, eksplorasi data | Setup koneksi AWS, baca dokumentasi | Tunggu Grafana siap | Baca panduan, riset konteks bisnis |
| **Hari 2** | Setup Grafana AWS, buka akses tim | Bangun model baseline | Evaluasi model baseline, mulai EDA | Bangun panel Grafana satu per satu | Susun struktur slide |
| **Hari 3** | Monitor kestabilan server | Tuning model (RF/XGBoost) | Feature Importance, laporan EDA | Finalisasi semua panel dashboard | Kumpulkan bahan dari semua anggota |
| **Hari 4** | - | Finalisasi model, simpan `.pkl` | Finalisasi laporan evaluasi | Rapikan tampilan dashboard | Tulis narasi & rekomendasi bisnis |
| **Hari 5** | **Rapat Tim: Sinkronisasi Semua Hasil** | | | | |
| **H-1** | Pastikan semua service AWS menyala | Stand-by untuk pertanyaan teknis | Stand-by untuk pertanyaan teknis | Siapkan demo Grafana di laptop | Gladi resik presentasi |
