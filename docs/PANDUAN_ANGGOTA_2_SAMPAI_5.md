# 🚀 Panduan Workflow Tim — Anggota 2 s/d 5
# Flight Delay Analysis & Prediction Pipeline

> **Dokumen ini wajib dibaca PENUH sebelum mulai bekerja.**
> Jangan langsung lompat ke bagian tugas Anda tanpa membaca Bagian 1 (Pemahaman Umum) terlebih dahulu.

**Proyek:** Flight Delay Analysis & Prediction  
**Data Sumber:** Bureau of Transportation Statistics (BTS), Penerbangan Domestik AS 2021–2025  
**Infrastruktur Dikelola:** Anggota 1 (Data Engineer / Putri)  
**Dokumen Ini Untuk:** Anggota 2, 3, 4, dan 5

---

## 📋 Daftar Isi

1. [Pemahaman Umum — Baca Ini Dulu!](#1-pemahaman-umum--baca-ini-dulu)
2. [Apa yang Sudah Dikerjakan Data Engineer](#2-apa-yang-sudah-dikerjakan-data-engineer)
3. [Peta Data — Ada Apa Saja di Server](#3-peta-data--ada-apa-saja-di-server)
4. [Prasyarat Semua Anggota](#4-prasyarat-semua-anggota--lakukan-ini-pertama)
5. [ANGGOTA 2 — Data Scientist: Model Builder](#5-anggota-2--data-scientist-model-builder)
6. [ANGGOTA 3 — Data Scientist: Evaluator & EDA](#6-anggota-3--data-scientist-evaluator--eda)
7. [ANGGOTA 4 — Data Analyst: Dashboard Builder](#7-anggota-4--data-analyst-dashboard-builder)
8. [ANGGOTA 5 — Business Analyst: Storyteller & Presenter](#8-anggota-5--business-analyst-storyteller--presenter)
9. [Timeline Kolaborasi](#9-timeline-kolaborasi)
10. [FAQ — Pertanyaan Yang Sering Muncul](#10-faq--pertanyaan-yang-sering-muncul)

---

---

## 1. Pemahaman Umum — Baca Ini Dulu!

### 1.1 Apa Proyek Ini Tentang?

Kita membangun sistem analitik data besar untuk menganalisis dan **memprediksi keterlambatan penerbangan** di Amerika Serikat. Datanya berasal dari pemerintah AS (BTS — Biro Statistik Transportasi), mencakup **sekitar 29–33 juta penerbangan domestik** dari tahun 2021 hingga 2025.

Pertanyaan bisnis yang kita jawab:
- **Berapa kemungkinan** sebuah penerbangan akan terlambat lebih dari 15 menit? *(Klasifikasi)*
- **Berapa menit** penerbangan itu akan terlambat? *(Regresi)*
- **Maskapai mana**, **rute mana**, dan **jam berapa** yang paling sering delay? *(Analisis Deskriptif)*

### 1.2 Mengapa Datanya Besar Sekali?

Satu bulan data penerbangan AS = ±500.000 baris. Lima tahun × 12 bulan = **±30 juta baris**. Itulah kenapa kita tidak bisa analisis ini di Excel atau laptop biasa — dan kenapa dipakai teknologi Big Data (Kafka, Spark, ClickHouse).

### 1.3 Gambaran Besar Arsitektur (Wajib Dipahami)

```
[FILE CSV BTS]          [KAFKA]           [CLICKHOUSE]
  Data mentah     →   Streaming     →    Penyimpanan
  29 juta baris       real-time          analitik

        ↓ [SPARK PREPROCESSING]
   - Bersihkan data kotor
   - Hapus duplikat
   - Rekayasa fitur baru
   - Pisahkan data untuk kebutuhan berbeda

        ↓ [CLICKHOUSE — Data Siap]
   ontime_curated        → Data bersih
   ontime_features       → Siap untuk model ML (Anggota 2 & 3)
   ontime_post_event_*   → Analisis penyebab delay (Anggota 3)
   agg_monthly_delay     → Ringkasan bulanan (Anggota 4)
   agg_carrier_perf      → Performa maskapai (Anggota 4)
   agg_airport_perf      → Performa bandara (Anggota 4)
   ... dan lainnya

        ↓ [GRAFANA]          ↓ [MODEL ML]
   Dashboard visual     Prediksi delay
   (Anggota 4)          (Anggota 2 & 3)

        ↓ [PRESENTASI]
   Narasi bisnis
   (Anggota 5)
```

### 1.4 Konsep Kunci yang Harus Kamu Pahami

#### 🔑 Apa itu "Leakage" dan Kenapa Penting?

**Leakage** (kebocoran data) adalah ketika model prediksi menggunakan informasi yang seharusnya **tidak diketahui** pada saat prediksi dilakukan.

**Contoh nyata:**
Bayangkan kamu diminta memprediksi: *"Apakah penerbangan AA123 pukul 08:00 besok akan delay?"*

- ✅ **Boleh dipakai:** Jadwal keberangkatan (CRSDepTime), maskapai, rute, jarak, riwayat delay historis rute ini
- ❌ **TIDAK BOLEH dipakai:** `CarrierDelay` (berapa menit delay karena maskapai), `DepTime` (jam berangkat aktual), `ArrTime` (jam tiba aktual)

Kenapa? Karena kolom-kolom yang ❌ itu **hanya tersedia SETELAH pesawat terbang dan mendarat**. Di dunia nyata, saat Anda akan prediksi, pesawatnya belum berangkat, jadi Anda tidak punya info itu.

Data Engineer sudah memisahkan kolom-kolom berbahaya ini:
- Kolom untuk **prediksi** → `ontime_features` (aman, bebas leakage)
- Kolom **pasca penerbangan** → `ontime_post_event_analysis` (hanya untuk analisis sebab-akibat, BUKAN input model)

#### 🔑 Apa itu "Split Berbasis Waktu" dan Kenapa Wajib?

Dalam ML biasa, kita split data secara random (acak). Tapi untuk data penerbangan ini, kita **wajib split berdasarkan waktu**:

- **Data Training (Latih):** Penerbangan 2021–2024 → dipakai untuk melatih model
- **Data Testing (Uji):** Penerbangan 2025 → dipakai untuk menguji model

**Kenapa tidak boleh random?** Karena kalau random, data tahun 2025 bisa masuk ke training. Model akan "belajar" dari masa depan — ini curang dan tidak realistis. Di dunia nyata, Anda tidak bisa prediksi penerbangan masa lalu menggunakan data masa depan.

#### 🔑 Apa itu Pipeline Run ID?

Setiap kali Spark dijalankan, semua tabel diberi tag `pipeline_run_id` yang unik. Ini seperti "nomor batch produksi". Kalau ada masalah, kita bisa lacak data dari run mana yang bermasalah. Abaikan saja untuk sekarang, tapi jangan hapus kolom ini.

---

---

## 2. Apa yang Sudah Dikerjakan Data Engineer

Berikut adalah daftar lengkap apa yang **sudah selesai** dikerjakan oleh Data Engineer sebelum kalian mulai:

### ✅ Infrastruktur (Docker & Layanan)
- Docker Compose dengan 5 service: Kafka, ClickHouse, Spark Master, Spark Worker, Grafana
- Konfigurasi jaringan antar service
- Volume permanen agar data tidak hilang

### ✅ Data Acquisition
- Download dataset BTS 5 tahun (2021–2025) — sekitar 29–33 juta baris
- Setiap file divalidasi: checksum, jumlah baris, format header

### ✅ Streaming Pipeline (Kafka)
- Script `stream_ontime.py` — membaca CSV dan mengirim ke Kafka per 1000 baris/batch
- Kafka topic `ontime.raw` — pintu masuk data
- Materialized View di ClickHouse — secara otomatis menyalin data dari Kafka ke tabel permanen

### ✅ Pemrosesan Data (Spark)
Script `preprocess_ontime.py` sudah selesai dan menghasilkan:
1. **Type casting** — semua kolom sudah dikonversi ke tipe data yang tepat (angka jadi Integer/Float, tanggal jadi Date)
2. **Validasi & Filtering** — baris yang tidak valid dipisahkan ke tabel `pipeline_rejected_rows` (tidak dihapus begitu saja)
3. **Deduplikasi** — penerbangan yang sama tidak muncul dua kali
4. **Outlier handling** — nilai ekstrem (delay > 99.5 percentile) diberi versi yang sudah di-cap, tapi versi aslinya tetap ada
5. **Feature Engineering** — kolom-kolom baru dibuat: `dep_hour`, `season`, `route`, `is_weekend`, `route_avg_arr_delay_prev`, dll.

### ✅ Agregasi (Spark)
Script `aggregate_ontime.py` sudah mengisi tabel-tabel ringkasan untuk dashboard:
- `agg_monthly_delay` — tren delay per bulan
- `agg_carrier_performance` — performa per maskapai
- `agg_airport_performance` — performa per bandara
- `agg_route_performance` — performa per rute
- `agg_hourly_delay` — pola delay per jam dan hari
- `agg_delay_reason` — kontribusi penyebab delay

### ✅ Cloud (AWS EC2)
- Server AWS EC2 sudah hidup dengan IP Publik tetap: **47.129.195.124**
- ClickHouse sudah berjalan di EC2 dengan semua data bersih sudah diimpor
- Port `8123` sudah terbuka untuk akses tim

---

---

## 3. Peta Data — Ada Apa Saja di Server

Semua data berada di database `flight_delay` di server AWS ClickHouse (`47.129.195.124:8123`).

### 3.1 Tabel Utama

| Nama Tabel | Jumlah Baris (est.) | Untuk Siapa | Keterangan |
|---|---|---|---|
| `ontime_raw` | ~29–33 juta | Referensi saja | Data mentah dari Kafka, jangan diubah |
| `ontime_curated` | ~28–32 juta | Semua tim | Data setelah dibersihkan Spark |
| `ontime_features` | ~28–32 juta | **Anggota 2 & 3** | Fitur siap model, bebas leakage |
| `ontime_post_event_analysis` | ~28–32 juta | **Anggota 3** | Kolom pasca penerbangan untuk analisis |

### 3.2 Tabel Agregasi (untuk Dashboard)

| Nama Tabel | Jumlah Baris (est.) | Untuk Siapa | Keterangan |
|---|---|---|---|
| `agg_monthly_delay` | ~60 baris | **Anggota 4** | 5 tahun × 12 bulan |
| `agg_carrier_performance` | ~300 baris | **Anggota 4** | Per maskapai per tahun |
| `agg_airport_performance` | ~2.000 baris | **Anggota 4** | Per bandara per tahun |
| `agg_route_performance` | ~50.000 baris | **Anggota 4** | Per rute per tahun |
| `agg_hourly_delay` | ~168 baris | **Anggota 4** | 24 jam × 7 hari |
| `agg_delay_reason` | ~300 baris | **Anggota 4** | Per maskapai per tahun |

### 3.3 Tabel Monitoring Pipeline (Opsional)

| Nama Tabel | Keterangan |
|---|---|
| `pipeline_run_log` | Log setiap Spark run |
| `pipeline_quality_metrics` | Metrik kualitas data |
| `pipeline_rejected_rows` | Baris yang ditolak + alasannya |

### 3.4 Kolom-Kolom Penting di `ontime_features`

**Kolom Fitur (Input Model):**

| Kolom | Tipe | Keterangan |
|---|---|---|
| `FlightDate` | Date | Tanggal penerbangan — dipakai untuk split |
| `flight_year`, `flight_month` | Int | Tahun dan bulan |
| `flight_day`, `day_of_week` | Int | Hari dalam bulan, hari dalam minggu (1=Senin) |
| `is_weekend` | Int | 1 jika Sabtu/Minggu |
| `dep_hour` | Int | Jam jadwal keberangkatan (0–23) |
| `dep_time_bucket` | String | "Morning", "Afternoon", "Evening", "Night" |
| `season` | String | "Winter", "Spring", "Summer", "Fall" |
| `IATA_CODE_Reporting_Airline` | String | Kode maskapai (AA, DL, UA, dll.) |
| `Origin`, `Dest` | String | Kode bandara asal dan tujuan |
| `OriginState`, `DestState` | String | Kode negara bagian |
| `Distance`, `DistanceGroup` | Int | Jarak penerbangan dalam mil |
| `CRSElapsedTime` | Int | Estimasi durasi penerbangan (menit) |
| `route` | String | Format "ORIGIN-DEST", contoh: "LAX-JFK" |
| `same_state_route` | Int | 1 jika terbang dalam negara bagian yang sama |
| `distance_bucket` | String | Kategori jarak ("Short", "Medium", "Long") |
| `route_avg_arr_delay_prev` | Float | Rata-rata delay historis rute ini |
| `carrier_arr_delay_rate_prev` | Float | Proporsi delay historis maskapai ini |
| `origin_arr_delay_rate_prev` | Float | Proporsi delay historis dari bandara asal |
| `dest_arr_delay_rate_prev` | Float | Proporsi delay historis di bandara tujuan |

**Kolom Label (Output/Target Model) — TIDAK BOLEH jadi input fitur:**

| Kolom | Tipe | Untuk |
|---|---|---|
| `arr_del15_label` | Int (0/1) | Klasifikasi: Apakah delay ≥ 15 menit? |
| `dep_del15_label` | Int (0/1) | Klasifikasi: Apakah berangkat terlambat ≥ 15 menit? |
| `arr_delay_minutes_label` | Float | Regresi: Berapa menit delay kedatangan? |
| `dep_delay_minutes_label` | Float | Regresi: Berapa menit delay keberangkatan? |
| `cancelled_label` | Int (0/1) | Klasifikasi: Apakah penerbangan dibatalkan? |

---

---

## 4. Prasyarat Semua Anggota — Lakukan Ini Pertama

### Step 1: Install Library Python

Buka terminal/cmd dan jalankan:

```bash
pip install clickhouse-connect pandas matplotlib seaborn scikit-learn xgboost joblib
```

### Step 2: Test Koneksi ke Server AWS

Buat file baru `test_koneksi.py` dan isi dengan:

```python
import clickhouse_connect

client = clickhouse_connect.get_client(
    host="47.129.195.124",
    port=8123,
    username="default",
    password="rahasia123",
    database="flight_delay"
)

# Tes 1: Cek koneksi
result = client.query("SELECT count() FROM ontime_features")
print("✅ Koneksi berhasil!")
print(f"   Jumlah data di ontime_features: {result.first_row[0]:,} baris")

# Tes 2: Cek tabel tersedia
result2 = client.query("SHOW TABLES FROM flight_delay")
tables = [row[0] for row in result2.result_rows]
print(f"\n📦 Tabel yang tersedia ({len(tables)} tabel):")
for t in sorted(tables):
    print(f"   - {t}")
```

Jalankan:
```bash
python test_koneksi.py
```

Kalau muncul angka baris dan daftar tabel → koneksi berhasil. Jika gagal, hubungi Data Engineer.

### Step 3: Eksplorasi Awal Data (Opsional tapi Disarankan)

```python
import clickhouse_connect
import pandas as pd

client = clickhouse_connect.get_client(
    host="47.129.195.124", port=8123,
    username="default", password="rahasia123",
    database="flight_delay"
)

# Lihat struktur kolom ontime_features
df_sample = client.query_df("""
    SELECT *
    FROM ontime_features
    LIMIT 5
""")
print(df_sample.to_string())
print("\nKolom yang tersedia:")
print(df_sample.dtypes)
```

---

---

## 5. ANGGOTA 2 — Data Scientist: Model Builder

**Nama Peran:** DS1 — Model Builder  
**Input:** Tabel `ontime_features` di AWS ClickHouse  
**Output yang Harus Dicapai:**
- [ ] File model terlatih: `model_klasifikasi_best.pkl` (dan opsional `model_regresi_best.pkl`)
- [ ] Laporan metrik dalam format tabel (Accuracy, Precision, Recall, F1, ROC-AUC)
- [ ] File `encoder.pkl` berisi LabelEncoder yang dipakai (penting untuk Anggota 3)
- [ ] File `feature_list.txt` berisi daftar kolom fitur yang dipakai (penting untuk Anggota 3)

### 5.1 Pemahaman Konteks

Tugasmu adalah membangun mesin prediksi yang bisa menjawab pertanyaan:

> *"Dengan hanya mengetahui jadwal penerbangan (sebelum pesawat berangkat), bisakah kita memprediksi apakah penerbangan itu akan delay?"*

Ini bukan sekadar latihan ML biasa — ini simulasi sistem prediksi nyata yang bisa dipakai maskapai, bandara, atau aplikasi perjalanan untuk memberi peringatan dini kepada penumpang.

### 5.2 Pilih Target Prediksi

Ada 2 masalah yang harus diselesaikan (keduanya harus dikerjakan):

| # | Target | Kolom Label | Tipe Masalah | Metrik Utama |
|---|---|---|---|---|
| 1 | Delay atau tidak? | `arr_del15_label` | Klasifikasi Biner | ROC-AUC, F1-Score |
| 2 | Berapa menit delay? | `arr_delay_minutes_label` | Regresi | RMSE, MAE, R² |

### 5.3 Step-by-Step Workflow

#### ⬇️ Step 1: Ambil Data Training dari AWS

```python
import clickhouse_connect
import pandas as pd

client = clickhouse_connect.get_client(
    host="47.129.195.124", port=8123,
    username="default", password="rahasia123",
    database="flight_delay"
)

print("Mengambil data training (2021-2024)...")
df_train_raw = client.query_df("""
    SELECT *
    FROM ontime_features
    WHERE FlightDate < '2025-01-01'
""")
print(f"✅ Data training berhasil diambil: {len(df_train_raw):,} baris")

print("\nMengambil data testing (2025)...")
df_test_raw = client.query_df("""
    SELECT *
    FROM ontime_features
    WHERE FlightDate >= '2025-01-01'
""")
print(f"✅ Data testing berhasil diambil: {len(df_test_raw):,} baris")
```

> **💡 Tips Memori:** Jika laptop kamu RAM-nya terbatas (< 16 GB), tambahkan `LIMIT 2000000` untuk training dan `LIMIT 500000` untuk testing dulu untuk eksperimen awal. Hapus LIMIT saat training final.

#### 🔧 Step 2: Definisikan Fitur dan Label

```python
# Kolom yang BOLEH dipakai sebagai input (diketahui SEBELUM pesawat terbang)
FEATURE_COLS = [
    # Waktu
    'flight_year', 'flight_quarter', 'flight_month',
    'flight_day', 'day_of_week', 'is_weekend',
    'dep_hour', 'arr_hour',
    # Rute
    'route', 'same_state_route', 'distance_bucket',
    # Teknis penerbangan
    'CRSElapsedTime', 'Distance', 'DistanceGroup',
    # Historis performa (sudah dihitung dari data masa lalu — aman)
    'route_avg_arr_delay_prev', 'route_arr_delay_rate_prev',
    'carrier_arr_delay_rate_prev', 'carrier_cancel_rate_prev',
    'origin_arr_delay_rate_prev', 'dest_arr_delay_rate_prev',
    # Kategorikal
    'IATA_CODE_Reporting_Airline', 'Origin', 'Dest',
    'season', 'dep_time_bucket', 'arr_time_bucket',
    'OriginState', 'DestState'
]

# Label untuk Klasifikasi (Tugas 1)
TARGET_KLASIFIKASI = 'arr_del15_label'   # 1 = delay ≥15 menit, 0 = tidak delay

# Label untuk Regresi (Tugas 2 — hanya untuk penerbangan yang tidak dibatalkan)
TARGET_REGRESI = 'arr_delay_minutes_label'
```

#### ⚙️ Step 3: Preprocessing untuk Model

```python
from sklearn.preprocessing import LabelEncoder
import numpy as np

# Kolom kategorikal yang perlu di-encode ke angka
CAT_COLS = [
    'IATA_CODE_Reporting_Airline', 'Origin', 'Dest',
    'season', 'dep_time_bucket', 'arr_time_bucket',
    'OriginState', 'DestState', 'route', 'distance_bucket'
]

# Simpan encoder (penting! Anggota 3 butuh ini)
encoders = {}
for col in CAT_COLS:
    le = LabelEncoder()
    # Fit di training, transform di keduanya
    all_values = pd.concat([df_train_raw[col], df_test_raw[col]]).astype(str)
    le.fit(all_values)
    df_train_raw[col] = le.transform(df_train_raw[col].astype(str))
    df_test_raw[col] = le.transform(df_test_raw[col].astype(str))
    encoders[col] = le

# Simpan encoder
import joblib
joblib.dump(encoders, 'encoder.pkl')
print("✅ Encoder disimpan ke encoder.pkl")

# Ambil fitur yang ada di dataframe (filter yang tidak tersedia)
available_features = [c for c in FEATURE_COLS if c in df_train_raw.columns]
print(f"\nFitur yang dipakai: {len(available_features)} kolom")

# Simpan daftar fitur (penting! Anggota 3 butuh ini)
with open('feature_list.txt', 'w') as f:
    f.write('\n'.join(available_features))
print("✅ Daftar fitur disimpan ke feature_list.txt")

# Buat matrix fitur
X_train = df_train_raw[available_features].fillna(0)
X_test  = df_test_raw[available_features].fillna(0)

y_train_cls = df_train_raw[TARGET_KLASIFIKASI].fillna(0).astype(int)
y_test_cls  = df_test_raw[TARGET_KLASIFIKASI].fillna(0).astype(int)

print(f"\nUkuran data:")
print(f"  X_train: {X_train.shape}, X_test: {X_test.shape}")
print(f"\nProporsi delay di training: {y_train_cls.mean():.1%}")
```

#### 🤖 Step 4: Latih Model Baseline (Logistic Regression)

Selalu mulai dari model sederhana dulu untuk mendapatkan angka acuan.

```python
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, roc_auc_score

print("Melatih model baseline (Logistic Regression)...")
model_lr = LogisticRegression(
    max_iter=1000,
    class_weight='balanced',  # Penting: tangani imbalanced data
    random_state=42
)
model_lr.fit(X_train, y_train_cls)

y_pred_lr = model_lr.predict(X_test)
y_proba_lr = model_lr.predict_proba(X_test)[:, 1]

print("\n=== Baseline: Logistic Regression ===")
print(classification_report(y_test_cls, y_pred_lr,
      target_names=["Tepat Waktu (0)", "Delay (1)"]))
print(f"ROC-AUC: {roc_auc_score(y_test_cls, y_proba_lr):.4f}")
```

#### 🚀 Step 5: Latih Model yang Lebih Kuat

```python
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier

# --- Model 2: Random Forest ---
print("\nMelatih Random Forest...")
model_rf = RandomForestClassifier(
    n_estimators=100,
    max_depth=15,
    class_weight='balanced',
    random_state=42,
    n_jobs=-1   # Gunakan semua core CPU
)
model_rf.fit(X_train, y_train_cls)
y_pred_rf = model_rf.predict(X_test)
y_proba_rf = model_rf.predict_proba(X_test)[:, 1]

print("=== Random Forest ===")
print(classification_report(y_test_cls, y_pred_rf,
      target_names=["Tepat Waktu (0)", "Delay (1)"]))
print(f"ROC-AUC: {roc_auc_score(y_test_cls, y_proba_rf):.4f}")

# --- Model 3: XGBoost (biasanya terbaik) ---
try:
    import xgboost as xgb
    print("\nMelatih XGBoost...")
    
    # Hitung bobot untuk imbalanced data
    neg = (y_train_cls == 0).sum()
    pos = (y_train_cls == 1).sum()
    scale_pos_weight = neg / pos
    
    model_xgb = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=8,
        learning_rate=0.1,
        scale_pos_weight=scale_pos_weight,
        eval_metric='auc',
        use_label_encoder=False,
        random_state=42,
        n_jobs=-1
    )
    model_xgb.fit(X_train, y_train_cls,
                  eval_set=[(X_test, y_test_cls)],
                  verbose=50)
    y_pred_xgb = model_xgb.predict(X_test)
    y_proba_xgb = model_xgb.predict_proba(X_test)[:, 1]
    
    print("\n=== XGBoost ===")
    print(classification_report(y_test_cls, y_pred_xgb,
          target_names=["Tepat Waktu (0)", "Delay (1)"]))
    print(f"ROC-AUC: {roc_auc_score(y_test_cls, y_proba_xgb):.4f}")
except ImportError:
    print("XGBoost tidak terinstall. Jalankan: pip install xgboost")
```

#### 💾 Step 6: Simpan Model Terbaik

```python
import joblib

# Ganti dengan model yang ROC-AUC-nya paling tinggi
model_terbaik = model_rf   # Ganti dengan model_xgb jika XGBoost lebih bagus

joblib.dump(model_terbaik, 'model_klasifikasi_best.pkl')
print("✅ Model tersimpan ke model_klasifikasi_best.pkl")

# Simpan juga metrik untuk laporan
metrik = {
    'model_name': 'RandomForest',  # Update sesuai model terbaik
    'roc_auc': roc_auc_score(y_test_cls, y_proba_rf),
    'feature_count': len(available_features),
    'train_size': len(X_train),
    'test_size': len(X_test)
}
import json
with open('metrik_model.json', 'w') as f:
    json.dump(metrik, f, indent=2)
print("✅ Metrik tersimpan ke metrik_model.json")
```

#### 📊 Step 7 (Opsional): Tugas Regresi — Berapa Menit Delay?

```python
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# Filter: hanya penerbangan yang tidak dibatalkan dan punya data delay
df_train_reg = df_train_raw[
    (df_train_raw['cancelled_label'] == 0) &
    (df_train_raw[TARGET_REGRESI].notna())
].copy()

df_test_reg = df_test_raw[
    (df_test_raw['cancelled_label'] == 0) &
    (df_test_raw[TARGET_REGRESI].notna())
].copy()

X_train_reg = df_train_reg[available_features].fillna(0)
X_test_reg  = df_test_reg[available_features].fillna(0)
y_train_reg = df_train_reg[TARGET_REGRESI]
y_test_reg  = df_test_reg[TARGET_REGRESI]

model_reg = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
model_reg.fit(X_train_reg, y_train_reg)
y_pred_reg = model_reg.predict(X_test_reg)

print("=== Regresi: Random Forest ===")
print(f"MAE  (rata-rata kesalahan absolut): {mean_absolute_error(y_test_reg, y_pred_reg):.2f} menit")
print(f"RMSE (akar rata-rata kuadrat error): {mean_squared_error(y_test_reg, y_pred_reg, squared=False):.2f} menit")
print(f"R²   (seberapa baik model menjelaskan variansi): {r2_score(y_test_reg, y_pred_reg):.4f}")

joblib.dump(model_reg, 'model_regresi_best.pkl')
print("✅ Model regresi tersimpan ke model_regresi_best.pkl")
```

### 5.4 Output yang Harus Diserahkan ke Tim

Setelah selesai, kirimkan ke Anggota 3:
1. `model_klasifikasi_best.pkl` — File model terbaik
2. `model_regresi_best.pkl` — File model regresi (jika ada)
3. `encoder.pkl` — Encoder kategorikal yang dipakai
4. `feature_list.txt` — Daftar nama kolom fitur
5. `metrik_model.json` — Angka metrik

Kirimkan ke Anggota 5:
- Angka ROC-AUC, Accuracy, Precision, Recall, F1-Score dalam tabel yang rapi

---

---

## 6. ANGGOTA 3 — Data Scientist: Evaluator & EDA

**Nama Peran:** DS2 — Evaluator & Analyst  
**Input:** Model dari Anggota 2 (`.pkl`) + tabel `ontime_features` + `ontime_post_event_analysis`  
**Output yang Harus Dicapai:**
- [ ] Plot `confusion_matrix.png` — visualisasi performa klasifikasi
- [ ] Plot `roc_curve.png` — kurva ROC
- [ ] Plot `feature_importance.png` — fitur paling berpengaruh
- [ ] Plot `delay_penyebab_per_maskapai.png` — analisis root cause
- [ ] Plot `tren_delay_per_tahun.png` — tren dari tahun ke tahun
- [ ] Notebook/laporan EDA dengan minimal 5 temuan menarik
- [ ] Konfirmasi tidak ada leakage di model Anggota 2

### 6.1 Pemahaman Konteks

Tugasmu adalah menjadi **quality assurance** dan **analis** sekaligus:
1. **QA Model:** Pastikan model yang dibuat Anggota 2 valid, tidak curang, dan metriknya dipahami secara benar
2. **EDA:** Temukan pola menarik di data untuk mendukung narasi presentasi

### 6.2 Step-by-Step Workflow

#### 🔍 Step 1: Evaluasi Model Anggota 2 Secara Menyeluruh

```python
import joblib
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.rcParams['figure.figsize'] = (10, 6)
matplotlib.rcParams['font.size'] = 12
import clickhouse_connect
from sklearn.metrics import (
    classification_report, roc_auc_score,
    confusion_matrix, ConfusionMatrixDisplay, RocCurveDisplay
)

# Load file dari Anggota 2
model = joblib.load('model_klasifikasi_best.pkl')
encoders = joblib.load('encoder.pkl')

with open('feature_list.txt', 'r') as f:
    FEATURE_COLS = f.read().splitlines()

print(f"Model dimuat: {type(model).__name__}")
print(f"Jumlah fitur: {len(FEATURE_COLS)}")

# Ambil data testing dari AWS
client = clickhouse_connect.get_client(
    host="47.129.195.124", port=8123,
    username="default", password="rahasia123",
    database="flight_delay"
)

df_test = client.query_df("""
    SELECT *
    FROM ontime_features
    WHERE FlightDate >= '2025-01-01'
""")
print(f"Data testing: {len(df_test):,} baris (2025)")

# Apply encoders
CAT_COLS = list(encoders.keys())
for col in CAT_COLS:
    if col in df_test.columns:
        le = encoders[col]
        df_test[col] = df_test[col].astype(str).apply(
            lambda x: le.transform([x])[0] if x in le.classes_ else -1
        )

X_test = df_test[FEATURE_COLS].fillna(0)
y_test = df_test['arr_del15_label'].fillna(0).astype(int)
```

#### 📊 Step 2: Buat Confusion Matrix

```python
y_pred  = model.predict(X_test)
y_proba = model.predict_proba(X_test)[:, 1]

print("=== Laporan Klasifikasi Lengkap ===")
print(classification_report(y_test, y_pred,
      target_names=["Tepat Waktu", "Delay"]))
print(f"ROC-AUC Score: {roc_auc_score(y_test, y_proba):.4f}")

# Plot Confusion Matrix
fig, ax = plt.subplots(figsize=(8, 6))
cm = confusion_matrix(y_test, y_pred)
disp = ConfusionMatrixDisplay(confusion_matrix=cm,
       display_labels=["Tepat Waktu", "Delay"])
disp.plot(ax=ax, cmap='Blues', values_format='d')
ax.set_title("Confusion Matrix — Model Prediksi Delay Penerbangan", fontsize=14, pad=15)

# Tambahkan anotasi penjelasan
total = cm.sum()
plt.figtext(0.5, -0.05,
    f"Total data uji: {total:,} penerbangan | "
    f"Akurasi: {(cm[0,0]+cm[1,1])/total:.1%}",
    ha='center', fontsize=10, style='italic')

plt.tight_layout()
plt.savefig("confusion_matrix.png", dpi=150, bbox_inches='tight')
plt.show()
print("✅ confusion_matrix.png tersimpan")
```

#### 📈 Step 3: Plot ROC Curve

```python
from sklearn.metrics import RocCurveDisplay

fig, ax = plt.subplots(figsize=(8, 6))
RocCurveDisplay.from_predictions(
    y_test, y_proba,
    name=f"{type(model).__name__}",
    ax=ax
)
ax.plot([0, 1], [0, 1], 'k--', label='Random Classifier (AUC=0.50)')
ax.set_title("ROC Curve — Model Prediksi Delay Penerbangan", fontsize=14)
ax.legend(loc='lower right')
plt.tight_layout()
plt.savefig("roc_curve.png", dpi=150, bbox_inches='tight')
plt.show()
print("✅ roc_curve.png tersimpan")
```

#### 🌟 Step 4: Analisis Feature Importance

```python
import numpy as np

# Ambil feature importance (berlaku untuk tree-based models: RF, XGBoost)
if hasattr(model, 'feature_importances_'):
    importances = model.feature_importances_
    indices = np.argsort(importances)[::-1][:20]  # Top 20 fitur

    fig, ax = plt.subplots(figsize=(10, 8))
    bars = ax.barh(
        [FEATURE_COLS[i] for i in indices[::-1]],
        [importances[i] for i in indices[::-1]],
        color='steelblue', edgecolor='white'
    )
    ax.set_xlabel("Tingkat Kepentingan Fitur (Importance Score)")
    ax.set_title("Top 20 Fitur Paling Berpengaruh terhadap Prediksi Delay", fontsize=13)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig("feature_importance.png", dpi=150, bbox_inches='tight')
    plt.show()
    print("✅ feature_importance.png tersimpan")
    
    print("\n📊 Top 10 Fitur Terpenting:")
    for i in indices[:10]:
        print(f"  {i+1:2d}. {FEATURE_COLS[i]:40s} → {importances[i]:.4f}")
else:
    print("Model ini tidak memiliki feature_importances_. Gunakan permutation importance.")
```

#### 🔬 Step 5: Cek Imbalanced Data

```python
# Seberapa seimbang data antara kelas "delay" dan "tepat waktu"?
delay_count    = (y_test == 1).sum()
ontime_count   = (y_test == 0).sum()
delay_rate     = delay_count / len(y_test)

print(f"📊 Distribusi Data Testing:")
print(f"   Tepat Waktu (0): {ontime_count:,} penerbangan ({1-delay_rate:.1%})")
print(f"   Delay (1)      : {delay_count:,} penerbangan ({delay_rate:.1%})")

if delay_rate < 0.2 or delay_rate > 0.8:
    print("⚠️  Data tidak seimbang (imbalanced)!")
    print("   Pastikan Anggota 2 sudah pakai class_weight='balanced' atau scale_pos_weight")
else:
    print("✅ Data cukup seimbang — tidak ada masalah major imbalance")
```

#### 🕵️ Step 6: Analisis Penyebab Delay (EDA Post-Event)

```python
# Ambil data analisis post-event (kolom penyebab delay)
df_cause = client.query_df("""
    SELECT
        IATA_CODE_Reporting_Airline     AS maskapai,
        sum(CarrierDelay)               AS total_carrier_delay_mnt,
        sum(WeatherDelay)               AS total_weather_delay_mnt,
        sum(NASDelay)                   AS total_nas_delay_mnt,
        sum(LateAircraftDelay)          AS total_late_aircraft_mnt,
        sum(SecurityDelay)              AS total_security_delay_mnt,
        count()                         AS total_penerbangan,
        countIf(arr_del15_label = 1)    AS total_delay
    FROM ontime_post_event_analysis
    GROUP BY maskapai
    ORDER BY total_carrier_delay_mnt DESC
    LIMIT 10
""")

print("\n📋 Penyebab Delay per Maskapai (Top 10):")
print(df_cause.to_string(index=False))

# Plot Stacked Bar
fig, ax = plt.subplots(figsize=(12, 7))
df_plot = df_cause.set_index('maskapai')[[
    'total_carrier_delay_mnt', 'total_weather_delay_mnt',
    'total_nas_delay_mnt', 'total_late_aircraft_mnt'
]]
df_plot.columns = ['Carrier', 'Weather', 'NAS', 'Late Aircraft']
df_plot.plot(kind='bar', stacked=True, ax=ax,
             color=['#e74c3c', '#3498db', '#f39c12', '#9b59b6'])

ax.set_title("Kontribusi Penyebab Delay per Maskapai (Total Menit)", fontsize=13)
ax.set_xlabel("Kode Maskapai")
ax.set_ylabel("Total Menit Delay")
ax.legend(loc='upper right')
ax.tick_params(axis='x', rotation=0)
plt.tight_layout()
plt.savefig("delay_penyebab_per_maskapai.png", dpi=150, bbox_inches='tight')
plt.show()
print("✅ delay_penyebab_per_maskapai.png tersimpan")
```

#### 📅 Step 7: EDA Tren Delay per Tahun

```python
df_tren = client.query_df("""
    SELECT
        flight_year                 AS tahun,
        countIf(arr_del15_label=1) AS total_delay,
        count()                     AS total_penerbangan,
        round(avg(arr_delay_minutes_label), 2) AS avg_delay_mnt
    FROM ontime_features
    GROUP BY tahun
    ORDER BY tahun
""")

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Subplot 1: Proporsi delay per tahun
df_tren['pct_delay'] = df_tren['total_delay'] / df_tren['total_penerbangan'] * 100
axes[0].bar(df_tren['tahun'].astype(str), df_tren['pct_delay'],
            color='#e74c3c', edgecolor='white')
axes[0].set_title("Proporsi Penerbangan Delay per Tahun (%)")
axes[0].set_ylabel("Persentase Delay (%)")
axes[0].set_ylim(0, max(df_tren['pct_delay']) * 1.2)
for i, (year, pct) in enumerate(zip(df_tren['tahun'], df_tren['pct_delay'])):
    axes[0].text(i, pct + 0.5, f"{pct:.1f}%", ha='center', fontsize=10)

# Subplot 2: Rata-rata menit delay per tahun
axes[1].plot(df_tren['tahun'].astype(str), df_tren['avg_delay_mnt'],
             marker='o', color='#3498db', linewidth=2, markersize=8)
axes[1].set_title("Rata-rata Menit Delay per Tahun")
axes[1].set_ylabel("Rata-rata Menit Delay")
axes[1].fill_between(df_tren['tahun'].astype(str), df_tren['avg_delay_mnt'],
                     alpha=0.15, color='#3498db')

plt.suptitle("Analisis Tren Delay 2021–2025", fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig("tren_delay_per_tahun.png", dpi=150, bbox_inches='tight')
plt.show()
print("✅ tren_delay_per_tahun.png tersimpan")
```

### 6.3 Checklist Validasi (Anggota 3 sebagai QA)

Sebelum menyatakan model "valid", pastikan semua item berikut:

- [ ] **Tidak ada leakage:** Cek bahwa `FEATURE_COLS` tidak mengandung kolom dari daftar berikut: `DepTime`, `ArrTime`, `TaxiOut`, `TaxiIn`, `WheelsOff`, `WheelsOn`, `ActualElapsedTime`, `AirTime`, `CarrierDelay`, `WeatherDelay`, `NASDelay`, `SecurityDelay`, `LateAircraftDelay`
- [ ] **Split berbasis waktu:** Training = 2021-2024, Testing = 2025 (bukan random)
- [ ] **Imbalanced data ditangani:** Model menggunakan `class_weight='balanced'` atau parameter setara
- [ ] **Metrik tidak menyesatkan:** Jika ROC-AUC > 0.99, curigai adanya leakage!
- [ ] **Confusion matrix masuk akal:** Model tidak memprediksi semua "Tepat Waktu" saja

### 6.4 Output yang Harus Diserahkan ke Tim

Kirimkan ke Anggota 5:
1. `confusion_matrix.png`
2. `roc_curve.png`
3. `feature_importance.png`
4. `delay_penyebab_per_maskapai.png`
5. `tren_delay_per_tahun.png`
6. Ringkasan 5 temuan EDA paling menarik (dalam format bullet points)

---

---

## 7. ANGGOTA 4 — Data Analyst: Dashboard Builder

**Nama Peran:** BI — Dashboard Analyst  
**Input:** Tabel agregasi `agg_*` di Grafana AWS  
**Output yang Harus Dicapai:**
- [ ] Dashboard Grafana dengan minimal 6 panel informatif
- [ ] Setiap panel memiliki judul, satuan, dan interpretasi yang jelas
- [ ] Dashboard bisa dibuka dan di-demo langsung dari browser
- [ ] Screenshot atau rekaman layar dashboard yang sudah jadi

### 7.1 Pemahaman Konteks

Tugasmu adalah menjadi **pencerita visual** menggunakan data. Dashboard yang kamu buat harus menjawab pertanyaan orang awam seperti:
- "Maskapai mana yang paling sering bikin penumpang kecewa?"
- "Kalau saya terbang sore hari, lebih berisiko delay?"
- "Apakah delay makin parah dari tahun ke tahun?"

> ⚠️ **ATURAN PENTING:** Jangan pernah query langsung ke tabel `ontime_raw` atau `ontime_curated` dari Grafana. Gunakan **HANYA** tabel yang berawalan `agg_`. Alasannya: tabel raw berisi 30 juta baris, kalau di-query dari dashboard setiap orang buka, server akan kewalahan.

### 7.2 Step-by-Step Workflow

#### 🔐 Step 1: Login ke Grafana AWS

1. Buka browser, ketik di address bar: `http://47.129.195.124:3000`
2. Login: Username `admin`, Password `admin123`
3. Kamu akan disambut halaman home Grafana

#### 🔌 Step 2: Tambahkan Data Source ClickHouse

1. Klik ikon ⚙️ (Settings) di menu kiri → **Data Sources**
2. Klik **+ Add new data source**
3. Cari "ClickHouse" di kotak pencarian, klik untuk memilih
4. Isi pengaturan berikut:

| Field | Nilai |
|---|---|
| Server address | `localhost` |
| Server port | `9000` |
| Protocol | Native |
| Username | `default` |
| Password | `rahasia123` |
| Default database | `flight_delay` |

5. Scroll ke bawah, klik **Save & Test**
6. Harus muncul notifikasi hijau ✅ "Data source is working"

> ⚠️ **Catatan:** Karena Grafana dan ClickHouse berada di server EC2 yang sama, koneksi menggunakan `localhost`, bukan IP publik.

#### 🎨 Step 3: Buat Dashboard Baru

1. Klik ikon `+` di menu kiri → **New Dashboard**
2. Klik **+ Add visualization**
3. Pilih data source ClickHouse yang baru ditambahkan

#### 📊 Step 4: Buat Panel-Panel Berikut

---

**Panel 1 — Tren Rata-rata Delay Bulanan (Time Series)**

Tipe: Time series  
Judul: `Tren Rata-rata Delay Kedatangan & Keberangkatan (2021–2025)`  
Unit: `menit`

```sql
SELECT
    makeDate(year, month)  AS tanggal,
    round(avg_arr_delay, 2) AS delay_kedatangan_mnt,
    round(avg_dep_delay, 2) AS delay_keberangkatan_mnt
FROM flight_delay.agg_monthly_delay
ORDER BY tanggal
```

*Interpretasi: Tren naik turun delay per bulan selama 5 tahun.*

---

**Panel 2 — Peringkat Maskapai Paling Delay (Bar Chart)**

Tipe: Bar chart (horizontal)  
Judul: `Top 10 Maskapai dengan Tingkat Delay Tertinggi (2024)`  
Unit: `%`

```sql
SELECT
    airline_code                            AS maskapai,
    round(arr_delay_rate * 100, 1)         AS persen_delay
FROM flight_delay.agg_carrier_performance
WHERE year = 2024
ORDER BY arr_delay_rate DESC
LIMIT 10
```

*Interpretasi: Maskapai dengan batang terpanjang = paling sering delay.*

---

**Panel 3 — Pola Delay per Jam & Hari (Heatmap)**

Tipe: Heatmap  
Judul: `Pola Delay berdasarkan Jam Keberangkatan & Hari (Heatmap)`  
Unit: `%`

```sql
SELECT
    dep_hour   AS jam_keberangkatan,
    day_of_week AS hari_dalam_minggu,   -- 1=Senin, 7=Minggu
    round(delay_rate * 100, 1)          AS persen_delay
FROM flight_delay.agg_hourly_delay
ORDER BY jam_keberangkatan, hari_dalam_minggu
```

*Interpretasi: Sel berwarna gelap = saat paling berisiko delay.*

---

**Panel 4 — 10 Rute Paling Sering Delay (Table)**

Tipe: Table  
Judul: `10 Rute Penerbangan dengan Delay Tertinggi (2024)`

```sql
SELECT
    route                                           AS rute,
    total_flights                                   AS total_penerbangan,
    round(arr_delay_rate * 100, 1)                 AS persen_delay,
    round(avg_arr_delay, 1)                        AS avg_delay_mnt
FROM flight_delay.agg_route_performance
WHERE year = 2024
    AND total_flights > 100      -- Filter rute yang cukup signifikan
ORDER BY arr_delay_rate DESC
LIMIT 10
```

*Interpretasi: Rute dengan % delay tinggi = butuh perhatian khusus.*

---

**Panel 5 — Penyebab Delay per Maskapai (Stacked Bar)**

Tipe: Bar chart (stacked)  
Judul: `Kontribusi Penyebab Delay per Maskapai (2024)`  
Unit: `%`

```sql
SELECT
    airline_code                              AS maskapai,
    round(pct_carrier * 100, 1)              AS persen_maskapai,
    round(pct_weather * 100, 1)              AS persen_cuaca,
    round(pct_nas * 100, 1)                  AS persen_nas,
    round(pct_late_aircraft * 100, 1)        AS persen_pesawat_terlambat
FROM flight_delay.agg_delay_reason
WHERE year = 2024
ORDER BY persen_maskapai DESC
LIMIT 10
```

*Interpretasi: Bagian merah (carrier) = delay karena maskapai sendiri. Cuaca tidak bisa dikontrol.*

---

**Panel 6 — 10 Bandara Paling Delay (Bar Chart)**

Tipe: Bar chart (horizontal)  
Judul: `10 Bandara Tersibuk dengan Delay Kedatangan Tertinggi (2024)`  
Unit: `%`

```sql
SELECT
    airport_code                             AS bandara,
    total_arrivals                           AS total_kedatangan,
    round(arr_delay_rate * 100, 1)          AS persen_delay_kedatangan
FROM flight_delay.agg_airport_performance
WHERE year = 2024
    AND total_arrivals > 1000
ORDER BY arr_delay_rate DESC
LIMIT 10
```

---

**Panel 7 (Bonus) — Statistik Ringkasan (Stat Panels)**

Buat 4 kotak statistik kecil di bagian atas dashboard:
- **Total Penerbangan:** `SELECT formatReadableQuantity(count()) FROM flight_delay.ontime_curated`
- **Rata-rata Delay Keseluruhan:** `SELECT round(avg(arr_delay_minutes_label), 1) FROM flight_delay.ontime_features WHERE arr_del15_label = 1`
- **Tingkat Pembatalan:** `SELECT round(countIf(cancelled_label=1) / count() * 100, 2) FROM flight_delay.ontime_features`
- **Tahun Data:** `SELECT concat(toString(min(flight_year)), '–', toString(max(flight_year))) FROM flight_delay.ontime_features`

#### 💄 Step 5: Atur Estetika Dashboard

- Gunakan **warna merah** (`#e74c3c`) untuk angka delay tinggi, **hijau** (`#27ae60`) untuk rendah
- Beri **deskripsi singkat** di setiap panel (klik panel → Edit → Description)
- Atur ukuran panel: Panel Heatmap dan Time Series di bagian atas (lebih besar), tabel dan bar kecil di bawah
- Beri **judul dashboard utama** yang menarik

#### 💾 Step 6: Simpan & Bagikan

1. Klik ikon 💾 (Save) di pojok kanan atas
2. Beri nama: **`Flight Delay Analysis — Final Project BigData 2025`**
3. Klik **Save**
4. Untuk mendapatkan link berbagi: Klik ikon `🔗 Share` → Copy link
5. Bagikan URL ke seluruh tim (terutama Anggota 5 untuk presentasi)

#### 📸 Step 7: Ambil Screenshot

Ambil screenshot seluruh dashboard dalam kondisi ter-render penuh. Screenshot ini akan dipakai di slide presentasi Anggota 5.

---

---

## 8. ANGGOTA 5 — Business Analyst: Storyteller & Presenter

**Nama Peran:** BA — Business Storyteller  
**Input:** Semua hasil dari Anggota 2, 3, dan 4  
**Output yang Harus Dicapai:**
- [ ] File presentasi final (PowerPoint/Google Slides) dengan ≥ 14 slide
- [ ] Narasi untuk demo live Grafana Dashboard
- [ ] Daftar Q&A — jawaban siap untuk pertanyaan dosen/juri

### 8.1 Pemahaman Konteks

Tugasmu adalah yang paling krusial di hari H: meyakinkan audiens bahwa **proyek ini punya nilai bisnis nyata**, bukan sekadar tugas teknis.

Orang yang mendengarkan presentasimu mungkin tidak tahu apa itu Kafka atau ClickHouse. Tugasmu adalah menerjemahkan semua kerumitan teknis itu menjadi cerita yang mudah dipahami dan berimpact.

**Kerangka berpikir yang harus selalu kamu pegang:**

```
MASALAH (Pain Point)
      ↓
SOLUSI KITA (Apa yang Kita Bangun)
      ↓
BUKTI (Data & Grafik)
      ↓
DAMPAK BISNIS (Apa Artinya untuk Dunia Nyata)
```

### 8.2 Step-by-Step Workflow

#### 📚 Step 1: Pahami Konteks Bisnis (Lakukan Sendiri Dulu)

Sebelum minta bahan ke siapapun, jawab pertanyaan-pertanyaan ini:

**Konteks Masalah:**
- Siapa yang dirugikan oleh delay pesawat? (Penumpang kehilangan koneksi, maskapai kena denda, bandara penuh sesak, bisnis perjalanan rugi)
- Berapa kerugian finansial delay pesawat per tahun? (Cari di internet: "cost of airline delay US billions")
- Kalau kita bisa memprediksi delay 24 jam sebelumnya, siapa yang diuntungkan?

**Konteks Teknis (Cukup Pahami Analoginya):**
- Kafka = ban berjalan yang mengangkut data
- Spark = mesin raksasa yang membersihkan data
- ClickHouse = gudang penyimpanan super cepat
- Grafana = papan dashboard yang semua orang bisa lihat

#### 📦 Step 2: Kumpulkan Bahan dari Semua Anggota

Buat grup chat atau spreadsheet untuk tracking bahan yang sudah terkumpul:

| Bahan | Dari | Status |
|---|---|---|
| Angka ROC-AUC model terbaik | Anggota 2 | [ ] |
| Tabel metrik (Accuracy, Precision, Recall, F1) | Anggota 2 | [ ] |
| `confusion_matrix.png` | Anggota 3 | [ ] |
| `roc_curve.png` | Anggota 3 | [ ] |
| `feature_importance.png` | Anggota 3 | [ ] |
| `delay_penyebab_per_maskapai.png` | Anggota 3 | [ ] |
| `tren_delay_per_tahun.png` | Anggota 3 | [ ] |
| 5 temuan EDA paling menarik | Anggota 3 | [ ] |
| URL dashboard Grafana | Anggota 4 | [ ] |
| Screenshot dashboard (full) | Anggota 4 | [ ] |
| Diagram arsitektur pipeline | Data Engineer | [ ] |

#### 🗂️ Step 3: Susun Struktur Slide Presentasi

Gunakan struktur narasi **"Problem → Solution → Data → Result → Impact"**:

| # | Judul Slide | Isi | Sumber Bahan |
|---|---|---|---|
| 1 | **Cover** | Nama proyek, nama anggota tim, tanggal presentasi | - |
| 2 | **Problem Statement** | "Setiap tahun, X juta penerbangan delay di AS, merugikan Y miliar dolar..." | Riset sendiri |
| 3 | **Mengapa Ini Masalah Big Data?** | 30 juta baris, 5 tahun, tidak bisa di Excel | - |
| 4 | **Tujuan Proyek** | 3 tujuan: Analisis historis, prediksi ML, dashboard real-time | - |
| 5 | **Dataset & Sumber** | BTS, 2021–2025, 29+ juta penerbangan, variabel kunci | - |
| 6 | **Arsitektur Pipeline** | Diagram: CSV → Kafka → Spark → ClickHouse → Grafana + AWS | Data Engineer |
| 7 | **Temuan EDA #1: Tren** | Grafik tren delay per tahun | `tren_delay_per_tahun.png` |
| 8 | **Temuan EDA #2: Maskapai** | Maskapai mana paling delay? | Screenshot Grafana Panel 2 |
| 9 | **Temuan EDA #3: Waktu** | Jam & hari paling berisiko? | Screenshot Grafana Panel 3 |
| 10 | **Temuan EDA #4: Penyebab** | Cuaca vs Maskapai vs NAS? | `delay_penyebab_per_maskapai.png` |
| 11 | **Model ML — Metodologi** | Fitur apa yang dipakai, algoritma yang dipilih, kenapa split berbasis waktu | Anggota 2 & 3 |
| 12 | **Model ML — Hasil** | Tabel metrik model, perbandingan baseline vs final | Anggota 2 & 3 |
| 13 | **Feature Importance** | Faktor apa yang paling menentukan delay? | `feature_importance.png` |
| 14 | **Demo Live** | "Sekarang kami tunjukkan dashboardnya langsung..." | URL Grafana |
| 15 | **Rekomendasi Bisnis** | 3–5 rekomendasi konkret berdasarkan data | Berdasarkan semua temuan |
| 16 | **Kesimpulan & Penutup** | Ringkasan 3 poin pencapaian + ucapan terima kasih | - |

#### 💡 Step 4: Tulis Rekomendasi Bisnis yang Kuat

Berdasarkan data, susun minimal 3 rekomendasi seperti berikut (isi angka nyata dari hasil Anggota 3):

1. **Maskapai:** *"Maskapai [X] secara konsisten memiliki tingkat delay [Y]% di atas rata-rata nasional. Otoritas penerbangan perlu melakukan audit operasional terhadap maskapai ini, terutama pada aspek ketepatan jadwal ground crew."*

2. **Waktu Terbang:** *"Penerbangan yang jadwal berangkat antara pukul 17:00–20:00 memiliki kemungkinan delay [X]x lebih tinggi dibanding penerbangan pagi. Penumpang yang sensitif waktu disarankan memilih penerbangan sebelum pukul 11:00."*

3. **Prediksi:** *"Dengan model prediksi yang mencapai ROC-AUC [X.XX], maskapai dan bandara bisa memprediksi penerbangan berpotensi delay sejak 24 jam sebelumnya. Ini memungkinkan manajemen proaktif: re-routing, notifikasi penumpang lebih awal, dan alokasi gate yang lebih baik."*

4. **Infrastruktur Bandara:** *"Bandara [X] menjadi titik kemacetan (bottleneck) terbesar berdasarkan tingkat delay kedatangan [Y]%. Investasi pada sistem manajemen apron dan koordinasi ATC di bandara ini akan berdampak luas ke seluruh jaringan penerbangan AS."*

5. **Sistem Monitoring:** *"Pipeline Big Data yang kami bangun dapat dioperasikan secara real-time untuk memonitor kondisi delay di seluruh jaringan. Dashboard Grafana yang terhubung ke data terkini memungkinkan tim operasional maskapai melihat anomali dalam hitungan menit, bukan jam."*

#### 🎤 Step 5: Siapkan Narasi Demo Dashboard

Latih kalimat pembuka demo yang meyakinkan:

> *"Baik, kami akan menunjukkan dashboard yang kami bangun secara langsung. Dashboard ini berjalan di server AWS kami dan terhubung langsung ke database ClickHouse yang berisi 29 juta data penerbangan.*
>
> *Panel pertama ini menampilkan tren rata-rata delay selama 5 tahun terakhir. Bisa dilihat bahwa... [jelaskan pola menarik]. Panel berikutnya adalah peringkat maskapai — dan menariknya... [jelaskan temuan]. Heatmap ini menunjukkan... [jelaskan jam/hari berisiko]."*

#### ❓ Step 6: Siapkan Jawaban Q&A

Latih jawaban untuk pertanyaan dosen yang paling sering muncul:

**Q: "Kenapa pakai Kafka? Tidak bisa langsung masukkan ke ClickHouse saja?"**  
*A: "Kafka berfungsi sebagai buffer dan message broker. Ini memungkinkan decoupling antara sistem yang menghasilkan data (producer) dan yang mengonsumsi data. Kalau ClickHouse sedang tidak tersedia atau perlu di-restart, data di Kafka tidak hilang dan bisa dilanjutkan dari titik terakhir. Selain itu, dalam implementasi nyata, banyak sistem bisa membaca stream yang sama — monitoring, alerting, ClickHouse — semuanya secara paralel tanpa saling mengganggu."*

**Q: "Kenapa tidak pakai Deep Learning/LSTM?"**  
*A: "Kami memilih Random Forest dan XGBoost karena: (1) Interpretabilitas — kita bisa melihat fitur mana yang paling berpengaruh, yang penting untuk rekomendasi bisnis; (2) Efisiensi — tree-based methods lebih cepat di-train pada data tabular skala besar; (3) Performa — pada data tabular seperti ini, XGBoost sering setara atau lebih baik dari deep learning; (4) Waktu — LSTM butuh sequential data dan hyperparameter tuning yang lebih kompleks."*

**Q: "Seberapa akurat modelnya di data dunia nyata?"**  
*A: "Model kami diuji pada data tahun 2025 yang tidak pernah dilihat selama training — ini mensimulasikan kondisi dunia nyata. Hasilnya: ROC-AUC [masukkan angka], artinya model [X]% lebih baik dari tebak acak. Dalam konteks bisnis, ini berarti dari [misalnya] 1000 penerbangan yang diprediksi delay, model kami benar untuk [X] penerbangan."*

**Q: "Apa itu temporal leakage dan bagaimana Anda menghindarinya?"**  
*A: "Temporal leakage terjadi ketika model 'mengetahui masa depan' saat training. Kami menghindarinya dengan dua cara: (1) Time-based split — training 2021-2024, testing 2025, sehingga model tidak pernah melihat data masa depan; (2) Feature isolation — kolom yang hanya diketahui setelah penerbangan (CarrierDelay, DepTime aktual, dll.) dipisahkan ke tabel ontime_post_event_analysis dan sama sekali tidak dimasukkan ke feature vector model."*

**Q: "Kenapa pakai ClickHouse bukan MySQL atau PostgreSQL?"**  
*A: "ClickHouse adalah columnar database yang dirancang khusus untuk analitik skala besar. Untuk query seperti 'rata-rata delay dari 30 juta baris', ClickHouse bisa 10-100x lebih cepat dari MySQL karena hanya membaca kolom yang dibutuhkan, bukan seluruh baris. MySQL lebih cocok untuk aplikasi transaksional (beli tiket, simpan booking), bukan analitik besar."*

### 8.3 Checklist Sebelum Presentasi

- [ ] Semua 16 slide sudah berisi konten final (bukan placeholder)
- [ ] Semua gambar/grafik sudah dimasukkan dan tidak blur
- [ ] Angka metrik model sudah diupdate ke angka final dari Anggota 2
- [ ] Demo Grafana sudah dicoba dari laptop yang akan dipakai presentasi
- [ ] Koneksi internet sudah dipastikan (untuk akses AWS Grafana live)
- [ ] Backup: screenshot semua panel dashboard jika internet bermasalah
- [ ] Sudah latihan presentasi minimal 2 kali
- [ ] Sudah tahu siapa yang akan pegang clicker/navigasi slide

---

---

## 9. Timeline Kolaborasi

| Hari | Anggota 2 (DS1) | Anggota 3 (DS2) | Anggota 4 (BI) | Anggota 5 (BA) |
|---|---|---|---|---|
| **Hari 1** | Test koneksi, eksplorasi data, mulai preprocess untuk model | Test koneksi, baca dokumentasi, pahami struktur data | Test koneksi Grafana, coba tambah data source | Baca semua dokumen ini, riset konteks bisnis delay pesawat |
| **Hari 2** | Latih model baseline (Logistic Regression), dokumentasi metrik | Minta file pkl dari DS1, mulai evaluasi, buat confusion matrix | Bangun panel 1 & 2 (tren + maskapai) | Susun outline 16 slide, kumpulkan bahan awal |
| **Hari 3** | Latih model lanjutan (RF, XGBoost), compare performa | Feature importance, EDA penyebab delay, plot tren | Bangun panel 3–6 (heatmap, rute, penyebab, bandara) | Tulis narasi setiap slide, mulai rekomendasi bisnis |
| **Hari 4** | Finalisasi model terbaik, simpan pkl + encoder + metrik | Finalisasi laporan evaluasi, ceklist validasi leakage | Rapikan layout dashboard, ambil screenshot | Masukkan semua bahan, finalkan slide + Q&A |
| **Hari 5** | 🤝 **Rapat tim: Sinkronisasi semua hasil** — pastikan angka konsisten di semua slide | | | |
| **H-1** | Stand-by untuk pertanyaan teknis | Stand-by untuk pertanyaan teknis | Pastikan Grafana bisa dibuka dari laptop presentasi | Gladi resik lengkap, latihan Q&A |

---

---

## 10. FAQ — Pertanyaan Yang Sering Muncul

### ❓ Saya tidak bisa konek ke server AWS. Apa yang harus dilakukan?

1. Pastikan kamu terhubung ke internet
2. Pastikan IP `47.129.195.124` dan port `8123` sudah benar
3. Coba buka di browser: `http://47.129.195.124:8123/play` — kalau muncul kotak SQL, server hidup
4. Kalau masih tidak bisa, hubungi Data Engineer untuk cek Security Group AWS

### ❓ Data terambil tapi ada kolom yang NULL, normal tidak?

Normal. NULL berarti "tidak ada data". Contoh: penerbangan yang dibatalkan tidak memiliki data `arr_delay_minutes_label` karena penerbangan itu tidak pernah mendarat. Gunakan `.fillna(0)` untuk angka dan `.fillna('Unknown')` untuk teks saat mempersiapkan data untuk model.

### ❓ Kenapa waktu ambil data lama sekali?

Karena datanya 30 juta baris. Tips untuk lebih cepat:
- Saat eksperimen, tambahkan `LIMIT 1000000` di query
- Gunakan query yang filter kolom yang dibutuhkan saja: `SELECT col1, col2, col3 FROM ...` (jangan `SELECT *`)
- Pertimbangkan simpan data ke file lokal setelah berhasil diambil: `df.to_parquet('train_data.parquet')`

### ❓ Model saya ROC-AUC-nya 0.99 lebih — apakah ini bagus?

Justru curigai ini! ROC-AUC di atas 0.99 hampir pasti ada **data leakage**. Pastikan:
- Tidak ada kolom label (arr_del15_label, arr_delay_minutes_label, dll.) yang masuk ke FEATURE_COLS
- Tidak ada kolom post-event (CarrierDelay, DepTime aktual, dll.) yang masuk ke fitur
- Split sudah berbasis waktu, bukan random

ROC-AUC yang realistis untuk masalah ini: **0.65 – 0.80**. Di atas itu patut dicurigai.

### ❓ Sebagai Anggota 4, apa yang dilakukan jika panel Grafana error "No data"?

1. Klik panel → Edit → buka tab Query
2. Jalankan query SQL secara manual di ClickHouse Play (`http://47.129.195.124:8123/play`) untuk verifikasi hasilnya ada
3. Pastikan nama database dan tabel sudah benar: `flight_delay.agg_monthly_delay`
4. Periksa syntax: ClickHouse menggunakan SQL dialect sendiri yang sedikit berbeda dari MySQL

### ❓ Sebagai Anggota 5, bagaimana kalau internet bermasalah saat demo?

Selalu siapkan **plan B**: Screenshot semua panel dashboard dalam resolusi tinggi (full screen). Simpan dalam folder terpisah. Kalau internet tidak bisa, tunjukkan screenshot sambil tetap jelaskan interaktivitasnya.

---

*Dokumen ini dibuat berdasarkan kode dan infrastruktur aktual proyek. Jika ada pertanyaan teknis, hubungi Data Engineer. Jika ada pertanyaan tentang dokumen ini, diskusikan di grup tim.*

**Last updated:** 2026-06-19 | **Status Infrastruktur:** ✅ AWS EC2 aktif, ClickHouse berisi data bersih siap dipakai
