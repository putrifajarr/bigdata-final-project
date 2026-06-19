# DE Setup Guide — OnTime Flight Delay Pipeline

Dokumen ini berisi semua langkah setup eksternal yang perlu Anda lakukan di luar kode,
sebelum pipeline bisa dijalankan. Ikuti urutan ini secara berurutan.

---

## Prasyarat — Yang Perlu Diinstall di Mesin Anda

### 1. Docker Desktop

Unduh dari: https://www.docker.com/products/docker-desktop

Verifikasi setelah install:

```powershell
docker --version
docker compose version
```

Pastikan Docker Desktop sudah berjalan (ikon di system tray aktif) sebelum menjalankan perintah apapun.

**Catatan penting untuk Windows:**
- Aktifkan WSL 2 integration di Docker Desktop Settings → Resources → WSL Integration
- Alokasikan minimal 8 GB RAM dan 4 CPU untuk Docker di Settings → Resources

### 2. Python 3.11+

Unduh dari: https://www.python.org/downloads/

Verifikasi:

```powershell
python --version
pip --version
```

### 3. Git

Unduh dari: https://git-scm.com/download/win

---

## Step 1: Clone Repository dan Setup Folder

```powershell
# Clone repo (atau pull jika sudah ada)
git clone <url-repo-tim-anda>

# Masuk ke folder project
cd final-project

# Buat folder yang dibutuhkan (belum ada di repo karena berisi data)
New-Item -ItemType Directory -Path data\raw\ontime -Force
New-Item -ItemType Directory -Path data\sample -Force
New-Item -ItemType Directory -Path data\rejected -Force
New-Item -ItemType Directory -Path spark\checkpoints -Force
New-Item -ItemType Directory -Path spark\conf -Force
```

---

## Step 2: Setup Environment Variables

```powershell
# Copy template env ke file aktif
Copy-Item .env.example .env
```

Buka `.env` dengan text editor dan sesuaikan jika perlu. Untuk development lokal, nilai default sudah cukup.

---

## Step 3: Install Python Dependencies untuk Producer

```powershell
pip install -r producer\requirements.txt
```

---

## Step 4: Jalankan Docker Compose

```powershell
# Pull base image (lakukan sekali, butuh internet)
docker compose pull

# Build custom Spark image dengan dependensi
docker compose build

# Jalankan semua service
docker compose up -d

# Cek status semua service
docker compose ps
```

Tunggu sampai semua service menunjukkan status `healthy`. Ini biasanya butuh 1-2 menit.

Untuk melihat log real-time:

```powershell
docker compose logs -f
```

---

## Step 5: Verifikasi Setiap Service

### Kafka

```powershell
# Cek topic tersedia
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### ClickHouse

Buka browser: http://localhost:8123

Atau test via command:

```powershell
# Cek database berhasil dibuat
Invoke-WebRequest -Uri "http://localhost:8123/?query=SHOW+DATABASES" -UseBasicParsing | Select-Object -ExpandProperty Content
```

Expected output mengandung `flight_delay`.

### Spark

Buka browser: http://localhost:8080 (Spark Master UI)

### Grafana

Buka browser: http://localhost:3000

Login dengan `admin` / `admin`.

---

## Step 6: Download Dataset Sample (Mulai dari 1 Bulan)

> **Penting:** Jangan langsung download 5 tahun. Mulai dari 1 bulan untuk validasi pipeline.

```powershell
# Download hanya Januari 2025 untuk testing awal
python producer\download_ontime.py --year 2025 --month 1
```

Cek hasilnya di `data/manifest.csv`. Status harus `extracted`.

---

## Step 7: Stream Sample ke Kafka

```powershell
# Stream 500 baris untuk validasi cepat
python producer\stream_ontime.py --year 2025 --month 1 --sample 500
```

Verifikasi data masuk ke ClickHouse:

```powershell
Invoke-WebRequest -Uri "http://localhost:8123/?query=SELECT+count()+FROM+flight_delay.ontime_raw" -UseBasicParsing | Select-Object -ExpandProperty Content
```

---

## Step 8: Jalankan Spark Jobs (Urutan Wajib Diikuti)

Spark jobs harus dijalankan berurutan. Gunakan `PIPELINE_RUN_ID` yang sama untuk satu pipeline run
agar semua metrik terhubung di ClickHouse.

```powershell
# Set run ID (gunakan ID yang sama untuk semua job dalam satu run)
$RUN_ID = [System.Guid]::NewGuid().ToString()

# Submit EDA job
docker exec -e PIPELINE_RUN_ID=$RUN_ID spark-master spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark-apps/eda_profile.py

# Submit preprocessing job
docker exec -e PIPELINE_RUN_ID=$RUN_ID spark-master spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark-apps/preprocess_ontime.py

# Submit quality gate job
docker exec -e PIPELINE_RUN_ID=$RUN_ID spark-master spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark-apps/validate_quality.py

# Submit aggregation job (hanya jika quality PASSED)
docker exec -e PIPELINE_RUN_ID=$RUN_ID spark-master spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark-apps/aggregate_ontime.py
```

---

## Step 9: Validasi Akhir

```powershell
# Cek status pipeline run terakhir
Invoke-WebRequest -Uri "http://localhost:8123/?query=SELECT+run_id,status,message+FROM+flight_delay.pipeline_run_log+ORDER+BY+created_at+DESC+LIMIT+10+FORMAT+TSV" -UseBasicParsing | Select-Object -ExpandProperty Content

# Cek row count di setiap tabel output
Invoke-WebRequest -Uri "http://localhost:8123/?query=SELECT+'curated'+AS+tbl,+count()+FROM+flight_delay.ontime_curated+UNION+ALL+SELECT+'features',count()+FROM+flight_delay.ontime_features+FORMAT+TSV" -UseBasicParsing | Select-Object -ExpandProperty Content
```

---

## Step 10: Scale ke Full Dataset (Setelah Sample Berhasil)

```powershell
# Download semua bulan 2021-2025 (butuh waktu panjang, biarkan berjalan di background)
python producer\download_ontime.py

# Stream semua data ke Kafka
python producer\stream_ontime.py

# Jalankan ulang semua Spark jobs dengan RUN_ID baru
```

---


### Ke Tim DevOps/Cloud (Jika Ada)

1. Bagikan `docker-compose.yml` dan `.env.example`.
2. Informasikan resource minimum yang dibutuhkan: 8GB RAM, 4 CPU, 100GB+ storage.
3. Sebutkan port yang perlu dibuka: 3000 (Grafana), 8123 (ClickHouse HTTP).

---

## Checklist Sebelum Handoff

- [ ] Docker Compose semua service healthy
- [ ] `ontime_raw` terisi dari Kafka
- [ ] `pipeline_run_log` menampilkan `QUALITY_PASSED`
- [ ] `ontime_curated` terisi
- [ ] `ontime_features` terisi dan bebas leakage
- [ ] Semua aggregate table terisi
- [ ] Grafana dashboard bisa diakses dan menampilkan data
- [ ] `docs/team_handoff.md` sudah dibagikan ke tim
