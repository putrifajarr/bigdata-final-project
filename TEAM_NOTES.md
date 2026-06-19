# Catatan Perbaikan & Troubleshooting Tim (Tim Data Engineering)

Dokumen ini berisi rangkuman seluruh *bug*, masalah, dan perbaikan (*fixes*) yang telah dilakukan di dalam proyek ini. Harap baca dokumen ini jika Anda baru bergabung ke dalam tim atau mengalami *error* yang serupa agar bisa mengembangkan proyek dengan lancar.

## 1. Docker Compose & Image Issues
- **Obsolete Version Attribute:** Menghapus tulisan `version: "3.9"` dari `docker-compose.yml` karena atribut ini sudah usang (*obsolete*) di Docker Compose versi terbaru dan memunculkan *warning*.
- **Kafka Image Tag:** Mengganti *image* `bitnami/kafka:latest` menjadi `bitnamilegacy/kafka:3.3.2`. 
  - **Alasan:** Tag `latest` untuk Kafka versi Bitnami sudah dihapus dari Docker Hub, sehingga `docker compose pull` selalu gagal dengan error *manifest not found*.

## 2. ClickHouse Schema & Ingestion Issues
- **Error 404 (Not Found) & allow_nullable_key:** 
  - Awalnya tabel ClickHouse gagal terbuat dan memunculkan error 404 ketika di-hit dari *browser/PowerShell*. 
  - **Penyebab:** Pada `01_raw_tables.sql`, kolom yang menjadi kunci utama (`ORDER BY`) bertipe data `Nullable()`. Secara bawaan (*default*), ClickHouse melarang penggunaan `Nullable` pada *Sorting Key*.
  - **Solusi:** Menambahkan `SETTINGS allow_nullable_key = 1` di akhir pendefinisian tabel `ontime_raw` agar pembuatan tabel berhasil.
- **Data Kafka Tidak Masuk ke Tabel (Count = 0):**
  - **Penyebab:** Produser Python (`stream_ontime.py`) mengirim data desimal seperti `"0.00"` dalam format *string* hasil bacaan CSV. Parser `JSONEachRow` milik ClickHouse menolak mengubah `"0.00"` menjadi tipe data `Int32` (misalnya untuk kolom `DepDel15` atau *delay*), sehingga pesan di-*drop*.
  - **Solusi:** Fungsi `clean_value()` pada `producer/stream_ontime.py` telah diperbarui. Jika string terdeteksi sebagai angka bulat (seperti `"0.00"` atau `"2475.00"`), Python akan mengonversinya menjadi integer `0` atau `2475` sebelum dikirim sebagai JSON ke Kafka.

## 3. Spark Submit & Environment Issues
Saat menjalankan *script* EDA & Preprocessing menggunakan perintah `spark-submit`, beberapa error terjadi dan telah diselesaikan di dalam `DE_SETUP.md`:

- **Error: `basedir must be absolute: ?/.ivy2/local`**
  - **Penyebab:** *Container* Spark dari Bitnami berjalan sebagai user ID `1001` yang tidak memiliki direktori "home" yang jelas (`?`). Saat Spark mencoba mengunduh *package* lewat Ivy, direktori menjadi tidak valid.
  - **Solusi:** Menambahkan konfigurasi `--conf spark.jars.ivy=/tmp/.ivy2` agar Ivy menggunakan folder `/tmp/` yang bersifat absolut.
- **Error: `NullPointerException` (UnixLoginModule / UserGroupInformation)**
  - **Penyebab:** Proses Hadoop/Spark mengecek siapa *user* OS yang menjalankan proses. Karena user ID `1001` tidak ada di dalam sistem `/etc/passwd`, Spark menjadi *crash* saat inisialisasi.
  - **Solusi:** Menambahkan `-u root` pada perintah `docker exec` (`docker exec -u root -e PIPELINE_RUN_ID=...`) sehingga *container* akan mengeksekusinya sebagai root.
- **Error: `java.lang.ClassNotFoundException: com.clickhouse.jdbc.ClickHouseDriver`**
  - **Penyebab:** Skrip `eda_profile.py` dan `preprocess_ontime.py` dirancang untuk menggunakan ekstensi format `.format("jdbc")` untuk menulis ke dalam ClickHouse. Namun, JDBC *driver* tersebut belum dimasukkan ke dalam daftar *packages* Spark.
  - **Solusi:** Menambahkan `com.clickhouse:clickhouse-jdbc:0.6.3` ke dalam properti `--packages` pada saat eksekusi `spark-submit`.
- **Error: `ORDER BY or PRIMARY KEY clause is missing`**
  - **Penyebab:** Karena Spark gagal mendeteksi tabel log (seperti `pipeline_run_log` dan `eda_quality_summary`), Spark mencoba membuatkan tabel tersebut menggunakan perintah `CREATE TABLE` otomatis versi JDBC yang mana tidak memiliki klausa `ORDER BY` untuk tipe `MergeTree`.
  - **Solusi:** Pastikan semua *file* inisialisasi `*.sql` di dalam folder `clickhouse/init/` sudah dieksekusi **sebelum** mengeksekusi *job* Spark. Semua tabel log memang harus dibuat secara statis melalui *script* inisialisasi (bukan otomatis oleh Spark).
- **Error: `Initial job has not accepted any resources` (Tugas Spark Berjalan Terus / Menggantung)**
  - **Penyebab:** Pada rancangan arsitektur awal `docker-compose.yml`, hanya ada layanan `spark-master` dan tidak ada `spark-worker`. Ketika *job* di-*submit* ke *master*, tidak ada *worker* (mesin pekerja) yang tersedia untuk menerima dan menjalankan tugas tersebut, sehingga Spark akan menunggu selamanya.
  - **Solusi:** Layanan `spark-worker` sudah ditambahkan ke dalam `docker-compose.yml` agar *resource* CPU dan RAM bisa dialokasikan.
- **Error: `java.time.format.DateTimeParseException: Text '...' could not be parsed`**
  - **Penyebab:** Kolom tipe `DateTime` di ClickHouse tidak bisa menerima format string standar ISO yang mengandung zona waktu dan milidetik (misalnya `2026-06-17T12:27:07.852998+00:00`) jika di-_insert_ melalui JDBC.
  - **Solusi:** Seluruh fungsi *logging timestamp* di dalam skrip Spark (seperti `datetime.now(timezone.utc).isoformat()`) telah diubah menjadi format `YYYY-MM-DD HH:MM:SS` (menggunakan `.strftime("%Y-%m-%d %H:%M:%S")`) sehingga driver JDBC ClickHouse bisa melakukan *parsing* dengan aman.

## 4. Localhost vs Public IP / Production
- **Tahap Development (Saat ini):** Kerjakan dan jalankan semuanya 100% menggunakan `localhost` agar lebih stabil, cepat, dan mudah di- *debug*.
- **Tahap Production / Publikasi:** Untuk menjadikan aplikasi ini bisa diakses publik secara *remote*, cukup ubah rujukan *host* (`localhost`) di file `docker-compose.yml`, *environment variables*, dan konfigurasi web UI menggunakan **IP Publik Komputer** yang baru. Anda **tidak perlu mengulang proses preprocessing data dari awal**. Semua data tetap aman. 

---
*Catatan ini dibuat untuk mempermudah anggota tim lain mereplikasi, menjalankan, dan melakukan debugging tanpa mengulangi kesalahan yang sama.*
