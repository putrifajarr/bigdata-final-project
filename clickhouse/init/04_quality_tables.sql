-- Quality layer: audit trail untuk setiap pipeline run.
-- Semua keputusan kualitas data harus bisa dilacak sampai ke run_id tertentu.

-- Log utama status setiap pipeline run dari awal sampai selesai
CREATE TABLE IF NOT EXISTS flight_delay.pipeline_run_log
(
    run_id          String,
    status          String,     -- STARTED, EDA_COMPLETED, PREPROCESSING_COMPLETED, QUALITY_PASSED, QUALITY_FAILED, AGGREGATION_COMPLETED, FAILED
    job_name        String,
    message         Nullable(String),
    created_at      DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (created_at, run_id)
SETTINGS index_granularity = 8192;


-- Metrik kualitas data per kolom, per run, per partisi tahun-bulan
CREATE TABLE IF NOT EXISTS flight_delay.pipeline_quality_metrics
(
    run_id          String,
    metric_name     String,
    metric_value    Float64,
    year            Nullable(Int32),
    month           Nullable(Int32),
    created_at      DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (created_at, run_id, metric_name)
SETTINGS index_granularity = 8192;


-- Row yang ditolak saat preprocessing: wajib disimpan untuk keperluan debugging dan audit
CREATE TABLE IF NOT EXISTS flight_delay.pipeline_rejected_rows
(
    run_id          String,
    source_file     Nullable(String),
    reject_reason   String,
    raw_row         String,     -- JSON string dari row yang ditolak
    created_at      DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (created_at, run_id)
SETTINGS index_granularity = 8192;


-- Ringkasan EDA per run: metrik distribusi dan kualitas dataset keseluruhan
CREATE TABLE IF NOT EXISTS flight_delay.eda_quality_summary
(
    run_id          String,
    metric_name     String,
    metric_value    Float64,
    year            Nullable(Int32),
    month           Nullable(Int32),
    created_at      DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (created_at, run_id, metric_name)
SETTINGS index_granularity = 8192;


-- Profil per kolom dari hasil EDA: null rate, distinct count, min/max
CREATE TABLE IF NOT EXISTS flight_delay.eda_column_profile
(
    run_id          String,
    column_name     String,
    data_type       String,
    row_count       Int64,
    null_count      Int64,
    null_ratio      Float64,
    distinct_count  Nullable(Int64),
    min_value       Nullable(String),
    max_value       Nullable(String),
    created_at      DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (created_at, run_id, column_name)
SETTINGS index_granularity = 8192;
