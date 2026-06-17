-- Aggregate layer: tabel siap pakai untuk Grafana dashboard.
-- Query dari dashboard tidak boleh langsung ke raw atau curated untuk menghindari scan besar.

-- Performa bulanan: volume, rata-rata delay, cancellation rate
CREATE TABLE IF NOT EXISTS flight_delay.agg_monthly_delay
(
    year                    Int32,
    month                   Int32,
    total_flights           Int64,
    avg_dep_delay           Nullable(Float64),
    avg_arr_delay           Nullable(Float64),
    dep_delay_rate          Nullable(Float64),      -- rasio penerbangan dengan DepDel15 = 1
    arr_delay_rate          Nullable(Float64),      -- rasio penerbangan dengan ArrDel15 = 1
    cancellation_rate       Nullable(Float64),
    diverted_rate           Nullable(Float64),
    pipeline_run_id         String,
    updated_at              DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (year, month)
SETTINGS index_granularity = 8192;


-- Performa maskapai: delay dan cancellation per carrier per bulan
CREATE TABLE IF NOT EXISTS flight_delay.agg_carrier_performance
(
    year                    Int32,
    month                   Int32,
    airline_code            String,
    total_flights           Int64,
    avg_arr_delay           Nullable(Float64),
    avg_dep_delay           Nullable(Float64),
    arr_delay_rate          Nullable(Float64),
    dep_delay_rate          Nullable(Float64),
    cancellation_rate       Nullable(Float64),
    pipeline_run_id         String,
    updated_at              DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (year, month, airline_code)
SETTINGS index_granularity = 8192;


-- Performa airport: sebagai origin dan destination
CREATE TABLE IF NOT EXISTS flight_delay.agg_airport_performance
(
    year                    Int32,
    month                   Int32,
    airport_code            String,
    airport_role            String,             -- "origin" atau "destination"
    total_flights           Int64,
    avg_dep_delay           Nullable(Float64),  -- relevan jika role = origin
    avg_arr_delay           Nullable(Float64),  -- relevan jika role = destination
    delay_rate              Nullable(Float64),
    cancellation_rate       Nullable(Float64),
    pipeline_run_id         String,
    updated_at              DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (year, month, airport_code, airport_role)
SETTINGS index_granularity = 8192;


-- Performa rute: delay dan cancellation per pasangan Origin-Dest per bulan
CREATE TABLE IF NOT EXISTS flight_delay.agg_route_performance
(
    year                    Int32,
    month                   Int32,
    route                   String,
    origin                  String,
    dest                    String,
    total_flights           Int64,
    avg_arr_delay           Nullable(Float64),
    avg_dep_delay           Nullable(Float64),
    arr_delay_rate          Nullable(Float64),
    cancellation_rate       Nullable(Float64),
    avg_distance            Nullable(Float64),
    pipeline_run_id         String,
    updated_at              DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (year, month, route)
SETTINGS index_granularity = 8192;


-- Pola delay per jam keberangkatan dan hari dalam seminggu
CREATE TABLE IF NOT EXISTS flight_delay.agg_hourly_delay
(
    month                   Int32,
    day_of_week             Int32,
    dep_hour                Int32,
    total_flights           Int64,
    avg_delay               Nullable(Float64),
    delay_rate              Nullable(Float64),
    cancellation_rate       Nullable(Float64),
    pipeline_run_id         String,
    updated_at              DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (month, day_of_week, dep_hour)
SETTINGS index_granularity = 8192;


-- Kontribusi penyebab delay per maskapai per bulan
-- Menggunakan post-event data sehingga tidak boleh dipakai untuk prediksi
CREATE TABLE IF NOT EXISTS flight_delay.agg_delay_reason
(
    year                            Int32,
    month                           Int32,
    airline_code                    String,
    total_carrier_delay_min         Nullable(Float64),
    total_weather_delay_min         Nullable(Float64),
    total_nas_delay_min             Nullable(Float64),
    total_security_delay_min        Nullable(Float64),
    total_late_aircraft_delay_min   Nullable(Float64),
    total_delay_min                 Nullable(Float64),
    pct_carrier                     Nullable(Float64),
    pct_weather                     Nullable(Float64),
    pct_nas                         Nullable(Float64),
    pct_security                    Nullable(Float64),
    pct_late_aircraft               Nullable(Float64),
    pipeline_run_id                 String,
    updated_at                      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (year, month, airline_code)
SETTINGS index_granularity = 8192;
