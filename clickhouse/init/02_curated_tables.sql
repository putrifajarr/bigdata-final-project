-- Curated layer: output Spark setelah cleaning, type casting, dan standardisasi.
-- Tabel ini menjadi sumber kebenaran untuk aggregation dan analisis lanjutan.
CREATE TABLE IF NOT EXISTS flight_delay.ontime_curated
(
    -- Identitas penerbangan
    FlightDate                      Date,
    Year                            Int32,
    Quarter                         Int32,
    Month                           Int32,
    DayofMonth                      Int32,
    DayOfWeek                       Int32,

    -- Maskapai
    IATA_CODE_Reporting_Airline     String,
    Reporting_Airline               String,
    Tail_Number                     Nullable(String),
    Flight_Number_Reporting_Airline Nullable(String),

    -- Origin
    OriginAirportID                 Nullable(Int32),
    Origin                          String,
    OriginCityName                  Nullable(String),
    OriginState                     Nullable(String),
    OriginStateName                 Nullable(String),

    -- Destination
    DestAirportID                   Nullable(Int32),
    Dest                            String,
    DestCityName                    Nullable(String),
    DestState                       Nullable(String),
    DestStateName                   Nullable(String),

    -- Jadwal (aman untuk prediksi: diketahui sebelum penerbangan)
    CRSDepTime                      Int32,
    CRSArrTime                      Int32,
    CRSElapsedTime                  Nullable(Float64),
    DepTimeBlk                      Nullable(String),
    ArrTimeBlk                      Nullable(String),

    -- Target dan label delay
    DepDelay                        Nullable(Float64),
    DepDelayMinutes                 Nullable(Float64),
    DepDel15                        Nullable(Int32),
    ArrDelay                        Nullable(Float64),
    ArrDelayMinutes                 Nullable(Float64),
    ArrDel15                        Nullable(Int32),

    -- Status operasional
    Cancelled                       Int32,
    CancellationCode                Nullable(String),
    Diverted                        Int32,

    -- Flag yang dihasilkan Spark untuk keperluan modeling
    is_cancelled                    Int32,
    has_arr_delay_label             Int32,
    has_dep_delay_label             Int32,

    -- Delay reason (post-flight; untuk analisis operasional, bukan untuk model prediksi)
    CarrierDelay                    Nullable(Float64),
    WeatherDelay                    Nullable(Float64),
    NASDelay                        Nullable(Float64),
    SecurityDelay                   Nullable(Float64),
    LateAircraftDelay               Nullable(Float64),

    -- Jarak
    Distance                        Nullable(Float64),
    DistanceGroup                   Nullable(Int32),

    -- Versi outlier: nilai asli dijaga untuk audit, nilai capped untuk baseline model
    dep_delay_minutes_original      Nullable(Float64),
    arr_delay_minutes_original      Nullable(Float64),
    dep_delay_minutes_capped        Nullable(Float64),
    arr_delay_minutes_capped        Nullable(Float64),

    -- Metadata pipeline
    ingest_ts                       DateTime,
    source_file                     Nullable(String),
    source_year                     Int32,
    source_month                    Int32,
    pipeline_run_id                 String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(FlightDate)
ORDER BY (FlightDate, IATA_CODE_Reporting_Airline, Origin, Dest)
SETTINGS index_granularity = 8192;


-- Post-event analysis: kolom yang hanya tersedia setelah penerbangan selesai.
-- Digunakan untuk analisis penyebab delay, BUKAN sebagai input feature model prediksi.
CREATE TABLE IF NOT EXISTS flight_delay.ontime_post_event_analysis
(
    FlightDate                      Date,
    IATA_CODE_Reporting_Airline     String,
    Flight_Number_Reporting_Airline Nullable(String),
    Origin                          String,
    Dest                            String,
    CRSDepTime                      Int32,

    -- Kolom post-flight yang tidak boleh masuk ke feature table
    CarrierDelay                    Nullable(Float64),
    WeatherDelay                    Nullable(Float64),
    NASDelay                        Nullable(Float64),
    SecurityDelay                   Nullable(Float64),
    LateAircraftDelay               Nullable(Float64),

    -- Label aktual untuk evaluasi model
    DepDelayMinutes                 Nullable(Float64),
    ArrDelayMinutes                 Nullable(Float64),
    DepDel15                        Nullable(Int32),
    ArrDel15                        Nullable(Int32),
    Cancelled                       Int32,
    Diverted                        Int32,

    pipeline_run_id                 String,
    created_at                      DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(FlightDate)
ORDER BY (FlightDate, IATA_CODE_Reporting_Airline, Origin, Dest)
SETTINGS index_granularity = 8192;
