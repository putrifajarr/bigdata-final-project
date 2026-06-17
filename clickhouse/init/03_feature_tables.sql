-- Feature layer: input siap pakai untuk model prediksi.
-- Semua kolom di sini diketahui SEBELUM penerbangan terjadi (pre-flight).
-- Anti-leakage adalah kontrak utama tabel ini.
CREATE TABLE IF NOT EXISTS flight_delay.ontime_features
(
    -- Identitas penerbangan (keys)
    FlightDate                          Date,
    IATA_CODE_Reporting_Airline         String,
    Flight_Number_Reporting_Airline     Nullable(String),
    Origin                              String,
    Dest                                String,
    CRSDepTime                          Int32,

    -- Fitur waktu (diturunkan dari jadwal, bukan dari data aktual)
    flight_year                         Int32,
    flight_quarter                      Int32,
    flight_month                        Int32,
    flight_day                          Int32,
    day_of_week                         Int32,
    is_weekend                          Int32,
    season                              String,         -- Winter, Spring, Summer, Fall
    dep_hour                            Int32,
    arr_hour                            Int32,
    dep_time_bucket                     String,         -- Early Morning, Morning, Afternoon, Evening, Night
    arr_time_bucket                     String,

    -- Fitur rute
    route                               String,         -- "{Origin}-{Dest}"
    same_state_route                    Int32,
    distance_bucket                     Nullable(Int32),

    -- Jarak jadwal
    CRSElapsedTime                      Nullable(Float64),
    Distance                            Nullable(Float64),
    DistanceGroup                       Nullable(Int32),

    -- Identitas maskapai dan airport (categorical)
    OriginAirportID                     Nullable(Int32),
    OriginState                         Nullable(String),
    DestAirportID                       Nullable(Int32),
    DestState                           Nullable(String),

    -- Historical features (dihitung hanya dari periode sebelum row ini)
    -- Nilai dihitung dari training split untuk menghindari leakage temporal
    route_avg_arr_delay_prev            Nullable(Float64),
    route_arr_delay_rate_prev           Nullable(Float64),
    route_cancel_rate_prev              Nullable(Float64),
    carrier_arr_delay_rate_prev         Nullable(Float64),
    carrier_cancel_rate_prev            Nullable(Float64),
    origin_arr_delay_rate_prev          Nullable(Float64),
    origin_cancel_rate_prev             Nullable(Float64),
    dest_arr_delay_rate_prev            Nullable(Float64),
    dest_cancel_rate_prev               Nullable(Float64),
    route_carrier_arr_delay_rate_prev   Nullable(Float64),

    -- Label target (disimpan terpisah; TIDAK dipakai sebagai input feature)
    dep_delay_minutes_label             Nullable(Float64),
    arr_delay_minutes_label             Nullable(Float64),
    dep_del15_label                     Nullable(Int32),
    arr_del15_label                     Nullable(Int32),
    cancelled_label                     Int32,

    -- Metadata pipeline
    pipeline_run_id                     String,
    created_at                          DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(FlightDate)
ORDER BY (FlightDate, IATA_CODE_Reporting_Airline, Origin, Dest)
SETTINGS index_granularity = 8192;
