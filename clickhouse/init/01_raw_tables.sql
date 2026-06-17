-- Raw layer: entry point pertama setelah data melewati Kafka.
-- Prinsip: tidak ada transformasi di sini. Data masuk apa adanya dari producer.

-- Kafka Engine table: ClickHouse membaca langsung dari topic ontime.raw.
-- Table ini hanya sebagai consumer interface, bukan untuk query analitik.
CREATE TABLE IF NOT EXISTS flight_delay.ontime_kafka_raw
(
    Year                            Nullable(Int32),
    Quarter                         Nullable(Int32),
    Month                           Nullable(Int32),
    DayofMonth                      Nullable(Int32),
    DayOfWeek                       Nullable(Int32),
    FlightDate                      Nullable(String),
    Reporting_Airline               Nullable(String),
    DOT_ID_Reporting_Airline        Nullable(Int32),
    IATA_CODE_Reporting_Airline     Nullable(String),
    Tail_Number                     Nullable(String),
    Flight_Number_Reporting_Airline Nullable(String),
    OriginAirportID                 Nullable(Int32),
    Origin                          Nullable(String),
    OriginCityName                  Nullable(String),
    OriginState                     Nullable(String),
    OriginStateName                 Nullable(String),
    DestAirportID                   Nullable(Int32),
    Dest                            Nullable(String),
    DestCityName                    Nullable(String),
    DestState                       Nullable(String),
    DestStateName                   Nullable(String),
    CRSDepTime                      Nullable(Int32),
    CRSArrTime                      Nullable(Int32),
    CRSElapsedTime                  Nullable(Float64),
    DepTimeBlk                      Nullable(String),
    ArrTimeBlk                      Nullable(String),
    DepDelay                        Nullable(Float64),
    DepDelayMinutes                 Nullable(Float64),
    DepDel15                        Nullable(Int32),
    ArrDelay                        Nullable(Float64),
    ArrDelayMinutes                 Nullable(Float64),
    ArrDel15                        Nullable(Int32),
    Cancelled                       Nullable(Int32),
    CancellationCode                Nullable(String),
    Diverted                        Nullable(Int32),
    Distance                        Nullable(Float64),
    DistanceGroup                   Nullable(Int32),
    CarrierDelay                    Nullable(Float64),
    WeatherDelay                    Nullable(Float64),
    NASDelay                        Nullable(Float64),
    SecurityDelay                   Nullable(Float64),
    LateAircraftDelay               Nullable(Float64),
    ingest_ts                       Nullable(String),
    source_file                     Nullable(String),
    source_year                     Nullable(Int32),
    source_month                    Nullable(Int32)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list  = 'ontime.raw',
    kafka_group_name  = 'clickhouse_raw_consumer',
    kafka_format      = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;


-- Raw landing table: data yang sudah berhasil dikonsumsi dari Kafka disimpan permanen di sini.
-- Dipakai sebagai source untuk Spark batch jobs dan sebagai audit trail.
CREATE TABLE IF NOT EXISTS flight_delay.ontime_raw
(
    Year                            Nullable(Int32),
    Quarter                         Nullable(Int32),
    Month                           Nullable(Int32),
    DayofMonth                      Nullable(Int32),
    DayOfWeek                       Nullable(Int32),
    FlightDate                      Nullable(Date),
    Reporting_Airline               Nullable(String),
    DOT_ID_Reporting_Airline        Nullable(Int32),
    IATA_CODE_Reporting_Airline     Nullable(String),
    Tail_Number                     Nullable(String),
    Flight_Number_Reporting_Airline Nullable(String),
    OriginAirportID                 Nullable(Int32),
    Origin                          Nullable(String),
    OriginCityName                  Nullable(String),
    OriginState                     Nullable(String),
    OriginStateName                 Nullable(String),
    DestAirportID                   Nullable(Int32),
    Dest                            Nullable(String),
    DestCityName                    Nullable(String),
    DestState                       Nullable(String),
    DestStateName                   Nullable(String),
    CRSDepTime                      Nullable(Int32),
    CRSArrTime                      Nullable(Int32),
    CRSElapsedTime                  Nullable(Float64),
    DepTimeBlk                      Nullable(String),
    ArrTimeBlk                      Nullable(String),
    DepDelay                        Nullable(Float64),
    DepDelayMinutes                 Nullable(Float64),
    DepDel15                        Nullable(Int32),
    ArrDelay                        Nullable(Float64),
    ArrDelayMinutes                 Nullable(Float64),
    ArrDel15                        Nullable(Int32),
    Cancelled                       Nullable(Int32),
    CancellationCode                Nullable(String),
    Diverted                        Nullable(Int32),
    Distance                        Nullable(Float64),
    DistanceGroup                   Nullable(Int32),
    CarrierDelay                    Nullable(Float64),
    WeatherDelay                    Nullable(Float64),
    NASDelay                        Nullable(Float64),
    SecurityDelay                   Nullable(Float64),
    LateAircraftDelay               Nullable(Float64),
    ingest_ts                       DateTime DEFAULT now(),
    source_file                     Nullable(String),
    source_year                     Nullable(Int32),
    source_month                    Nullable(Int32)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(FlightDate)
ORDER BY (FlightDate, IATA_CODE_Reporting_Airline, Origin, Dest)
SETTINGS index_granularity = 8192;


-- Materialized view: jembatan otomatis dari Kafka Engine ke raw landing table.
-- Setiap message yang masuk ke ontime_kafka_raw akan langsung disalin ke ontime_raw.
CREATE MATERIALIZED VIEW IF NOT EXISTS flight_delay.mv_ontime_kafka_to_raw
TO flight_delay.ontime_raw
AS
SELECT
    Year,
    Quarter,
    Month,
    DayofMonth,
    DayOfWeek,
    toDateOrNull(FlightDate)        AS FlightDate,
    Reporting_Airline,
    DOT_ID_Reporting_Airline,
    IATA_CODE_Reporting_Airline,
    Tail_Number,
    Flight_Number_Reporting_Airline,
    OriginAirportID,
    Origin,
    OriginCityName,
    OriginState,
    OriginStateName,
    DestAirportID,
    Dest,
    DestCityName,
    DestState,
    DestStateName,
    CRSDepTime,
    CRSArrTime,
    CRSElapsedTime,
    DepTimeBlk,
    ArrTimeBlk,
    DepDelay,
    DepDelayMinutes,
    DepDel15,
    ArrDelay,
    ArrDelayMinutes,
    ArrDel15,
    Cancelled,
    CancellationCode,
    Diverted,
    Distance,
    DistanceGroup,
    CarrierDelay,
    WeatherDelay,
    NASDelay,
    SecurityDelay,
    LateAircraftDelay,
    now()                           AS ingest_ts,
    source_file,
    source_year,
    source_month
FROM flight_delay.ontime_kafka_raw;
