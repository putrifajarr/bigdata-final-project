import os

# Konfigurasi ClickHouse
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "flight_delay")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

# Konfigurasi Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "ontime.raw")

# Parameter streaming
STREAM_BATCH_SIZE = int(os.getenv("STREAM_BATCH_SIZE", "1000"))
STREAM_SLEEP_SECONDS = float(os.getenv("STREAM_SLEEP_SECONDS", "0.2"))

# Rentang tahun data yang diproses
DATA_START_YEAR = int(os.getenv("DATA_START_YEAR", "2021"))
DATA_END_YEAR = int(os.getenv("DATA_END_YEAR", "2025"))

# Mode penyimpanan: "local" atau "s3"
STORAGE_MODE = os.getenv("STORAGE_MODE", "local")
S3_BUCKET = os.getenv("S3_BUCKET", "")
AWS_REGION = os.getenv("AWS_REGION", "")

# Path lokal relatif terhadap root project
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "data/raw/ontime")
SAMPLE_DATA_PATH = os.getenv("SAMPLE_DATA_PATH", "data/sample")
REJECTED_DATA_PATH = os.getenv("REJECTED_DATA_PATH", "data/rejected")
MANIFEST_PATH = os.getenv("MANIFEST_PATH", "data/manifest.csv")

# URL base BTS untuk download file bulanan
BTS_BASE_URL = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"

# Kolom yang diambil dari CSV sumber — sesuai kesepakatan pipeline
SELECTED_COLUMNS = [
    "Year", "Quarter", "Month", "DayofMonth", "DayOfWeek", "FlightDate",
    "Reporting_Airline", "DOT_ID_Reporting_Airline", "IATA_CODE_Reporting_Airline",
    "Tail_Number", "Flight_Number_Reporting_Airline",
    "OriginAirportID", "Origin", "OriginCityName", "OriginState", "OriginStateName",
    "DestAirportID", "Dest", "DestCityName", "DestState", "DestStateName",
    "CRSDepTime", "CRSArrTime", "CRSElapsedTime", "DepTimeBlk", "ArrTimeBlk",
    "DepDelay", "DepDelayMinutes", "DepDel15",
    "ArrDelay", "ArrDelayMinutes", "ArrDel15",
    "Cancelled", "CancellationCode", "Diverted",
    "Distance", "DistanceGroup",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
]

# Kolom minimum yang harus ada di CSV agar file dianggap valid
REQUIRED_COLUMNS = [
    "FlightDate", "IATA_CODE_Reporting_Airline", "Origin", "Dest",
    "CRSDepTime", "CRSArrTime", "Cancelled",
]
