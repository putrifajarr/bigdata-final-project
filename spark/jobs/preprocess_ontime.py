"""
preprocess_ontime.py

Spark preprocessing job: membaca dari ontime_raw, melakukan cleaning,
type casting, deduplication, outlier handling, dan feature engineering.

Output:
    - flight_delay.ontime_curated
    - flight_delay.ontime_features
    - flight_delay.ontime_post_event_analysis
    - flight_delay.pipeline_rejected_rows
    - flight_delay.pipeline_quality_metrics
    - pipeline_run_log: status PREPROCESSING_COMPLETED

Cara submit:
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --packages com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.8.0,\
com.clickhouse:clickhouse-http-client:0.6.3,\
org.apache.httpcomponents.client5:httpclient5:5.3 \
        /opt/spark-apps/preprocess_ontime.py
"""

import os
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, IntegerType, DoubleType, StringType,
    StructType, StructField, FloatType, LongType
)

# ---------------------------------------------------------------------------
# Konfigurasi
# ---------------------------------------------------------------------------

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "flight_delay")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
DATA_START_YEAR = 2024
DATA_END_YEAR = int(os.getenv("DATA_END_YEAR", "2025"))

CH_URL = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}?compress=0"
CH_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"
CH_PROPS = {
    "driver": CH_DRIVER, 
    "user": CLICKHOUSE_USER, 
    "password": CLICKHOUSE_PASSWORD,
    "socket_timeout": "300000",
    "socketTimeout": "300000"
}

RUN_ID = os.getenv("PIPELINE_RUN_ID", str(uuid.uuid4()))

# Kolom yang dilarang masuk ke feature table (post-flight atau leakage)
LEAKAGE_COLS = {
    "DepTime", "ArrTime", "TaxiOut", "TaxiIn", "WheelsOff", "WheelsOn",
    "ActualElapsedTime", "AirTime",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
}

# Kolom target: boleh tersimpan sebagai label, tapi tidak boleh sebagai input fitur
TARGET_COLS = {
    "DepDelay", "DepDelayMinutes", "ArrDelay", "ArrDelayMinutes",
    "DepDel15", "ArrDel15", "Cancelled",
}

# Dedup key: kombinasi ini unik per penerbangan
DEDUP_KEYS = [
    "FlightDate", "IATA_CODE_Reporting_Airline", "Flight_Number_Reporting_Airline",
    "Origin", "Dest", "CRSDepTime",
]


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"preprocess_ontime_{RUN_ID[:8]}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "100")
        .getOrCreate()
    )


def read_raw(spark: SparkSession, year: int) -> DataFrame:
    return (
        spark.read
        .format("jdbc")
        .option("url", CH_URL)
        .option("dbtable", f"(SELECT * FROM {CLICKHOUSE_DB}.ontime_raw WHERE source_year = {year}) AS tmp")
        .option("fetchsize", "50000")
        .option("numPartitions", "4")
        .option("partitionColumn", "source_month")
        .option("lowerBound", "1")
        .option("upperBound", "12")
        .options(**CH_PROPS)
        .load()
    )


def write_to_ch(df: DataFrame, table: str) -> None:
    (
        df.write
        .format("jdbc")
        .option("url", CH_URL)
        .option("dbtable", f"{CLICKHOUSE_DB}.{table}")
        .option("batchsize", "10000")
        .options(**CH_PROPS)
        .mode("append")
        .save()
    )


def log_status(spark: SparkSession, status: str, message: str = "") -> None:
    schema = StructType([
        StructField("run_id", StringType()),
        StructField("status", StringType()),
        StructField("job_name", StringType()),
        StructField("message", StringType()),
        StructField("created_at", StringType()),
    ])
    row = [(RUN_ID, status, "preprocess_ontime", message, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))]
    write_to_ch(spark.createDataFrame(row, schema), "pipeline_run_log")


def write_quality_metric(spark: SparkSession, name: str, value: float,
                         year: int | None = None, month: int | None = None) -> None:
    schema = StructType([
        StructField("run_id", StringType()),
        StructField("metric_name", StringType()),
        StructField("metric_value", FloatType()),
        StructField("year", IntegerType()),
        StructField("month", IntegerType()),
        StructField("created_at", StringType()),
    ])
    row = [(RUN_ID, name, float(value), year, month, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))]
    write_to_ch(spark.createDataFrame(row, schema), "pipeline_quality_metrics")


# ---------------------------------------------------------------------------
# Step 1: Type casting
# ---------------------------------------------------------------------------

def cast_types(df: DataFrame) -> DataFrame:
    """
    Casting ke tipe yang tepat. Nilai yang tidak bisa di-cast menjadi null.
    FlightDate di-parse dari string; kegagalan casting ditangani di filter berikutnya.
    """
    return (
        df
        .withColumn("FlightDate", F.to_date(F.col("FlightDate"), "yyyy-MM-dd"))
        .withColumn("Year", F.col("Year").cast(IntegerType()))
        .withColumn("Quarter", F.col("Quarter").cast(IntegerType()))
        .withColumn("Month", F.col("Month").cast(IntegerType()))
        .withColumn("DayofMonth", F.col("DayofMonth").cast(IntegerType()))
        .withColumn("DayOfWeek", F.col("DayOfWeek").cast(IntegerType()))
        .withColumn("CRSDepTime", F.col("CRSDepTime").cast(IntegerType()))
        .withColumn("CRSArrTime", F.col("CRSArrTime").cast(IntegerType()))
        .withColumn("CRSElapsedTime", F.col("CRSElapsedTime").cast(DoubleType()))
        .withColumn("Distance", F.col("Distance").cast(DoubleType()))
        .withColumn("DistanceGroup", F.col("DistanceGroup").cast(IntegerType()))
        .withColumn("Cancelled", F.col("Cancelled").cast(IntegerType()))
        .withColumn("Diverted", F.col("Diverted").cast(IntegerType()))
        .withColumn("DepDel15", F.col("DepDel15").cast(IntegerType()))
        .withColumn("ArrDel15", F.col("ArrDel15").cast(IntegerType()))
        .withColumn("DepDelay", F.col("DepDelay").cast(DoubleType()))
        .withColumn("DepDelayMinutes", F.col("DepDelayMinutes").cast(DoubleType()))
        .withColumn("ArrDelay", F.col("ArrDelay").cast(DoubleType()))
        .withColumn("ArrDelayMinutes", F.col("ArrDelayMinutes").cast(DoubleType()))
        .withColumn("CarrierDelay", F.col("CarrierDelay").cast(DoubleType()))
        .withColumn("WeatherDelay", F.col("WeatherDelay").cast(DoubleType()))
        .withColumn("NASDelay", F.col("NASDelay").cast(DoubleType()))
        .withColumn("SecurityDelay", F.col("SecurityDelay").cast(DoubleType()))
        .withColumn("LateAircraftDelay", F.col("LateAircraftDelay").cast(DoubleType()))
        # String cleanup: trim dan empty menjadi null
        .withColumn("IATA_CODE_Reporting_Airline",
                    F.nullif(F.trim(F.col("IATA_CODE_Reporting_Airline")), F.lit("")))
        .withColumn("Origin", F.nullif(F.trim(F.col("Origin")), F.lit("")))
        .withColumn("Dest", F.nullif(F.trim(F.col("Dest")), F.lit("")))
        .withColumn("OriginState", F.nullif(F.trim(F.col("OriginState")), F.lit("")))
        .withColumn("DestState", F.nullif(F.trim(F.col("DestState")), F.lit("")))
        .withColumn("CancellationCode", F.nullif(F.trim(F.col("CancellationCode")), F.lit("")))
        .withColumn("source_year",
                    F.coalesce(F.col("source_year").cast(IntegerType()), F.year(F.col("FlightDate"))))
        .withColumn("source_month",
                    F.coalesce(F.col("source_month").cast(IntegerType()), F.month(F.col("FlightDate"))))
    )


# ---------------------------------------------------------------------------
# Step 2: Filter baris invalid dan rekam rejected rows
# ---------------------------------------------------------------------------

def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Pisahkan row valid dari yang tidak valid berdasarkan aturan cleaning.
    Row invalid dicatat ke rejected table, bukan dihapus diam-diam.
    """
    # Kondisi invalid
    is_null_date = F.col("FlightDate").isNull()
    is_null_origin = F.col("Origin").isNull()
    is_null_dest = F.col("Dest").isNull()
    is_null_carrier = F.col("IATA_CODE_Reporting_Airline").isNull()
    is_invalid_year = ~F.col("Year").between(DATA_START_YEAR, DATA_END_YEAR)
    is_invalid_distance = F.col("Distance") <= 0
    is_invalid_dep = (F.col("CRSDepTime") < 0) | (F.col("CRSDepTime") > 2359)
    is_invalid_arr = (F.col("CRSArrTime") < 0) | (F.col("CRSArrTime") > 2359)

    invalid_mask = (
        is_null_date | is_null_origin | is_null_dest | is_null_carrier |
        is_invalid_year | is_invalid_distance | is_invalid_dep | is_invalid_arr
    )

    # Tambahkan kolom reject_reason untuk debugging
    df_with_reason = df.withColumn(
        "reject_reason",
        F.when(is_null_date, "FlightDate_null")
        .when(is_null_origin, "Origin_null")
        .when(is_null_dest, "Dest_null")
        .when(is_null_carrier, "Carrier_null")
        .when(is_invalid_year, "Year_out_of_range")
        .when(is_invalid_distance, "Distance_invalid")
        .when(is_invalid_dep, "CRSDepTime_invalid")
        .when(is_invalid_arr, "CRSArrTime_invalid")
        .otherwise(None)
    )

    df_invalid = df_with_reason.filter(F.col("reject_reason").isNotNull())
    df_valid = df_with_reason.filter(F.col("reject_reason").isNull()).drop("reject_reason")

    return df_valid, df_invalid


def write_rejected_rows(spark: SparkSession, df_invalid: DataFrame) -> int:
    if df_invalid.rdd.isEmpty():
        return 0

    df_to_write = df_invalid.select(
        F.lit(RUN_ID).alias("run_id"),
        F.col("source_file"),
        F.col("reject_reason"),
        F.to_json(F.struct(*[c for c in df_invalid.columns if c != "reject_reason"])).alias("raw_row"),
        F.lit(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")).alias("created_at"),
    )
    write_to_ch(df_to_write, "pipeline_rejected_rows")
    return df_to_write.count()


# ---------------------------------------------------------------------------
# Step 3: Deduplication
# ---------------------------------------------------------------------------

def deduplicate(df: DataFrame) -> DataFrame:
    """
    Untuk baris duplikat (same flight key), simpan yang paling baru berdasarkan ingest_ts.
    """
    window = Window.partitionBy(*DEDUP_KEYS).orderBy(F.desc("ingest_ts"))
    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# ---------------------------------------------------------------------------
# Step 4: Standardisasi dan outlier handling
# ---------------------------------------------------------------------------

def standardize_categoricals(df: DataFrame) -> DataFrame:
    """
    Null pada kolom kategoris penting diisi UNKNOWN agar tidak hilang saat join.
    Delay reason null diisi 0 karena null bermakna tidak ada delay dari kategori itu.
    """
    df = (
        df
        .withColumn("IATA_CODE_Reporting_Airline",
                    F.coalesce(F.col("IATA_CODE_Reporting_Airline"), F.lit("UNKNOWN")))
        .withColumn("OriginState", F.coalesce(F.col("OriginState"), F.lit("UNKNOWN")))
        .withColumn("DestState", F.coalesce(F.col("DestState"), F.lit("UNKNOWN")))
    )

    for delay_col in ["CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"]:
        df = df.withColumn(delay_col, F.coalesce(F.col(delay_col), F.lit(0.0)))

    return df


def add_cancelled_flags(df: DataFrame) -> DataFrame:
    """
    Buat flag operasional untuk membedakan baris yang layak dipakai per use case model.
    """
    return (
        df
        .withColumn("is_cancelled", F.col("Cancelled").cast(IntegerType()))
        .withColumn("has_arr_delay_label",
                    F.when(F.col("ArrDelayMinutes").isNotNull() & (F.col("Cancelled") == 0), 1).otherwise(0))
        .withColumn("has_dep_delay_label",
                    F.when(F.col("DepDelayMinutes").isNotNull() & (F.col("Cancelled") == 0), 1).otherwise(0))
    )


def compute_outlier_caps(df: DataFrame) -> tuple[float, float]:
    """
    Hitung upper bound percentile 99.5 untuk dep dan arr delay minutes.
    Nilai ini digunakan untuk membuat versi capped tanpa menghapus outlier.
    """
    stats = df.filter(
        (F.col("Cancelled") == 0) & (F.col("Diverted") == 0)
    ).agg(
        F.percentile_approx("DepDelayMinutes", 0.995).alias("dep_cap"),
        F.percentile_approx("ArrDelayMinutes", 0.995).alias("arr_cap"),
    ).collect()[0]

    dep_cap = float(stats["dep_cap"]) if stats["dep_cap"] is not None else 999.0
    arr_cap = float(stats["arr_cap"]) if stats["arr_cap"] is not None else 999.0
    return dep_cap, arr_cap


def add_outlier_columns(df: DataFrame, dep_cap: float, arr_cap: float) -> DataFrame:
    """
    Simpan nilai asli dan buat versi capped.
    Model regresi boleh memilih mana yang dipakai tergantung kebutuhannya.
    """
    return (
        df
        .withColumn("dep_delay_minutes_original", F.col("DepDelayMinutes"))
        .withColumn("arr_delay_minutes_original", F.col("ArrDelayMinutes"))
        .withColumn("dep_delay_minutes_capped",
                    F.when(F.col("DepDelayMinutes") > dep_cap, dep_cap)
                    .otherwise(F.greatest(F.lit(0.0), F.col("DepDelayMinutes"))))
        .withColumn("arr_delay_minutes_capped",
                    F.when(F.col("ArrDelayMinutes") > arr_cap, arr_cap)
                    .otherwise(F.greatest(F.lit(0.0), F.col("ArrDelayMinutes"))))
    )


# ---------------------------------------------------------------------------
# Step 5: Feature engineering
# ---------------------------------------------------------------------------

def build_time_features(df: DataFrame) -> DataFrame:
    """
    Fitur waktu diturunkan dari jadwal (CRS), bukan dari waktu aktual.
    Semua nilai ini diketahui sebelum penerbangan berangkat.
    """
    return (
        df
        .withColumn("flight_year", F.year(F.col("FlightDate")))
        .withColumn("flight_quarter", F.quarter(F.col("FlightDate")))
        .withColumn("flight_month", F.month(F.col("FlightDate")))
        .withColumn("flight_day", F.dayofmonth(F.col("FlightDate")))
        .withColumn("day_of_week", F.col("DayOfWeek"))
        .withColumn("is_weekend", F.when(F.col("DayOfWeek").isin(6, 7), 1).otherwise(0))
        .withColumn("season",
                    F.when(F.col("Month").isin(12, 1, 2), "Winter")
                    .when(F.col("Month").isin(3, 4, 5), "Spring")
                    .when(F.col("Month").isin(6, 7, 8), "Summer")
                    .otherwise("Fall"))
        .withColumn("dep_hour", (F.col("CRSDepTime") / 100).cast(IntegerType()))
        .withColumn("arr_hour", (F.col("CRSArrTime") / 100).cast(IntegerType()))
        .withColumn("dep_time_bucket",
                    F.when(F.col("dep_hour") < 6, "Early Morning")
                    .when(F.col("dep_hour") < 12, "Morning")
                    .when(F.col("dep_hour") < 17, "Afternoon")
                    .when(F.col("dep_hour") < 21, "Evening")
                    .otherwise("Night"))
        .withColumn("arr_time_bucket",
                    F.when(F.col("arr_hour") < 6, "Early Morning")
                    .when(F.col("arr_hour") < 12, "Morning")
                    .when(F.col("arr_hour") < 17, "Afternoon")
                    .when(F.col("arr_hour") < 21, "Evening")
                    .otherwise("Night"))
    )


def build_route_features(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("route", F.concat(F.col("Origin"), F.lit("-"), F.col("Dest")))
        .withColumn("same_state_route",
                    F.when(F.col("OriginState") == F.col("DestState"), 1).otherwise(0))
        .withColumn("distance_bucket", F.col("DistanceGroup"))
    )


def build_historical_features(df: DataFrame) -> DataFrame:
    """
    Historical features dihitung menggunakan hanya data dari periode sebelum row tersebut.
    Implementasi baseline: gunakan rata-rata keseluruhan training period sebagai approximation.
    Catatan: untuk implementasi yang lebih ketat, perlu lookback window per tanggal — documented
    di data_dictionary.md sebagai enhancement target Pekan 2.
    """
    # Hitung rata-rata per grup dari seluruh data (approximation untuk baseline)
    route_agg = df.groupBy("route").agg(
        F.mean("ArrDelayMinutes").alias("route_avg_arr_delay_prev"),
        F.mean(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)).alias("route_arr_delay_rate_prev"),
        F.mean(F.col("Cancelled").cast(DoubleType())).alias("route_cancel_rate_prev"),
    )
    carrier_agg = df.groupBy("IATA_CODE_Reporting_Airline").agg(
        F.mean(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)).alias("carrier_arr_delay_rate_prev"),
        F.mean(F.col("Cancelled").cast(DoubleType())).alias("carrier_cancel_rate_prev"),
    )
    origin_agg = df.groupBy("Origin").agg(
        F.mean(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)).alias("origin_arr_delay_rate_prev"),
        F.mean(F.col("Cancelled").cast(DoubleType())).alias("origin_cancel_rate_prev"),
    )
    dest_agg = df.groupBy("Dest").agg(
        F.mean(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)).alias("dest_arr_delay_rate_prev"),
        F.mean(F.col("Cancelled").cast(DoubleType())).alias("dest_cancel_rate_prev"),
    )
    route_carrier_agg = df.groupBy("route", "IATA_CODE_Reporting_Airline").agg(
        F.mean(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)).alias("route_carrier_arr_delay_rate_prev"),
    )

    return (
        df
        .join(route_agg, on="route", how="left")
        .join(carrier_agg, on="IATA_CODE_Reporting_Airline", how="left")
        .join(origin_agg, on="Origin", how="left")
        .join(dest_agg, on="Dest", how="left")
        .join(route_carrier_agg, on=["route", "IATA_CODE_Reporting_Airline"], how="left")
    )


# ---------------------------------------------------------------------------
# Step 6: Susun output tables
# ---------------------------------------------------------------------------

CURATED_COLS = [
    "FlightDate", "Year", "Quarter", "Month", "DayofMonth", "DayOfWeek",
    "IATA_CODE_Reporting_Airline", "Reporting_Airline", "Tail_Number",
    "Flight_Number_Reporting_Airline",
    "OriginAirportID", "Origin", "OriginCityName", "OriginState", "OriginStateName",
    "DestAirportID", "Dest", "DestCityName", "DestState", "DestStateName",
    "CRSDepTime", "CRSArrTime", "CRSElapsedTime", "DepTimeBlk", "ArrTimeBlk",
    "DepDelay", "DepDelayMinutes", "DepDel15",
    "ArrDelay", "ArrDelayMinutes", "ArrDel15",
    "Cancelled", "CancellationCode", "Diverted",
    "is_cancelled", "has_arr_delay_label", "has_dep_delay_label",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
    "Distance", "DistanceGroup",
    "dep_delay_minutes_original", "arr_delay_minutes_original",
    "dep_delay_minutes_capped", "arr_delay_minutes_capped",
    "ingest_ts", "source_file", "source_year", "source_month",
]

FEATURE_COLS = [
    "FlightDate", "IATA_CODE_Reporting_Airline", "Flight_Number_Reporting_Airline",
    "Origin", "Dest", "CRSDepTime",
    "flight_year", "flight_quarter", "flight_month", "flight_day",
    "day_of_week", "is_weekend", "season", "dep_hour", "arr_hour",
    "dep_time_bucket", "arr_time_bucket",
    "route", "same_state_route", "distance_bucket",
    "CRSElapsedTime", "Distance", "DistanceGroup",
    "OriginAirportID", "OriginState", "DestAirportID", "DestState",
    "route_avg_arr_delay_prev", "route_arr_delay_rate_prev", "route_cancel_rate_prev",
    "carrier_arr_delay_rate_prev", "carrier_cancel_rate_prev",
    "origin_arr_delay_rate_prev", "origin_cancel_rate_prev",
    "dest_arr_delay_rate_prev", "dest_cancel_rate_prev",
    "route_carrier_arr_delay_rate_prev",
    # Label (bukan input fitur — harus selalu di bagian akhir)
    "DepDelayMinutes", "ArrDelayMinutes", "DepDel15", "ArrDel15", "Cancelled",
]

POST_EVENT_COLS = [
    "FlightDate", "IATA_CODE_Reporting_Airline", "Flight_Number_Reporting_Airline",
    "Origin", "Dest", "CRSDepTime",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
    "DepDelayMinutes", "ArrDelayMinutes", "DepDel15", "ArrDel15",
    "Cancelled", "Diverted",
]


def build_feature_table(df: DataFrame) -> DataFrame:
    """
    Susun feature table dengan nama kolom final.
    Label target di-rename agar konsisten dan jelas bagi tim data science.
    """
    return (
        df
        .select(*[c for c in FEATURE_COLS if c in df.columns])
        .withColumnRenamed("DepDelayMinutes", "dep_delay_minutes_label")
        .withColumnRenamed("ArrDelayMinutes", "arr_delay_minutes_label")
        .withColumnRenamed("DepDel15", "dep_del15_label")
        .withColumnRenamed("ArrDel15", "arr_del15_label")
        .withColumnRenamed("Cancelled", "cancelled_label")
        .withColumn("pipeline_run_id", F.lit(RUN_ID))
        .withColumn("created_at", F.lit(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
    )


def leakage_check(df_features: DataFrame) -> bool:
    """
    Verifikasi bahwa tidak ada kolom leakage yang lolos ke feature table.
    Return True jika bersih, False jika ada kolom terlarang ditemukan.
    """
    feature_col_set = set(df_features.columns)
    leaked = feature_col_set.intersection(LEAKAGE_COLS)
    if leaked:
        print(f"[LEAKAGE DETECTED] Kolom terlarang ditemukan di feature table: {leaked}")
        return False
    return True


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    log_status(spark, "STARTED", "Preprocessing dimulai")

    total_valid_all = 0
    total_rejected_all = 0
    total_dup_all = 0

    for year in range(2021, 2026):
        print(f"\\n{'='*50}\\nMemproses Tahun {year}\\n{'='*50}")
        df_raw = read_raw(spark, year)
        total_raw = df_raw.count()

        if total_raw == 0:
            print(f"Data kosong untuk tahun {year}, melewati...")
            continue

        write_quality_metric(spark, f"raw_total_rows_{year}", float(total_raw))

        # Step 1: Casting
        df_casted = cast_types(df_raw)

        # Step 2: Filter dan catat rejected rows
        df_valid, df_invalid = split_valid_invalid(df_casted)
        # df_invalid.cache()
        rejected_count = write_rejected_rows(spark, df_invalid)
        # df_invalid.unpersist()

        valid_count = df_valid.count()
        valid_ratio = valid_count / total_raw if total_raw > 0 else 0.0
        write_quality_metric(spark, f"valid_row_count_{year}", float(valid_count))
        write_quality_metric(spark, f"rejected_row_count_{year}", float(rejected_count))
        write_quality_metric(spark, f"valid_row_ratio_{year}", valid_ratio)

        # Step 3: Deduplication
        df_deduped = deduplicate(df_valid)
        dup_count = valid_count - df_deduped.count()
        write_quality_metric(spark, f"duplicate_count_{year}", float(dup_count))

        total_valid_all += valid_count
        total_rejected_all += rejected_count
        total_dup_all += dup_count

        if valid_count == 0:
            print(f"Tidak ada data valid untuk tahun {year}, melewati...")
            continue

        # Step 4: Standardisasi
        df_std = standardize_categoricals(df_deduped)
        df_std = add_cancelled_flags(df_std)
        # df_std.cache()

        # Hitung outlier caps dari data yang sudah bersih
        dep_cap, arr_cap = compute_outlier_caps(df_std)
        write_quality_metric(spark, f"dep_delay_cap_p995_{year}", float(dep_cap))
        write_quality_metric(spark, f"arr_delay_cap_p995_{year}", float(arr_cap))

        df_std = add_outlier_columns(df_std, dep_cap, arr_cap)

        # Step 5: Feature engineering
        df_featured = build_time_features(df_std)
        df_featured = build_route_features(df_featured)
        df_featured = build_historical_features(df_featured)
        # df_featured.cache()

        # Tulis curated table
        df_curated = (
            df_featured
            .select(*[c for c in CURATED_COLS if c in df_featured.columns])
            .withColumn("pipeline_run_id", F.lit(RUN_ID))
        )
        write_to_ch(df_curated, "ontime_curated")

        # Tulis post-event analysis table
        df_post_event = (
            df_featured
            .select(*[c for c in POST_EVENT_COLS if c in df_featured.columns])
            .withColumn("pipeline_run_id", F.lit(RUN_ID))
            .withColumn("created_at", F.lit(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
        )
        write_to_ch(df_post_event, "ontime_post_event_analysis")

        # Tulis feature table dan jalankan leakage check
        df_features = build_feature_table(df_featured)
        if not leakage_check(df_features):
            log_status(spark, "FAILED", f"Leakage terdeteksi di feature table tahun {year} - pipeline dihentikan")
            spark.stop()
            return

        write_to_ch(df_features, "ontime_features")
        write_quality_metric(spark, f"feature_row_count_{year}", float(df_features.count()))

        # df_std.unpersist()
        # df_featured.unpersist()
        
        print(f"\\nSelesai memproses tahun {year} | Curated: {valid_count} | Rejected: {rejected_count}\\n")

    log_status(spark, "PREPROCESSING_COMPLETED",
               f"Curated: {total_valid_all:,} rows | Rejected: {total_rejected_all} | Dup: {total_dup_all}")
    spark.stop()


if __name__ == "__main__":
    main()
