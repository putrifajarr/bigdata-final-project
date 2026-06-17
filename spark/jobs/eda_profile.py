"""
eda_profile.py

EDA job: membaca ontime_raw dari ClickHouse dan menghasilkan profil statistik
per kolom serta ringkasan kualitas dataset secara keseluruhan.

Output:
    - flight_delay.eda_quality_summary
    - flight_delay.eda_column_profile
    - pipeline_run_log: status EDA_COMPLETED

Cara submit:
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --packages com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.8.0,\
com.clickhouse:clickhouse-http-client:0.6.3,\
org.apache.httpcomponents.client5:httpclient5:5.3 \
        /opt/spark-apps/eda_profile.py
"""

import os
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Konfigurasi dan koneksi
# ---------------------------------------------------------------------------

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "flight_delay")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

CH_URL = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
CH_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"
CH_PROPS = {
    "driver": CH_DRIVER,
    "user": CLICKHOUSE_USER,
    "password": CLICKHOUSE_PASSWORD,
}

RUN_ID = os.getenv("PIPELINE_RUN_ID", str(uuid.uuid4()))


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"eda_profile_{RUN_ID[:8]}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "50")
        .getOrCreate()
    )


def read_raw(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .format("jdbc")
        .option("url", CH_URL)
        .option("dbtable", f"{CLICKHOUSE_DB}.ontime_raw")
        .options(**CH_PROPS)
        .load()
    )


def write_to_clickhouse(df: DataFrame, table: str) -> None:
    (
        df.write
        .format("jdbc")
        .option("url", CH_URL)
        .option("dbtable", f"{CLICKHOUSE_DB}.{table}")
        .options(**CH_PROPS)
        .mode("append")
        .save()
    )


def log_run_status(spark: SparkSession, status: str, message: str = "") -> None:
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("run_id", StringType()),
        StructField("status", StringType()),
        StructField("job_name", StringType()),
        StructField("message", StringType()),
        StructField("created_at", StringType()),
    ])
    row = [(RUN_ID, status, "eda_profile", message, datetime.now(timezone.utc).isoformat())]
    df = spark.createDataFrame(row, schema)
    write_to_clickhouse(df, "pipeline_run_log")


# ---------------------------------------------------------------------------
# Profil per kolom
# ---------------------------------------------------------------------------

NUMERIC_COLS = [
    "DepDelay", "DepDelayMinutes", "ArrDelay", "ArrDelayMinutes",
    "Cancelled", "Diverted", "Distance", "CRSElapsedTime",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
]

STRING_COLS = [
    "IATA_CODE_Reporting_Airline", "Origin", "Dest",
    "OriginState", "DestState", "CancellationCode",
]

ALL_PROFILE_COLS = NUMERIC_COLS + STRING_COLS


def build_column_profiles(df: DataFrame, run_id: str) -> DataFrame:
    """
    Hitung null count, null ratio, distinct count, min, max per kolom.
    Hasilnya berbentuk long table satu baris per kolom.
    """
    spark = df.sparkSession
    total = df.count()
    rows = []

    for col_name in ALL_PROFILE_COLS:
        if col_name not in df.columns:
            continue

        col_type = dict(df.dtypes)[col_name]

        null_count = df.filter(F.col(col_name).isNull()).count()
        null_ratio = round(null_count / total, 6) if total > 0 else 0.0

        distinct_count = df.select(col_name).distinct().count()

        if col_name in NUMERIC_COLS:
            stats = df.agg(
                F.min(col_name).cast("string").alias("min_val"),
                F.max(col_name).cast("string").alias("max_val"),
            ).collect()[0]
            min_val = stats["min_val"]
            max_val = stats["max_val"]
        else:
            min_val = None
            max_val = None

        rows.append((run_id, col_name, col_type, total, null_count, null_ratio, distinct_count, min_val, max_val,
                     datetime.now(timezone.utc).isoformat()))

    from pyspark.sql.types import (
        StructType, StructField, StringType, LongType, FloatType
    )
    schema = StructType([
        StructField("run_id", StringType()),
        StructField("column_name", StringType()),
        StructField("data_type", StringType()),
        StructField("row_count", LongType()),
        StructField("null_count", LongType()),
        StructField("null_ratio", FloatType()),
        StructField("distinct_count", LongType()),
        StructField("min_value", StringType()),
        StructField("max_value", StringType()),
        StructField("created_at", StringType()),
    ])
    return spark.createDataFrame(rows, schema)


# ---------------------------------------------------------------------------
# Ringkasan kualitas dan distribusi
# ---------------------------------------------------------------------------

def build_quality_summary(df: DataFrame, run_id: str) -> DataFrame:
    """
    Hitung ringkasan kualitas dataset: volume per tahun/bulan,
    missing critical fields, duplikat, distribusi delay, imbalance.
    Semua metrik disimpan dalam format long table (metric_name, metric_value).
    """
    spark = df.sparkSession
    metrics = []
    now = datetime.now(timezone.utc).isoformat()

    total = df.count()
    metrics.append((run_id, "total_rows", float(total), None, None, now))

    # Volume per tahun — dikelompokkan agar bisa ditampilkan di dashboard
    year_counts = df.groupBy("Year").count().orderBy("Year").collect()
    for row in year_counts:
        metrics.append((run_id, f"total_rows_year_{row['Year']}", float(row["count"]), int(row["Year"]), None, now))

    # Volume per bulan (semua tahun digabung)
    month_counts = df.groupBy("Month").count().orderBy("Month").collect()
    for row in month_counts:
        metrics.append((run_id, f"total_rows_month_{row['Month']}", float(row["count"]), None, int(row["Month"]), now))

    # Missing value pada kolom kritis
    for col_name in ["FlightDate", "Origin", "Dest", "IATA_CODE_Reporting_Airline", "Year"]:
        if col_name in df.columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            metrics.append((run_id, f"null_count_{col_name}", float(null_count), None, None, now))

    # Baris dengan Year di luar rentang yang valid
    invalid_year = df.filter(~F.col("Year").between(2021, 2025)).count()
    metrics.append((run_id, "invalid_year_count", float(invalid_year), None, None, now))

    # Estimasi duplikat berdasarkan dedup key
    dedup_keys = ["FlightDate", "IATA_CODE_Reporting_Airline", "Flight_Number_Reporting_Airline",
                  "Origin", "Dest", "CRSDepTime"]
    available_keys = [c for c in dedup_keys if c in df.columns]
    if available_keys:
        unique_count = df.dropDuplicates(available_keys).count()
        dup_count = total - unique_count
        dup_rate = round(dup_count / total, 6) if total > 0 else 0.0
        metrics.append((run_id, "duplicate_estimate", float(dup_count), None, None, now))
        metrics.append((run_id, "duplicate_rate", dup_rate, None, None, now))

    # Distribusi delay
    for col_name in ["DepDelayMinutes", "ArrDelayMinutes"]:
        if col_name in df.columns:
            stat = df.agg(
                F.mean(col_name).alias("mean"),
                F.stddev(col_name).alias("std"),
                F.percentile_approx(col_name, 0.50).alias("p50"),
                F.percentile_approx(col_name, 0.95).alias("p95"),
                F.percentile_approx(col_name, 0.99).alias("p99"),
                F.percentile_approx(col_name, 0.995).alias("p995"),
                F.max(col_name).alias("max_val"),
            ).collect()[0]
            for k, v in stat.asDict().items():
                metrics.append((run_id, f"{col_name}_{k}", float(v) if v is not None else 0.0, None, None, now))

    # Class imbalance
    for flag_col in ["DepDel15", "ArrDel15", "Cancelled"]:
        if flag_col in df.columns:
            positive = df.filter(F.col(flag_col) == 1).count()
            rate = round(positive / total, 6) if total > 0 else 0.0
            metrics.append((run_id, f"{flag_col}_positive_rate", rate, None, None, now))
            metrics.append((run_id, f"{flag_col}_positive_count", float(positive), None, None, now))

    # Top 5 carrier berdasarkan volume
    top_carriers = df.groupBy("IATA_CODE_Reporting_Airline").count().orderBy(F.desc("count")).limit(5).collect()
    for i, row in enumerate(top_carriers):
        metrics.append((run_id, f"top_carrier_rank_{i + 1}", float(row["count"]), None, None, now))

    from pyspark.sql.types import (
        StructType, StructField, StringType, FloatType, IntegerType
    )
    schema = StructType([
        StructField("run_id", StringType()),
        StructField("metric_name", StringType()),
        StructField("metric_value", FloatType()),
        StructField("year", IntegerType()),
        StructField("month", IntegerType()),
        StructField("created_at", StringType()),
    ])
    return spark.createDataFrame(metrics, schema)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    log_run_status(spark, "STARTED", "EDA job dimulai")

    df_raw = read_raw(spark)
    df_raw.cache()

    row_count = df_raw.count()
    if row_count == 0:
        log_run_status(spark, "FAILED", "ontime_raw kosong — tidak ada data untuk di-profile")
        spark.stop()
        return

    df_summary = build_quality_summary(df_raw, RUN_ID)
    write_to_clickhouse(df_summary, "eda_quality_summary")

    df_profile = build_column_profiles(df_raw, RUN_ID)
    write_to_clickhouse(df_profile, "eda_column_profile")

    log_run_status(spark, "EDA_COMPLETED", f"Profil selesai untuk {row_count:,} rows")

    df_raw.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
