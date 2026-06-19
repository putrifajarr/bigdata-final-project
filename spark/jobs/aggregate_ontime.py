"""
aggregate_ontime.py

Aggregation job: membaca dari ontime_curated dan ontime_post_event_analysis,
lalu menghasilkan semua aggregate table yang dibutuhkan oleh Grafana dashboard.

Output:
    - flight_delay.agg_monthly_delay
    - flight_delay.agg_carrier_performance
    - flight_delay.agg_airport_performance
    - flight_delay.agg_route_performance
    - flight_delay.agg_hourly_delay
    - flight_delay.agg_delay_reason
    - pipeline_run_log: status AGGREGATION_COMPLETED

Cara submit:
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-apps/aggregate_ontime.py
"""

import os
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "flight_delay")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

CH_URL = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
CH_PROPS = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": CLICKHOUSE_USER,
    "password": CLICKHOUSE_PASSWORD,
    "socket_timeout": "300000",
    "socketTimeout": "300000"
}

RUN_ID = os.getenv("PIPELINE_RUN_ID", str(uuid.uuid4()))


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"aggregate_ontime_{RUN_ID[:8]}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "50")
        .getOrCreate()
    )


def read_table(spark: SparkSession, table: str) -> DataFrame:
    return (
        spark.read
        .format("jdbc")
        .option("url", CH_URL)
        .option("dbtable", f"{CLICKHOUSE_DB}.{table}")
        .options(**CH_PROPS)
        .load()
    )


def write_to_ch(df: DataFrame, table: str) -> None:
    (
        df.write
        .format("jdbc")
        .option("url", CH_URL)
        .option("dbtable", f"{CLICKHOUSE_DB}.{table}")
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
    row = [(RUN_ID, status, "aggregate_ontime", message, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))]
    write_to_ch(spark.createDataFrame(row, schema), "pipeline_run_log")


def add_run_meta(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("pipeline_run_id", F.lit(RUN_ID))
        .withColumn("updated_at", F.lit(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
    )


# ---------------------------------------------------------------------------
# Aggregate builders
# ---------------------------------------------------------------------------

def agg_monthly_delay(df: DataFrame) -> DataFrame:
    """Performa bulanan: volume, rata-rata delay, delay rate, cancellation, diverted."""
    return add_run_meta(
        df.groupBy("Year", "Month").agg(
            F.count("*").alias("total_flights"),
            F.mean("DepDelayMinutes").alias("avg_dep_delay"),
            F.mean("ArrDelayMinutes").alias("avg_arr_delay"),
            F.mean(F.when(F.col("DepDel15") == 1, 1).otherwise(0)).alias("dep_delay_rate"),
            F.mean(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)).alias("arr_delay_rate"),
            F.mean(F.col("Cancelled").cast("double")).alias("cancellation_rate"),
            F.mean(F.col("Diverted").cast("double")).alias("diverted_rate"),
        ).withColumnRenamed("Year", "year").withColumnRenamed("Month", "month")
    )


def agg_carrier_performance(df: DataFrame) -> DataFrame:
    """Performa per maskapai per bulan: delay dan cancellation."""
    return add_run_meta(
        df.groupBy("Year", "Month", "IATA_CODE_Reporting_Airline").agg(
            F.count("*").alias("total_flights"),
            F.mean("ArrDelayMinutes").alias("avg_arr_delay"),
            F.mean("DepDelayMinutes").alias("avg_dep_delay"),
            F.mean(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)).alias("arr_delay_rate"),
            F.mean(F.when(F.col("DepDel15") == 1, 1).otherwise(0)).alias("dep_delay_rate"),
            F.mean(F.col("Cancelled").cast("double")).alias("cancellation_rate"),
        )
        .withColumnRenamed("Year", "year")
        .withColumnRenamed("Month", "month")
        .withColumnRenamed("IATA_CODE_Reporting_Airline", "airline_code")
    )


def agg_airport_performance(df: DataFrame) -> DataFrame:
    """
    Performa airport sebagai origin (departure) dan destination (arrival) dipisah.
    Digabung sebagai union agar mudah di-filter di Grafana berdasarkan airport_role.
    """
    as_origin = (
        df.groupBy("Year", "Month", "Origin").agg(
            F.count("*").alias("total_flights"),
            F.mean("DepDelayMinutes").alias("avg_dep_delay"),
            F.lit(None).cast("double").alias("avg_arr_delay"),
            F.mean(F.when(F.col("DepDel15") == 1, 1).otherwise(0)).alias("delay_rate"),
            F.mean(F.col("Cancelled").cast("double")).alias("cancellation_rate"),
        )
        .withColumnRenamed("Year", "year")
        .withColumnRenamed("Month", "month")
        .withColumnRenamed("Origin", "airport_code")
        .withColumn("airport_role", F.lit("origin"))
    )

    as_dest = (
        df.groupBy("Year", "Month", "Dest").agg(
            F.count("*").alias("total_flights"),
            F.lit(None).cast("double").alias("avg_dep_delay"),
            F.mean("ArrDelayMinutes").alias("avg_arr_delay"),
            F.mean(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)).alias("delay_rate"),
            F.mean(F.col("Cancelled").cast("double")).alias("cancellation_rate"),
        )
        .withColumnRenamed("Year", "year")
        .withColumnRenamed("Month", "month")
        .withColumnRenamed("Dest", "airport_code")
        .withColumn("airport_role", F.lit("destination"))
    )

    return add_run_meta(as_origin.unionByName(as_dest))


def agg_route_performance(df: DataFrame) -> DataFrame:
    """Performa per rute Origin-Dest per bulan."""
    return add_run_meta(
        df.withColumn("route", F.concat(F.col("Origin"), F.lit("-"), F.col("Dest")))
        .groupBy("Year", "Month", "route", "Origin", "Dest").agg(
            F.count("*").alias("total_flights"),
            F.mean("ArrDelayMinutes").alias("avg_arr_delay"),
            F.mean("DepDelayMinutes").alias("avg_dep_delay"),
            F.mean(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)).alias("arr_delay_rate"),
            F.mean(F.col("Cancelled").cast("double")).alias("cancellation_rate"),
            F.mean("Distance").alias("avg_distance"),
        )
        .withColumnRenamed("Year", "year")
        .withColumnRenamed("Month", "month")
        .withColumnRenamed("Origin", "origin")
        .withColumnRenamed("Dest", "dest")
    )


def agg_hourly_delay(df: DataFrame) -> DataFrame:
    """Pola delay per jam keberangkatan, hari dalam seminggu, dan bulan."""
    return add_run_meta(
        df
        .withColumn("dep_hour", (F.col("CRSDepTime") / 100).cast("int"))
        .groupBy("Month", "DayOfWeek", "dep_hour").agg(
            F.count("*").alias("total_flights"),
            F.mean("ArrDelayMinutes").alias("avg_delay"),
            F.mean(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)).alias("delay_rate"),
            F.mean(F.col("Cancelled").cast("double")).alias("cancellation_rate"),
        )
        .withColumnRenamed("Month", "month")
        .withColumnRenamed("DayOfWeek", "day_of_week")
    )


def agg_delay_reason(df_post: DataFrame) -> DataFrame:
    """
    Kontribusi tiap kategori penyebab delay per maskapai per bulan.
    Sumber: post-event analysis table (bukan dari curated yang dipakai model).
    """
    return add_run_meta(
        df_post.groupBy("FlightDate", "IATA_CODE_Reporting_Airline").agg(
            F.year("FlightDate").alias("year"),
            F.month("FlightDate").alias("month"),
            F.sum("CarrierDelay").alias("total_carrier_delay_min"),
            F.sum("WeatherDelay").alias("total_weather_delay_min"),
            F.sum("NASDelay").alias("total_nas_delay_min"),
            F.sum("SecurityDelay").alias("total_security_delay_min"),
            F.sum("LateAircraftDelay").alias("total_late_aircraft_delay_min"),
        )
        .groupBy("year", "month", "IATA_CODE_Reporting_Airline").agg(
            F.sum("total_carrier_delay_min").alias("total_carrier_delay_min"),
            F.sum("total_weather_delay_min").alias("total_weather_delay_min"),
            F.sum("total_nas_delay_min").alias("total_nas_delay_min"),
            F.sum("total_security_delay_min").alias("total_security_delay_min"),
            F.sum("total_late_aircraft_delay_min").alias("total_late_aircraft_delay_min"),
        )
        .withColumn("total_delay_min",
                    F.col("total_carrier_delay_min") + F.col("total_weather_delay_min") +
                    F.col("total_nas_delay_min") + F.col("total_security_delay_min") +
                    F.col("total_late_aircraft_delay_min"))
        .withColumn("pct_carrier",
                    F.col("total_carrier_delay_min") / F.col("total_delay_min"))
        .withColumn("pct_weather",
                    F.col("total_weather_delay_min") / F.col("total_delay_min"))
        .withColumn("pct_nas",
                    F.col("total_nas_delay_min") / F.col("total_delay_min"))
        .withColumn("pct_security",
                    F.col("total_security_delay_min") / F.col("total_delay_min"))
        .withColumn("pct_late_aircraft",
                    F.col("total_late_aircraft_delay_min") / F.col("total_delay_min"))
        .withColumnRenamed("IATA_CODE_Reporting_Airline", "airline_code")
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    log_status(spark, "STARTED", "Aggregation dimulai")

    df_curated = read_table(spark, "ontime_curated")
    df_post = read_table(spark, "ontime_post_event_analysis")

    agg_steps = [
        ("agg_monthly_delay", lambda: agg_monthly_delay(df_curated)),
        ("agg_carrier_performance", lambda: agg_carrier_performance(df_curated)),
        ("agg_airport_performance", lambda: agg_airport_performance(df_curated)),
        ("agg_route_performance", lambda: agg_route_performance(df_curated)),
        ("agg_hourly_delay", lambda: agg_hourly_delay(df_curated)),
        ("agg_delay_reason", lambda: agg_delay_reason(df_post)),
    ]

    completed = []
    for table_name, builder in agg_steps:
        try:
            df_agg = builder()
            write_to_ch(df_agg, table_name)
            completed.append(table_name)
            print(f"[OK] {table_name} selesai ditulis")
        except Exception as exc:
            print(f"[ERROR] {table_name} gagal: {exc}")
            log_status(spark, "FAILED", f"{table_name} gagal: {exc}")
            raise



    log_status(spark, "AGGREGATION_COMPLETED",
               f"Selesai: {len(completed)} aggregate tables tersedia")
    spark.stop()


if __name__ == "__main__":
    main()
