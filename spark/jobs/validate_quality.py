"""
validate_quality.py

Quality gate job: membaca hasil preprocessing dari ClickHouse dan memvalidasi
apakah pipeline run ini layak diteruskan ke aggregation dan handoff ke data science.

Output:
    - pipeline_run_log: status QUALITY_PASSED atau QUALITY_FAILED
    - pipeline_quality_metrics: hasil setiap gate check

Cara submit:
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-apps/validate_quality.py
"""

import os
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "flight_delay")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
DATA_START_YEAR = int(os.getenv("DATA_START_YEAR", "2021"))
DATA_END_YEAR = int(os.getenv("DATA_END_YEAR", "2025"))

CH_URL = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
CH_PROPS = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": CLICKHOUSE_USER,
    "password": CLICKHOUSE_PASSWORD,
}

RUN_ID = os.getenv("PIPELINE_RUN_ID", str(uuid.uuid4()))

# Threshold yang harus dipenuhi agar quality gate PASSED
VALID_ROW_RATIO_THRESHOLD = 0.95
DUPLICATE_RATE_THRESHOLD = 0.01

# Kolom yang dilarang ada di feature table
LEAKAGE_COLS = {
    "DepTime", "ArrTime", "TaxiOut", "TaxiIn", "WheelsOff", "WheelsOn",
    "ActualElapsedTime", "AirTime",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
}


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"validate_quality_{RUN_ID[:8]}")
        .config("spark.sql.adaptive.enabled", "true")
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
    row = [(RUN_ID, status, "validate_quality", message, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))]
    write_to_ch(spark.createDataFrame(row, schema), "pipeline_run_log")


def write_gate_result(spark: SparkSession, gate_name: str, passed: bool, value: float) -> None:
    schema = StructType([
        StructField("run_id", StringType()),
        StructField("metric_name", StringType()),
        StructField("metric_value", FloatType()),
        StructField("year", IntegerType()),
        StructField("month", IntegerType()),
        StructField("created_at", StringType()),
    ])
    row = [(RUN_ID, f"gate_{gate_name}", float(value), None, None, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))]
    write_to_ch(spark.createDataFrame(row, schema), "pipeline_quality_metrics")


# ---------------------------------------------------------------------------
# Gate checks
# ---------------------------------------------------------------------------

class GateResult:
    def __init__(self, name: str, passed: bool, value: float, detail: str = ""):
        self.name = name
        self.passed = passed
        self.value = value
        self.detail = detail

    def __repr__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.name}: {self.value:.4f} — {self.detail}"


def check_valid_row_ratio(spark: SparkSession) -> GateResult:
    """Valid row ratio harus di atas threshold yang disepakati."""
    metrics = read_table(spark, "pipeline_quality_metrics").filter(
        (F.col("run_id") == RUN_ID) & (F.col("metric_name") == "valid_row_ratio")
    )
    row = metrics.first()
    if row is None:
        return GateResult("valid_row_ratio", False, 0.0, "Metrik tidak ditemukan di pipeline_quality_metrics")

    ratio = float(row["metric_value"])
    passed = ratio >= VALID_ROW_RATIO_THRESHOLD
    return GateResult("valid_row_ratio", passed, ratio,
                       f"threshold={VALID_ROW_RATIO_THRESHOLD}")


def check_null_critical_fields(df_curated: DataFrame) -> list[GateResult]:
    """Kolom kritis tidak boleh ada null di curated table."""
    critical_cols = ["FlightDate", "Origin", "Dest", "IATA_CODE_Reporting_Airline"]
    results = []
    for col_name in critical_cols:
        null_count = df_curated.filter(F.col(col_name).isNull()).count()
        results.append(GateResult(
            f"null_{col_name}", null_count == 0, float(null_count),
            f"null count di curated table"
        ))
    return results


def check_year_range(df_curated: DataFrame) -> GateResult:
    """Semua Year di curated harus dalam rentang yang valid."""
    out_of_range = df_curated.filter(~F.col("Year").between(DATA_START_YEAR, DATA_END_YEAR)).count()
    return GateResult("year_range", out_of_range == 0, float(out_of_range),
                       f"rows dengan Year di luar {DATA_START_YEAR}-{DATA_END_YEAR}")


def check_duplicate_rate(spark: SparkSession) -> GateResult:
    """Duplicate rate harus di bawah threshold."""
    metrics = read_table(spark, "pipeline_quality_metrics").filter(
        (F.col("run_id") == RUN_ID) & (F.col("metric_name") == "duplicate_count")
    )
    raw_count_row = read_table(spark, "pipeline_quality_metrics").filter(
        (F.col("run_id") == RUN_ID) & (F.col("metric_name") == "raw_total_rows")
    ).first()

    dup_row = metrics.first()
    if dup_row is None or raw_count_row is None:
        return GateResult("duplicate_rate", False, 0.0, "Metrik tidak ditemukan")

    dup_count = float(dup_row["metric_value"])
    total = float(raw_count_row["metric_value"])
    rate = dup_count / total if total > 0 else 0.0
    passed = rate < DUPLICATE_RATE_THRESHOLD
    return GateResult("duplicate_rate", passed, rate,
                       f"threshold={DUPLICATE_RATE_THRESHOLD}")


def check_binary_flag_values(df_curated: DataFrame) -> list[GateResult]:
    """Kolom flag hanya boleh berisi nilai 0, 1, atau null."""
    results = []
    for col_name, allow_null in [("Cancelled", False), ("Diverted", False),
                                   ("DepDel15", True), ("ArrDel15", True)]:
        if allow_null:
            invalid = df_curated.filter(
                F.col(col_name).isNotNull() & ~F.col(col_name).isin(0, 1)
            ).count()
        else:
            invalid = df_curated.filter(~F.col(col_name).isin(0, 1)).count()

        results.append(GateResult(
            f"binary_flag_{col_name}", invalid == 0, float(invalid),
            f"rows dengan nilai di luar [0,1{'|null' if allow_null else ''}]"
        ))
    return results


def check_leakage_free(df_features: DataFrame) -> GateResult:
    """Feature table tidak boleh mengandung kolom leakage."""
    leaked = set(df_features.columns).intersection(LEAKAGE_COLS)
    return GateResult(
        "leakage_free", len(leaked) == 0, float(len(leaked)),
        f"kolom terlarang: {leaked if leaked else 'none'}"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    df_curated = read_table(spark, "ontime_curated")
    df_features = read_table(spark, "ontime_features")

    all_results: list[GateResult] = []

    all_results.append(check_valid_row_ratio(spark))
    all_results.extend(check_null_critical_fields(df_curated))
    all_results.append(check_year_range(df_curated))
    all_results.append(check_duplicate_rate(spark))
    all_results.extend(check_binary_flag_values(df_curated))
    all_results.append(check_leakage_free(df_features))

    print("\n===== QUALITY GATE REPORT =====")
    all_passed = True
    for result in all_results:
        print(result)
        write_gate_result(spark, result.name, result.passed, result.value)
        if not result.passed:
            all_passed = False
    print("================================\n")

    if all_passed:
        log_status(spark, "QUALITY_PASSED",
                   f"Semua {len(all_results)} gate check berhasil")
        print(f"[QUALITY PASSED] Run {RUN_ID} lolos semua gate. Pipeline siap dilanjutkan ke aggregation.")
    else:
        failed = [r.name for r in all_results if not r.passed]
        log_status(spark, "QUALITY_FAILED",
                   f"Gate gagal: {failed}")
        print(f"[QUALITY FAILED] Gate yang gagal: {failed}")
        print("Data curated/features dari run ini TIDAK dianggap valid.")
        print("Periksa pipeline_rejected_rows dan pipeline_quality_metrics untuk detail.")

    spark.stop()


if __name__ == "__main__":
    main()
