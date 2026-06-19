"""
stream_ontime.py

Membaca file CSV dari direktori raw dan mengalirkan data ke Kafka topic ontime.raw
secara batch dengan jeda antar batch untuk mensimulasikan streaming real-time.

Cara pakai:
    # Stream semua file yang sudah terdownload
    python producer/stream_ontime.py

    # Stream hanya satu bulan tertentu
    python producer/stream_ontime.py --year 2025 --month 1

    # Stream sample 500 baris untuk testing
    python producer/stream_ontime.py --sample 500
"""

import argparse
import csv
import glob
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def build_producer() -> KafkaProducer:
    """
    Buat KafkaProducer dengan retry dan serialisasi JSON.
    Menggunakan acks='all' untuk memastikan message tidak hilang.
    """
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
        retries=5,
        retry_backoff_ms=500,
        compression_type="gzip",
        linger_ms=50,        # Buffer kecil untuk mengurangi overhead network
        batch_size=65536,    # 64KB batch size
    )


def clean_value(val: str | None) -> str | float | int | None:
    """
    Konversi nilai string dari CSV ke tipe yang tepat.
    Empty string dan whitespace dikembalikan sebagai None (null di JSON).
    """
    if val is None:
        return None
    val = val.strip()
    if val == "":
        return None
    
    try:
        f = float(val)
        if f.is_integer():
            return int(f)
        return f
    except ValueError:
        return val


def build_message(row: dict, source_file: str, source_year: int, source_month: int) -> dict:
    """
    Bangun JSON message untuk Kafka dari satu baris CSV.
    Hanya kolom yang disepakati yang dimasukkan, plus metadata ingestion.
    """
    msg = {}
    for col in config.SELECTED_COLUMNS:
        msg[col] = clean_value(row.get(col))

    msg["ingest_ts"] = datetime.now(timezone.utc).isoformat()
    msg["source_file"] = source_file
    msg["source_year"] = source_year
    msg["source_month"] = source_month

    return msg


def find_csv_files(year: int | None, month: int | None) -> list[Path]:
    """Temukan semua CSV di direktori raw, bisa difilter per tahun/bulan."""
    if year and month:
        pattern = f"{config.RAW_DATA_PATH}/year={year}/month={month:02d}/*.csv"
    elif year:
        pattern = f"{config.RAW_DATA_PATH}/year={year}/**/*.csv"
    else:
        pattern = f"{config.RAW_DATA_PATH}/**/*.csv"

    files = sorted(Path(p) for p in glob.glob(pattern, recursive=True))
    return files


def stream_csv(
    producer: KafkaProducer,
    csv_path: Path,
    sample_limit: int | None,
    sent_total: int,
) -> int:
    """
    Stream satu file CSV ke Kafka topic.
    Return jumlah total message yang sudah dikirim setelah file ini selesai.
    """
    source_file = csv_path.name

    # Ekstrak tahun dan bulan dari path folder (year=YYYY/month=MM)
    parts = csv_path.parts
    year_part = next((p for p in parts if p.startswith("year=")), "year=0")
    month_part = next((p for p in parts if p.startswith("month=")), "month=0")
    source_year = int(year_part.split("=")[1])
    source_month = int(month_part.split("=")[1])

    log.info(f"Streaming {csv_path} ({source_year}/{source_month:02d})")

    batch = []
    batch_count = 0
    file_sent = 0

    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if sample_limit and (sent_total + file_sent) >= sample_limit:
                break

            msg = build_message(row, source_file, source_year, source_month)
            batch.append(msg)

            if len(batch) >= config.STREAM_BATCH_SIZE:
                _flush_batch(producer, batch, source_year, source_month)
                file_sent += len(batch)
                batch_count += 1
                batch = []
                time.sleep(config.STREAM_SLEEP_SECONDS)

        # Flush sisa batch yang belum penuh
        if batch:
            _flush_batch(producer, batch, source_year, source_month)
            file_sent += len(batch)
            batch_count += 1

    producer.flush()
    log.info(f"  Selesai: {file_sent:,} rows dalam {batch_count} batch dari {source_file}")
    return sent_total + file_sent


def _flush_batch(producer: KafkaProducer, batch: list[dict], year: int, month: int) -> None:
    """
    Kirim satu batch message ke Kafka.
    Error per-message dilog dan tidak menghentikan seluruh batch.
    """
    futures = []
    for msg in batch:
        future = producer.send(config.KAFKA_TOPIC_RAW, value=msg)
        futures.append(future)

    # Tunggu konfirmasi semua message dalam batch ini sebelum lanjut
    for i, future in enumerate(futures):
        try:
            future.get(timeout=30)
        except KafkaError as exc:
            log.error(f"  [KAFKA ERROR] Message {i} gagal: {exc}")


def main(year: int | None, month: int | None, sample: int | None) -> None:
    csv_files = find_csv_files(year, month)

    if not csv_files:
        log.warning("Tidak ada file CSV ditemukan. Pastikan download_ontime.py sudah dijalankan.")
        return

    log.info(f"Ditemukan {len(csv_files)} file CSV. Mulai streaming ke topic '{config.KAFKA_TOPIC_RAW}'")

    producer = build_producer()
    total_sent = 0

    try:
        for csv_path in csv_files:
            if sample and total_sent >= sample:
                log.info(f"Sample limit {sample} rows tercapai. Streaming dihentikan.")
                break
            total_sent = stream_csv(producer, csv_path, sample, total_sent)
    finally:
        producer.flush()
        producer.close()
        log.info(f"Total {total_sent:,} rows berhasil dikirim ke Kafka.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream OnTime CSV ke Kafka")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    parser.add_argument("--sample", type=int, default=None, help="Batas jumlah row untuk testing")
    args = parser.parse_args()

    main(args.year, args.month, args.sample)
