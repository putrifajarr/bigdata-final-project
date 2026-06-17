"""
download_ontime.py

Mendownload dataset BTS OnTime Performance dari sumber resmi untuk rentang tahun
yang dikonfigurasi. Setiap file divalidasi, diekstrak, dan dicatat ke manifest.

Cara pakai:
    python producer/download_ontime.py
    python producer/download_ontime.py --year 2025 --month 1
    python producer/download_ontime.py --year 2025 --month 1 --force-redownload
"""

import argparse
import csv
import hashlib
import logging
import os
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path

import requests
from tqdm import tqdm

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

MANIFEST_FIELDNAMES = [
    "year", "month", "file_name", "file_path", "file_size_bytes",
    "checksum_sha256", "download_timestamp", "row_count_estimate",
    "status", "error_message",
]


def sha256_of_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def estimate_row_count(csv_path: Path) -> int:
    """Hitung estimasi row dengan membaca baris secara cepat tanpa load ke memory."""
    count = 0
    with open(csv_path, "rb") as f:
        # Baca header dulu, lalu hitung sisanya
        f.readline()
        for _ in f:
            count += 1
    return count


def validate_csv_header(csv_path: Path) -> tuple[bool, str]:
    """
    Cek apakah CSV memiliki header dan mengandung kolom minimum yang dibutuhkan.
    Return (is_valid, error_message).
    """
    try:
        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                return False, "CSV tidak memiliki header"
            header_set = set(h.strip() for h in header)
            missing = [c for c in config.REQUIRED_COLUMNS if c not in header_set]
            if missing:
                return False, f"Kolom wajib tidak ditemukan: {missing}"
        return True, ""
    except Exception as exc:
        return False, str(exc)


def load_manifest(manifest_path: Path) -> dict:
    """Load manifest yang sudah ada ke dict {(year, month): record}."""
    existing = {}
    if not manifest_path.exists():
        return existing
    with open(manifest_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (int(row["year"]), int(row["month"]))
            existing[key] = row
    return existing


def write_manifest(manifest_path: Path, records: dict) -> None:
    """Tulis ulang seluruh manifest dari dict records."""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_FIELDNAMES)
        writer.writeheader()
        for record in sorted(records.values(), key=lambda r: (int(r["year"]), int(r["month"]))):
            writer.writerow(record)


def download_month(year: int, month: int, force: bool, manifest: dict) -> dict:
    """
    Download, validasi, dan ekstrak satu file ZIP bulanan dari BTS.
    Return record manifest untuk bulan tersebut.
    """
    key = (year, month)
    dest_dir = Path(config.RAW_DATA_PATH) / f"year={year}" / f"month={month:02d}"
    dest_dir.mkdir(parents=True, exist_ok=True)

    url = config.BTS_BASE_URL.format(year=year, month=month)
    zip_name = url.split("/")[-1]
    zip_path = dest_dir / zip_name

    # Jika sudah didownload dan tidak force, skip
    if not force and key in manifest and manifest[key]["status"] == "extracted":
        log.info(f"  [SKIP] {year}/{month:02d} sudah ada di manifest sebagai 'extracted'")
        return manifest[key]

    record = {
        "year": year,
        "month": month,
        "file_name": zip_name,
        "file_path": str(zip_path),
        "file_size_bytes": 0,
        "checksum_sha256": "",
        "download_timestamp": datetime.utcnow().isoformat(),
        "row_count_estimate": 0,
        "status": "missing",
        "error_message": "",
    }

    try:
        log.info(f"  Downloading {year}/{month:02d} dari {url}")
        response = requests.get(url, timeout=120, stream=True)
        if response.status_code != 200:
            record["status"] = "missing"
            record["error_message"] = f"HTTP {response.status_code}"
            log.warning(f"  [WARN] {year}/{month:02d}: HTTP {response.status_code}")
            return record

        raw_bytes = response.content
        if len(raw_bytes) == 0:
            record["status"] = "invalid"
            record["error_message"] = "File kosong (0 bytes)"
            return record

        # Simpan ZIP
        zip_path.write_bytes(raw_bytes)
        record["file_size_bytes"] = len(raw_bytes)
        record["checksum_sha256"] = sha256_of_bytes(raw_bytes)

        # Ekstrak ZIP
        with zipfile.ZipFile(BytesIO(raw_bytes)) as zf:
            csv_members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
            if not csv_members:
                record["status"] = "invalid"
                record["error_message"] = "Tidak ada file CSV di dalam ZIP"
                return record

            for member in csv_members:
                zf.extract(member, dest_dir)
                csv_path = dest_dir / member

                # Validasi header dan kolom
                is_valid, err = validate_csv_header(csv_path)
                if not is_valid:
                    record["status"] = "invalid"
                    record["error_message"] = err
                    return record

                row_count = estimate_row_count(csv_path)
                if row_count == 0:
                    record["status"] = "invalid"
                    record["error_message"] = "CSV tidak memiliki baris data"
                    return record

                record["row_count_estimate"] = row_count
                log.info(f"  [OK] {year}/{month:02d}: {row_count:,} rows → {csv_path.name}")

        record["status"] = "extracted"

    except zipfile.BadZipFile:
        record["status"] = "invalid"
        record["error_message"] = "File bukan ZIP yang valid"
    except Exception as exc:
        record["status"] = "invalid"
        record["error_message"] = str(exc)
        log.exception(f"  [ERROR] {year}/{month:02d}: {exc}")

    return record


def main(target_year: int | None, target_month: int | None, force: bool) -> None:
    manifest_path = Path(config.MANIFEST_PATH)
    manifest = load_manifest(manifest_path)

    years = [target_year] if target_year else range(config.DATA_START_YEAR, config.DATA_END_YEAR + 1)
    months = [target_month] if target_month else range(1, 13)

    pairs = [(y, m) for y in years for m in months]
    log.info(f"Memulai download untuk {len(pairs)} file ...")

    for year, month in tqdm(pairs, desc="Download progress"):
        record = download_month(year, month, force, manifest)
        manifest[(int(record["year"]), int(record["month"]))] = record
        write_manifest(manifest_path, manifest)

    ok = sum(1 for r in manifest.values() if r["status"] == "extracted")
    fail = sum(1 for r in manifest.values() if r["status"] in ("missing", "invalid"))
    log.info(f"Selesai. {ok} berhasil, {fail} gagal. Lihat {manifest_path} untuk detail.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download BTS OnTime dataset bulanan")
    parser.add_argument("--year", type=int, default=None, help="Tahun spesifik (opsional)")
    parser.add_argument("--month", type=int, default=None, help="Bulan spesifik (opsional)")
    parser.add_argument(
        "--force-redownload",
        action="store_true",
        default=False,
        help="Re-download meskipun file sudah ada di manifest",
    )
    args = parser.parse_args()

    if (args.year is None) != (args.month is None):
        parser.error("--year dan --month harus diisi keduanya atau tidak sama sekali")

    main(args.year, args.month, args.force_redownload)
