import csv
import json
import os
import re
from typing import Dict, Iterable

from flask import Request, jsonify
from google.cloud import storage

BUCKET_NAME = os.getenv("GCS_BUCKET")
STRUCTURED_PREFIX = os.getenv("STRUCTURED_PREFIX", "structured")

storage_client = storage.Client()

RUN_ID_ISO_RE = re.compile(r"^\d{8}T\d{6}Z$")
RUN_ID_PLAIN_RE = re.compile(r"^\d{14}$")

CSV_COLUMNS = [
    "post_id",
    "run_id",
    "scraped_at",
    "source_txt",
    "price",
    "year",
    "make",
    "model",
    "mileage",
    "fuel_type",
    "drivetrain",
    "transmission",
    "num_doors",
    "is_truck",
]


def _list_run_ids(bucket: str, structured_prefix: str) -> list[str]:
    it = storage_client.list_blobs(bucket, prefix=f"{structured_prefix}/", delimiter="/")
    for _ in it:
        pass

    run_ids = []
    for p in getattr(it, "prefixes", []):
        tail = p.rstrip("/").split("/")[-1]
        if tail.startswith("run_id="):
            rid = tail.split("run_id=", 1)[1]
            if RUN_ID_ISO_RE.match(rid) or RUN_ID_PLAIN_RE.match(rid):
                run_ids.append(rid)

    return sorted(run_ids)


def _jsonl_records_for_run(bucket: str, structured_prefix: str, run_id: str):
    b = storage_client.bucket(bucket)
    prefix = f"{structured_prefix}/run_id={run_id}/jsonl/"

    for blob in b.list_blobs(prefix=prefix):
        if not blob.name.endswith(".jsonl"):
            continue

        data = blob.download_as_text().strip()
        if not data:
            continue

        try:
            rec = json.loads(data)
            rec.setdefault("run_id", run_id)
            yield rec
        except Exception:
            continue


def _open_gcs_text_writer(bucket: str, key: str):
    b = storage_client.bucket(bucket)
    blob = b.blob(key)
    return blob.open("w")


def _write_csv(records: Iterable[Dict], dest_key: str, columns=CSV_COLUMNS) -> int:
    n = 0
    with _open_gcs_text_writer(BUCKET_NAME, dest_key) as out:
        writer = csv.DictWriter(out, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()

        for rec in records:
            row = {c: rec.get(c, None) for c in columns}
            writer.writerow(row)
            n += 1

    return n


def materialize_v2(request: Request):
    if not BUCKET_NAME:
        return jsonify({"error": "GCS_BUCKET environment variable is required"}), 500

    run_ids = _list_run_ids(BUCKET_NAME, STRUCTURED_PREFIX)

    all_records = []
    for run_id in run_ids:
        for rec in _jsonl_records_for_run(BUCKET_NAME, STRUCTURED_PREFIX, run_id):
            all_records.append(rec)

    dest_key = f"{STRUCTURED_PREFIX}/datasets/listings_master_v2.csv"
    rows_written = _write_csv(all_records, dest_key, CSV_COLUMNS)

    return jsonify({
        "status": "success",
        "rows_written": rows_written,
        "csv_path": dest_key,
        "columns": CSV_COLUMNS
    }), 200
