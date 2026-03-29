def materialize_v2(request):
    # main.py
    # Build a single CSV from all structured JSONL files

    import csv
    import json
    import os
    import re
    from datetime import datetime, timezone
    from flask import jsonify
    from google.cloud import storage

    # -------------------- ENV --------------------
    BUCKET_NAME = os.getenv("GCS_BUCKET")
    STRUCTURED_PREFIX = os.getenv("STRUCTURED_PREFIX", "structured")

    storage_client = storage.Client()

    RUN_ID_ISO_RE = re.compile(r"^\d{8}T\d{6}Z$")
    RUN_ID_PLAIN_RE = re.compile(r"^\d{14}$")

    # ✅ YOUR FINAL CSV SCHEMA
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
        "is_truck"
    ]

    # 🔥 PRINT columns (important for logs + screenshot proof)
    print("Materialize-v2 CSV Columns:")
    for col in CSV_COLUMNS:
        print(f" - {col}")

    def _list_run_ids():
        it = storage_client.list_blobs(BUCKET_NAME, prefix=f"{STRUCTURED_PREFIX}/", delimiter="/")
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

    def _jsonl_records_for_run(run_id):
        b = storage_client.bucket(BUCKET_NAME)
        prefix = f"{STRUCTURED_PREFIX}/run_id={run_id}/jsonl/"
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
            except:
                continue

    def _open_writer(dest_key):
        b = storage_client.bucket(BUCKET_NAME)
        blob = b.blob(dest_key)
        return blob.open("w")

    # -------------------- BUILD DATA --------------------
    run_ids = _list_run_ids()
    print(f"Found {len(run_ids)} run_ids")

    all_records = []
    for rid in run_ids:
        for rec in _jsonl_records_for_run(rid):
            all_records.append(rec)

    print(f"Total records collected: {len(all_records)}")

    # -------------------- WRITE CSV --------------------
    dest_key = f"{STRUCTURED_PREFIX}/datasets/listings_master_v2.csv"

    count = 0
    with _open_writer(dest_key) as out:
        writer = csv.DictWriter(out, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()

        for rec in all_records:
            row = {c: rec.get(c, None) for c in CSV_COLUMNS}
            writer.writerow(row)
            count += 1

    print(f"CSV written with {count} rows → {dest_key}")

    # ✅ RETURN columns for proof
    return jsonify({
        "status": "success",
        "rows_written": count,
        "csv_path": dest_key,
        "columns": CSV_COLUMNS
    })
