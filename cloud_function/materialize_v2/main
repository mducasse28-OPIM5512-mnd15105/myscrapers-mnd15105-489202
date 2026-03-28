def materialize_v2(request):
# main.py
# Build a single, ever-growing CSV from all structured JSONL files.
# Reads:  gs://<bucket>/<STRUCTURED_PREFIX>/run_id=*/jsonl/*.jsonl
# Writes: gs://<bucket>/<STRUCTURED_PREFIX>/datasets/listings_master.csv  (atomic publish)

import csv
import io
import json
import os
import re
from datetime import datetime, timezone
from typing import Dict, Iterable

from flask import Request, jsonify
from google.cloud import storage

# -------------------- ENV --------------------
BUCKET_NAME        = os.getenv("GCS_BUCKET")                      # REQUIRED
STRUCTURED_PREFIX  = os.getenv("STRUCTURED_PREFIX", "structured") # e.g., "structured"
OUTPUT_BLOB = os.environ.get("OUTPUT_BLOB", "materialized/materialized_v2.csv")

storage_client = storage.Client()

# Accept BOTH runIDs:
RUN_ID_ISO_RE   = re.compile(r"^\d{8}T\d{6}Z$")  # 20251026T170002Z
RUN_ID_PLAIN_RE = re.compile(r"^\d{14}$")        # 20251026170002

# Stable CSV schema for students
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
            "is_truck" ]


def materialize_v2(request: Request):
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)

        blobs = list(client.list_blobs(BUCKET_NAME, prefix=RAW_PREFIX))
        print(f"Found {len(blobs)} blobs under prefix '{RAW_PREFIX}'")

        frames = []
        files_read = 0
        files_skipped = 0

        for blob in blobs:
            if not blob.name.endswith(".json"):
                print(f"Skipping non-JSON file: {blob.name}")
                continue

            try:
                content = blob.download_as_text()

                if not content.strip():
                    print(f"Skipping empty file: {blob.name}")
                    files_skipped += 1
                    continue

                obj = json.loads(content)

                # Handle either list of records or one record
                if isinstance(obj, list):
                    if len(obj) == 0:
                        print(f"Skipping empty JSON list: {blob.name}")
                        files_skipped += 1
                        continue
                    df = pd.DataFrame(obj)

                elif isinstance(obj, dict):
                    df = pd.DataFrame([obj])

                else:
                    print(f"Skipping unexpected JSON format in {blob.name}")
                    files_skipped += 1
                    continue

                # Optional: only keep files that contain at least one of your new ETL fields
                new_fields = {"fuel_type", "drivetrain", "transmission", "num_doors", "is_truck"}
                if not any(col in df.columns for col in new_fields):
                    print(f"Skipping old-schema file: {blob.name}")
                    files_skipped += 1
                    continue

                frames.append(df)
                files_read += 1
                print(f"Loaded {blob.name} with {len(df)} rows")

            except Exception as e:
                print(f"Skipping {blob.name} because of error: {e}")
                files_skipped += 1
                continue

        if not frames:
            return ("No valid v2 data found.", 200)

        # Safe concat handles different column sets
        final_df = pd.concat(frames, ignore_index=True, sort=False)

        # Optional: drop duplicate records if post_id exists
        if "post_id" in final_df.columns:
            before = len(final_df)
            final_df = final_df.drop_duplicates(subset=["post_id"])
            after = len(final_df)
            print(f"Dropped {before - after} duplicate rows based on post_id")


        existing_cols = [col for col in preferred_order if col in final_df.columns]
        other_cols = [col for col in final_df.columns if col not in existing_cols]
        final_df = final_df[existing_cols + other_cols]

        # Save locally to temporary file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            final_df.to_csv(tmp.name, index=False)
            tmp_path = tmp.name

        # Upload CSV to GCS
        out_blob = bucket.blob(OUTPUT_BLOB)
        out_blob.upload_from_filename(tmp_path)

        os.remove(tmp_path)

        msg = (
            f"materialize-v2 complete: {len(final_df)} rows written. "
            f"Files read: {files_read}. Files skipped: {files_skipped}."
        )
        print(msg)
        return (msg, 200)

    except Exception as e:
        error_msg = f"materialize-v2 failed: {e}"
        print(error_msg)
        return (error_msg, 500)

