import os
import json
import tempfile
import pandas as pd
from flask import Request
from google.cloud import storage

BUCKET_NAME = "YOUR_BUCKET_NAME"
RAW_PREFIX = "raw/"
OUTPUT_BLOB = "materialized/materialized_v2.csv"

def materialize_v2(request: Request):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blobs = list(client.list_blobs(BUCKET_NAME, prefix=RAW_PREFIX))

    frames = []

    for blob in blobs:
        if not blob.name.endswith(".json"):
            continue

        content = blob.download_as_text()

        try:
            obj = json.loads(content)

            if isinstance(obj, list):
                df = pd.DataFrame(obj)
            else:
                df = pd.DataFrame([obj])

            frames.append(df)

        except Exception as e:
            print(f"Skipping {blob.name}: {e}")
            continue

    if not frames:
        return ("No data found", 200)

    # SAFE CONCAT (handles new columns)
    final_df = pd.concat(frames, ignore_index=True, sort=False)

    # Save to temp
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
        final_df.to_csv(tmp.name, index=False)
        tmp_path = tmp.name

    # Upload to GCS
    blob = bucket.blob(OUTPUT_BLOB)
    blob.upload_from_filename(tmp_path)

    os.remove(tmp_path)

    return (f"materialize-v2 complete: {len(final_df)} rows", 200)
