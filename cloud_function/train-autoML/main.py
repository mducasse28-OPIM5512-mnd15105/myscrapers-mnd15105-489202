# train-autoML/main.py

import os
import io
import json
import logging
import time
import traceback

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from google.cloud import storage
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance, PartialDependenceDisplay
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from tpot import TPOTRegressor

# ---- ENV ----
PROJECT_ID = os.getenv("PROJECT_ID", "")
GCS_BUCKET = os.getenv("GCS_BUCKET", "")
DATA_KEY = os.getenv("DATA_KEY", "structured/datasets/listings_master_llm.csv")
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "preds-autoML")
TIMEZONE = os.getenv("TIMEZONE", "America/New_York")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")


# ---- HELPERS ----
def _read_csv_from_gcs(client, bucket, key):
    blob = client.bucket(bucket).blob(key)
    if not blob.exists():
        raise FileNotFoundError(f"gs://{bucket}/{key} not found")
    return pd.read_csv(io.BytesIO(blob.download_as_bytes()))


def _write_csv_to_gcs(client, bucket, key, df):
    blob = client.bucket(bucket).blob(key)
    blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")


def _append_csv_to_gcs(client, bucket, key, df_new):
    blob = client.bucket(bucket).blob(key)

    if blob.exists():
        existing_df = pd.read_csv(io.BytesIO(blob.download_as_bytes()))
        combined_df = pd.concat([existing_df, df_new], ignore_index=True)
    else:
        combined_df = df_new

    blob.upload_from_string(combined_df.to_csv(index=False), content_type="text/csv")


def _clean_numeric(series):
    return pd.to_numeric(series, errors="coerce")


def _clean_zip(series):
    return series.astype(str).str.extract(r"(\d{5})", expand=False)


def _safe_mape(y_true, y_pred):
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)
    mask = y_true != 0
    if mask.sum() == 0:
        return np.nan
    return float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100)


def _safe_bias(y_true, y_pred):
    return float(np.mean(y_pred - y_true))


# ---- MAIN FUNCTION ----
def run_once(dry_run=False):
    client = storage.Client(project=PROJECT_ID)
    df = _read_csv_from_gcs(client, GCS_BUCKET, DATA_KEY)

    required = {
        "scraped_at",
        "price",
        "make",
        "model",
        "year",
        "mileage",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # ---- Time processing ----
    dt = pd.to_datetime(df["scraped_at"], errors="coerce", utc=True)
    df["scraped_at_local"] = dt.dt.tz_convert(TIMEZONE)
    df["date_local"] = df["scraped_at_local"].dt.date

    # ---- Clean numeric fields ----
    df["price_num"] = _clean_numeric(df["price"])
    df["year_num"] = _clean_numeric(df["year"])
    df["mileage_num"] = _clean_numeric(df["mileage"])
    df["num_doors_num"] = _clean_numeric(df["num_doors"]) if "num_doors" in df.columns else np.nan

    # ---- Clean boolean-ish field ----
    if "is_truck" in df.columns:
        df["is_truck_num"] = (
            df["is_truck"]
            .astype(str)
            .str.lower()
            .map({"true": 1, "false": 0, "1": 1, "0": 0})
        )
    else:
        df["is_truck_num"] = np.nan

    # ---- Normalize string columns ----
    string_cols = [
        "make",
        "model",
        "fuel_type",
        "drivetrain",
        "transmission",
        "body_type",
        "condition",
        "title_status",
        "color",
        "seller_type",
        "city",
        "state",
        "zip_code",
    ]

    for c in string_cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.lower()
            df.loc[df[c].isin(["nan", "none", "null", ""]), c] = np.nan
        else:
            df[c] = np.nan

    df["zip_code"] = _clean_zip(df["zip_code"])

    # ---- Optional filtering ----
    df = df[df["price_num"].notna()]
    df = df[(df["price_num"] >= 500) & (df["price_num"] <= 150000)]

    # ---- Train / Holdout split (all dates before latest day train, latest day holdout) ----
    unique_dates = sorted(df["date_local"].dropna().unique())
    if len(unique_dates) < 2:
        return {"status": "noop", "reason": "need at least two dates"}

    today_local = unique_dates[-1]
    train_df = df[df["date_local"] < today_local].copy()
    holdout_df = df[df["date_local"] == today_local].copy()

    train_df = train_df[train_df["price_num"].notna()]
    holdout_df = holdout_df[holdout_df["price_num"].notna()]

    if len(train_df) < 40:
        return {"status": "noop", "reason": "too few training rows"}

    # ---- Features ----
    target = "price_num"

    num_cols = [
        "year_num",
        "mileage_num",
        "num_doors_num",
        "is_truck_num",
    ]

    cat_cols = [
        "make",
        "model",
        "fuel_type",
        "drivetrain",
        "transmission",
        "body_type",
        "condition",
        "title_status",
        "color",
        "seller_type",
        "city",
        "state",
        "zip_code",
    ]

    feats = num_cols + cat_cols

    X_train = train_df[feats]
    y_train = train_df[target]
    X_holdout = holdout_df[feats]
    y_holdout = holdout_df[target]

    # ---- Preprocessing ----
    pre = ColumnTransformer([
        (
            "num",
            SimpleImputer(strategy="median"),
            num_cols,
        ),
        (
            "cat",
            Pipeline([
                ("imp", SimpleImputer(strategy="most_frequent")),
                ("oh", OneHotEncoder(handle_unknown="ignore")),
            ]),
            cat_cols,
        ),
    ])

    # ---- Transform first, then TPOT ----
    X_train_transformed = pre.fit_transform(X_train)
    X_holdout_transformed = pre.transform(X_holdout)

    # ---- TPOT autoML ----
    tpot = TPOTRegressor(
        generations=1,
        population_size=20,
        random_state=42,
        max_time_mins=10,
        verbosity=2,
    )

    start = time.time()
    tpot.fit(X_train_transformed, y_train)
    logging.info("Training completed in %.2f seconds", time.time() - start)

    # ---- Holdout predictions ----
    preds_df = pd.DataFrame()
    mae_today = None
    rmse_today = None
    mape_today = None
    bias_today = None

    if not holdout_df.empty:
        y_hat = tpot.predict(X_holdout_transformed)

        preds_df = holdout_df[
            [
                "post_id",
                "scraped_at",
                "make",
                "model",
                "year",
                "mileage",
                "fuel_type",
                "drivetrain",
                "transmission",
                "body_type",
                "condition",
                "title_status",
                "color",
                "city",
                "state",
                "zip_code",
            ]
        ].copy()

        preds_df["actual_price"] = y_holdout.values
        preds_df["pred_price"] = np.round(y_hat, 2)
        preds_df["error"] = preds_df["pred_price"] - preds_df["actual_price"]

        mask = y_holdout.notna()
        if mask.any():
            mae_today = float(mean_absolute_error(y_holdout[mask], y_hat[mask]))
            rmse_today = float(np.sqrt(mean_squared_error(y_holdout[mask], y_hat[mask])))
            mape_today = _safe_mape(y_holdout[mask], y_hat[mask])
            bias_today = _safe_bias(y_holdout[mask], y_hat[mask])

    # ---- Save outputs locally ----
    timestamp = pd.Timestamp.utcnow().strftime("%Y%m%d%H%M%S")
    base_path = f"/tmp/{timestamp}"
    os.makedirs(base_path, exist_ok=True)

    metrics_df = pd.DataFrame([{
        "timestamp": timestamp,
        "mae": mae_today,
        "rmse": rmse_today,
        "mape": mape_today,
        "bias": bias_today,
        "n_train": len(train_df),
        "n_holdout": len(holdout_df),
        "target_date": str(today_local),
    }])

    preds_path = f"{base_path}/preds_{timestamp}.csv"
    preds_df.to_csv(preds_path, index=False)

    # ---- Permutation Importance ----
    feature_names = pre.get_feature_names_out()

    best_model = tpot.fitted_pipeline_
    result = permutation_importance(
        best_model,
        X_holdout_transformed,
        y_holdout,
        n_repeats=20,
        random_state=42,
        scoring="neg_mean_absolute_error",
    )

    perm_df = pd.DataFrame({
        "feature": feature_names,
        "importances_mean": result.importances_mean,
    }).sort_values(by="importances_mean", ascending=False).reset_index(drop=True)

    perm_csv_path = f"{base_path}/perm_importance_{timestamp}.csv"
    perm_df.to_csv(perm_csv_path, index=False)

    # ---- Permutation importance plot ----
    top_idx = np.argsort(result.importances_mean)[::-1][:10]
    top_features = np.array(feature_names)[top_idx]
    top_importances = result.importances[top_idx]

    fig, ax = plt.subplots(figsize=(8, max(4, len(top_features) * 0.35)))
    ax.boxplot(top_importances.T, vert=False, labels=top_features)
    fig.suptitle("Permutation Importance (Top 10 Features)", y=1.02)
    fig.tight_layout()

    perm_plot_path = f"{base_path}/perm_importance_top10_{timestamp}.png"
    fig.savefig(perm_plot_path, bbox_inches="tight")
    plt.close(fig)

    # ---- PDPs for top 3 features ----
    X_holdout_transformed_df = pd.DataFrame(
        X_holdout_transformed.toarray() if hasattr(X_holdout_transformed, "toarray") else X_holdout_transformed,
        columns=feature_names,
    )

    top3_idx = np.argsort(result.importances_mean)[::-1][:3]
    top3_features = X_holdout_transformed_df.columns[top3_idx]

    pdp_paths = []
    for feat in top3_features:
        fig, ax = plt.subplots(figsize=(6, 4))
        PartialDependenceDisplay.from_estimator(
            best_model,
            X_holdout_transformed_df,
            [feat],
            ax=ax,
        )
        plt.title(f"Partial Dependence Plot for {feat}")
        fig.tight_layout()
        pdp_path = f"{base_path}/pdp_{feat}_{timestamp}.png"
        fig.savefig(pdp_path, bbox_inches="tight")
        plt.close(fig)
        pdp_paths.append(pdp_path)

    # ---- Save final pipeline ----
    model_path = f"{base_path}/tpot_pipeline_{timestamp}.joblib"
    final_pipe = Pipeline([
        ("pre", pre),
        ("model", best_model),
    ])
    joblib.dump(final_pipe, model_path)

    # ---- Upload to GCS ----
    if not dry_run:
        gcs_base = f"{OUTPUT_PREFIX}/{timestamp}"
        metrics_global_path = f"{OUTPUT_PREFIX}/metrics/model_accuracy.csv"

        _write_csv_to_gcs(client, GCS_BUCKET, f"{gcs_base}/predictions.csv", preds_df)
        _write_csv_to_gcs(client, GCS_BUCKET, f"{gcs_base}/permutation_importance.csv", perm_df)

        client.bucket(GCS_BUCKET).blob(f"{gcs_base}/perm_importance_top10.png").upload_from_filename(perm_plot_path)

        for pdp_path in pdp_paths:
            blob_name = f"{gcs_base}/{os.path.basename(pdp_path)}"
            client.bucket(GCS_BUCKET).blob(blob_name).upload_from_filename(pdp_path)

        client.bucket(GCS_BUCKET).blob(f"{gcs_base}/tpot_pipeline_{timestamp}.joblib").upload_from_filename(model_path)
        _append_csv_to_gcs(client, GCS_BUCKET, metrics_global_path, metrics_df)

    return {
        "status": "ok",
        "timestamp": timestamp,
        "mae_today": mae_today,
        "rmse_today": rmse_today,
        "mape_today": mape_today,
        "bias_today": bias_today,
        "preds_path": preds_path,
        "perm_csv_path": perm_csv_path,
        "perm_plot_path": perm_plot_path,
        "pdp_paths": pdp_paths,
        "model_path": model_path,
    }


# ---- HTTP Entrypoint ----
def train_autoML_http(request):
    try:
        body = request.get_json(silent=True) or {}
        result = run_once(dry_run=bool(body.get("dry_run", False)))
        return (json.dumps(result), 200, {"Content-Type": "application/json"})
    except Exception as e:
        logging.error(traceback.format_exc())
        return (json.dumps({"status": "error", "error": str(e)}), 500, {"Content-Type": "application/json"})
