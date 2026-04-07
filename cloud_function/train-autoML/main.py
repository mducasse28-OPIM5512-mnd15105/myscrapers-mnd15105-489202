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
name: Deploy train-autoML (A08 Project)

on:
  workflow_dispatch:
  push:
    branches: [ main ]
    paths:
      - 'cloud-functions/train-autoML/**'
      - '.github/workflows/deploy-train-autoML.yml'

permissions:
  contents: read
  id-token: write

env:
  PROJECT_ID: ${{ vars.PROJECT_ID }}
  REGION: us-central1

  FUNCTION_NAME: train-autoML
  FUNCTION_DIR: cloud-functions/train-autoML
  ENTRY_POINT: train_autoML_http

  RUNTIME: python312
  BUCKET_NAME: ${{ vars.BUCKET_NAME }}

  TIMEOUT_SECONDS: "900"
  MEMORY: "2Gi"

  DATA_KEY: "structured/datasets/listings_master_llm.csv"
  OUTPUT_PREFIX: "structured/preds-autoML"

  RUNTIME_SA: ${{ vars.RUNTIME_SA }}
  DEPLOYER_SA: ${{ vars.DEPLOYER_SA }}
  WORKLOAD_IDENTITY_PROVIDER: ${{ vars.WORKLOAD_IDENTITY_PROVIDER }}

  SCHEDULE_BODY: '{"dry_run":false}'
  CRON_EXPR: "20 * * * *"

jobs:
  deploy:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout Repository
        uses: actions/checkout@v4

      - name: Authenticate to Google Cloud
        uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ env.WORKLOAD_IDENTITY_PROVIDER }}
          service_account: ${{ env.DEPLOYER_SA }}
          create_credentials_file: true
          export_environment_variables: true

      - name: Setup gcloud
        uses: google-github-actions/setup-gcloud@v2
        with:
          project_id: ${{ env.PROJECT_ID }}

      - name: Verify source
        shell: bash
        run: |
          set -euo pipefail
          ls -la "${FUNCTION_DIR}"
          test -f "${FUNCTION_DIR}/main.py"
          test -f "${FUNCTION_DIR}/requirements.txt"

      - name: Deploy Cloud Function
        shell: bash
        run: |
          set -euo pipefail
          gcloud functions deploy "${FUNCTION_NAME}" \
            --gen2 \
            --region="${REGION}" \
            --runtime="${RUNTIME}" \
            --source="${FUNCTION_DIR}" \
            --entry-point="${ENTRY_POINT}" \
            --trigger-http \
            --no-allow-unauthenticated \
            --timeout="${TIMEOUT_SECONDS}" \
            --memory="${MEMORY}" \
            --service-account="${RUNTIME_SA}" \
            --set-env-vars="PROJECT_ID=${PROJECT_ID},GCS_BUCKET=${BUCKET_NAME},DATA_KEY=${DATA_KEY},OUTPUT_PREFIX=${OUTPUT_PREFIX}"

      - name: Grant invoker to deployer and runtime service accounts
        shell: bash
        run: |
          set -euo pipefail
          gcloud functions add-invoker-policy-binding "${FUNCTION_NAME}" \
            --gen2 \
            --region="${REGION}" \
            --member="serviceAccount:${DEPLOYER_SA}"

          if [[ -n "${RUNTIME_SA:-}" ]]; then
            gcloud functions add-invoker-policy-binding "${FUNCTION_NAME}" \
              --gen2 \
              --region="${REGION}" \
              --member="serviceAccount:${RUNTIME_SA}"
          fi

      - name: Get function URL
        id: get_url
        shell: bash
        run: |
          set -euo pipefail
          URL=$(gcloud functions describe "${FUNCTION_NAME}" \
            --gen2 \
            --region="${REGION}" \
            --format="value(serviceConfig.uri)")
          echo "url=${URL}" >> "$GITHUB_OUTPUT"
          echo "Function URL: ${URL}"

      - name: Ensure Scheduler API enabled
        shell: bash
        run: |
          set -euo pipefail
          gcloud services list --enabled \
            --filter="cloudscheduler.googleapis.com" \
            --format="value(config.name)" | grep -q cloudscheduler.googleapis.com \
            || gcloud services enable cloudscheduler.googleapis.com

      - name: Bootstrap IAM for Scheduler OIDC
        shell: bash
        run: |
          set -euo pipefail

          if [[ -z "${RUNTIME_SA:-}" ]]; then
            echo "::error::RUNTIME_SA is not set."
            exit 1
          fi

          PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')"
          SCHED_AGENT="service-${PROJECT_NUMBER}@gcp-sa-cloudscheduler.iam.gserviceaccount.com"

          gcloud iam service-accounts add-iam-policy-binding "${RUNTIME_SA}" \
            --member="serviceAccount:${SCHED_AGENT}" \
            --role="roles/iam.serviceAccountTokenCreator" || true

          gcloud iam service-accounts add-iam-policy-binding "${RUNTIME_SA}" \
            --member="serviceAccount:${DEPLOYER_SA}" \
            --role="roles/iam.serviceAccountUser" || true

      - name: Create or update Scheduler job
        shell: bash
        run: |
          set -euo pipefail
          JOB_NAME="${FUNCTION_NAME}-hourly"
          URL="${{ steps.get_url.outputs.url }}"

          if gcloud scheduler jobs describe "$JOB_NAME" --location="${REGION}" >/dev/null 2>&1; then
            gcloud scheduler jobs update http "$JOB_NAME" \
              --location="${REGION}" \
              --schedule="${CRON_EXPR}" \
              --time-zone="America/New_York" \
              --uri="$URL" \
              --http-method=POST \
              --message-body='${{ env.SCHEDULE_BODY }}' \
              --attempt-deadline="${TIMEOUT_SECONDS}s" \
              --oidc-service-account-email="${RUNTIME_SA}" \
              --oidc-token-audience="$URL"
          else
            gcloud scheduler jobs create http "$JOB_NAME" \
              --location="${REGION}" \
              --schedule="${CRON_EXPR}" \
              --time-zone="America/New_York" \
              --uri="$URL" \
              --http-method=POST \
              --message-body='${{ env.SCHEDULE_BODY }}' \
              --attempt-deadline="${TIMEOUT_SECONDS}s" \
              --oidc-service-account-email="${RUNTIME_SA}" \
              --oidc-token-audience="$URL"
          fi

      - name: Mint ID token for test
        id: auth-test
        uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ env.WORKLOAD_IDENTITY_PROVIDER }}
          service_account: ${{ env.DEPLOYER_SA }}
          token_format: "id_token"
          id_token_audience: ${{ steps.get_url.outputs.url }}
          id_token_include_email: true

      - name: Test function
        shell: bash
        run: |
          set -euo pipefail

          URL="${{ steps.get_url.outputs.url }}"
          TOKEN="${{ steps.auth-test.outputs.id_token }}"

          echo "Calling function..."
          curl -i -X POST "$URL" \
            -H "Authorization: Bearer ${TOKEN}" \
            -H "Content-Type: application/json" \
            -d '{"dry_run":false}'
