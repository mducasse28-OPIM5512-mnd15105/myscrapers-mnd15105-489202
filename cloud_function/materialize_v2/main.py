# main.py
# Build a single, ever-growing CSV from all structured JSONL files.
# Reads:  gs://<bucket>/<STRUCTURED_PREFIX>/run_id=*/jsonl/*.jsonl
# Writes: gs://<bucket>/<STRUCTURED_PREFIX>/datasets/listings_master.csv  (atomic publish)
### modified from materialize-master/main.py to implement 3 new fields: transmission, fuel_type, drivetrain


name: Deploy materialize-v2

on:
  workflow_dispatch:
  push:
    branches: [ main ]
    paths:
      - 'my-cloud-functions/materialize_v2/**'
      - '.github/workflows/deploy-materialize-v2.yml'

permissions:
  contents: read
  id-token: write

env:
  PROJECT_ID: ${{ vars.PROJECT_ID }}
  REGION: us-central1

  FUNCTION_NAME: materialize-v2
  FUNCTION_DIR: my-cloud-functions/materialize_v2
  ENTRY_POINT: materialize_http

  RUNTIME: python312
  BUCKET_NAME: ${{ vars.BUCKET_NAME }}
  RUNTIME_SA: ${{ vars.RUNTIME_SA }}
  DEPLOYER_SA: ${{ vars.DEPLOYER_SA }}
  WORKLOAD_IDENTITY_PROVIDER: ${{ vars.WORKLOAD_IDENTITY_PROVIDER }}

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

      - name: Show repo tree
        shell: bash
        run: |
          set -euo pipefail
          pwd
          find . -maxdepth 3 -type d | sort
          find . -maxdepth 3 -type f | sort

      - name: Verify source
        shell: bash
        run: |
          set -euo pipefail
          echo "FUNCTION_DIR=${FUNCTION_DIR}"
          ls -la "${FUNCTION_DIR}"
          test -f "${FUNCTION_DIR}/main.py"
          test -f "${FUNCTION_DIR}/requirements.txt"

      - name: Show main.py
        shell: bash
        run: |
          set -euo pipefail
          nl -ba "${FUNCTION_DIR}/main.py" | sed -n '1,200p'

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
            --allow-unauthenticated \
            --service-account="${RUNTIME_SA}" \
            --set-env-vars="GCS_BUCKET=${BUCKET_NAME},STRUCTURED_PREFIX=structured"

      - name: Get Function URL
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

      - name: Test function
        shell: bash
        run: |
          set -euo pipefail
          curl -i -X POST "${{ steps.get_url.outputs.url }}"
