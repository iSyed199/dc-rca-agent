#!/bin/bash
# ==============================================================================
# Helper script to deploy dc-rca-agent to Cloud Run on datcom-infosys-dev
# ==============================================================================

set -e

# Configurable variables with environment variable override support
PROJECT_ID="${PROJECT_ID:-datcom-infosys-dev}"
REGION="${REGION:-us-central1}"
SERVICE_NAME="${SERVICE_NAME:-dc-rca-agent}"
IMPORTS_BUCKET="${IMPORTS_BUCKET:-datcom-import-test}"
DATABASE_TYPE="${DATABASE_TYPE:-FIRESTORE}"


echo "========================================================"
echo "Deploying ${SERVICE_NAME} to Cloud Run on dev..."
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Bucket: ${IMPORTS_BUCKET}"
echo "DB Type: ${DATABASE_TYPE}"
echo "========================================================"

# Run deployment command
gcloud run deploy "${SERVICE_NAME}" \
    --source . \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --set-env-vars="RCA_PROJECT_ID=${PROJECT_ID},RCA_IMPORTS_BUCKET=${IMPORTS_BUCKET},DATABASE_TYPE=${DATABASE_TYPE},RCA_PUBSUB_SUBSCRIPTION_ID=ingestion-failures-pull-sub" \
    --memory=4Gi \
    --min-instances=1 \
    --execution-environment=gen2 \
    --allow-unauthenticated

echo "Deployment completed successfully!"
