#!/usr/bin/env bash
# Terra Google Ads ETL — Deploy to DEV
# Usage: ./deploy.sh
# After verifying in dev, change PROJECT to terra-analytics-prod and redeploy.

set -euo pipefail

PROJECT="terra-analytics-dev"
REGION="us-central1"
JOB="terra-google-ads-etl-dev"
IMAGE="gcr.io/${PROJECT}/${JOB}:latest"
SA="terra-etl-runner@${PROJECT}.iam.gserviceaccount.com"

echo "🔨 Building and pushing image..."
gcloud builds submit . \
  --tag="${IMAGE}" \
  --project="${PROJECT}"

echo "🚀 Creating/updating Cloud Run Job..."
gcloud run jobs update "${JOB}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --project="${PROJECT}" \
  --service-account="${SA}" \
  --memory=512Mi \
  --cpu=1 \
  --task-timeout=1800 \
  --max-retries=2 \
  --set-secrets="GOOGLE_ADS_DEVELOPER_TOKEN=google-ads-developer-token:latest,GOOGLE_ADS_CLIENT_ID=google-ads-client-id:latest,GOOGLE_ADS_CLIENT_SECRET=google-ads-client-secret:latest,GOOGLE_ADS_REFRESH_TOKEN=google-ads-refresh-token:latest,GOOGLE_ADS_CUSTOMER_ID=google-ads-customer-id:latest" \
  --set-env-vars="BQ_PROJECT=terra-analytics-dev" \
  2>/dev/null || \
gcloud run jobs create "${JOB}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --project="${PROJECT}" \
  --service-account="${SA}" \
  --memory=512Mi \
  --cpu=1 \
  --task-timeout=1800 \
  --max-retries=2 \
  --set-secrets="GOOGLE_ADS_DEVELOPER_TOKEN=google-ads-developer-token:latest,GOOGLE_ADS_CLIENT_ID=google-ads-client-id:latest,GOOGLE_ADS_CLIENT_SECRET=google-ads-client-secret:latest,GOOGLE_ADS_REFRESH_TOKEN=google-ads-refresh-token:latest,GOOGLE_ADS_CUSTOMER_ID=google-ads-customer-id:latest" \
  --set-env-vars="BQ_PROJECT=terra-analytics-dev"

echo "⏰ Creating/updating Cloud Scheduler..."
gcloud scheduler jobs update http "schedule-${JOB}" \
  --location="${REGION}" \
  --project="${PROJECT}" \
  --schedule="0 8 * * *" \
  --time-zone="America/Los_Angeles" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB}:run" \
  --message-body="{}" \
  --oauth-service-account-email="${SA}" \
  2>/dev/null || \
gcloud scheduler jobs create http "schedule-${JOB}" \
  --location="${REGION}" \
  --project="${PROJECT}" \
  --schedule="0 8 * * *" \
  --time-zone="America/Los_Angeles" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB}:run" \
  --message-body="{}" \
  --oauth-service-account-email="${SA}"

echo "✅ Done — ${JOB} scheduled daily at 8am PT → terra-analytics-dev"
