# Script to help setup Google Cloud Service Account
# Run this in PowerShell

$PROJECT_ID = "aero-project-2025"
$SA_NAME = "aero-sa"
$KEY_FILE = "key.json"

Write-Host "Creating Service Account: $SA_NAME in project $PROJECT_ID..."

# Create Service Account
gcloud iam service-accounts create $SA_NAME --display-name "AERO Service Account" --project $PROJECT_ID

# Grant permissions (Storage Admin, BigQuery Admin)
gcloud projects add-iam-policy-binding $PROJECT_ID `
    --member "serviceAccount:$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" `
    --role "roles/storage.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID `
    --member "serviceAccount:$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" `
    --role "roles/bigquery.admin"

# Create Key
Write-Host "Creating key file: $KEY_FILE..."
gcloud iam service-accounts keys create $KEY_FILE `
    --iam-account "$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" `
    --project $PROJECT_ID

Write-Host "✅ Service Account setup complete."
Write-Host "🔑 Key saved to: $PWD\$KEY_FILE"
Write-Host "⚠️  IMPORTANT: Keep this file secure! Do not commit it to Git."
Write-Host "👉 Set environment variable: $env:GOOGLE_APPLICATION_CREDENTIALS = '$PWD\$KEY_FILE'"
