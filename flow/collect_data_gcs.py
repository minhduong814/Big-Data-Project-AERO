import os
import requests
from bs4 import BeautifulSoup
import zipfile
import tempfile
from google.cloud import storage
from prefect import task

json_credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
storage_client = storage.Client.from_service_account_json(
    json_credentials_path=json_credentials_path,
    project="double-arbor-475907-s5"
)


@task
def collect_data_to_gcs(year, month, bucket_name):
    url = "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGJ&QO_fu146_anzr=b0-gvzr"
    
    fields_to_select = [
        "FL_DATE", "YEAR", "QUARTER", "MONTH", "DAY_OF_MONTH", "DAY_OF_WEEK",
        "OP_CARRIER", "TAIL_NUM", "OP_CARRIER_FL_NUM",
        "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY",
        "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY", "CANCELLED", "DIVERTED", "DISTANCE",
        "ORIGIN", "ORIGIN_CITY_NAME", "ORIGIN_STATE_ABR", "ORIGIN_STATE_FIPS", "ORIGIN_STATE_NM",
        "DEST", "DEST_CITY_NAME", "DEST_STATE_ABR", "DEST_STATE_FIPS", "DEST_STATE_NM",
        "OP_UNIQUE_CARRIER", "OP_CARRIER_AIRLINE_ID",
        "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY",
        "CANCELLATION_CODE"
    ]

    try:
        # Start a session to persist cookies
        session = requests.Session()
        response = session.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        # Prepare form data
        form_data = {}
        for hidden_field in soup.find_all("input", {"type": "hidden"}):
            form_data[hidden_field.get("name")] = hidden_field.get("value", "")

        for field in fields_to_select:
            form_data[field] = "on"
        form_data["cboYear"] = str(year)
        form_data["cboPeriod"] = str(month)
        form_data["chkDownloadZip"] = "on"
        form_data["btnDownload"] = "Download"

        import time
        download_url = "https://www.transtats.bts.gov/DL_SelectFields.aspx"
        max_attempts = 3
        download_response = None
        for attempt in range(1, max_attempts + 1):
            try:
                download_response = session.post(download_url, data=form_data, stream=True)
                if download_response.status_code == 200:
                    break
                else:
                    print(f"Attempt {attempt} failed with status {download_response.status_code}. Retrying...")
                    time.sleep(2 ** attempt)
            except requests.exceptions.RequestException as e:
                print(f"Attempt {attempt} raised an exception: {e}. Retrying...")
                time.sleep(2 ** attempt)

        if download_response is None:
            raise Exception("Failed to download after multiple attempts")

        download_response.raise_for_status()

        with tempfile.TemporaryDirectory() as tmp_dir:
            zip_path = os.path.join(tmp_dir, f"{year}_{month}.zip")
            with open(zip_path, "wb") as f:
                for chunk in download_response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            print(f"📦 Downloaded ZIP to {zip_path}")

            # Extract ZIP contents
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(tmp_dir)

            # Find the CSV file
            csv_files = [f for f in os.listdir(tmp_dir) if f.endswith(".csv")]
            if not csv_files:
                print("⚠️ No CSV found in downloaded archive.")
                return

            # Rename the first CSV file to a clean name
            original_csv_path = os.path.join(tmp_dir, csv_files[0])
            renamed_csv_path = os.path.join(tmp_dir, f"{year}_{month:02d}.csv")
            os.rename(original_csv_path, renamed_csv_path)
            print(f"✅ Renamed {csv_files[0]} → {year}_{month:02d}.csv")

            # Handle bucket creation/retrieval
            bucket = storage_client.lookup_bucket(bucket_name)
            if bucket is None:
                print(f"⚠️ Bucket {bucket_name} does not exist. Creating...")
                bucket = storage_client.create_bucket(
                    bucket_name,
                    location="asia-east2"
                )
                print(f"✅ Created bucket {bucket_name} in {bucket.location}")
            else:
                print(f"🌐 Using existing bucket: {bucket.name}")
            
            print(f"📁 Bucket location: {bucket.location}")
            
            # Fixed: blob_path should not include bucket name
            blob_path = f"{year}_{month:02d}.csv"
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(renamed_csv_path)

            print(f"☁️ Uploaded to: gs://{bucket_name}/{blob_path}")
            return f"gs://{bucket_name}/{blob_path}"
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error downloading data: {e}")
        raise
    except zipfile.BadZipFile as e:
        print(f"❌ Error extracting ZIP: {e}")
        raise
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        raise