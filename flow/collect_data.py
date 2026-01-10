import os
import requests
from bs4 import BeautifulSoup
import zipfile
from prefect import task

@task
def collect_data(year, month, output_dir="./downloads/"):
    url = "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGJ&QO_fu146_anzr=b0-gvzr"
    
    # Fields to select
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

    # Start a session to persist cookies
    session = requests.Session()

    # Fetch the page content
    response = session.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Prepare form data
    form_data = {}

    # Include hidden fields
    for hidden_field in soup.find_all("input", {"type": "hidden"}):
        form_data[hidden_field.get("name")] = hidden_field.get("value", "")

    # Add checkboxes for the selected fields
    for field in fields_to_select:
        form_data[field] = "on"

    # Add year and month filters
    form_data["cboYear"] = str(year)
    form_data["cboPeriod"] = str(month)

    # Ensure that the "Prezipped File" checkbox is checked
    form_data["chkDownloadZip"] = "on"

    # Add the download button
    form_data["btnDownload"] = "Download"

    # Submit the form with retries and exponential backoff
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

    # Create the directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    # Define the output file name for the ZIP
    output_file = os.path.join(output_dir, f"{year}_{month}.zip")

    # Save the ZIP file
    with open(output_file, "wb") as f:
        for chunk in download_response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

    import logging
    logger = logging.getLogger("flow.collect_data")
    logger.info(f"Data downloaded and saved to: {output_file}")

    # Extract the ZIP file
    zip_file_path = output_file

    # Ensure the file exists before extraction
    if os.path.exists(zip_file_path):
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            extract_path = os.path.join(output_dir, "extracted")
            os.makedirs(extract_path, exist_ok=True)
            zip_ref.extractall(extract_path)
            logger.info(f"Files extracted to: {extract_path}")
        
        # Ensure the ZIP file is properly closed before deleting
        del zip_ref  # Explicitly delete the zipfile object

        # Cleanup: Remove the ZIP file after extraction
        os.remove(zip_file_path)
        logger.info(f"Removed ZIP file: {zip_file_path}")

        # Remove unrelated extracted files (except for CSV)
        for extracted_file in os.listdir(extract_path):
            extracted_file_path = os.path.join(extract_path, extracted_file)
            if not extracted_file.endswith('.csv'):
                os.remove(extracted_file_path)
                logger.debug(f"Removed extracted file: {extracted_file_path}")
    else:
        logger.warning(f"ZIP file not found at {zip_file_path}.")
