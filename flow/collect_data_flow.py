from prefect import flow
from collect_data import collect_data
from collect_data_gcs import collect_data_to_gcs
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging

log_file = 'log/data_collection.log'
# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
# Function to rename the files
def rename_files(extracted_dir, year, month):
    for file_name in os.listdir(extracted_dir):
        if file_name.startswith("On_Time_Reporting_Carrier_On_Time_Performance"):
            old_file_path = os.path.join(extracted_dir, file_name)
            new_file_path = os.path.join(extracted_dir, f"{year}_{month:02d}.csv")
            os.rename(old_file_path, new_file_path)
            print(f"Renamed file to: {new_file_path}")

# Define a Prefect Flow
@flow(name="Data Collection Flow")
def data_collection_flow(to_gcs = False, bucket_name="aero_data"):
    # Use small range for testing
    years = range(1987, 2025) 
    months = range(1, 13)

    # Loop through each year and month
    for year in years:
        for month in months:
            if year == 1987 and month < 10:  # Skip months before October 1987
                continue
            
            if to_gcs:
                try:
                    # Collect data and upload to GCS
                    collect_data_to_gcs.submit(year, month, bucket_name)
                except Exception as e:
                    print(f"Failed to collect/upload data for {year}-{month:02d}: {e}")
            else:
                # Collect data
                collect_data.submit(year, month)

                # Extract and rename the files after download
                extracted_dir = "./downloads/extracted"
                rename_files(extracted_dir, year, month)

                # Clean up: Remove the ZIP and unrelated extracted files
                for file_name in os.listdir(extracted_dir):
                    file_path = os.path.join(extracted_dir, file_name)
                    if not file_name.endswith('.csv'):
                        os.remove(file_path)
                        print(f"Removed extracted file: {file_path}")

                # Optional: Clean up the directory itself after processing
                if not any(file.endswith('.csv') for file in os.listdir(extracted_dir)):
                    os.rmdir(extracted_dir)
                    print(f"Removed empty extracted directory: {extracted_dir}")



@flow(name="Data Collection Flow (Optimized)", log_prints=True)
def data_collection_flow_optimized(
    bucket_name="aero_data", 
    max_workers=8,
    start_year=1987,
    end_year=2025,
    delay_between_requests=0.5  # seconds delay between requests
):
    start_time = time.time()
    logger.info(f"Starting data collection flow from {start_year} to {end_year}")
    
    # Prepare tasks
    tasks = []
    for year in range(start_year, end_year):
        for month in range(1, 13):
            if year == 1987 and month < 10:
                continue
            tasks.append((year, month))
    
    logger.info(f"📋 Total tasks: {len(tasks)}")
    print(f"📋 Total tasks: {len(tasks)}")
    
    failed_downloads = []
    completed = 0
    
    # Wrapper function with delay
    def download_with_delay(year, month, bucket_name, delay):
        try:
            time.sleep(delay)  # Add delay before each request
            result = collect_data_to_gcs(year, month, bucket_name)
            logger.info(f"Successfully downloaded {year}-{month:02d}")
            return result
        except Exception as e:
            logger.error(f"Failed to download {year}-{month:02d}: {str(e)}")
            raise
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(download_with_delay, year, month, bucket_name, delay_between_requests): (year, month)
            for year, month in tasks
        }
        
        for future in as_completed(future_to_task):
            year, month = future_to_task[future]
            try:
                result = future.result()
                completed += 1
                elapsed = time.time() - start_time
                rate = completed / (elapsed / 60) if elapsed > 0 else 0
                eta = (len(tasks) - completed) / rate if rate > 0 else 0
                
                success_msg = (
                    f"✅ [{completed}/{len(tasks)}] {year}-{month:02d} | "
                    f"Rate: {rate:.1f}/min | ETA: {eta:.1f} min"
                )
                print(success_msg)
                logger.info(success_msg)
                
                if result:
                    logger.info(f"File uploaded to: {result}")
                
            except Exception as e:
                error_msg = f"❌ Failed {year}-{month:02d}: {e}"
                print(error_msg)
                logger.error(error_msg)
                failed_downloads.append((year, month))
    
    total_time = (time.time() - start_time) / 60
    completion_msg = f"\n🎉 Completed in {total_time:.1f} minutes"
    success_rate_msg = f"📊 Success rate: {(completed/len(tasks))*100:.1f}%"
    
    print(completion_msg)
    print(success_rate_msg)
    logger.info(completion_msg)
    logger.info(success_rate_msg)
    
    if failed_downloads:
        logger.warning(f"Failed downloads: {failed_downloads}")
        print(f"\n⚠️ {len(failed_downloads)} downloads failed:")
        for year, month in failed_downloads:
            print(f"   - {year}-{month:02d}")
            logger.warning(f"Failed: {year}-{month:02d}")
    
    logger.info("Data collection flow completed")
    return failed_downloads

# Run it
failed_downloads = data_collection_flow_optimized(max_workers=8)

# # Run the flow
# data_collection_flow(to_gcs=True)