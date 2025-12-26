import sys
import os
import json
import time
from kafka import KafkaConsumer
from google.cloud import storage
from datetime import datetime

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.settings import Config

def upload_to_gcs(bucket_name, data, filename):
    """
    Uploads a string (JSON) to GCS.
    """
    try:
        client = storage.Client(project=Config.PROJECT_ID)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"realtime/{filename}")
        blob.upload_from_string(data, content_type='application/json')
        print(f"Uploaded {filename} to GCS.")
    except Exception as e:
        print(f"Failed to upload to GCS: {e}")

def run_consumer():
    consumer = KafkaConsumer(
        Config.KAFKA_TOPIC,
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='flight-data-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print(f"Starting Kafka Consumer for topic: {Config.KAFKA_TOPIC}")
    
    batch = []
    batch_size = 100
    last_upload_time = time.time()
    upload_interval = 300 # 5 minutes

    for message in consumer:
        flight_data = message.value
        batch.append(flight_data)
        
        current_time = time.time()
        
        # Upload if batch is full or time interval passed
        if len(batch) >= batch_size or (current_time - last_upload_time) > upload_interval:
            if batch:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"flights_{timestamp}.json"
                json_data = json.dumps(batch)
                
                upload_to_gcs(Config.BUCKET_NAME, json_data, filename)
                
                batch = []
                last_upload_time = current_time

if __name__ == "__main__":
    run_consumer()
