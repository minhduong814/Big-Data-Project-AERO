import os

class Config:
    # GCP Settings
    PROJECT_ID = os.getenv("GCP_PROJECT_ID", "aero-project-2025")
    REGION = os.getenv("GCP_REGION", "asia-southeast1")
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "aero-data-lake-2025")
    
    # BigQuery Settings
    BQ_DATASET = "flight_data_warehouse"
    
    # API Settings
    AVIATION_STACK_API_KEY = os.getenv("AVIATION_STACK_API_KEY", "53aaa5bebf4b0042ff0ac57fff17d421")
    
    # Kafka Settings
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap:9092")
    KAFKA_TOPIC = "flight-realtime"

    # Paths
    RAW_DATA_PATH = f"gs://{BUCKET_NAME}/raw"
    CLEANED_DATA_PATH = f"gs://{BUCKET_NAME}/cleaned"
    DIM_PATH = f"gs://{BUCKET_NAME}/warehouse/dims"
    FACT_PATH = f"gs://{BUCKET_NAME}/warehouse/facts"
