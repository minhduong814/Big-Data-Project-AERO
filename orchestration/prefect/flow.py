"""
Prefect workflows for AERO data pipeline orchestration
"""
from prefect import flow, task
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from datetime import timedelta
import sys
import logging
import os

sys.path.append('/home/quangminh/Documents/code/Python/Big-Data-Project-AERO')

from src.extract.kafka_producer import AeroDataProducer, FlightDataExtractor
from src.load.kafka_consumer import AeroDataLoader
from src.transform.spark_streaming import AeroSparkProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task(retries=3, retry_delay_seconds=60, log_prints=True)
def extract_flight_data(
    api_endpoint: str,
    kafka_servers: str,
    topic: str
) -> int:
    """
    Extract flight data from API and send to Kafka
    
    Args:
        api_endpoint: API endpoint URL
        kafka_servers: Kafka bootstrap servers
        topic: Target Kafka topic
        
    Returns:
        Number of records extracted
    """
    logger.info(f"Starting data extraction from {api_endpoint}")
    
    producer = AeroDataProducer(
        bootstrap_servers=kafka_servers,
        topic=topic,
        config_path="config/pipeline_config.yaml"
    )
    
    extractor = FlightDataExtractor(producer)
    
    try:
        # Extract from file for testing
        test_file = "data/test.json"
        if os.path.exists(test_file):
            extractor.extract_from_file(test_file)
            return 1
        else:
            logger.warning(f"Test file {test_file} not found")
            return 0
    finally:
        producer.close()


@task(retries=3, retry_delay_seconds=60, log_prints=True)
def load_to_bigquery(
    kafka_servers: str,
    topic: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
    batch_size: int = 100,
    max_messages: int = 1000
) -> int:
    """
    Load data from Kafka to BigQuery
    
    Args:
        kafka_servers: Kafka bootstrap servers
        topic: Source Kafka topic
        project_id: GCP project ID
        dataset_id: BigQuery dataset
        table_id: BigQuery table
        batch_size: Batch size for loading
        max_messages: Maximum messages to process
        
    Returns:
        Number of messages processed
    """
    logger.info(f"Starting data load to BigQuery: {project_id}.{dataset_id}.{table_id}")
    
    loader = AeroDataLoader(
        bootstrap_servers=kafka_servers,
        topic=topic,
        group_id="aero-prefect-consumer",
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        config_path="config/pipeline_config.yaml"
    )
    
    try:
        count = loader.consume_and_load(
            batch_size=batch_size,
            max_messages=max_messages
        )
        logger.info(f"Loaded {count} messages to BigQuery")
        return count
    finally:
        loader.close()


@task(log_prints=True)
def transform_with_spark(
    kafka_servers: str,
    input_topic: str,
    output_topic: str
) -> None:
    """
    Transform data using Spark Streaming
    
    Args:
        kafka_servers: Kafka bootstrap servers
        input_topic: Input topic
        output_topic: Output topic
    """
    logger.info(f"Starting Spark transformation: {input_topic} -> {output_topic}")
    
    processor = AeroSparkProcessor(
        app_name="Prefect-Spark-Transform",
        config_path="config/pipeline_config.yaml"
    )
    
    try:
        processor.process_stream(
            kafka_bootstrap_servers=kafka_servers,
            input_topic=input_topic,
            output_topic=output_topic
        )
    finally:
        processor.stop()


@flow(name="aero-etl-pipeline", log_prints=True)
def aero_etl_pipeline(
    api_endpoint: str = "http://api.example.com/flights",
    kafka_servers: str = "kafka:29092",
    raw_topic: str = "flights-raw",
    processed_topic: str = "flights-processed",
    project_id: str = "double-arbor-475907-s5",
    dataset_id: str = "aero_dataset",
    table_id: str = "flights"
):
    """
    Main ETL pipeline flow
    
    Args:
        api_endpoint: API endpoint for data extraction
        kafka_servers: Kafka bootstrap servers
        raw_topic: Raw data topic
        processed_topic: Processed data topic
        project_id: GCP project ID
        dataset_id: BigQuery dataset
        table_id: BigQuery table
    """
    logger.info("Starting AERO ETL Pipeline")
    
    # Extract
    extract_result = extract_flight_data(
        api_endpoint=api_endpoint,
        kafka_servers=kafka_servers,
        topic=raw_topic
    )
    
    # Transform (runs in parallel with load)
    transform_with_spark(
        kafka_servers=kafka_servers,
        input_topic=raw_topic,
        output_topic=processed_topic
    )
    
    # Load
    load_result = load_to_bigquery(
        kafka_servers=kafka_servers,
        topic=processed_topic,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id
    )
    
    logger.info(f"Pipeline completed. Loaded {load_result} records")


@flow(name="aero-batch-analytics", log_prints=True)
def aero_batch_analytics(
    input_path: str = "gs://aero-data/raw",
    output_path: str = "gs://aero-data/analytics",
    date_from: str = "2024-01-01",
    date_to: str = "2024-12-31"
):
    """
    Batch analytics pipeline for historical data
    
    Args:
        input_path: Input data path
        output_path: Output path
        date_from: Start date
        date_to: End date
    """
    from src.transform.spark_batch import AeroSparkBatchProcessor
    
    logger.info("Starting batch analytics pipeline")
    
    processor = AeroSparkBatchProcessor()
    
    try:
        processor.process_historical_data(
            input_path=input_path,
            output_path=output_path,
            date_from=date_from,
            date_to=date_to
        )
        logger.info("Batch analytics completed")
    finally:
        processor.stop()


if __name__ == "__main__":
    # Run the pipeline
    aero_etl_pipeline()
