"""
Kafka Consumer for AERO data loading
Consumes from Kafka and loads to BigQuery/Storage
"""
import json
import logging
import os
from typing import List, Dict, Any, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from google.cloud import bigquery
from google.api_core import retry
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AeroDataLoader:
    """Consumer for loading AERO data from Kafka to BigQuery"""
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        project_id: str,
        dataset_id: str,
        table_id: str,
        config_path: Optional[str] = None
    ):
        """
        Initialize Kafka consumer and BigQuery client
        
        Args:
            bootstrap_servers: Kafka broker addresses
            topic: Kafka topic to consume from
            group_id: Consumer group ID
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            config_path: Optional path to config file
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.logger = logging.getLogger(__name__)
        
        # Load config if provided
        if config_path:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                self.kafka_config = config.get('kafka', {})
                self.gcp_config = config.get('gcp', {})
        else:
            self.kafka_config = {}
            self.gcp_config = {}
        
        # Build consumer configuration
        consumer_config = {
            'bootstrap_servers': self.bootstrap_servers,
            'group_id': self.group_id,
            'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
            'key_deserializer': lambda k: k.decode('utf-8') if k else None,
            'enable_auto_commit': True,
            'max_poll_records': 500,
        }
        
        # Merge with config from file, with defaults
        config_from_file = self.kafka_config.get('consumer_config', {})
        if config_from_file:
            consumer_config.update(config_from_file)
        else:
            # Default configuration if no config file
            consumer_config['auto_offset_reset'] = 'earliest'
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            self.topic,
            **consumer_config
        )
        
        # Initialize BigQuery client
        self.bq_client = bigquery.Client(project=project_id)
        self.table_ref = f"{project_id}.{dataset_id}.{table_id}"
        
        self.logger.info(
            f"Consumer initialized for topic: {self.topic}, "
            f"group: {self.group_id}, table: {self.table_ref}"
        )
    
    def consume_and_load(
        self,
        batch_size: int = 100,
        max_messages: Optional[int] = None,
        timeout_ms: int = 5000
    ) -> int:
        """
        Consume messages from Kafka and load to BigQuery
        
        Args:
            batch_size: Number of messages to batch before loading
            max_messages: Maximum number of messages to process (None = infinite)
            timeout_ms: Timeout in milliseconds when polling for messages (default: 5000ms = 5s)
            
        Returns:
            int: Total number of messages processed
        """
        batch = []
        total_processed = 0
        consecutive_empty_polls = 0
        max_empty_polls = 3  # Exit after 3 consecutive empty polls (15 seconds)
        
        try:
            self.logger.info(f"Starting to consume from topic: {self.topic}")
            self.logger.info(f"Timeout: {timeout_ms}ms, Max messages: {max_messages or 'unlimited'}")
            
            while True:
                # Use poll with timeout instead of blocking iterator
                message_pack = self.consumer.poll(timeout_ms=timeout_ms)
                
                if not message_pack:
                    # No messages received in this poll
                    consecutive_empty_polls += 1
                    
                    # If we've processed messages before, continue waiting
                    if total_processed > 0:
                        consecutive_empty_polls = 0  # Reset counter if we've processed some messages
                        continue
                    
                    # If no messages processed and multiple empty polls, exit gracefully
                    if consecutive_empty_polls >= max_empty_polls:
                        self.logger.info(
                            f"No messages available after {max_empty_polls * timeout_ms / 1000}s. "
                            f"Topic may be empty. Exiting gracefully."
                        )
                        break
                    
                    continue
                
                # Reset empty poll counter when we get messages
                consecutive_empty_polls = 0
                
                # Process messages from all partitions
                for topic_partition, messages in message_pack.items():
                    for message in messages:
                        try:
                            batch.append(message.value)
                            total_processed += 1
                            
                            # Load batch when size reached
                            if len(batch) >= batch_size:
                                self._load_to_bigquery(batch)
                                batch = []
                            
                            # Check if max messages reached
                            if max_messages and total_processed >= max_messages:
                                self.logger.info(f"Reached max_messages limit: {max_messages}")
                                break
                            
                            if total_processed % 1000 == 0:
                                self.logger.info(f"Processed {total_processed} messages")
                                
                        except Exception as e:
                            self.logger.error(f"Error processing message: {e}")
                            continue
                
                # Break if max messages reached
                if max_messages and total_processed >= max_messages:
                    break
            
            # Load remaining messages
            if batch:
                self._load_to_bigquery(batch)
            
            self.logger.info(f"Total messages processed: {total_processed}")
            return total_processed
            
        except KeyboardInterrupt:
            self.logger.info("Consumer interrupted by user")
            if batch:
                self._load_to_bigquery(batch)
            return total_processed
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            if batch:
                self._load_to_bigquery(batch)
            raise
    
    @retry.Retry(deadline=60)
    def _load_to_bigquery(self, records: List[Dict[str, Any]]) -> None:
        """
        Load batch of records to BigQuery
        
        Args:
            records: List of record dictionaries
        """
        try:
            errors = self.bq_client.insert_rows_json(
                self.table_ref,
                records,
                retry=retry.Retry(deadline=60)
            )
            
            if errors:
                self.logger.error(f"BigQuery insert errors: {errors}")
                # Log failed records
                for error in errors:
                    self.logger.error(f"Failed record: {error}")
            else:
                self.logger.info(
                    f"Successfully loaded {len(records)} records to BigQuery"
                )
                
        except Exception as e:
            self.logger.error(f"Error loading to BigQuery: {e}")
            # Optionally write to dead letter queue or file
            self._write_to_dlq(records, str(e))
    
    def _write_to_dlq(self, records: List[Dict[str, Any]], error: str) -> None:
        """
        Write failed records to dead letter queue
        
        Args:
            records: Failed records
            error: Error message
        """
        import datetime
        
        dlq_file = f"dlq_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            with open(dlq_file, 'w') as f:
                json.dump({
                    'error': error,
                    'records': records
                }, f, indent=2)
            self.logger.info(f"Failed records written to {dlq_file}")
        except Exception as e:
            self.logger.error(f"Error writing to DLQ: {e}")
    
    def create_table_if_not_exists(self, schema: List[bigquery.SchemaField]) -> None:
        """
        Create BigQuery table if it doesn't exist
        
        Args:
            schema: BigQuery schema
        """
        try:
            table = bigquery.Table(self.table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="timestamp"
            )
            
            table = self.bq_client.create_table(table, exists_ok=True)
            self.logger.info(f"Table {self.table_ref} ready")
            
        except Exception as e:
            self.logger.error(f"Error creating table: {e}")
            raise
    
    def close(self):
        """Close consumer connection"""
        self.logger.info("Closing consumer...")
        self.consumer.close()
        self.logger.info("Consumer closed")


if __name__ == "__main__":
    # Get configuration from environment variables
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "flights-processed")
    project_id = os.environ.get("GCP_PROJECT_ID", "double-arbor-475907-s5")
    
    loader = AeroDataLoader(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id="aero-consumer-group",
        project_id=project_id,
        dataset_id="aero_dataset",
        table_id="flights",
        config_path="config/pipeline_config.yaml"
    )
    
    # Define schema
    schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("flight_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("airline", "STRING"),
        bigquery.SchemaField("origin", "STRING"),
        bigquery.SchemaField("destination", "STRING"),
    ]
    
    loader.create_table_if_not_exists(schema)
    loader.consume_and_load(batch_size=100)
    loader.close()
