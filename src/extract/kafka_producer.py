"""
Kafka Producer for AERO data extraction
Extracts flight data and sends to Kafka topics
"""
import json
import logging
import time
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
import yaml
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def json_serializer(obj):
    """Custom JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    elif isinstance(obj, pd.Series):
        return obj.to_dict()
    elif hasattr(obj, '__dict__'):
        return obj.__dict__
    raise TypeError(f"Type {type(obj)} not serializable")


class AeroDataProducer:
    """Producer for extracting and sending AERO flight data to Kafka"""
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        config_path: Optional[str] = None
    ):
        """
        Initialize Kafka producer
        
        Args:
            bootstrap_servers: Kafka broker addresses
            topic: Target Kafka topic
            config_path: Optional path to config file
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.logger = logging.getLogger(__name__)
        
        # Load config if provided
        if config_path:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                self.config = config.get('kafka', {})
        else:
            self.config = {}
        
        # Build producer configuration
        producer_config = {
            'bootstrap_servers': self.bootstrap_servers,
            'value_serializer': lambda v: json.dumps(v, default=json_serializer).encode('utf-8'),
            'key_serializer': lambda k: k.encode('utf-8') if k else None,
        }
        
        # Add config from file if provided, otherwise use defaults
        config_from_file = self.config.get('producer_config', {})
        if config_from_file:
            producer_config.update(config_from_file)
        else:
            # Default configuration if no config file
            producer_config.update({
                'acks': 'all',
                'retries': 3,
                'max_in_flight_requests_per_connection': 5,
                'compression_type': 'gzip',
            })
        
        # Initialize producer
        self.producer = KafkaProducer(**producer_config)
        self.logger.info(f"Producer initialized for topic: {self.topic}")
    
    def send_data(
        self,
        data: Dict[str, Any],
        key: Optional[str] = None,
        partition: Optional[int] = None
    ) -> bool:
        """
        Send data to Kafka topic
        
        Args:
            data: Data dictionary to send
            key: Optional message key for partitioning
            partition: Optional specific partition
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            future = self.producer.send(
                self.topic,
                value=data,
                key=key,
                partition=partition
            )
            
            # Wait for send to complete
            record_metadata = future.get(timeout=10)
            
            self.logger.debug(
                f"Message sent to {record_metadata.topic} "
                f"partition {record_metadata.partition} "
                f"offset {record_metadata.offset}"
            )
            return True
            
        except KafkaError as e:
            self.logger.error(f"Error sending message: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return False
    
    def send_batch(self, data_list: list[Dict[str, Any]]) -> int:
        """
        Send batch of messages
        
        Args:
            data_list: List of data dictionaries
            
        Returns:
            int: Number of successfully sent messages
        """
        success_count = 0
        for data in data_list:
            if self.send_data(data):
                success_count += 1
        
        self.producer.flush()
        self.logger.info(f"Batch sent: {success_count}/{len(data_list)} successful")
        return success_count
    
    def close(self):
        """Close the producer and flush pending messages"""
        self.logger.info("Closing producer...")
        self.producer.flush()
        self.producer.close()
        self.logger.info("Producer closed")


class FlightDataExtractor:
    """Extractor for flight data from various sources"""
    
    def __init__(self, producer: AeroDataProducer):
        """
        Initialize extractor
        
        Args:
            producer: AeroDataProducer instance
        """
        self.producer = producer
        self.logger = logging.getLogger(__name__)
    
    def extract_from_api(self, api_endpoint: str, interval: int = 60):
        """
        Extract data from API continuously
        
        Args:
            api_endpoint: API endpoint URL
            interval: Polling interval in seconds
        """
        import requests
        
        self.logger.info(f"Starting API extraction from {api_endpoint}")
        
        while True:
            try:
                response = requests.get(api_endpoint, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                # Send to Kafka
                if isinstance(data, list):
                    self.producer.send_batch(data)
                else:
                    self.producer.send_data(data)
                
                self.logger.info(f"Extracted and sent data from API")
                time.sleep(interval)
                
            except requests.RequestException as e:
                self.logger.error(f"API request error: {e}")
                time.sleep(interval)
            except Exception as e:
                self.logger.error(f"Extraction error: {e}")
                time.sleep(interval)
    
    def extract_from_file(self, file_path: str):
        """
        Extract data from file
        
        Args:
            file_path: Path to data file (JSON/CSV)
        """
        import pandas as pd
        
        self.logger.info(f"Extracting from file: {file_path}")
        
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith('.json'):
                df = pd.read_json(file_path)
            else:
                raise ValueError("Unsupported file format")
            
            # Convert to records and send
            records = df.to_dict('records')
            self.producer.send_batch(records)
            
            self.logger.info(f"Extracted {len(records)} records from file")
            
        except Exception as e:
            self.logger.error(f"File extraction error: {e}")


if __name__ == "__main__":
    import os
    
    # Get configuration from environment variables
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "flights-raw")
    
    # Initialize producer
    producer = AeroDataProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        config_path="config/pipeline_config.yaml"
    )
    
    extractor = FlightDataExtractor(producer)
    
    # Check if test data file exists
    test_file = "data/test.json"
    if os.path.exists(test_file):
        logger.info(f"Found test data file: {test_file}")
        extractor.extract_from_file(test_file)
    else:
        logger.warning(f"Test data file not found: {test_file}")
        logger.info("Sending sample flight data...")
        
        # Send sample data
        sample_flight = {
            "timestamp": "2024-01-10T10:00:00Z",
            "flight_id": "AA101",
            "airline": "American Airlines",
            "flight_number": "AA101",
            "origin": "JFK",
            "destination": "LAX",
            "scheduled_departure": "2024-01-10T10:00:00Z",
            "actual_departure": "2024-01-10T10:15:00Z",
            "scheduled_arrival": "2024-01-10T13:00:00Z",
            "actual_arrival": "2024-01-10T13:20:00Z",
            "status": "arrived",
            "aircraft_type": "Boeing 737",
            "latitude": 40.6413,
            "longitude": -73.7781,
            "altitude": 35000,
            "speed": 550
        }
        
        producer.send_data(sample_flight)
        logger.info("Sample flight data sent successfully!")
    
    producer.close()