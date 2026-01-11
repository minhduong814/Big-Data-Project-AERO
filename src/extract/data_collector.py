"""
Data Collector for AERO flight data
Collects data from various sources (API, files, databases)
"""
import logging
import os
from typing import Dict, Any, List, Optional
import requests
import pandas as pd
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataCollector:
    """Collects flight data from various sources"""
    
    def __init__(self):
        """Initialize data collector"""
        self.logger = logging.getLogger(__name__)
    
    def collect_from_api(
        self,
        api_url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Collect data from REST API
        
        Args:
            api_url: API endpoint URL
            headers: Optional HTTP headers
            params: Optional query parameters
            
        Returns:
            List of flight records
        """
        try:
            self.logger.info(f"Collecting data from API: {api_url}")
            response = requests.get(api_url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if isinstance(data, list):
                self.logger.info(f"Collected {len(data)} records from API")
                return data
            else:
                self.logger.info(f"Collected 1 record from API")
                return [data]
                
        except requests.RequestException as e:
            self.logger.error(f"Error collecting from API: {e}")
            return []
    
    def collect_from_csv(
        self,
        file_path: str,
        chunk_size: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Collect data from CSV file
        
        Args:
            file_path: Path to CSV file
            chunk_size: Optional chunk size for large files
            
        Returns:
            List of flight records
        """
        try:
            self.logger.info(f"Collecting data from CSV: {file_path}")
            
            if chunk_size:
                # Read in chunks for large files
                chunks = []
                for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)
            else:
                df = pd.read_csv(file_path)
            
            records = df.to_dict('records')
            self.logger.info(f"Collected {len(records)} records from CSV")
            return records
            
        except Exception as e:
            self.logger.error(f"Error collecting from CSV: {e}")
            return []
    
    def collect_from_json(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Collect data from JSON file
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            List of flight records
        """
        try:
            self.logger.info(f"Collecting data from JSON: {file_path}")
            
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            if isinstance(data, list):
                self.logger.info(f"Collected {len(data)} records from JSON")
                return data
            else:
                self.logger.info(f"Collected 1 record from JSON")
                return [data]
                
        except Exception as e:
            self.logger.error(f"Error collecting from JSON: {e}")
            return []
    
    def collect_from_parquet(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Collect data from Parquet file
        
        Args:
            file_path: Path to Parquet file
            
        Returns:
            List of flight records
        """
        try:
            self.logger.info(f"Collecting data from Parquet: {file_path}")
            df = pd.read_parquet(file_path)
            
            records = df.to_dict('records')
            self.logger.info(f"Collected {len(records)} records from Parquet")
            return records
            
        except Exception as e:
            self.logger.error(f"Error collecting from Parquet: {e}")
            return []
    
    def enrich_flight_data(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich flight data with additional fields
        
        Args:
            record: Flight record
            
        Returns:
            Enriched record
        """
        # Add timestamp if not present
        if 'timestamp' not in record:
            record['timestamp'] = datetime.utcnow().isoformat()
        
        # Add metadata
        record['_ingestion_time'] = datetime.utcnow().isoformat()
        record['_source'] = 'data_collector'
        
        return record


if __name__ == "__main__":
    collector = DataCollector()
    
    # Example: Collect from JSON file
    test_file = "data/test.json"
    if os.path.exists(test_file):
        data = collector.collect_from_json(test_file)
        print(f"Collected {len(data)} records")
    else:
        print(f"Test file {test_file} not found")
