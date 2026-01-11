"""
Data Ingestion module for loading data to various storage systems
Supports BigQuery, GCS, and local storage
"""
import logging
import os
from typing import List, Dict, Any, Optional
from google.cloud import bigquery, storage
from google.api_core import retry
import pandas as pd
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataIngestion:
    """Handles data ingestion to various storage backends"""
    
    def __init__(
        self,
        project_id: str,
        credentials_path: Optional[str] = None
    ):
        """
        Initialize data ingestion
        
        Args:
            project_id: GCP project ID
            credentials_path: Path to GCP credentials
        """
        self.project_id = project_id
        self.logger = logging.getLogger(__name__)
        
        # Set credentials if provided
        if credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        
        # Initialize clients
        self.bq_client = bigquery.Client(project=project_id)
        self.gcs_client = storage.Client(project=project_id)
        
        self.logger.info(f"Data ingestion initialized for project: {project_id}")
    
    def ingest_to_bigquery(
        self,
        data: List[Dict[str, Any]],
        dataset_id: str,
        table_id: str,
        write_disposition: str = "WRITE_APPEND"
    ) -> int:
        """
        Ingest data to BigQuery
        
        Args:
            data: List of records
            dataset_id: BigQuery dataset
            table_id: BigQuery table
            write_disposition: Write mode (WRITE_APPEND, WRITE_TRUNCATE)
            
        Returns:
            Number of records ingested
        """
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            
            errors = self.bq_client.insert_rows_json(
                table_ref,
                data,
                retry=retry.Retry(deadline=60)
            )
            
            if errors:
                self.logger.error(f"BigQuery errors: {errors}")
                return 0
            else:
                self.logger.info(f"Ingested {len(data)} records to {table_ref}")
                return len(data)
                
        except Exception as e:
            self.logger.error(f"Error ingesting to BigQuery: {e}")
            return 0
    
    def ingest_to_gcs(
        self,
        data: List[Dict[str, Any]],
        bucket_name: str,
        blob_name: str,
        format: str = "json"
    ) -> bool:
        """
        Ingest data to Google Cloud Storage
        
        Args:
            data: List of records
            bucket_name: GCS bucket name
            blob_name: Blob/file name
            format: Output format (json, csv, parquet)
            
        Returns:
            True if successful
        """
        try:
            bucket = self.gcs_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            if format == "json":
                content = json.dumps(data, indent=2)
                blob.upload_from_string(content, content_type="application/json")
            
            elif format == "csv":
                df = pd.DataFrame(data)
                csv_data = df.to_csv(index=False)
                blob.upload_from_string(csv_data, content_type="text/csv")
            
            elif format == "parquet":
                df = pd.DataFrame(data)
                parquet_data = df.to_parquet()
                blob.upload_from_string(parquet_data, content_type="application/octet-stream")
            
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            self.logger.info(f"Ingested {len(data)} records to gs://{bucket_name}/{blob_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error ingesting to GCS: {e}")
            return False
    
    def ingest_to_local(
        self,
        data: List[Dict[str, Any]],
        file_path: str,
        format: str = "json"
    ) -> bool:
        """
        Ingest data to local storage
        
        Args:
            data: List of records
            file_path: Local file path
            format: Output format (json, csv, parquet)
            
        Returns:
            True if successful
        """
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            if format == "json":
                with open(file_path, 'w') as f:
                    json.dump(data, f, indent=2)
            
            elif format == "csv":
                df = pd.DataFrame(data)
                df.to_csv(file_path, index=False)
            
            elif format == "parquet":
                df = pd.DataFrame(data)
                df.to_parquet(file_path)
            
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            self.logger.info(f"Ingested {len(data)} records to {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error ingesting to local storage: {e}")
            return False
    
    def create_bigquery_table(
        self,
        dataset_id: str,
        table_id: str,
        schema: List[bigquery.SchemaField],
        partition_field: Optional[str] = "timestamp"
    ) -> bool:
        """
        Create BigQuery table if not exists
        
        Args:
            dataset_id: Dataset ID
            table_id: Table ID
            schema: Table schema
            partition_field: Field to partition on
            
        Returns:
            True if successful
        """
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            table = bigquery.Table(table_ref, schema=schema)
            
            if partition_field:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_field
                )
            
            table = self.bq_client.create_table(table, exists_ok=True)
            self.logger.info(f"Table {table_ref} created/verified")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating table: {e}")
            return False


if __name__ == "__main__":
    # Example usage
    project_id = os.environ.get("GCP_PROJECT_ID", "double-arbor-475907-s5")
    
    ingestion = DataIngestion(project_id=project_id)
    
    # Sample data
    sample_data = [
        {
            "timestamp": datetime.utcnow().isoformat(),
            "flight_id": "FL001",
            "airline": "AA",
            "origin": "JFK",
            "destination": "LAX"
        }
    ]
    
    # Ingest to local
    ingestion.ingest_to_local(sample_data, "data/output/sample.json")
