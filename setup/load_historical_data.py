#!/usr/bin/env python3
"""
Load Historical Flight Data from GCS to BigQuery
Loads CSV files from gs://aero_data into BigQuery flights_raw table
"""

import os
import logging
import time
from typing import List, Optional
from google.cloud import bigquery
from google.cloud import storage

from google.api_core import exceptions

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HistoricalDataLoader:
    """Load historical flight data from GCS to BigQuery"""
    
    def __init__(
        self,
        project_id: str,
        dataset_id: str = "aero_dataset",
        table_id: str = "flights_raw",
        bucket_name: str = "aero_data"
    ):
        """
        Initialize the data loader
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            bucket_name: GCS bucket name
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.bucket_name = bucket_name
        
        # Initialize clients
        self.bq_client = bigquery.Client(project=project_id)
        self.storage_client = storage.Client(project=project_id)
        
        self.table_ref = f"{project_id}.{dataset_id}.{table_id}"
        
        logger.info(f"Initialized loader for {self.table_ref}")
        logger.info(f"Source bucket: gs://{bucket_name}")
    
    def list_csv_files(self, prefix: str = "") -> List[str]:
        """
        List all CSV files in the GCS bucket
        
        Args:
            prefix: Optional prefix to filter files
            
        Returns:
            List of GCS URIs for CSV files
        """
        bucket = self.storage_client.bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        
        csv_files = []
        for blob in blobs:
            if blob.name.endswith('.csv') and '/' not in blob.name.replace(prefix, ''):
                csv_uri = f"gs://{self.bucket_name}/{blob.name}"
                csv_files.append(csv_uri)
        
        logger.info(f"Found {len(csv_files)} CSV files in gs://{self.bucket_name}/{prefix}")
        return sorted(csv_files)
    
    def get_table_schema(self) -> List[bigquery.SchemaField]:
        """
        Get the schema for the flights_raw table
        
        Returns:
            List of BigQuery schema fields
        """
        try:
            table = self.bq_client.get_table(self.table_ref)
            return table.schema
        except exceptions.NotFound:
            logger.error(f"Table {self.table_ref} not found. Please run setup_bigquery.py first.")
            raise
    
    def load_batch(
        self,
        source_uris: List[str],
        write_disposition: str = "WRITE_APPEND",
        skip_leading_rows: int = 1
    ) -> bigquery.LoadJob:
        """
        Load a batch of files from GCS to BigQuery
        
        Args:
            source_uris: List of GCS URIs
            write_disposition: How to write data (WRITE_APPEND, WRITE_TRUNCATE)
            skip_leading_rows: Number of header rows to skip
            
        Returns:
            BigQuery LoadJob
        """
        # Get existing schema
        schema = self.get_table_schema()
        
        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=skip_leading_rows,
            write_disposition=write_disposition,
            allow_quoted_newlines=True,
            allow_jagged_rows=True,  # Allow rows with missing columns
            ignore_unknown_values=True,  # Ignore extra columns
            max_bad_records=1000,  # Allow some bad records
        )
        
        logger.info(f"Starting load job for {len(source_uris)} files...")
        logger.info(f"Write disposition: {write_disposition}")
        
        # Start the load job
        load_job = self.bq_client.load_table_from_uri(
            source_uris,
            self.table_ref,
            job_config=job_config
        )
        
        return load_job
    
    def wait_for_job(self, job: bigquery.LoadJob) -> dict:
        """
        Wait for a BigQuery job to complete and return statistics
        
        Args:
            job: BigQuery LoadJob
            
        Returns:
            Dictionary with job statistics
        """
        logger.info(f"Waiting for job {job.job_id} to complete...")
        
        try:
            job.result()  # Wait for job to complete
            
            stats = {
                "job_id": job.job_id,
                "state": job.state,
                "input_files": job.input_files,
                "input_file_bytes": job.input_file_bytes,
                "output_rows": job.output_rows,
                "errors": job.errors,
            }
            
            if job.errors:
                logger.warning(f"Job completed with errors: {job.errors}")
            else:
                logger.info(f"✅ Job {job.job_id} completed successfully")
                logger.info(f"   Input files: {stats['input_files']}")
                logger.info(f"   Input bytes: {stats['input_file_bytes']:,}")
                logger.info(f"   Output rows: {stats['output_rows']:,}")
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Job {job.job_id} failed: {e}")
            if hasattr(job, 'errors') and job.errors:
                logger.error(f"Job errors: {job.errors}")
            raise
    
    def load_all_files(
        self,
        batch_size: int = 50,
        write_disposition: str = "WRITE_APPEND"
    ) -> dict:
        """
        Load all CSV files from GCS to BigQuery in batches
        
        Args:
            batch_size: Number of files to load per batch
            write_disposition: How to write data
            
        Returns:
            Dictionary with load statistics
        """
        # List all CSV files
        all_files = self.list_csv_files()
        
        if not all_files:
            logger.warning("No CSV files found in the bucket")
            return {"status": "no_files", "total_files": 0}
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Loading {len(all_files)} files to {self.table_ref}")
        logger.info(f"Batch size: {batch_size} files per job")
        logger.info(f"{'='*60}\n")
        
        total_stats = {
            "total_files": len(all_files),
            "total_rows": 0,
            "total_bytes": 0,
            "successful_batches": 0,
            "failed_batches": 0,
            "jobs": []
        }
        
        # Process files in batches
        for i in range(0, len(all_files), batch_size):
            batch = all_files[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(all_files) + batch_size - 1) // batch_size
            
            logger.info(f"\n--- Batch {batch_num}/{total_batches} ---")
            logger.info(f"Files: {len(batch)}")
            logger.info(f"Range: {os.path.basename(batch[0])} to {os.path.basename(batch[-1])}")
            
            try:
                # For first batch, optionally truncate table
                disposition = write_disposition if i > 0 else write_disposition
                
                # Load the batch
                job = self.load_batch(batch, write_disposition=disposition)
                stats = self.wait_for_job(job)
                
                # Update total statistics
                total_stats["total_rows"] += stats.get("output_rows", 0)
                total_stats["total_bytes"] += stats.get("input_file_bytes", 0)
                total_stats["successful_batches"] += 1
                total_stats["jobs"].append(stats)
                
            except Exception as e:
                logger.error(f"❌ Failed to load batch {batch_num}: {e}")
                total_stats["failed_batches"] += 1
                continue
            
            # Brief pause between batches
            if i + batch_size < len(all_files):
                logger.info("Pausing for 2 seconds before next batch...")
                time.sleep(2)
        
        return total_stats
    
    def load_year_range(
        self,
        start_year: int,
        end_year: int,
        batch_size: int = 50
    ) -> dict:
        """
        Load data for a specific year range
        
        Args:
            start_year: Starting year (inclusive)
            end_year: Ending year (inclusive)
            batch_size: Number of files per batch
            
        Returns:
            Dictionary with load statistics
        """
        logger.info(f"Loading data from {start_year} to {end_year}")
        
        # List all files
        all_files = self.list_csv_files()
        
        # Filter files by year range
        year_files = []
        for file_uri in all_files:
            filename = os.path.basename(file_uri)
            try:
                # Extract year from filename (e.g., "1987_10.csv" -> 1987)
                file_year = int(filename.split('_')[0])
                if start_year <= file_year <= end_year:
                    year_files.append(file_uri)
            except (ValueError, IndexError):
                continue
        
        logger.info(f"Found {len(year_files)} files for years {start_year}-{end_year}")
        
        if not year_files:
            return {"status": "no_files", "total_files": 0}
        
        # Load the filtered files
        total_stats = {
            "year_range": f"{start_year}-{end_year}",
            "total_files": len(year_files),
            "total_rows": 0,
            "total_bytes": 0,
            "successful_batches": 0,
            "failed_batches": 0,
            "jobs": []
        }
        
        # Process in batches
        for i in range(0, len(year_files), batch_size):
            batch = year_files[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(year_files) + batch_size - 1) // batch_size
            
            logger.info(f"\n--- Batch {batch_num}/{total_batches} ---")
            
            try:
                job = self.load_batch(batch, write_disposition="WRITE_APPEND")
                stats = self.wait_for_job(job)
                
                total_stats["total_rows"] += stats.get("output_rows", 0)
                total_stats["total_bytes"] += stats.get("input_file_bytes", 0)
                total_stats["successful_batches"] += 1
                total_stats["jobs"].append(stats)
                
            except Exception as e:
                logger.error(f"Failed to load batch: {e}")
                total_stats["failed_batches"] += 1
                continue
            
            if i + batch_size < len(year_files):
                time.sleep(2)
        
        return total_stats
    
    def get_table_stats(self) -> dict:
        """
        Get current table statistics
        
        Returns:
            Dictionary with table information
        """
        try:
            table = self.bq_client.get_table(self.table_ref)
            
            return {
                "table_id": table.table_id,
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "created": table.created,
                "modified": table.modified,
            }
        except exceptions.NotFound:
            return {"error": "Table not found"}
        except Exception as e:
            return {"error": str(e)}
    
    def print_summary(self, stats: dict) -> None:
        """
        Print a summary of the load operation
        
        Args:
            stats: Statistics dictionary
        """
        logger.info("\n" + "="*60)
        logger.info("LOAD OPERATION SUMMARY")
        logger.info("="*60)
        
        if "year_range" in stats:
            logger.info(f"Year Range: {stats['year_range']}")
        
        logger.info(f"Total Files: {stats.get('total_files', 0)}")
        logger.info(f"Successful Batches: {stats.get('successful_batches', 0)}")
        logger.info(f"Failed Batches: {stats.get('failed_batches', 0)}")
        logger.info(f"Total Rows Loaded: {stats.get('total_rows', 0):,}")
        logger.info(f"Total Bytes Processed: {stats.get('total_bytes', 0):,} ({stats.get('total_bytes', 0) / (1024**3):.2f} GB)")
        
        # Get current table stats
        table_stats = self.get_table_stats()
        if "error" not in table_stats:
            logger.info(f"\nCurrent Table Statistics:")
            logger.info(f"  Total Rows: {table_stats.get('num_rows', 0):,}")
            logger.info(f"  Table Size: {table_stats.get('num_bytes', 0) / (1024**3):.2f} GB")
        
        logger.info("="*60 + "\n")


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Load historical flight data from GCS to BigQuery')
    parser.add_argument('--project-id', default=os.environ.get("GCP_PROJECT_ID", "double-arbor-475907-s5"),
                        help='GCP project ID')
    parser.add_argument('--dataset-id', default='aero_dataset',
                        help='BigQuery dataset ID')
    parser.add_argument('--table-id', default='flights_raw',
                        help='BigQuery table ID')
    parser.add_argument('--bucket-name', default='aero_data',
                        help='GCS bucket name')
    parser.add_argument('--batch-size', type=int, default=50,
                        help='Number of files to load per batch')
    parser.add_argument('--start-year', type=int, default=None,
                        help='Start year (optional, for loading specific year range)')
    parser.add_argument('--end-year', type=int, default=None,
                        help='End year (optional, for loading specific year range)')
    parser.add_argument('--write-mode', choices=['append', 'truncate'], default='append',
                        help='Write mode: append or truncate')
    
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("Historical Data Loader for AERO Project")
    logger.info("="*60)
    logger.info(f"Project: {args.project_id}")
    logger.info(f"Dataset: {args.dataset_id}")
    logger.info(f"Table: {args.table_id}")
    logger.info(f"Bucket: gs://{args.bucket_name}")
    logger.info(f"Batch Size: {args.batch_size} files")
    logger.info(f"Write Mode: {args.write_mode}")
    logger.info("="*60 + "\n")
    
    # Initialize loader
    loader = HistoricalDataLoader(
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        table_id=args.table_id,
        bucket_name=args.bucket_name
    )
    
    # Determine write disposition
    write_disposition = "WRITE_TRUNCATE" if args.write_mode == 'truncate' else "WRITE_APPEND"
    
    # Load data
    if args.start_year and args.end_year:
        logger.info(f"Loading data for years {args.start_year} to {args.end_year}...")
        stats = loader.load_year_range(
            start_year=args.start_year,
            end_year=args.end_year,
            batch_size=args.batch_size
        )
    else:
        logger.info("Loading all available data...")
        stats = loader.load_all_files(
            batch_size=args.batch_size,
            write_disposition=write_disposition
        )
    
    # Print summary
    loader.print_summary(stats)
    
    # Success message
    if stats.get("failed_batches", 0) == 0:
        logger.info("✅ All batches loaded successfully!")
    else:
        logger.warning(f"⚠️  {stats.get('failed_batches', 0)} batches failed")
    
    logger.info("\nNext steps:")
    logger.info("1. Query the data:")
    logger.info(f"   bq query 'SELECT COUNT(*) FROM `{args.project_id}.{args.dataset_id}.{args.table_id}`'")
    logger.info("2. Run analytics queries")
    logger.info("3. Create visualizations with the data")


if __name__ == "__main__":
    main()
