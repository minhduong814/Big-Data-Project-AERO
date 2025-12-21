#!/usr/bin/env python3
"""
BigQuery Dataset and Tables Setup Script
Creates the AERO dataset and tables for flight data analytics
"""

import os
import logging
from typing import List, Dict
from google.cloud import bigquery
from google.api_core import exceptions

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BigQuerySetup:

    """Setup BigQuery dataset and tables for AERO project"""

    def __init__(self, project_id: str, dataset_id: str = "aero_dataset", location: str = "asia-east2"):
        """
        Initialize BigQuery setup

        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            location: Dataset location (should match GCS bucket region)
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.location = location
        self.client = bigquery.Client(project=project_id)
        self.dataset_ref = f"{project_id}.{dataset_id}"

        logger.info(f"Initialized BigQuery setup for project: {project_id}")

    def create_dataset(self, description: str = "AERO Flight Data Analytics Dataset") -> None:
        """
        Create BigQuery dataset if it doesn't exist

        Args:
            description: Dataset description
        """
        try:
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = self.location
            dataset.description = description

            # Create dataset
            dataset = self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"✅ Dataset {self.dataset_ref} created or already exists in {self.location}")

        except Exception as e:
            logger.error(f"❌ Error creating dataset: {e}")
            raise

    def get_flights_raw_schema(self) -> List[bigquery.SchemaField]:
        """
        Define schema for raw flights data table
        Based on the 110 columns from the CSV files

        Returns:
            List of BigQuery schema fields
        """
        return [
            # Time dimensions
            bigquery.SchemaField("Year", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Quarter", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Month", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("DayofMonth", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("DayOfWeek", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("FlightDate", "DATE", mode="REQUIRED"),

            # Airline information
            bigquery.SchemaField("Reporting_Airline", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("DOT_ID_Reporting_Airline", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("IATA_CODE_Reporting_Airline", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Tail_Number", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Flight_Number_Reporting_Airline", "INTEGER", mode="NULLABLE"),

            # Origin airport information
            bigquery.SchemaField("OriginAirportID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("OriginAirportSeqID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("OriginCityMarketID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Origin", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("OriginCityName", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("OriginState", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("OriginStateFips", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("OriginStateName", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("OriginWac", "INTEGER", mode="NULLABLE"),

            # Destination airport information
            bigquery.SchemaField("DestAirportID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("DestAirportSeqID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("DestCityMarketID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Dest", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("DestCityName", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("DestState", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("DestStateFips", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("DestStateName", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("DestWac", "INTEGER", mode="NULLABLE"),

            # Departure information
            bigquery.SchemaField("CRSDepTime", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("DepTime", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("DepDelay", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("DepDelayMinutes", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("DepDel15", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("DepartureDelayGroups", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("DepTimeBlk", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("TaxiOut", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("WheelsOff", "INTEGER", mode="NULLABLE"),

            # Arrival information
            bigquery.SchemaField("WheelsOn", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("TaxiIn", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("CRSArrTime", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("ArrTime", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("ArrDelay", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("ArrDelayMinutes", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("ArrDel15", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("ArrivalDelayGroups", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("ArrTimeBlk", "STRING", mode="NULLABLE"),

            # Flight status
            bigquery.SchemaField("Cancelled", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("CancellationCode", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Diverted", "FLOAT64", mode="NULLABLE"),

            # Flight time information
            bigquery.SchemaField("CRSElapsedTime", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("ActualElapsedTime", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("AirTime", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Flights", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Distance", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("DistanceGroup", "INTEGER", mode="NULLABLE"),

            # Delay causes
            bigquery.SchemaField("CarrierDelay", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("WeatherDelay", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("NASDelay", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("SecurityDelay", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("LateAircraftDelay", "FLOAT64", mode="NULLABLE"),

            # Gate return information
            bigquery.SchemaField("FirstDepTime", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("TotalAddGTime", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("LongestAddGTime", "FLOAT64", mode="NULLABLE"),

            # Diverted flight information
            bigquery.SchemaField("DivAirportLandings", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("DivReachedDest", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("DivActualElapsedTime", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("DivArrDelay", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("DivDistance", "FLOAT64", mode="NULLABLE"),

            # Diversion 1
            bigquery.SchemaField("Div1Airport", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Div1AirportID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div1AirportSeqID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div1WheelsOn", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div1TotalGTime", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Div1LongestGTime", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Div1WheelsOff", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div1TailNum", "STRING", mode="NULLABLE"),

            # Diversion 2
            bigquery.SchemaField("Div2Airport", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Div2AirportID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div2AirportSeqID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div2WheelsOn", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div2TotalGTime", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Div2LongestGTime", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Div2WheelsOff", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div2TailNum", "STRING", mode="NULLABLE"),

            # Diversion 3
            bigquery.SchemaField("Div3Airport", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Div3AirportID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div3AirportSeqID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div3WheelsOn", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div3TotalGTime", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Div3LongestGTime", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Div3WheelsOff", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div3TailNum", "STRING", mode="NULLABLE"),

            # Diversion 4
            bigquery.SchemaField("Div4Airport", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Div4AirportID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div4AirportSeqID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div4WheelsOn", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div4TotalGTime", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Div4LongestGTime", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Div4WheelsOff", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div4TailNum", "STRING", mode="NULLABLE"),

            # Diversion 5
            bigquery.SchemaField("Div5Airport", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Div5AirportID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div5AirportSeqID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div5WheelsOn", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div5TotalGTime", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Div5LongestGTime", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Div5WheelsOff", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("Div5TailNum", "STRING", mode="NULLABLE"),
        ]

    def get_flights_processed_schema(self) -> List[bigquery.SchemaField]:
        """
        Define schema for processed flights data (from Kafka stream)
        Simplified schema for real-time data

        Returns:
            List of BigQuery schema fields
        """
        return [
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("flight_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("airline", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("flight_number", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("origin", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("destination", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("scheduled_departure", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("actual_departure", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("scheduled_arrival", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("actual_arrival", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("aircraft_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("latitude", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("longitude", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("altitude", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("speed", "FLOAT64", mode="NULLABLE"),
        ]

    def create_table(
        self,
        table_id: str,
        schema: List[bigquery.SchemaField],
        partition_field: str = None,
        clustering_fields: List[str] = None,
        description: str = ""
    ) -> None:
        """
        Create BigQuery table with specified schema

        Args:
            table_id: Table ID
            schema: Table schema
            partition_field: Field to partition by (must be DATE or TIMESTAMP)
            clustering_fields: Fields to cluster by
            description: Table description
        """
        try:
            table_ref = f"{self.dataset_ref}.{table_id}"
            table = bigquery.Table(table_ref, schema=schema)
            table.description = description

            # Add partitioning if specified
            if partition_field:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_field
                )
                logger.info(f"  Partitioning by: {partition_field}")

            # Add clustering if specified
            if clustering_fields:
                table.clustering_fields = clustering_fields
                logger.info(f"  Clustering by: {clustering_fields}")

            # Create table
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"✅ Table {table_ref} created or already exists")

        except Exception as e:
            logger.error(f"❌ Error creating table {table_id}: {e}")
            raise

    def setup_all_tables(self) -> None:
        """Create all required tables for AERO project"""
        logger.info("\n" + "="*60)
        logger.info("Creating BigQuery Tables")
        logger.info("="*60 + "\n")

        # 1. Raw flights data table (from GCS CSV files)
        logger.info("Creating flights_raw table...")
        self.create_table(
            table_id="flights_raw",
            schema=self.get_flights_raw_schema(),
            partition_field="FlightDate",
            clustering_fields=["Reporting_Airline", "Origin", "Dest"],
            description="Raw historical flight data from 1987-2024 (CSV files from GCS)"
        )

        # 2. Processed flights table (from Kafka stream)
        logger.info("\nCreating flights_processed table...")
        self.create_table(
            table_id="flights_processed",
            schema=self.get_flights_processed_schema(),
            partition_field="timestamp",
            clustering_fields=["airline", "origin", "destination"],
            description="Processed real-time flight data from Kafka stream"
        )

        # 3. Flights analytics table (aggregated data)
        logger.info("\nCreating flights_analytics table...")
        analytics_schema = [
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("airline", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("origin", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("destination", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("total_flights", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("cancelled_flights", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("diverted_flights", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("avg_departure_delay", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("avg_arrival_delay", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("total_distance", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("avg_air_time", "FLOAT64", mode="NULLABLE"),
        ]
        self.create_table(
            table_id="flights_analytics",
            schema=analytics_schema,
            partition_field="date",
            clustering_fields=["airline", "origin", "destination"],
            description="Aggregated flight analytics data"
        )

    def load_data_from_gcs(
        self,
        table_id: str,
        source_uris: List[str],
        write_disposition: str = "WRITE_TRUNCATE"
    ) -> None:
        """
        Load data from GCS into BigQuery table

        Args:
            table_id: Target table ID
            source_uris: List of GCS URIs (e.g., gs://bucket/path/*.csv)
            write_disposition: How to write data (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY)
        """
        try:
            table_ref = f"{self.dataset_ref}.{table_id}"

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=False,
                write_disposition=write_disposition,
            )

            logger.info(f"\nLoading data from GCS to {table_ref}...")
            logger.info(f"Source URIs: {source_uris}")

            load_job = self.client.load_table_from_uri(
                source_uris,
                table_ref,
                job_config=job_config
            )

            load_job.result()  # Wait for job to complete

            destination_table = self.client.get_table(table_ref)
            logger.info(f"✅ Loaded {destination_table.num_rows} rows into {table_ref}")

        except Exception as e:
            logger.error(f"❌ Error loading data from GCS: {e}")
            raise

    def get_table_info(self, table_id: str) -> Dict:
        """
        Get information about a table

        Args:
            table_id: Table ID

        Returns:
            Dictionary with table information
        """
        try:
            table_ref = f"{self.dataset_ref}.{table_id}"
            table = self.client.get_table(table_ref)

            return {
                "table_id": table.table_id,
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "created": table.created,
                "modified": table.modified,
                "partitioning": str(table.time_partitioning) if table.time_partitioning else None,
                "clustering": table.clustering_fields if table.clustering_fields else None,
            }
        except exceptions.NotFound:
            return {"error": f"Table {table_id} not found"}
        except Exception as e:
            return {"error": str(e)}

    def print_dataset_summary(self) -> None:
        """Print summary of all tables in the dataset"""
        logger.info("\n" + "="*60)
        logger.info("Dataset Summary")
        logger.info("="*60 + "\n")

        tables = self.client.list_tables(self.dataset_ref)

        for table in tables:
            info = self.get_table_info(table.table_id)
            logger.info(f"Table: {info.get('table_id', 'Unknown')}")
            logger.info(f"  Rows: {info.get('num_rows', 0):,}")
            logger.info(f"  Size: {info.get('num_bytes', 0) / (1024**3):.2f} GB")
            logger.info(f"  Partitioning: {info.get('partitioning', 'None')}")
            logger.info(f"  Clustering: {info.get('clustering', 'None')}")
            logger.info("")


def main():
    """Main execution function"""
    # Get project ID from environment or use default
    project_id = os.environ.get("GCP_PROJECT_ID", "double-arbor-475907-s5")
    dataset_id = "aero_dataset"

    logger.info("="*60)
    logger.info("BigQuery Setup for AERO Project")
    logger.info("="*60)
    logger.info(f"Project ID: {project_id}")
    logger.info(f"Dataset ID: {dataset_id}")
    logger.info("Location: asia-east2")
    logger.info("="*60 + "\n")

    # Initialize setup
    bq_setup = BigQuerySetup(
        project_id=project_id,
        dataset_id=dataset_id,
        location="asia-east2"
    )

    # Create dataset
    logger.info("Step 1: Creating dataset...")
    bq_setup.create_dataset()

    # Create tables
    logger.info("\nStep 2: Creating tables...")
    bq_setup.setup_all_tables()

    # Print summary
    logger.info("\nStep 3: Dataset summary...")
    bq_setup.print_dataset_summary()

    logger.info("\n" + "="*60)
    logger.info("✅ BigQuery setup completed successfully!")
    logger.info("="*60)
    logger.info("\nNext steps:")
    logger.info("1. Load historical data from GCS:")
    logger.info("   python setup/load_historical_data.py")
    logger.info("2. Start Kafka producer/consumer for real-time data")
    logger.info("3. Run analytics queries on BigQuery")
    logger.info("="*60 + "\n")


if __name__ == "__main__":
    main()
