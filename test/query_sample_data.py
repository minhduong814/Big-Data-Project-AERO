#!/usr/bin/env python3
"""
Query Sample Data from BigQuery
Extracts sample flight data for testing and saves to test folder
"""

import os
import json
import logging
from typing import List, Dict, Optional
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SampleDataQuerier:
    """Query and extract sample data from BigQuery for testing"""
    
    def __init__(
        self,
        project_id: str,
        dataset_id: str = "aero_dataset",
        output_dir: str = "test"
    ):
        """
        Initialize the sample data querier
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            output_dir: Directory to save sample data
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.output_dir = output_dir
        
        # Initialize client
        self.client = bigquery.Client(project=project_id)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"Initialized querier for {project_id}.{dataset_id}")
        logger.info(f"Output directory: {output_dir}")
    
    def query_to_dataframe(self, query: str) -> pd.DataFrame:
        """
        Execute a query and return results as DataFrame
        
        Args:
            query: SQL query string
            
        Returns:
            pandas DataFrame with query results
        """
        logger.info("Executing query...")
        logger.info(f"Query: {query[:200]}...")
        
        df = self.client.query(query).to_dataframe()
        logger.info(f"✅ Query returned {len(df)} rows")
        
        return df
    
    def get_random_sample(
        self,
        table_id: str,
        sample_size: int = 1000,
        year: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get random sample from a table
        
        Args:
            table_id: Table name
            sample_size: Number of rows to sample
            year: Optional year filter
            
        Returns:
            pandas DataFrame with sample data
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        
        # Build query
        if year:
            query = f"""
            SELECT *
            FROM `{table_ref}`
            WHERE Year = {year}
            ORDER BY RAND()
            LIMIT {sample_size}
            """
        else:
            query = f"""
            SELECT *
            FROM `{table_ref}`
            TABLESAMPLE SYSTEM (1 PERCENT)
            LIMIT {sample_size}
            """
        
        return self.query_to_dataframe(query)
    
    def get_recent_data(
        self,
        table_id: str,
        days: int = 30,
        limit: int = 1000
    ) -> pd.DataFrame:
        """
        Get recent data from a table
        
        Args:
            table_id: Table name
            days: Number of recent days
            limit: Maximum number of rows
            
        Returns:
            pandas DataFrame with recent data
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        
        query = f"""
        SELECT *
        FROM `{table_ref}`
        WHERE FlightDate >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        ORDER BY FlightDate DESC
        LIMIT {limit}
        """
        
        return self.query_to_dataframe(query)
    
    def get_specific_routes(
        self,
        table_id: str,
        origins: List[str],
        destinations: List[str],
        limit: int = 500
    ) -> pd.DataFrame:
        """
        Get data for specific routes
        
        Args:
            table_id: Table name
            origins: List of origin airport codes
            destinations: List of destination airport codes
            limit: Maximum number of rows
            
        Returns:
            pandas DataFrame with route data
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        
        origins_str = "', '".join(origins)
        dests_str = "', '".join(destinations)
        
        query = f"""
        SELECT *
        FROM `{table_ref}`
        WHERE Origin IN ('{origins_str}')
          AND Dest IN ('{dests_str}')
        ORDER BY FlightDate DESC
        LIMIT {limit}
        """
        
        return self.query_to_dataframe(query)
    
    def get_airlines_sample(
        self,
        table_id: str,
        airlines: List[str],
        limit_per_airline: int = 200
    ) -> pd.DataFrame:
        """
        Get sample data for specific airlines
        
        Args:
            table_id: Table name
            airlines: List of airline IATA codes
            limit_per_airline: Rows per airline
            
        Returns:
            pandas DataFrame with airline data
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        
        airlines_str = "', '".join(airlines)
        
        query = f"""
        WITH RankedFlights AS (
          SELECT *,
                 ROW_NUMBER() OVER (PARTITION BY Reporting_Airline ORDER BY RAND()) as rn
          FROM `{table_ref}`
          WHERE Reporting_Airline IN ('{airlines_str}')
        )
        SELECT * EXCEPT(rn)
        FROM RankedFlights
        WHERE rn <= {limit_per_airline}
        ORDER BY Reporting_Airline, FlightDate DESC
        """
        
        return self.query_to_dataframe(query)
    
    def save_to_csv(self, df: pd.DataFrame, filename: str) -> str:
        """
        Save DataFrame to CSV file
        
        Args:
            df: pandas DataFrame
            filename: Output filename
            
        Returns:
            Full path to saved file
        """
        filepath = os.path.join(self.output_dir, filename)
        df.to_csv(filepath, index=False)
        logger.info(f"✅ Saved {len(df)} rows to {filepath}")
        
        return filepath
    
    def save_to_json(self, df: pd.DataFrame, filename: str) -> str:
        """
        Save DataFrame to JSON file
        
        Args:
            df: pandas DataFrame
            filename: Output filename
            
        Returns:
            Full path to saved file
        """
        filepath = os.path.join(self.output_dir, filename)
        df.to_json(filepath, orient='records', date_format='iso', indent=2)
        logger.info(f"✅ Saved {len(df)} rows to {filepath}")
        
        return filepath
    
    def save_to_parquet(self, df: pd.DataFrame, filename: str) -> str:
        """
        Save DataFrame to Parquet file
        
        Args:
            df: pandas DataFrame
            filename: Output filename
            
        Returns:
            Full path to saved file
        """
        filepath = os.path.join(self.output_dir, filename)
        df.to_parquet(filepath, index=False)
        logger.info(f"✅ Saved {len(df)} rows to {filepath}")
        
        return filepath
    
    def get_table_summary(self, table_id: str) -> Dict:
        """
        Get summary statistics for a table
        
        Args:
            table_id: Table name
            
        Returns:
            Dictionary with summary statistics
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        
        query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT Reporting_Airline) as unique_airlines,
            COUNT(DISTINCT Origin) as unique_origins,
            COUNT(DISTINCT Dest) as unique_destinations,
            MIN(FlightDate) as earliest_date,
            MAX(FlightDate) as latest_date,
            AVG(DepDelay) as avg_dep_delay,
            AVG(ArrDelay) as avg_arr_delay,
            SUM(CASE WHEN Cancelled > 0 THEN 1 ELSE 0 END) as cancelled_flights,
            SUM(CASE WHEN Diverted > 0 THEN 1 ELSE 0 END) as diverted_flights
        FROM `{table_ref}`
        """
        
        df = self.query_to_dataframe(query)
        return df.iloc[0].to_dict()
    
    def print_summary(self, summary: Dict) -> None:
        """
        Print table summary
        
        Args:
            summary: Summary statistics dictionary
        """
        logger.info("\n" + "="*60)
        logger.info("TABLE SUMMARY")
        logger.info("="*60)
        logger.info(f"Total Rows: {summary.get('total_rows', 0):,}")
        logger.info(f"Unique Airlines: {summary.get('unique_airlines', 0)}")
        logger.info(f"Unique Origins: {summary.get('unique_origins', 0)}")
        logger.info(f"Unique Destinations: {summary.get('unique_destinations', 0)}")
        logger.info(f"Date Range: {summary.get('earliest_date')} to {summary.get('latest_date')}")
        logger.info(f"Avg Departure Delay: {summary.get('avg_dep_delay', 0):.2f} minutes")
        logger.info(f"Avg Arrival Delay: {summary.get('avg_arr_delay', 0):.2f} minutes")
        logger.info(f"Cancelled Flights: {summary.get('cancelled_flights', 0):,}")
        logger.info(f"Diverted Flights: {summary.get('diverted_flights', 0):,}")
        logger.info("="*60 + "\n")


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Query sample data from BigQuery')
    parser.add_argument('--project-id', default=os.environ.get("GCP_PROJECT_ID", "double-arbor-475907-s5"),
                        help='GCP project ID')
    parser.add_argument('--dataset-id', default='aero_dataset',
                        help='BigQuery dataset ID')
    parser.add_argument('--table-id', default='flights_raw',
                        help='BigQuery table ID')
    parser.add_argument('--output-dir', default='test',
                        help='Output directory for sample data')
    parser.add_argument('--sample-size', type=int, default=1000,
                        help='Number of rows to sample')
    parser.add_argument('--year', type=int, default=None,
                        help='Filter by specific year')
    parser.add_argument('--format', choices=['csv', 'json', 'parquet', 'all'], default='all',
                        help='Output format')
    parser.add_argument('--sample-type', choices=['random', 'recent', 'routes', 'airlines'], 
                        default='random',
                        help='Type of sample to extract')
    
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("Sample Data Querier for AERO Project")
    logger.info("="*60)
    logger.info(f"Project: {args.project_id}")
    logger.info(f"Dataset: {args.dataset_id}")
    logger.info(f"Table: {args.table_id}")
    logger.info(f"Sample Type: {args.sample_type}")
    logger.info(f"Sample Size: {args.sample_size}")
    if args.year:
        logger.info(f"Year Filter: {args.year}")
    logger.info("="*60 + "\n")
    
    # Initialize querier
    querier = SampleDataQuerier(
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        output_dir=args.output_dir
    )
    
    # Get table summary first
    try:
        logger.info("Getting table summary...")
        summary = querier.get_table_summary(args.table_id)
        querier.print_summary(summary)
    except Exception as e:
        logger.warning(f"Could not get table summary: {e}")
    
    # Extract sample data based on type
    try:
        if args.sample_type == 'random':
            logger.info("Extracting random sample...")
            df = querier.get_random_sample(
                table_id=args.table_id,
                sample_size=args.sample_size,
                year=args.year
            )
            base_filename = f"sample_random_{args.sample_size}"
            if args.year:
                base_filename += f"_{args.year}"
        
        elif args.sample_type == 'recent':
            logger.info("Extracting recent data...")
            df = querier.get_recent_data(
                table_id=args.table_id,
                days=30,
                limit=args.sample_size
            )
            base_filename = f"sample_recent_30days_{args.sample_size}"
        
        elif args.sample_type == 'routes':
            logger.info("Extracting popular routes...")
            # Major US airports
            major_airports = ['ATL', 'ORD', 'LAX', 'DFW', 'JFK', 'DEN', 'SFO', 'LAS', 'SEA', 'MCO']
            df = querier.get_specific_routes(
                table_id=args.table_id,
                origins=major_airports,
                destinations=major_airports,
                limit=args.sample_size
            )
            base_filename = f"sample_major_routes_{args.sample_size}"
        
        elif args.sample_type == 'airlines':
            logger.info("Extracting airline samples...")
            # Major US airlines
            major_airlines = ['AA', 'DL', 'UA', 'WN', 'B6', 'AS', 'NK', 'F9']
            df = querier.get_airlines_sample(
                table_id=args.table_id,
                airlines=major_airlines,
                limit_per_airline=args.sample_size // len(major_airlines)
            )
            base_filename = f"sample_major_airlines_{len(df)}"
        
        # Save in requested format(s)
        if args.format in ['csv', 'all']:
            querier.save_to_csv(df, f"{base_filename}.csv")
        
        if args.format in ['json', 'all']:
            querier.save_to_json(df, f"{base_filename}.json")
        
        if args.format in ['parquet', 'all']:
            querier.save_to_parquet(df, f"{base_filename}.parquet")
        
        # Print sample statistics
        logger.info("\n" + "="*60)
        logger.info("SAMPLE DATA STATISTICS")
        logger.info("="*60)
        logger.info(f"Total Rows: {len(df):,}")
        logger.info(f"Columns: {len(df.columns)}")
        logger.info(f"Memory Usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        if 'Reporting_Airline' in df.columns:
            logger.info(f"Unique Airlines: {df['Reporting_Airline'].nunique()}")
        if 'Origin' in df.columns:
            logger.info(f"Unique Origins: {df['Origin'].nunique()}")
        if 'Dest' in df.columns:
            logger.info(f"Unique Destinations: {df['Dest'].nunique()}")
        if 'FlightDate' in df.columns:
            logger.info(f"Date Range: {df['FlightDate'].min()} to {df['FlightDate'].max()}")
        
        logger.info("="*60 + "\n")
        
        # Show first few rows
        logger.info("First 5 rows preview:")
        print(df.head().to_string())
        
        logger.info("\n✅ Sample data extraction completed successfully!")
        logger.info(f"\nOutput files saved to: {args.output_dir}/")
        
    except Exception as e:
        logger.error(f"❌ Error extracting sample data: {e}")
        raise


if __name__ == "__main__":
    main()
