"""
Spark Batch processing for historical AERO data
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Optional
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AeroSparkBatchProcessor:
    """Batch processor for historical flight data analysis"""
    
    def __init__(
        self,
        app_name: str = "AeroBatchProcessor",
        config_path: Optional[str] = None
    ):
        """Initialize Spark session for batch processing"""
        self.logger = logging.getLogger(__name__)
        
        # Load config
        if config_path:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                self.spark_config = config.get('spark', {})
        else:
            self.spark_config = {}
        
        # Initialize Spark
        builder = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        for key, value in self.spark_config.get('configs', {}).items():
            builder = builder.config(key, value)
        
        self.spark = builder.getOrCreate()
        self.logger.info("Batch Spark session initialized")
    
    def process_historical_data(
        self,
        input_path: str,
        output_path: str,
        date_from: str,
        date_to: str
    ) -> None:
        """
        Process historical flight data
        
        Args:
            input_path: Input data path (parquet/csv)
            output_path: Output path
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
        """
        # Read data
        df = self.spark.read.parquet(input_path)
        
        # Filter by date range
        df = df.filter(
            (col("date") >= date_from) &
            (col("date") <= date_to)
        )
        
        # Perform analytics
        analytics_df = self._perform_analytics(df)
        
        # Write results
        analytics_df.write \
            .mode("overwrite") \
            .partitionBy("date") \
            .parquet(output_path)
        
        self.logger.info(f"Processed data written to {output_path}")
    
    def _perform_analytics(self, df):
        """Perform analytical transformations"""
        # Daily statistics by airline
        daily_stats = df.groupBy("date", "airline") \
            .agg(
                count("*").alias("total_flights"),
                avg("departure_delay_minutes").alias("avg_delay"),
                sum(when(col("departure_delay_minutes") > 15, 1).otherwise(0)).alias("delayed_flights")
            )
        
        return daily_stats
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()