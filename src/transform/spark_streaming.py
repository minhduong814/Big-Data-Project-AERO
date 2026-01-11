"""
Spark Streaming for AERO data transformation
Processes data from Kafka using Spark Structured Streaming
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Optional
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AeroSparkProcessor:
    """Spark processor for streaming AERO flight data"""
    
    def __init__(
        self,
        app_name: str = "AeroDataProcessor",
        config_path: Optional[str] = None
    ):
        """
        Initialize Spark session
        
        Args:
            app_name: Spark application name
            config_path: Optional path to config file
        """
        self.app_name = app_name
        self.logger = logging.getLogger(__name__)
        
        # Load config if provided
        if config_path:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                self.spark_config = config.get('spark', {})
        else:
            self.spark_config = {}
        
        # Initialize Spark session
        builder = SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.jars.packages",
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                   "org.apache.spark:spark-avro_2.12:3.5.0") \
            .config("spark.sql.streaming.checkpointLocation",
                   self.spark_config.get('checkpoint_location', '/tmp/checkpoint'))
        
        # Add additional configs
        for key, value in self.spark_config.get('configs', {}).items():
            builder = builder.config(key, value)
        
        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.logger.info(f"Spark session initialized: {self.app_name}")
    
    def get_flight_schema(self) -> StructType:
        """
        Define schema for flight data
        
        Returns:
            StructType: Spark schema
        """
        return StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("flight_id", StringType(), True),
            StructField("airline", StringType(), True),
            StructField("flight_number", StringType(), True),
            StructField("origin", StringType(), True),
            StructField("destination", StringType(), True),
            StructField("scheduled_departure", TimestampType(), True),
            StructField("actual_departure", TimestampType(), True),
            StructField("scheduled_arrival", TimestampType(), True),
            StructField("actual_arrival", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("aircraft_type", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("altitude", DoubleType(), True),
            StructField("speed", DoubleType(), True),
        ])
    
    def process_stream(
        self,
        kafka_bootstrap_servers: str,
        input_topic: str,
        output_topic: str,
        processing_time: str = "10 seconds"
    ) -> None:
        """
        Process Kafka stream with Spark Structured Streaming
        
        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            input_topic: Input Kafka topic
            output_topic: Output Kafka topic
            processing_time: Trigger interval
        """
        self.logger.info(f"Starting stream processing: {input_topic} -> {output_topic}")
        
        # Read from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", input_topic) \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Parse JSON and transform
        schema = self.get_flight_schema()
        
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset")
        ).select("data.*", "kafka_timestamp", "partition", "offset")
        
        # Apply transformations
        transformed_df = self._apply_transformations(parsed_df)
        
        # Write back to Kafka
        query = transformed_df \
            .selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("topic", output_topic) \
            .option("checkpointLocation", f"/tmp/checkpoint/{output_topic}") \
            .trigger(processingTime=processing_time) \
            .start()
        
        self.logger.info("Stream processing started")
        query.awaitTermination()
    
    def _apply_transformations(self, df):
        """
        Apply business logic transformations
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        # Add processing timestamp
        df = df.withColumn("processed_timestamp", current_timestamp())
        
        # Calculate delay in minutes
        df = df.withColumn(
            "departure_delay_minutes",
            when(
                col("actual_departure").isNotNull() & col("scheduled_departure").isNotNull(),
                (unix_timestamp("actual_departure") - unix_timestamp("scheduled_departure")) / 60
            ).otherwise(None)
        )
        
        df = df.withColumn(
            "arrival_delay_minutes",
            when(
                col("actual_arrival").isNotNull() & col("scheduled_arrival").isNotNull(),
                (unix_timestamp("actual_arrival") - unix_timestamp("scheduled_arrival")) / 60
            ).otherwise(None)
        )
        
        # Add delay category
        df = df.withColumn(
            "delay_category",
            when(col("departure_delay_minutes") <= 0, "On-Time")
            .when(col("departure_delay_minutes") <= 15, "Minor Delay")
            .when(col("departure_delay_minutes") <= 60, "Moderate Delay")
            .otherwise("Major Delay")
        )
        
        # Calculate distance (simplified - in production use proper geo calculations)
        df = df.withColumn(
            "flight_duration_minutes",
            when(
                col("actual_departure").isNotNull() & col("actual_arrival").isNotNull(),
                (unix_timestamp("actual_arrival") - unix_timestamp("actual_departure")) / 60
            ).otherwise(None)
        )
        
        # Add date partitions
        df = df.withColumn("date", to_date("timestamp"))
        df = df.withColumn("hour", hour("timestamp"))
        
        return df
    
    def process_batch_aggregations(
        self,
        kafka_bootstrap_servers: str,
        input_topic: str,
        output_path: str,
        window_duration: str = "1 hour",
        slide_duration: str = "10 minutes"
    ) -> None:
        """
        Process streaming aggregations with windowing
        
        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            input_topic: Input Kafka topic
            output_path: Output path for results
            window_duration: Window duration
            slide_duration: Slide duration
        """
        # Read from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", input_topic) \
            .load()
        
        schema = self.get_flight_schema()
        
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Perform windowed aggregations
        aggregated_df = parsed_df \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window("timestamp", window_duration, slide_duration),
                "airline",
                "origin",
                "destination"
            ) \
            .agg(
                count("*").alias("flight_count"),
                avg("departure_delay_minutes").alias("avg_departure_delay"),
                avg("arrival_delay_minutes").alias("avg_arrival_delay"),
                max("departure_delay_minutes").alias("max_departure_delay"),
                min("departure_delay_minutes").alias("min_departure_delay")
            )
        
        # Write to parquet
        query = aggregated_df \
            .writeStream \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", f"{output_path}_checkpoint") \
            .partitionBy("window") \
            .start()
        
        self.logger.info("Aggregation processing started")
        query.awaitTermination()
    
    def stop(self):
        """Stop Spark session"""
        self.logger.info("Stopping Spark session...")
        self.spark.stop()
        self.logger.info("Spark session stopped")


if __name__ == "__main__":
    processor = AeroSparkProcessor()
    
    # Example: Process stream
    processor.process_stream(
        kafka_bootstrap_servers="localhost:9092",
        input_topic="flights",
        output_topic="flights-processed"
    )
    
    processor.stop()