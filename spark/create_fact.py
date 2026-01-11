import pyspark
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import broadcast

PROJECT_ID = 'totemic-program-442307-i9'
BUCKET_NAME = 'gs://bk9999airline/fact/'
BIGQUERY_NAME = 'flightdata'


class DataProcessor:
    def __init__(self, spark):
        self.spark = spark

    def load_data(self, file_path="gs://bk9999airline/cleaned_airline_data"):
        """
        Load combined data from the given GCS file path.
        """
        return self.spark.read.parquet(file_path)
    
    def read_dim_table(self):
        dim_dict = {}
        dim_time = self.spark.read.format('bigquery').option('table', f'{PROJECT_ID}.{BIGQUERY_NAME}.dim-time').load()
        dim_airport = self.spark.read.format('bigquery').option('table', f'{PROJECT_ID}.{BIGQUERY_NAME}.dim-airport').load()
        dim_airline = self.spark.read.format('bigquery').option('table', f'{PROJECT_ID}.{BIGQUERY_NAME}.dim-airline').load()
        dim_delay = self.spark.read.format('bigquery').option('table', f'{PROJECT_ID}.{BIGQUERY_NAME}.dim-delay').load()
        dim_cancel = self.spark.read.format('bigquery').option('table', f'{PROJECT_ID}.{BIGQUERY_NAME}.dim-cancel').load()

        dim_dict['dim_time'] = dim_time
        dim_dict['dim_airport'] = dim_airport
        dim_dict['dim_airline'] = dim_airline
        dim_dict['dim_delay'] = dim_delay
        dim_dict['dim_cancel'] = dim_cancel

        return dim_dict

    def create_fact_table(self, flights_df, dim_time, dim_airport, dim_airline, dim_delay, dim_cancel):
        """
        Create the fact table using Spark SQL joins based on the provided schema design.

        Parameters:
        - flights_df: Spark DataFrame representing the Flights table.
        - dim_time: Spark DataFrame representing the Time dimension.
        - dim_airport: Spark DataFrame representing the Airports dimension.
        - dim_airline: Spark DataFrame representing the Airlines dimension.
        - dim_delay: Spark DataFrame representing the FlightDelays dimension.
        - dim_cancel: Spark DataFrame representing the FlightCancellations dimension.

        Returns:
        - fact_table: Spark DataFrame representing the joined fact table.
        """

        fact_table = flights_df.withColumn(
            "fact_key", 
            F.concat(
                F.col("FlightDate"), 
                F.lit("_"), 
                F.col("IATA_CODE_Reporting_Airline"), 
                F.col("Flight_Number_Reporting_Airline"),
                F.lit("_"),
                F.col("Origin"),
                F.lit("_"),
                F.col("Dest")
            )
        )

        # Join with Time dimension on FlightDate
        fact_table = fact_table.join(dim_time, on='FlightDate', how='left')

        # Join with Airports dimension for Origin and Destination
        fact_table = fact_table \
            .join(dim_airport.withColumnRenamed("AirportID", "OriginAirportID"), on="OriginAirportID", how='left') \
            .join(dim_airport.withColumnRenamed("AirportID", "DestAirportID"), on="DestAirportID", how='left')

        # Join with Airlines dimension on IATA_CODE_Reporting_Airline
        fact_table = fact_table.join(dim_airline, on='IATA_CODE_Reporting_Airline', how='left')

        # Join with FlightDelays dimension
        fact_table = fact_table.join(
            dim_delay,
            on='fact_key',
            how='left'
        )

        # Join with FlightCancellations dimension
        fact_table = fact_table.join(
            dim_cancel,
            on='fact_key',
            how='left'
        )

        # Select relevant columns for the fact table
        fact_table = fact_table.select(
            'FlightDate',
            'IATA_CODE_Reporting_Airline',
            'Flight_Number_Reporting_Airline',
            'Tail_Number',
            'fact_key',
            'OriginAirportID',
            'Origin',
            'DestAirportID',
            'Dest',
            'CRSDepTime',
            'DepTime',
            'CRSArrTime',
            'ArrTime',
            'Distance',
            'Cancelled',
            'Diverted',
        ).dropDuplicates()

        return fact_table

    

    def create_fact_table_sort_merge(self, flights_df, dim_time, dim_airport, 
                                 dim_airline, dim_delay, dim_cancel):
        """
        Create the fact table using Spark SQL joins with broadcast optimization for smaller tables
        and sort-merge join for the large dim_delay table.

        Parameters:
        - flights_df: Spark DataFrame representing the Flights table.
        - dim_time: Spark DataFrame representing the Time dimension.
        - dim_airport: Spark DataFrame representing the Airports dimension.
        - dim_airline: Spark DataFrame representing the Airlines dimension.
        - dim_delay: Spark DataFrame representing the FlightDelays dimension (large table).
        - dim_cancel: Spark DataFrame representing the FlightCancellations dimension.

        Returns:
        - fact_table: Spark DataFrame representing the joined fact table.
        """

        # Add a composite key to the flights DataFrame for easier joins
        flights_df = flights_df.withColumn(
            "fact_key", 
            F.concat(
                F.col("FlightDate"), 
                F.lit("_"), 
                F.col("IATA_CODE_Reporting_Airline"), 
                F.col("Flight_Number_Reporting_Airline"),
                F.lit("_"),
                F.col("Origin"),
                F.lit("_"),
                F.col("Dest")
            )
        )

        # Join with the Time dimension using broadcast
        fact_table = flights_df.join(F.broadcast(dim_time), on='FlightDate', how='left')

        # Join with Airports dimension for Origin and Destination using broadcast
        fact_table = fact_table \
            .join(F.broadcast(dim_airport.withColumnRenamed("AirportID", "OriginAirportID")), on="OriginAirportID", how='left') \
            .join(F.broadcast(dim_airport.withColumnRenamed("AirportID", "DestAirportID")), on="DestAirportID", how='left')

        # Join with Airlines dimension on IATA_CODE_Reporting_Airline using broadcast
        fact_table = fact_table.join(F.broadcast(dim_airline), on='IATA_CODE_Reporting_Airline', how='left')

        # Join with FlightCancellations dimension using broadcast
        fact_table = fact_table.join(F.broadcast(dim_cancel), on='fact_key', how='left')

        # Repartition and sort dim_delay for sort-merge join
        dim_delay = dim_delay.repartitionByRange("fact_key").sortWithinPartitions("fact_key")

        # Perform sort-merge join with dim_delay
        fact_table = fact_table.join(dim_delay, on='fact_key', how='left')

        # Select relevant columns for the fact table
        fact_table = fact_table.select(
            'FlightDate',
            'IATA_CODE_Reporting_Airline',
            'Flight_Number_Reporting_Airline',
            'Tail_Number',
            'fact_key',
            'OriginAirportID',
            'Origin',
            'DestAirportID',
            'Dest',
            'CRSDepTime',
            'DepTime',
            'CRSArrTime',
            'ArrTime',
            'Distance',
            'Cancelled',
            'Diverted',
        ).dropDuplicates()

        return fact_table
    

spark = SparkSession.builder.appName('create_fact').getOrCreate()
spark.conf.set('temporaryGcsBucket', 'dataproc-staging-asia-east2-190992537547-y9bsgfwa')
data_processor = DataProcessor(spark)

combined_df = data_processor.load_data()

# Create and process dimension tables
dim_tables = data_processor.read_dim_table()
dim_time = dim_tables['dim_time']
dim_airport = dim_tables['dim_airport']
dim_airline = dim_tables['dim_airline']
dim_delay = dim_tables['dim_delay']
dim_cancel = dim_tables['dim_cancel']


fact_table = data_processor.create_fact_table(combined_df, dim_time, dim_airport, dim_airline, dim_delay, dim_cancel)

print("Writing fact table to GCS...")

gcs_path = f'{BUCKET_NAME}fact-table.parquet'
print(f"Writing fact table to GCS path: {gcs_path}")

fact_table.coalesce(1).write.parquet(gcs_path, mode='overwrite')

print(f"Uploaded dataframe to GCS path {gcs_path}.")