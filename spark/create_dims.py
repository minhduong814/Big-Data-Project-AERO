from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import argparse

# Initialize Spark session
spark = SparkSession.builder.appName('create_dims').getOrCreate()
spark.conf.set('temporaryGcsBucket', 'dataproc-staging-asia-east2-190992537547-y9bsgfwa')



class DataProcessor:
    def __init__(self, spark):
        self.spark = spark

    def load_data(self, file_path="gs://bk9999airline/cleaned_airline_data"):
        """
        Load combined data from the given GCS file path.
        """
        return self.spark.read.parquet(file_path)

    def create_fact_flights(self, flights_df):
        """
        Create the Flights fact table.
        """
        return flights_df.select(
            "FlightDate",
            "IATA_CODE_Reporting_Airline",
            "Flight_Number_Reporting_Airline",
            "Tail_Number",
            "OriginAirportID",
            "DestAirportID",
            "CRSDepTime",
            "DepTime",
            "DepDelay",
            "CRSArrTime",
            "ArrTime",
            "ArrDelay",
            "Cancelled",
            "Diverted"
        )

    def create_dim_time(self, flights_df):
        """
        Create the Time dimension table with ordered IDs.
        """
        # Select the required columns and remove duplicates
        time_table = flights_df.select(
            F.col("FlightDate").alias("FlightDate"),
            F.year("FlightDate").alias("Year"),
            F.quarter("FlightDate").alias("Quarter"),
            F.month("FlightDate").alias("Month"),
            F.dayofmonth("FlightDate").alias("DayOfMonth"),
            F.dayofweek("FlightDate").alias("DayOfWeek")
        ).dropDuplicates()

        # Sort by FlightDate (ascending) to ensure order from past to present
        time_table_sorted = time_table.orderBy("FlightDate")

        # Add a sequential time_id based on the sorted FlightDate
        time_table_with_id = time_table_sorted.withColumn(
            "time_id", F.row_number().over(Window.orderBy("FlightDate"))
        )

        return time_table_with_id

    def create_dim_airports(self, flights_df):
        """
        Create the Airports dimension table with ordered IDs.
        """
        # Select origin and destination airport data
        origin_airports = flights_df.select(
            F.col("OriginAirportID").alias("AirportID"),
            F.col("Origin").alias("AirportCode"),
            F.col("OriginCityName").alias("CityName"),
            F.col("OriginState").alias("State"),
            F.col("OriginStateFips").alias("StateFips"),
            F.col("OriginStateName").alias("StateName")
        ).dropDuplicates()

        dest_airports = flights_df.select(
            F.col("DestAirportID").alias("AirportID"),
            F.col("Dest").alias("AirportCode"),
            F.col("DestCityName").alias("CityName"),
            F.col("DestState").alias("State"),
            F.col("DestStateFips").alias("StateFips"),
            F.col("DestStateName").alias("StateName")
        ).dropDuplicates()

        # Combine both origin and destination airports
        airports_table = origin_airports.union(dest_airports).dropDuplicates()

        # Add a sequential airport_id based on AirportID
        airports_table_sorted = airports_table.orderBy("AirportID")
        airports_table_with_id = airports_table_sorted.withColumn(
            "airport_id", F.row_number().over(Window.orderBy("AirportID"))
        )

        return airports_table_with_id

    

    def create_dim_airlines(self, airlines_df):
        """
        Create the Airlines dimension table with ordered IDs.
        """
        # Select the required columns and remove duplicates
        airlines_table = airlines_df.select(
            F.col("IATA_CODE_Reporting_Airline").alias("IATA_CODE_Reporting_Airline"),
            F.col("Reporting_Airline").alias("Reporting_Airline"),
            F.col("DOT_ID_Reporting_Airline").alias("DOT_ID_Reporting_Airline")
        ).dropDuplicates()

        # Sort by IATA_CODE_Reporting_Airline to ensure order
        airlines_table_sorted = airlines_table.orderBy("IATA_CODE_Reporting_Airline")

        # Add a sequential airline_id based on the sorted IATA_CODE_Reporting_Airline
        airlines_table_with_id = airlines_table_sorted.withColumn(
            "airline_id", F.row_number().over(Window.orderBy("IATA_CODE_Reporting_Airline"))
        )

        return airlines_table_with_id

    def create_dim_delays(self, flights_df):
        """
        Create the FlightDelays table with a foreign key reference from the fact table.
        """
        # Select the required columns and drop duplicates
        dim_delays = flights_df.select(
            "FlightDate",
            "IATA_CODE_Reporting_Airline",
            "Flight_Number_Reporting_Airline",
            "Origin",
            "Dest",
            "DepDelay",
            "ArrDelay",
            "CarrierDelay",
            "WeatherDelay",
            "NASDelay",
            "SecurityDelay",
            "LateAircraftDelay"
        ).dropDuplicates()

        dim_delays = dim_delays.filter(F.col("Cancelled") == 0).filter(F.col("Diverted") == 0)

        dim_delays_with_key = dim_delays.withColumn(
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

        dim_delays_remove = dim_delays_with_key.select(
            "fact_key",
            "DepDelay",
            "ArrDelay",
            "CarrierDelay",
            "WeatherDelay",
            "NASDelay",
            "SecurityDelay",
            "LateAircraftDelay"
        ).dropDuplicates()

        # Add a sequential ID as a foreign key for referencing in the fact table
        dim_delays_with_id = dim_delays_remove.withColumn(
            "delay_id", F.row_number().over(Window.orderBy("fact_key"))
        )

        return dim_delays_with_id


    def create_dim_cancellations(self, flights_df):
        """
        Create the FlightCancellations table with a foreign key reference from the fact table.
        """
        cancelled_flights = flights_df.filter(F.col("Cancelled") == 1)

        # Select the required columns and drop duplicates
        dim_cancellations = cancelled_flights.select(
            "FlightDate",
            "IATA_CODE_Reporting_Airline",
            "Flight_Number_Reporting_Airline",
            "Origin",
            "Dest",
            "CancellationCode"
        ).dropDuplicates()

        # Create a new 'fact_key' column in the format {FlightDate}_{IATA_Code}{Flight_Number}
        dim_cancellations_with_key = dim_cancellations.withColumn(
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

        dim_cancellations_remove = dim_cancellations_with_key.select(
            "fact_key",
            "CancellationCode"
        ).dropDuplicates()

        # Add a sequential ID as a foreign key for referencing in the fact table
        dim_cancellations_with_id = dim_cancellations_remove.withColumn(
            "cancellation_id", F.row_number().over(Window.orderBy("fact_key"))
        )

        return dim_cancellations_with_id

    def save_table(self, df, output_path):
        """
        Save a DataFrame to a specified output path in parquet format.
        """
        df.write.format("parquet").mode("overwrite").save(output_path)


parser = argparse.ArgumentParser(description='Create and save dimension tables.')
parser.add_argument('--dims', type=str, nargs='+', choices=[
    'time', 'airport', 'airline', 'delay', 'cancel', 'all'
], default=['all'], help='Specify which dimension tables to create (default: all).')

args = parser.parse_args()

# Create an instance of DataProcessor
data_processor = DataProcessor(spark)

# Load the data
flights_df = data_processor.load_data()

# Dictionary to map dimension table creation functions
dim_functions = {
    'time': data_processor.create_dim_time,
    'airport': data_processor.create_dim_airports,
    'airline': data_processor.create_dim_airlines,
    'delay': data_processor.create_dim_delays,
    'cancel': data_processor.create_dim_cancellations
}

# Define GCS bucket and project variables
PROJECT_ID = 'totemic-program-442307-i9'
BUCKET_NAME = 'gs://bk9999airline/dims/'  # GCS path for saving

# Process and save selected dimensions
selected_dims = args.dims if 'all' not in args.dims else dim_functions.keys()

for dim in selected_dims:
    dim_function = dim_functions.get(dim)
    if dim_function:
        print(f"Processing dimension: {dim}")
        dim_df = dim_function(flights_df)
        
        # Define GCS path
        sanitized_name = f"dim-{dim}"
        gcs_path = f'{BUCKET_NAME}{sanitized_name}.parquet'
        
        # Save to GCS
        data_processor.save_table(dim_df, gcs_path)
        print(f"Uploaded {dim} dimension to {gcs_path}.")