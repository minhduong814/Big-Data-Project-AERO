from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, upper

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("FlightDataCleaning") \
        .getOrCreate()
    
    print("=" * 50)
    print("Starting Flight Data Cleaning Job")
    print("=" * 50)
    
    # Read raw data from GCS
    input_path = "gs://aero_data/full_data_new.parquet"
    output_path = "gs://aero_data/cleaned_data/"
    
    print(f"Reading data from: {input_path}")
    df = spark.read.parquet(input_path)
    
    print(f"Original row count: {df.count()}")
    
    # Data cleaning operations
    df_cleaned = df \
        .dropDuplicates() \
        .dropna(subset=["flight_id", "departure_time", "arrival_time"])
    
    # Example: trim whitespace from string columns
    # df_cleaned = df_cleaned.withColumn("airline", trim(col("airline")))
    
    print(f"Cleaned row count: {df_cleaned.count()}")
    
    # Write cleaned data to GCS
    print(f"Writing cleaned data to: {output_path}")
    df_cleaned.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    print("=" * 50)
    print("Flight Data Cleaning Job Completed!")
    print("=" * 50)
    
    spark.stop()

if __name__ == "__main__":
    main()