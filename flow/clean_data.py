from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, upper

def main():
    # Initialize Spark session with optimized memory settings
    spark = SparkSession.builder \
        .appName("FlightDataCleaning") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memoryOverhead", "1g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    print("=" * 50)
    print("Starting Flight Data Cleaning Job")
    print("=" * 50)
    
    # Read raw data from GCS
    input_path = "gs://aero_data/full_data_new.parquet"
    output_path = "gs://aero_data/cleaned_data/"
    
    print(f"Reading data from: {input_path}")
    df = spark.read.parquet(input_path)
    
    # Repartition to avoid memory issues with large dataset
    print("Repartitioning data for better memory management...")
    df = df.repartition(200)
    
    print(f"Original row count: {df.count()}")
    
    # Data cleaning operations - avoid multiple count() calls
    print("Dropping duplicates...")
    df_cleaned = df.dropDuplicates()
    
    print("Dropping rows with null values in critical columns...")
    # Check which columns actually exist
    critical_columns = []
    for col_name in ["FlightDate", "Origin", "Dest", "Reporting_Airline"]:
        if col_name in df.columns:
            critical_columns.append(col_name)
    
    if critical_columns:
        df_cleaned = df_cleaned.dropna(subset=critical_columns)
    
    # Trim whitespace from string columns
    print("Trimming whitespace from string columns...")
    string_columns = [field.name for field in df_cleaned.schema.fields 
                     if str(field.dataType) == "StringType"]
    
    for col_name in string_columns:
        df_cleaned = df_cleaned.withColumn(col_name, trim(col(col_name)))
    
    print(f"Cleaned row count: {df_cleaned.count()}")
    
    # Write cleaned data to GCS with partitioning for better query performance
    print(f"Writing cleaned data to: {output_path}")
    df_cleaned.write \
        .mode("overwrite") \
        .partitionBy("Year", "Month") \
        .parquet(output_path)
    
    print("=" * 50)
    print("Flight Data Cleaning Job Completed!")
    print("=" * 50)
    
    spark.stop()

if __name__ == "__main__":
    main()