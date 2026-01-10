from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ReadFilesFromGCS") \
    .getOrCreate()

# Define bucket and file paths
bucket_name = "bk9999airline"
folder_path = f"gs://{bucket_name}/"
csv_file_path = f"{folder_path}airline_full.csv"
parquet_file_path = f"{folder_path}cleaned_airline_data"

# Read the CSV file
csv_df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Read the Parquet file
parquet_df = spark.read.parquet(parquet_file_path)

# Count rows in each file
csv_row_count = csv_df.count()
parquet_row_count = parquet_df.count()

# Print row counts
print(f"Number of rows in airline_full.csv: {csv_row_count}")
print(f"Number of rows in cleaned_airline_data: {parquet_row_count}")

# Stop the Spark session
spark.stop()