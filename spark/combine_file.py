from pyspark.sql import SparkSession

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("CombineFiles") \
    .getOrCreate()

# Step 2: Set GCS bucket path (using GCS URI format)
input_path = "gs://aero_data/*.csv"  # Path to the CSV files in the GCS bucket
output_path = "gs://aero_data/full_data.parquet"  # Path to save the Parquet file in the GCS bucket

# Step 3: Read all CSV files from the GCS bucket directory into a single DataFrame
df = spark.read.option("header", "true").csv(input_path)

# Step 4: Write the combined DataFrame to a Parquet file in GCS
df.write.parquet(output_path)

# Stop the Spark session
spark.stop()