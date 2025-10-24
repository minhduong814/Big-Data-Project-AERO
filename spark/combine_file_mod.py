from pyspark.sql import SparkSession
from google.cloud import storage
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path=".env.local")

# Set credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.abspath(
    'key/double-arbor-475907-s5-75ee7fda0a13.json'
)

# Get explicit list of CSV files from GCS
print("Getting list of CSV files from GCS...")
storage_client = storage.Client()
bucket = storage_client.bucket('aero_data')

# Get full GCS paths for all CSV files
csv_file_paths = []
for blob in bucket.list_blobs():
    if blob.name.endswith('.csv'):
        csv_file_paths.append(f"gs://aero_data/{blob.name}")

print(f"Found {len(csv_file_paths)} CSV files")
print("First 5 files:")
for f in csv_file_paths[:5]:
    print(f"  {f}")

if not csv_file_paths:
    print("ERROR: No CSV files found!")
    exit(1)

# Initialize Spark
key_path = os.path.abspath("key/double-arbor-475907-s5-75ee7fda0a13.json")

spark = SparkSession.builder \
    .appName("CombineFiles") \
    .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_path) \
    .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", key_path) \
    .config("spark.hadoop.fs.gs.project.id", "double-arbor-475907-s5") \
    .getOrCreate()

print(f"\nSpark session created")
# print(f"Hadoop version: {spark.sparkContext._jsc.sc().hadoopVersion()}")

# Read all CSV files using explicit list (no wildcards)
print(f"\nReading {len(csv_file_paths)} CSV files...")

try:
    # Pass the list of file paths directly
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(csv_file_paths)
    
    row_count = df.count()
    print(f"Successfully read {row_count:,} rows")
    print(f"Columns: {len(df.columns)} - {df.columns[:5]}...")
    
    # Write to CSV (single file)
    output_path = "gs://aero_data/full_data.csv"
    print(f"\nWriting to: {output_path}")
    
    # Option 1: Write as a single CSV file (coalesce to 1 partition)
    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)
    
    print("✓ Success! CSV file created.")
    
    # # Write to Parquet
    # output_path = "gs://aero_data/full_data.parquet"
    # print(f"\nWriting to: {output_path}")
    
    # df.write.mode("overwrite").parquet(output_path)
    
    # print("✓ Success! Parquet file created.")
    
except Exception as e:
    print(f"\n✗ Error: {e}")
    import traceback
    traceback.print_exc()
finally:
    spark.stop()