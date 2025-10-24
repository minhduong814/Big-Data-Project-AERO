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

# Verify files exist first
print("Checking bucket contents...")
storage_client = storage.Client()
bucket = storage_client.bucket('aero_data')
blobs = list(bucket.list_blobs())

csv_files = [blob.name for blob in blobs if blob.name.endswith('.csv')]
print(f"Found {len(csv_files)} CSV files:")
for f in csv_files[:5]:  # Print first 5
    print(f"  - gs://aero_data/{f}")

if not csv_files:
    print("ERROR: No CSV files found in bucket!")
    exit(1)

# Initialize Spark
jar_path = os.path.abspath("spark/jars/gcs-connector-hadoop3-latest.jar")
key_path = os.path.abspath("key/double-arbor-475907-s5-75ee7fda0a13.json")

print(f"\nJAR exists: {os.path.exists(jar_path)}")
print(f"Key exists: {os.path.exists(key_path)}")

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

# Determine the correct input path
if csv_files:
    # Check if files are in root or subfolder
    first_file = csv_files[0]
    if '/' in first_file:
        # Files are in a subfolder
        folder = first_file.rsplit('/', 1)[0]
        input_path = f"gs://aero_data/{folder}/*.csv"
    else:
        # Files are in root
        input_path = "gs://aero_data/*.csv"
    
    print(f"\nReading from: {input_path}")
    
    try:
        df = spark.read.option("header", "true").csv(input_path)
        print(f"Successfully read {df.count()} rows")
        
        output_path = "gs://aero_data/full_data.parquet"
        df.write.mode("overwrite").parquet(output_path)
        print(f"Written to: {output_path}")
        
    except Exception as e:
        print(f"Error reading files: {e}")
    finally:
        spark.stop()