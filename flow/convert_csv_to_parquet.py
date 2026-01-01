from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("CreateParquet") \
        .getOrCreate()
    
    print("Reading CSV files from GCS...")
    
    # Read all CSV files from the bucket
    input_path = "gs://aero_data/*.csv"
    output_path = "gs://aero_data/parquet"
    
    try:
        # Read CSV files
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(input_path)
        
        print(f"Read {df.count()} rows")
        print("Schema:")
        df.printSchema()
        
        # Write to parquet
        print(f"Writing to {output_path}...")
        df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .parquet(output_path)
        
        print("Parquet file created successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()