from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("MergeParquetFiles") \
        .getOrCreate()
    
    print("=" * 50)
    print("Starting Parquet Merge Job")
    print("=" * 50)
    
    # Input: folder containing multiple parquet files
    input_path = "gs://aero_data/full_data_new.parquet/"  # Folder with multiple parquet files
    output_path = "gs://aero_data/full_data_new.parquet"  # Single merged parquet file
    
    try:
        print(f"Reading parquet files from: {input_path}")
        
        # Read all parquet files from the folder
        df = spark.read.parquet(input_path)
        
        print(f"Total rows: {df.count()}")
        print("\nSchema:")
        df.printSchema()
        
        # Write to a single parquet file (or small number of partitions)
        print(f"\nWriting merged parquet to: {output_path}")
        
        # Option 1: Single file (use coalesce(1))
        df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .parquet(output_path)
        
        # Option 2: Multiple files but fewer partitions (use repartition)
        # df.repartition(4) \
        #     .write \
        #     .mode("overwrite") \
        #     .parquet(output_path)
        
        print("=" * 50)
        print("Parquet merge completed successfully!")
        print("=" * 50)
        
    except Exception as e:
        print(f"Error occurred: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()