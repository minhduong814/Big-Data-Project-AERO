from pyspark.sql import SparkSession
import argparse
import os
import logging


def main(input_path: str, output_path: str, coalesce: int = 0, partition_by: str | None = None):
    logger = logging.getLogger("flow.convert_csv_to_parquet")
    logger.info("Starting convert_csv_to_parquet; input=%s output=%s coalesce=%s partition_by=%s",
                input_path, output_path, coalesce, partition_by)

    spark = SparkSession.builder.appName("CreateParquet").getOrCreate()

    logger.info("Reading CSV files from %s", input_path)

    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

        try:
            row_count = df.count()
            logger.info("Read %d rows", row_count)
        except Exception:
            logger.warning("Counting rows failed or is expensive; skipping count")

        logger.debug("Schema: %s", df.schema.simpleString())

        if coalesce and int(coalesce) > 0:
            df = df.coalesce(int(coalesce))

        writer = df.write.mode("overwrite").format("parquet")
        if partition_by:
            writer = writer.partitionBy(partition_by)

        writer.save(output_path)

        logger.info("Parquet file(s) created successfully at %s", output_path)

    except Exception as e:
        logger.exception("Error in convert_csv_to_parquet: %s", e)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert CSV files to Parquet")
    parser.add_argument("--input", default=os.getenv("CONVERT_INPUT_PATH", "gs://aero_data/*.csv"), help="Input CSV path or glob")
    parser.add_argument("--output", default=os.getenv("CONVERT_OUTPUT_PATH", "gs://aero_data/parquet"), help="Output parquet path")
    parser.add_argument("--coalesce", type=int, default=int(os.getenv("CONVERT_COALESCE", "0")), help="Number of partitions to coalesce to (0 = no coalesce)")
    parser.add_argument("--partition-by", dest="partition_by", default=os.getenv("CONVERT_PARTITION_BY", None), help="Column to partition by")

    args = parser.parse_args()

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=getattr(logging, log_level, logging.INFO))

    main(args.input, args.output, args.coalesce, args.partition_by)