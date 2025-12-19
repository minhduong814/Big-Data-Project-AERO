import argparse
import logging
from typing import Optional

from spark.utils import get_spark_session


def parse_args():
    parser = argparse.ArgumentParser(description="Convert CSV files to Parquet using Spark")
    parser.add_argument("--input", default="gs://aero_data/*.csv", help="Input CSV glob or path")
    parser.add_argument("--output", default="gs://aero_data/parquet", help="Output parquet path")
    parser.add_argument("--header", default="true", help="CSV header option")
    parser.add_argument("--infer-schema", default="true", help="Infer schema option")
    parser.add_argument("--partition-by", default=None, help="Comma-separated columns to partition by")
    parser.add_argument("--coalesce", type=int, default=1, help="Number of output files (coalesce)")
    parser.add_argument("--dry-run", action="store_true", help="Do not write output")
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)
    args = parse_args()

    spark = get_spark_session(app_name="CreateParquet")

    logging.info("Reading CSV files from %s", args.input)

    df = spark.read.option("header", args.header).option("inferSchema", args.infer_schema).csv(args.input)

    count = df.count()
    logging.info("Read %d rows", count)
    df.printSchema()

    if count == 0:
        logging.warning("No rows read from input path %s. Exiting.", args.input)
        spark.stop()
        return

    if args.dry_run:
        logging.info("Dry-run mode enabled; skipping write to %s", args.output)
        spark.stop()
        return

    logging.info("Writing to %s", args.output)
    writer = df.coalesce(args.coalesce).write.mode("overwrite")

    if args.partition_by:
        parts = [c.strip() for c in args.partition_by.split(",") if c.strip()]
        if parts:
            logging.info("Partitioning by %s", parts)
            writer = writer.partitionBy(*parts)

    writer.parquet(args.output)

    logging.info("Parquet conversion completed successfully")
    spark.stop()


if __name__ == "__main__":
    main()