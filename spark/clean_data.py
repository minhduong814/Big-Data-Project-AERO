import argparse
import logging
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Define the columns to retain
COMMON_VALUE = [
    "FlightDate", "Year", "Quarter", "Month", "DayofMonth", "DayOfWeek",
    "IATA_CODE_Reporting_Airline", "Tail_Number", "Flight_Number_Reporting_Airline",
    "OriginAirportID", "DestAirportID", "CRSDepTime", "DepTime", "DepDelay",
    "CRSArrTime", "ArrTime", "ArrDelay", "Cancelled", "Diverted", "Distance",
    "Origin", "OriginCityName", "OriginState", "OriginStateFips", "OriginStateName",
    "Dest", "DestCityName", "DestState", "DestStateFips", "DestStateName",
    "Reporting_Airline", "DOT_ID_Reporting_Airline",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
    "CancellationCode"
]


from spark.utils import get_spark_session


def clean_flight_data(df: DataFrame) -> DataFrame:
    """
    Cleans flight data by retaining relevant columns and applying logical filters.

    - Retains only columns in `COMMON_VALUE`.
    - Filters out rows with missing critical identifiers.
    - Ensures Cancelled/Diverted fields are 0 or 1.
    - Normalizes `CancellationCode` to valid codes or null when not applicable.
    """
    # Keep a defensive copy of columns present in the input
    available_cols = [c for c in COMMON_VALUE if c in df.columns]

    cleaned_df = (
        df.select(available_cols)
        .filter(F.col("FlightDate").isNotNull())
        .filter(F.col("IATA_CODE_Reporting_Airline").isNotNull())
        .filter(F.col("Flight_Number_Reporting_Airline").isNotNull())
        .filter(F.col("OriginAirportID").isNotNull())
        .filter(F.col("DestAirportID").isNotNull())
        .filter(F.col("Year").isNotNull())
        .filter(F.col("Quarter").isNotNull())
        .filter(F.col("Month").isNotNull())
        .filter(F.col("DayofMonth").isNotNull())
        .filter(F.col("DayOfWeek").isNotNull())
        .filter(F.col("Cancelled").isin([0, 1]))
        .filter(F.col("Diverted").isin([0, 1]))
        .filter(F.col("Reporting_Airline").isNotNull())
        .filter(F.col("DOT_ID_Reporting_Airline").isNotNull())
        # Ensure CancellationCode is valid only when Cancelled = 1
        .withColumn(
            "CancellationCode",
            F.when(
                (F.col("Cancelled") == 1) & F.col("CancellationCode").isin(["A", "B", "C", "D"]),
                F.col("CancellationCode"),
            ).otherwise(F.lit(None))
        )
        # DepTime and ArrTime must be present unless Cancelled = 1
        .filter(
            (F.col("DepTime").isNotNull() & F.col("ArrTime").isNotNull())
            | (F.col("Cancelled") == 1)
        )
    )
    return cleaned_df


def data_quality_report(df: DataFrame, sample_limit: int = 5) -> Dict[str, object]:
    """Return a simple data quality report for the given DataFrame."""
    total = df.count()
    null_counts = {c: df.filter(F.col(c).isNull()).count() for c in [
        "FlightDate", "IATA_CODE_Reporting_Airline", "Flight_Number_Reporting_Airline",
        "OriginAirportID", "DestAirportID", "Reporting_Airline", "DOT_ID_Reporting_Airline"
    ] if c in df.columns}
    sample_rows = df.limit(sample_limit).toPandas().to_dict(orient="records") if total > 0 else []
    return {"total_rows": total, "null_counts": null_counts, "sample": sample_rows}


def parse_args():
    parser = argparse.ArgumentParser(description="Clean flight CSV data and write Parquet output")
    parser.add_argument("--input", default="gs://bk9999airline/airline_full.csv", help="Input CSV path")
    parser.add_argument("--output", default="gs://bk9999airline/new_cleaned_airline_data", help="Output Parquet path")
    parser.add_argument("--dry-run", action="store_true", help="Run without writing output")
    parser.add_argument("--coalesce", type=int, default=1, help="Number of output files (coalesce)")
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)
    args = parse_args()

    spark = get_spark_session()
    logging.info("Reading CSV from %s", args.input)

    df = spark.read.csv(args.input, header=True, inferSchema=True)
    logging.info("Read %d rows", df.count())

    cleaned = clean_flight_data(df)
    report = data_quality_report(cleaned)
    logging.info("Data quality report: %s", report)

    if args.dry_run:
        logging.info("Dry run mode - not writing output")
    else:
        logging.info("Writing cleaned parquet to %s", args.output)
        cleaned.coalesce(args.coalesce).write.mode("overwrite").parquet(args.output)

    spark.stop()


if __name__ == "__main__":
    main()