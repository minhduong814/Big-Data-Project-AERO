import os

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat, lit, lpad, to_timestamp, when

load_dotenv(override=True)


# 2. Transformation Logic
def full_transform(spark: SparkSession, df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            # Convert HHMM integer (e.g. 1430) to a real Timestamp
            "CRSDepTimestamp",
            to_timestamp(
                concat(
                    col("FlightDate"),
                    lit(" "),
                    lpad(col("CRSDepTime").cast("string"), 4, "0"),
                ),
                "yyyy-MM-dd HHmm",
            ),
        )
        .withColumn(
            # Looker-friendly flags (1 for true, 0 for false)
            "Is_Delayed",
            when(col("DepDel15") == 1.0, 1).otherwise(0),
        )
        .withColumn("Is_Cancelled", when(col("Cancelled") == 1.0, 1).otherwise(0))
        .select(
            "FlightDate",
            "Year",
            "Quarter",
            "Month",
            "Reporting_Airline",
            col("Flight_Number_Reporting_Airline").alias("Flight_Number"),
            "Origin",
            "OriginCityName",
            "Dest",
            "DestCityName",
            "CRSDepTimestamp",
            "DepDelay",
            "ArrDelay",
            "Is_Delayed",
            "Is_Cancelled",
            # Filling nulls in delay reasons with 0.0
            when(col("CarrierDelay").isNull(), 0.0)
            .otherwise(col("CarrierDelay"))
            .alias("CarrierDelay"),
            when(col("WeatherDelay").isNull(), 0.0)
            .otherwise(col("WeatherDelay"))
            .alias("WeatherDelay"),
            when(col("NASDelay").isNull(), 0.0)
            .otherwise(col("NASDelay"))
            .alias("NASDelay"),
            when(col("SecurityDelay").isNull(), 0.0)
            .otherwise(col("SecurityDelay"))
            .alias("SecurityDelay"),
            when(col("LateAircraftDelay").isNull(), 0.0)
            .otherwise(col("LateAircraftDelay"))
            .alias("LateAircraftDelay"),
        )
    )


def main():
    project_id = os.environ.get("GCP_PROJECT_ID", "teencare-2026")
    dataset_id = "aero_dataset"

    jar_path = "./jar"
    bq_jar = f"{jar_path}/spark-bigquery-with-dependencies_2.12-0.41.1.jar"
    gcs_jar = f"{jar_path}/gcs-connector-hadoop3-2.2.22-shaded.jar"

    key_path = "./.credentials.json"

    spark: SparkSession = (
        SparkSession.builder.appName("Initial_Flight_History_Load")
        .config("spark.jars", f"{bq_jar},{gcs_jar}")
        .config("credentialsFile", key_path)
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_path)
        .config("spark.driver.extraClassPath", f"{bq_jar}:{gcs_jar}")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("viewsEnabled", "true")
        .config("materializationDataset", dataset_id)
        .getOrCreate()
    )

    # 1. Read all historical data
    raw_df = (
        spark.read.format("bigquery")
        .option("table", f"{project_id}.{dataset_id}.flights_raw")
        .load()
    )

    print("-" * 60)
    print(f"Read {raw_df.count()} rows from flights_raw table.")
    print("-" * 60)

    processed_df = full_transform(spark, raw_df)

    print("-"*60)
    print(f"Transformed DataFrame has {processed_df.count()} rows.")
    print("-"*60)

    # 3. Write to the new BigQuery table (Overwrite for the first run)
    processed_df.write.format("bigquery").option("writeMethod", "direct").option(
        "table", f"{project_id}.{dataset_id}.flights"
    ).mode("overwrite").save()

    print("Historical data load complete.")


if __name__ == "__main__":
    main()
