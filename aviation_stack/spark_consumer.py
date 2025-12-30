from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

load_dotenv(override=True)


def write_to_bq(batch_df: DataFrame, batch_id):
    batch_df.write.format("bigquery").option(
        "table", "teencare-2026.aero_dataset.aviation_flights"
    ).option("writeMethod", "direct").mode("append").save()


def main():
    jar_path = "./jar"
    bq_jar = f"{jar_path}/spark-bigquery-with-dependencies_2.12-0.41.1.jar"
    key_path = "./.credentials.json"

    # Define the schema based on your example data
    schema = StructType(
        [
            StructField("flight_date", StringType()),
            StructField("flight_status", StringType()),
            StructField(
                "departure",
                StructType(
                    [
                        StructField("airport", StringType()),
                        StructField("timezone", StringType()),
                        StructField("iata", StringType()),
                        StructField("icao", StringType()),
                        StructField("terminal", StringType()),
                        StructField("gate", StringType()),
                        StructField("delay", IntegerType()),
                        StructField("scheduled", StringType()),
                        StructField("estimated", StringType()),
                        StructField("actual", StringType()),
                        StructField("estimated_runway", StringType()),
                        StructField("actual_runway", StringType()),
                    ]
                ),
            ),
            StructField(
                "arrival",
                StructType(
                    [
                        StructField("airport", StringType()),
                        StructField("timezone", StringType()),
                        StructField("iata", StringType()),
                        StructField("icao", StringType()),
                        StructField("terminal", StringType()),
                        StructField("gate", StringType()),
                        StructField("baggage", StringType()),
                        StructField("scheduled", StringType()),
                        StructField("delay", IntegerType()),
                        StructField("estimated", StringType()),
                        StructField("actual", StringType()),
                        StructField("estimated_runway", StringType()),
                        StructField("actual_runway", StringType()),
                    ]
                ),
            ),
            StructField(
                "airline",
                StructType(
                    [
                        StructField("name", StringType()),
                        StructField("iata", StringType()),
                        StructField("icao", StringType()),
                    ]
                ),
            ),
            StructField(
                "flight",
                StructType(
                    [
                        StructField("number", StringType()),
                        StructField("iata", StringType()),
                        StructField("icao", StringType()),
                        StructField("codeshared", StringType()),
                    ]
                ),
            ),
            StructField(
                "aircraft",
                StructType(
                    [
                        StructField("registration", StringType()),
                        StructField("iata", StringType()),
                        StructField("icao", StringType()),
                        StructField("icao24", StringType()),
                    ]
                ),
            ),
            StructField("live", StringType()),
        ]
    )

    spark: SparkSession = (
        SparkSession.builder.appName("AviationStackStream")
        .config("spark.ui.port", "4040")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        .config("spark.jars", f"{bq_jar}")
        .config("credentialsFile", key_path)
        .getOrCreate()
    )

    # Read from Kafka
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "aviation.flights")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Convert binary value to String and Parse JSON
    processed_df = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    # Flatten nested fields (Example: Departure Airport)
    final_df = processed_df.withColumn("dept_airport", col("departure.airport"))

    # Start the stream
    print("Starting the stream...")

    query = (
        final_df.writeStream.foreachBatch(write_to_bq)
        .option("checkpointLocation", "/tmp/checkpoints/aviation_stack/")
        .start()
    )

    print("Streaming started...")

    query.awaitTermination()


if __name__ == "__main__":
    main()
