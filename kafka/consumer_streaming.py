# CONSUME MESSAGE FROM KAFKA AND PROCESS THEN PUSH TO GCS AND BIGQUERY
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)
import os

PROJECT_ID = 'totemic-program-442307-i9'
CONSUME_TOPIC_FLIGHTS = 'flights'
KAFKA_ADDRESS= "35.240.239.52"
KAFKA_BOOTSTRAP_SERVERS = f'{KAFKA_ADDRESS}:9092,{KAFKA_ADDRESS}:9093,{KAFKA_ADDRESS}:9094'

GCP_GCS_BUCKET = "uk-airline-big-data"
GCS_STORAGE_PATH = 'gs://' + GCP_GCS_BUCKET + '/realtime2'
CHECKPOINT_PATH = 'gs://' + GCP_GCS_BUCKET + '/realtime2/checkpoint/'
CHECKPOINT_PATH_BQ = 'gs://' + GCP_GCS_BUCKET + '/realtime2/checkpoint_bq/'

JSON_SCHEMA = StructType([
    StructField("flight_date", StringType(), True),
    StructField("flight_status", StringType(), True),
    StructField("departure", StructType([
        StructField("airport", StringType(), True),
        StructField("timezone", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True),
        StructField("terminal", StringType(), True),
        StructField("gate", StringType(), True),
        StructField("delay", StringType(), True),
        StructField("scheduled", StringType(), True),
        StructField("estimated", StringType(), True),
        StructField("actual", StringType(), True),
        StructField("estimated_runway", StringType(), True),
        StructField("actual_runway", StringType(), True)
    ]), True),
    StructField("arrival", StructType([
        StructField("airport", StringType(), True),
        StructField("timezone", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True),
        StructField("terminal", StringType(), True),
        StructField("gate", StringType(), True),
        StructField("baggage", StringType(), True),
        StructField("delay", StringType(), True),
        StructField("scheduled", StringType(), True),
        StructField("estimated", StringType(), True),
        StructField("actual", StringType(), True),
        StructField("estimated_runway", StringType(), True),
        StructField("actual_runway", StringType(), True)
    ]), True),
    StructField("airline", StructType([
        StructField("name", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True)
    ]), True),
    StructField("flight", StructType([
        StructField("number", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True),
        StructField("codeshared", StructType([
            StructField("airline_name", StringType(), True),
            StructField("airline_iata", StringType(), True),
            StructField("airline_icao", StringType(), True),
            StructField("flight_number", StringType(), True),
            StructField("flight_iata", StringType(), True),
            StructField("flight_icao", StringType(), True)
        ]), True)
    ]), True)
])

KEY_SCHEMA = StructType([
    StructField("flight_id", StringType(), False)
])

def read_from_kafka(consume_topic: str):
    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "latest") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .option("failOnDataLoss", "false") \
        .load()
    # .option("startingOffsets", "earliest") \
    return df_stream

def parse_schema_from_kafka_message(kafka_df, key_schema, value_schema):
    """ take a Spark Streaming df and parse value col based on <schema>, 
    return streaming df cols in schema """
    assert kafka_df.isStreaming is True, "DataFrame doesn't receive streaming data"
    kafka_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") 
    streaming_df = kafka_df.withColumn("values_json", F.from_json(F.col("value"), value_schema)) \
        .withColumn("key_json", F.from_json(F.col("key"), key_schema)) \
        .withColumn("flight_id", F.col("key_json.flight_id") ) \
        .selectExpr("flight_id", "values_json.*")
    flight_df = streaming_df.select(
        # Flight-level fields
        "flight_id",
        "flight_date",
        "flight_status",
        
        # Flatten 'departure' struct
        F.col("departure.airport").alias("departure_airport"),
        F.col("departure.timezone").alias("departure_timezone"),
        F.col("departure.iata").alias("departure_iata"),
        F.col("departure.icao").alias("departure_icao"),
        F.col("departure.terminal").alias("departure_terminal"),
        F.col("departure.gate").alias("departure_gate"),
        F.col("departure.delay").cast(IntegerType()).alias("departure_delay"),
        F.col("departure.scheduled").cast(TimestampType()).alias("departure_scheduled"),
        F.col("departure.estimated").cast(TimestampType()).alias("departure_estimated"),
        F.col("departure.actual").cast(TimestampType()).alias("departure_actual"),
        F.col("departure.estimated_runway").cast(TimestampType()).alias("departure_estimated_runway"),
        F.col("departure.actual_runway").cast(TimestampType()).alias("departure_actual_runway"),
        
        # Flatten 'arrival' struct
        F.col("arrival.airport").alias("arrival_airport"),
        F.col("arrival.timezone").alias("arrival_timezone"),
        F.col("arrival.iata").alias("arrival_iata"),
        F.col("arrival.icao").alias("arrival_icao"),
        F.col("arrival.terminal").alias("arrival_terminal"),
        F.col("arrival.gate").alias("arrival_gate"),
        F.col("arrival.baggage").alias("arrival_baggage"),
        F.col("arrival.delay").cast(IntegerType()).alias("arrival_delay"),
        F.col("arrival.scheduled").cast(TimestampType()).alias("arrival_scheduled"),
        F.col("arrival.estimated").cast(TimestampType()).alias("arrival_estimated"),
        F.col("arrival.actual").cast(TimestampType()).alias("arrival_actual"),
        F.col("arrival.estimated_runway").cast(TimestampType()).alias("arrival_estimated_runway"),
        F.col("arrival.actual_runway").cast(TimestampType()).alias("arrival_actual_runway"),
        
        # Flatten 'airline' struct
        F.col("airline.iata").alias("airline_id"),
        
        # Flatten 'flight' struct
        F.col("flight.number").alias("flight_number"),
        
        # Flatten 'codeshared' struct
        F.col("flight.codeshared.flight_iata").alias("codeshared_flight_id")
    )

    return flight_df

def create_file_write_stream(stream, storage_path, 
                             checkpoint_path='/checkpoint', 
                             trigger="5 seconds", 
                             output_mode="append", 
                             file_format="parquet",
                             partition_by="flight_date"):
    write_stream = (stream
                    .writeStream
                    .format(file_format)
                    .partitionBy(partition_by)
                    .option("path", storage_path)
                    .option("checkpointLocation", checkpoint_path)
                    .trigger(processingTime=trigger)
                    .outputMode(output_mode))

    return write_stream

def create_file_write_stream_bq(stream, 
                                checkpoint_path='/checkpoint', 
                                trigger="5 seconds", 
                                output_mode="append"):
    write_stream = (stream
                    .writeStream
                    .format("bigquery")
                    .option("table", f"{PROJECT_ID}.realtimes.flights")
                    .option("checkpointLocation", checkpoint_path)
                    .trigger(processingTime=trigger)
                    .outputMode(output_mode))

    return write_stream

if __name__=="__main__":
    os.environ['KAFKA_ADDRESS'] = KAFKA_ADDRESS
    os.environ['GCP_GCS_BUCKET'] = 'uk-airline-big-data'

    spark = SparkSession.builder \
        .appName("Streaming from Kafka") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .getOrCreate()
    
    spark.conf.set("temporaryGcsBucket", "dataproc-staging-asia-southeast1-190992537547-yuriiara")
    spark.sparkContext.setLogLevel("DEBUG")
    spark.streams.resetTerminated()

    df_consume_stream = read_from_kafka(consume_topic=CONSUME_TOPIC_FLIGHTS)
    print(df_consume_stream.printSchema())

    # parse streaming data
    df_flight = parse_schema_from_kafka_message(df_consume_stream, KEY_SCHEMA, JSON_SCHEMA)
    print(df_flight.printSchema())

    # Write to GCS
    write_stream_flights = create_file_write_stream(df_flight, GCS_STORAGE_PATH, checkpoint_path=CHECKPOINT_PATH, partition_by="flight_date")
    write_stream_flights_bq = create_file_write_stream_bq(df_flight,
                                                          checkpoint_path=CHECKPOINT_PATH_BQ)
 
    write_stream_flights.start()
    write_stream_flights_bq.start()
                                                        
    spark.streams.awaitAnyTermination()