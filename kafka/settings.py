from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, IntegerType, TimestampType
)

BOOTSTRAP_SERVERS = ['localhost:9092', 'localhost:9093',]

PRODUCE_TOPIC_FLIGHTS = CONSUME_TOPIC_FLIGHTS = 'flight'

AS_API_KEY = "7a261a4fd48779425e4a75783f089b68"
FLIGHT_URL = "http://api.aviationstack.com/v1/flights"

# Flights Table Schema
FLIGHT_SCHEMA = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("flight_date", DateType(), nullable=True),
    StructField("flight_status", StringType(), nullable=True),
    StructField("departure_airport", StringType(), nullable=True),
    StructField("departure_timezone", StringType(), nullable=True),
    StructField("departure_iata", StringType(), nullable=True),
    StructField("departure_icao", StringType(), nullable=True),
    StructField("departure_terminal", StringType(), nullable=True),
    StructField("departure_gate", StringType(), nullable=True),
    StructField("departure_delay", IntegerType(), nullable=True),
    StructField("departure_scheduled", TimestampType(), nullable=True),
    StructField("departure_estimated", TimestampType(), nullable=True),
    StructField("departure_actual", TimestampType(), nullable=True),
    StructField("departure_estimated_runway", TimestampType(), nullable=True),
    StructField("departure_actual_runway", TimestampType(), nullable=True),
    StructField("arrival_airport", StringType(), nullable=True),
    StructField("arrival_timezone", StringType(), nullable=True),
    StructField("arrival_iata", StringType(), nullable=True),
    StructField("arrival_icao", StringType(), nullable=True),
    StructField("arrival_terminal", StringType(), nullable=True),
    StructField("arrival_gate", StringType(), nullable=True),
    StructField("arrival_baggage", StringType(), nullable=True),
    StructField("arrival_delay", IntegerType(), nullable=True),
    StructField("arrival_scheduled", TimestampType(), nullable=True),
    StructField("arrival_estimated", TimestampType(), nullable=True),
    StructField("arrival_actual", TimestampType(), nullable=True),
    StructField("arrival_estimated_runway", TimestampType(), nullable=True),
    StructField("arrival_actual_runway", TimestampType(), nullable=True),
    StructField("airline_id", StringType(), nullable=True),
    StructField("flight_number", StringType(), nullable=True),
    StructField("codeshared_flight_id", StringType(), nullable=True)
])

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