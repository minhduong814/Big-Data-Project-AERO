from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Initialize SparkSession
spark = SparkSession.builder.appName("CleanFlightData").getOrCreate()

# Path to the file in the bucket
file_path = "gs://bk9999airline/airline_full.csv"

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

def clean_flight_data(df: DataFrame) -> DataFrame:
    """
    Cleans flight data by retaining relevant columns and applying logical filters.
    """
    cleaned_df = (
        df.select(COMMON_VALUE)  # Retain only the required columns
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
        .filter(F.col("Cancelled").isin([0, 1]))  # Valid values: 0 or 1
        .filter(F.col("Diverted").isin([0, 1]))  # Valid values: 0 or 1
        .filter(F.col("Reporting_Airline").isNotNull())
        .filter(F.col("DOT_ID_Reporting_Airline").isNotNull())
        # Validate CancellationCode only when Cancelled = 1
        .withColumn(
            "CancellationCode",
            F.when(
                F.col("Cancelled") == 1,
                F.when(F.col("CancellationCode").isin(["A", "B", "C", "D"]), F.col("CancellationCode"))
            ).otherwise(None)
        )
        # DepTime and ArrTime must be present unless Cancelled = 1
        .filter(
            (F.col("DepTime").isNotNull() & F.col("ArrTime").isNotNull())
            | (F.col("Cancelled") == 1)
        )
    )
    return cleaned_df

# Read the CSV file from Google Cloud Storage
raw_data = spark.read.csv(file_path, header=True, inferSchema=True)

# Clean the data
cleaned_data = clean_flight_data(raw_data)

# Save the cleaned data back to Google Cloud Storage (optional)
output_path = "gs://bk9999airline/new_cleaned_airline_data"
cleaned_data.write.mode("overwrite").parquet(output_path)

print(f"Cleaned data saved to {output_path}")