import sys
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from spark.clean_data import clean_flight_data


def run_in_memory_tests():
    spark = SparkSession.builder.appName("CleanFlightDataTests").getOrCreate()

    schema = T.StructType([
        T.StructField("FlightDate", T.StringType()),
        T.StructField("Year", T.IntegerType()),
        T.StructField("Quarter", T.IntegerType()),
        T.StructField("Month", T.IntegerType()),
        T.StructField("DayofMonth", T.IntegerType()),
        T.StructField("DayOfWeek", T.IntegerType()),
        T.StructField("IATA_CODE_Reporting_Airline", T.StringType()),
        T.StructField("Flight_Number_Reporting_Airline", T.StringType()),
        T.StructField("OriginAirportID", T.IntegerType()),
        T.StructField("DestAirportID", T.IntegerType()),
        T.StructField("DepTime", T.IntegerType()),
        T.StructField("ArrTime", T.IntegerType()),
        T.StructField("Cancelled", T.IntegerType()),
        T.StructField("CancellationCode", T.StringType()),
        T.StructField("Reporting_Airline", T.StringType()),
        T.StructField("DOT_ID_Reporting_Airline", T.IntegerType()),
    ])

    data = [
        # valid row
        ("2021-01-01", 2021, 1, 1, 1, 5, "AA", "100", 12478, 12892, 700, 900, 0, None, "American Airlines", 19805),
        # cancelled row with valid code
        ("2021-01-02", 2021, 1, 1, 2, 6, "DL", "200", 12479, 12893, None, None, 1, "A", "Delta", 19791),
        # invalid row: missing FlightDate (should be filtered out)
        (None, 2021, 1, 1, 3, 7, "UA", "300", 12480, 12894, 800, 1000, 0, None, "United", 19805),
    ]

    df = spark.createDataFrame(data, schema=schema)

    cleaned = clean_flight_data(df)

    cleaned_count = cleaned.count()
    print(f"Cleaned row count: {cleaned_count}")

    # Expectations: first and second rows kept (3rd removed), so 2 rows
    assert cleaned_count == 2, f"Expected 2 rows after cleaning, got {cleaned_count}"

    # Check that Cancelled=1 has CancellationCode preserved and Cancelled=0 has None
    cancelled_row = cleaned.filter((cleaned.Cancelled == 1)).collect()
    assert len(cancelled_row) == 1 and cancelled_row[0].CancellationCode == "A", "Cancelled row should keep valid cancellation code"

    non_cancelled = cleaned.filter((cleaned.Cancelled == 0)).collect()
    assert len(non_cancelled) == 1 and non_cancelled[0].CancellationCode is None, "Non-cancelled row should have null CancellationCode"

    print("All in-memory tests passed ✅")

    spark.stop()


if __name__ == "__main__":
    try:
        run_in_memory_tests()
    except AssertionError as e:
        print(f"Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error running tests: {e}")
        sys.exit(2)
    sys.exit(0)