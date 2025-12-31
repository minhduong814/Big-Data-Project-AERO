# AERO: Air Traffic Exploration & Real-time Orchestration

## 1. Introduction
Air travel is a major contributor to global transportation, generating vast amounts of operational and logistical data every day. From flight schedules and delays to passenger volumes and aircraft metrics, the aviation industry produces millions of records annually, offering a valuable resource for data-driven insights and decision-making.
This project aims to build an automated data pipeline that focuses specifically on air traffic data from the United States—one of the busiest and most complex airspaces in the world. Using Prefect for orchestration, the pipeline collects real-time flight data from external APIs like aviationstack, stores it in Google Cloud Storage (GCS), and loads it into Google BigQuery. Spark is then used to clean and transform the data, preparing it for analysis. The processed information is visualized through interactive dashboards in Looker Studio, allowing users to explore patterns, delays, and trends in air travel. To simulate real-time conditions, the team also integrates Kafka for streaming flight data, enabling near-instantaneous monitoring and analysis.

## 2. Dataset
Data is collected from US Bureau of Transportation Website and from Aviation Stack API. In this project, we analyze US air traffic data from 1987 to 2024 and realtime data from API calls.
Dataset size: over 80GB with approximate 200 million records.
These are some of the fields in the dataset:
| Field | Description |
|---|---|
| Year | Year of the flight. Helps in identifying long-term trends or seasonal patterns. |
| Quarter | Quarter of the year (1–4). Useful for analyzing seasonal variations. |
| Month | Month of the flight. Allows for monthly breakdown of data. |
| DayOfMonth | Day of the month when the flight occurred. Useful for precise date-specific analysis. |
| DayOfWeek | Day of the week (1 = Monday, 7 = Sunday). Important for identifying weekly trends. |
| FlightDate | Exact date of the flight. A critical field for time-series analysis. |
| Reporting Airline | Unique carrier code for the reporting airline. Helps distinguish between airlines. |
| Tail Number | Unique aircraft tail number. Useful for tracking performance by aircraft. |
| Flight Number | Reporting flight number reported by the airline. Enables tracking individual flights. |
| OriginAirportID | Unique ID of the origin airport. Essential for identifying departure locations. |
| OriginCityName | City name of the origin airport. Useful for grouping and summarizing data by city. |
| DestAirportID | Unique ID of the destination airport. Important for analyzing arrival locations. |
| DestCityName | City name of the destination airport. Enables grouping data by destination city. |
| ArrDelay | Arrival delay in minutes. A critical performance metric. |

## 3. Technology and System Architecture
- **Flows**: Contains Prefect flows that orchestrate the data pipeline.
- **Kafka**: Implements Kafka producer and consumer scripts for data streaming.
- **Spark/dbt**: Handles data cleaning and transformation using Spark and dbt.
- **Docker**: Containerizes the application for easy deployment and scalability.
- **Scripts**: Includes utility scripts for bootstrapping and loading data.

## 4. Results
- [looker studio](https://lookerstudio.google.com/reporting/081b7d2c-260e-4a14-b09e-cf9e0e2e1da8)
