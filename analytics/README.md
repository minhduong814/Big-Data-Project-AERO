# Flight Analytics and Visualization

This directory contains analytics and visualization scripts for the AERO flight data project.

## Overview

The `flight_analytics.py` script performs comprehensive analytics on the historical flight data stored in BigQuery's `flights_raw` table. It generates various queries and creates visualizations based on the data loaded by `load_historical_data.py`.

## Features

The analytics script provides the following analysis:

1. **Delays by Year**: Trends in flight delays over time, including delay causes
2. **Top Airlines**: Analysis of airlines by flight volume, delays, and cancellation rates
3. **Top Routes**: Most popular flight routes and their delay patterns
4. **Monthly Trends**: Seasonal patterns in flights, delays, and cancellations
5. **Day of Week Analysis**: Weekly patterns in flight operations
6. **Cancellation Analysis**: Breakdown of cancellation reasons

## Requirements

- Python 3.8+
- Google Cloud BigQuery access
- Required packages (from requirements.txt):
  - google-cloud-bigquery
  - pandas
  - matplotlib
  - seaborn

## Usage

### Basic Usage

```bash
python analytics/flight_analytics.py
```

### With Custom Parameters

```bash
python analytics/flight_analytics.py \
    --project-id your-project-id \
    --dataset-id aero_dataset \
    --table-id flights_raw \
    --output-dir analytics/output
```

### Command Line Arguments

- `--project-id`: GCP project ID (default: from GCP_PROJECT_ID env var or "double-arbor-475907-s5")
- `--dataset-id`: BigQuery dataset ID (default: "aero_dataset")
- `--table-id`: BigQuery table ID (default: "flights_raw")
- `--output-dir`: Output directory for visualizations (default: "analytics/output")

## Output

The script generates:

1. **Visualizations** (PNG files):
   - `delays_by_year.png`: Delay trends over time
   - `top_airlines.png`: Airline performance metrics
   - `top_routes.png`: Popular routes analysis
   - `monthly_trends.png`: Seasonal patterns
   - `day_of_week.png`: Weekly patterns
   - `cancellation_analysis.png`: Cancellation breakdown

2. **Data Files** (CSV files):
   - Corresponding CSV files with the raw query results for further analysis

## Example

After running the script, you'll find output in the `analytics/output/` directory:

```
analytics/output/
├── delays_by_year.png
├── delays_by_year.csv
├── top_airlines.png
├── top_airlines.csv
├── top_routes.png
├── top_routes.csv
├── monthly_trends.png
├── monthly_trends.csv
├── day_of_week.png
├── day_of_week.csv
├── cancellation_analysis.png
└── cancellation_analysis.csv
```

## Data Source

The analytics script queries the `flights_raw` table which contains historical flight data loaded by `setup/load_historical_data.py`. This table includes:

- Time dimensions (Year, Month, Day of Week, etc.)
- Airline information
- Origin and destination airports
- Delay information (departure, arrival, causes)
- Cancellation data
- Flight duration and distance

## Notes

- The script requires the `flights_raw` table to be populated with data first
- Make sure you have appropriate BigQuery permissions
- Large datasets may take some time to query
- Visualizations are saved as high-resolution PNG files (300 DPI)
