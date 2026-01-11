# AERO Big Data Pipeline - Refactored Architecture

## Overview

This project implements a complete Extract-Load-Transform-Visualize (ELTV) pipeline for flight data analytics, aligned with the SystemArchitecture.png workflow.

## Architecture

```
Extract (Kafka Producer) → Load (Kafka Consumer → BigQuery) → Transform (Spark) → Visualize (Looker/Dashboards)
```

### Components

1. **Extract Layer** (`src/extract/`)
   - `kafka_producer.py`: Produces flight data to Kafka topics
   - `data_collector.py`: Collects data from APIs, CSV, JSON, Parquet files

2. **Load Layer** (`src/load/`)
   - `kafka_consumer.py`: Consumes from Kafka and loads to BigQuery
   - `data_ingestion.py`: Handles ingestion to BigQuery, GCS, and local storage

3. **Transform Layer** (`src/transform/`)
   - `spark_streaming.py`: Real-time stream processing with Spark Structured Streaming
   - `spark_batch.py`: Batch processing for historical data analysis

4. **Visualize Layer** (`src/visualize/`)
   - `looker_connector.py`: Creates BigQuery views for Looker dashboards
   - `dashboard.py`: Generates analytical dashboards and reports

5. **Orchestration** (`orchestration/prefect/`)
   - `flow.py`: Prefect workflows for pipeline orchestration

## Project Structure

```
Big-Data-Project-AERO/
├── src/
│   ├── extract/          # Data extraction from sources
│   ├── load/             # Data loading to storage
│   ├── transform/        # Data transformation with Spark
│   └── visualize/        # Data visualization and dashboards
├── config/
│   └── pipeline_config.yaml  # Pipeline configuration
├── orchestration/
│   ├── docker-compose.yml    # Orchestration services
│   └── prefect/              # Prefect flows
├── kubernetes/           # K8s deployment configs
├── infrastructure/       # Terraform and Docker configs
├── data/                 # Sample data files
└── key/                  # GCP credentials

```

## Configuration

All pipeline configuration is centralized in `config/pipeline_config.yaml`:

- Kafka settings (bootstrap servers, topics, consumer/producer configs)
- Spark settings (master URL, memory, cores, checkpoints)
- GCP settings (project ID, dataset, credentials)
- Pipeline settings (batch sizes, windowing, watermarks)

## Quick Start

### 1. Start Infrastructure

```bash
# Start Kafka, Spark, Prefect
cd orchestration
docker-compose up -d
```

### 2. Run Data Pipeline

```bash
# Extract: Send data to Kafka
python src/extract/kafka_producer.py

# Load: Consume from Kafka and load to BigQuery
python src/load/kafka_consumer.py

# Transform: Process with Spark Streaming
python src/transform/spark_streaming.py

# Visualize: Generate dashboards
python src/visualize/dashboard.py
```

### 3. Run Orchestrated Pipeline

```bash
# Run with Prefect
python orchestration/prefect/flow.py
```

## Docker Deployment

Build and run with Docker:

```bash
# Build image
docker build -t aero-pipeline .

# Run producer
docker run -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 aero-pipeline python src/extract/kafka_producer.py

# Run consumer
docker run -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
           -e GCP_PROJECT_ID=double-arbor-475907-s5 \
           -v ./key:/app/key \
           aero-pipeline python src/load/kafka_consumer.py
```

## Kubernetes Deployment

Deploy to GKE:

```bash
# Build and push to GCR
./build-and-push.sh

# Deploy to Kubernetes
kubectl apply -f kubernetes/kafka-infrastructure.yaml
kubectl apply -f kubernetes/deployment.yaml
kubectl apply -f kubernetes/spark-deployment.yaml
```

## Data Flow

1. **Extract**: Flight data is collected from APIs or files and sent to `flights-raw` Kafka topic
2. **Load**: Raw data is consumed from Kafka and loaded to BigQuery `aero_dataset.flights` table
3. **Transform**: Spark processes the data, adds delay calculations, categories, and sends to `flights-processed` topic
4. **Visualize**: Looker/Dashboards connect to BigQuery views for real-time analytics

## Key Features

- **Scalability**: Horizontal scaling with Kafka and Spark
- **Fault Tolerance**: Kafka consumer groups, Spark checkpointing, retry mechanisms
- **Monitoring**: Prometheus metrics, comprehensive logging
- **Orchestration**: Prefect for workflow management and scheduling
- **Cloud-Native**: GCP integration (BigQuery, GCS, GKE)

## Environment Variables

Required environment variables:

```bash
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=flights-raw
GCP_PROJECT_ID=double-arbor-475907-s5
GOOGLE_APPLICATION_CREDENTIALS=/app/key/double-arbor-475907-s5-a5863b0c230a.json
PYTHONPATH=/app
```

## Development

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Run Tests

```bash
pytest test/
```

### Local Development

```bash
# Start local Kafka and Spark
docker-compose -f orchestration/docker-compose.yml up -d

# Run components individually
python src/extract/kafka_producer.py
python src/load/kafka_consumer.py
python src/transform/spark_streaming.py
```

## Monitoring

- **Kafka**: http://localhost:9092
- **Spark UI**: http://localhost:8080
- **Prefect UI**: http://localhost:4200
- **Schema Registry**: http://localhost:8081

## Troubleshooting

### Kafka Connection Issues
- Ensure Kafka is running: `docker-compose ps`
- Check broker connectivity: `kafka-topics.sh --list --bootstrap-server localhost:9092`

### BigQuery Permission Issues
- Verify credentials: `gcloud auth application-default login`
- Check service account permissions in GCP Console

### Spark Job Failures
- Check Spark logs: `docker logs spark-master`
- Verify checkpoint directory permissions

## License

See LICENSE file for details.
