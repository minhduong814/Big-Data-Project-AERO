# AERO Big Data Pipeline - Deployment Guide

## Prerequisites

- Docker & Docker Compose
- Python 3.11+
- GCP Account with BigQuery access
- Kubernetes cluster (for production deployment)
- Git

## Local Development Setup

### 1. Clone Repository

```bash
git clone <repository-url>
cd Big-Data-Project-AERO
```

### 2. Install Dependencies

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
make install
# or
pip install -r requirements.txt
```

### 3. Configure GCP Credentials

```bash
# Set up GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS="key/double-arbor-475907-s5-a5863b0c230a.json"
export GCP_PROJECT_ID="double-arbor-475907-s5"

# Login to GCP
gcloud auth application-default login
gcloud config set project double-arbor-475907-s5
```

### 4. Start Infrastructure Services

```bash
# Start Kafka, Spark, Prefect
make up

# Verify services are running
docker ps
```

### 5. Run Tests



```bash
# Test all components
python test_pipeline.py

# Or use make
make test
```

### 6. Run Pipeline

```bash
# Run complete pipeline
make pipeline

# Or run individual components
make extract      # Extract data
make transform    # Transform data
make load         # Load to BigQuery
make visualize    # Generate dashboards
```

## Docker Deployment

### Build Image

```bash
make build
# or
docker build -t aero-pipeline:latest .
```

### Run with Docker Compose

```bash
cd orchestration
docker-compose up -d

# View logs
docker-compose logs -f aero-producer
docker-compose logs -f aero-consumer

# Stop services
docker-compose down
```

## Kubernetes Deployment

### 1. Build and Push to Container Registry

```bash
# Build and push to Google Container Registry
./build-and-push.sh

# Or manually
docker build -t gcr.io/double-arbor-475907-s5/big-data-project-aero:latest .
docker push gcr.io/double-arbor-475907-s5/big-data-project-aero:latest
```

### 2. Create GCP Secrets

```bash
# Create secret for GCP credentials
kubectl create secret generic gcp-credentials \
  --from-file=key/double-arbor-475907-s5-a5863b0c230a.json

# Create secret for pulling from GCR
kubectl create secret docker-registry gcr-json-key \
  --docker-server=gcr.io \
  --docker-username=_json_key \
  --docker-password="$(cat key/double-arbor-475907-s5-a5863b0c230a.json)"
```

### 3. Deploy Infrastructure

```bash
# Deploy Kafka and Zookeeper
kubectl apply -f kubernetes/kafka-infrastructure.yaml

# Wait for Kafka to be ready
kubectl wait --for=condition=ready pod -l app=kafka --timeout=300s

# Deploy Spark cluster
kubectl apply -f kubernetes/spark-deployment.yaml

# Wait for Spark to be ready
kubectl wait --for=condition=ready pod -l app=spark-master --timeout=300s
```

### 4. Deploy Application

```bash
# Deploy producer and consumer
kubectl apply -f kubernetes/deployment.yaml

# Check deployment status
kubectl get pods
kubectl get services
```

### 5. Monitor Deployment

```bash
# View logs
kubectl logs -f deployment/aero-producer
kubectl logs -f deployment/aero-consumer

# Access Spark UI (port-forward)
kubectl port-forward svc/spark-master 8080:8080

# Access Prefect UI (port-forward)
kubectl port-forward svc/prefect-server 4200:4200
```

## Production Deployment

### 1. Set up GKE Cluster

```bash
# Create GKE cluster
gcloud container clusters create aero-cluster \
  --zone=us-central1-a \
  --num-nodes=3 \
  --machine-type=n1-standard-4 \
  --enable-autoscaling \
  --min-nodes=3 \
  --max-nodes=10

# Get credentials
gcloud container clusters get-credentials aero-cluster --zone=us-central1-a
```

### 2. Set up BigQuery

```bash
# Create dataset
bq mk --dataset --location=US double-arbor-475907-s5:aero_dataset

# Create tables (run from Python)
python -c "
from google.cloud import bigquery
from src.load.kafka_consumer import AeroDataLoader

loader = AeroDataLoader(
    bootstrap_servers='kafka:29092',
    topic='flights',
    group_id='setup',
    project_id='double-arbor-475907-s5',
    dataset_id='aero_dataset',
    table_id='flights'
)

schema = [
    bigquery.SchemaField('timestamp', 'TIMESTAMP', mode='REQUIRED'),
    bigquery.SchemaField('flight_id', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('airline', 'STRING'),
    bigquery.SchemaField('origin', 'STRING'),
    bigquery.SchemaField('destination', 'STRING'),
]

loader.create_table_if_not_exists(schema)
"
```

### 3. Configure Monitoring

```bash
# Deploy Prometheus for monitoring
kubectl apply -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/bundle.yaml

# Create ServiceMonitor for your services
kubectl apply -f monitoring/service-monitor.yaml
```

### 4. Set up CI/CD (GitHub Actions)

Create `.github/workflows/deploy.yaml`:

```yaml
name: Deploy AERO Pipeline

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Set up Cloud SDK
        uses: google-github-actions/setup-gcloud@v0
        with:
          service_account_key: ${{ secrets.GCP_SA_KEY }}
          project_id: double-arbor-475907-s5
      
      - name: Build and Push
        run: |
          gcloud auth configure-docker
          docker build -t gcr.io/double-arbor-475907-s5/big-data-project-aero:${{ github.sha }} .
          docker push gcr.io/double-arbor-475907-s5/big-data-project-aero:${{ github.sha }}
      
      - name: Deploy to GKE
        run: |
          gcloud container clusters get-credentials aero-cluster --zone=us-central1-a
          kubectl set image deployment/aero-producer producer=gcr.io/double-arbor-475907-s5/big-data-project-aero:${{ github.sha }}
          kubectl set image deployment/aero-consumer consumer=gcr.io/double-arbor-475907-s5/big-data-project-aero:${{ github.sha }}
```

## Troubleshooting

### Kafka Issues

```bash
# Check Kafka broker
kubectl exec -it kafka-0 -- kafka-topics.sh --list --bootstrap-server localhost:9092

# Create topic manually
kubectl exec -it kafka-0 -- kafka-topics.sh --create --topic flights-raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### Spark Issues

```bash
# Check Spark master logs
kubectl logs -f deployment/spark-master

# Access Spark shell
kubectl exec -it <spark-master-pod> -- spark-shell
```

### BigQuery Issues

```bash
# Test BigQuery connection
python -c "from google.cloud import bigquery; client = bigquery.Client(); print(list(client.list_datasets()))"

# Check permissions
gcloud projects get-iam-policy double-arbor-475907-s5
```

### Network Issues

```bash
# Check service connectivity
kubectl run -it --rm debug --image=busybox --restart=Never -- nslookup kafka-service

# Port forward for debugging
kubectl port-forward svc/kafka-service 9092:9092
```

## Scaling

### Horizontal Pod Autoscaling

```bash
# Create HPA for producer
kubectl autoscale deployment aero-producer --cpu-percent=70 --min=2 --max=10

# Create HPA for consumer
kubectl autoscale deployment aero-consumer --cpu-percent=70 --min=2 --max=10
```

### Kafka Scaling

```bash
# Scale Kafka brokers
kubectl scale statefulset kafka --replicas=5

# Increase partitions
kubectl exec kafka-0 -- kafka-topics.sh --alter --topic flights-raw --partitions 10 --bootstrap-server localhost:9092
```

## Backup and Recovery

### BigQuery Snapshots

```bash
# Create snapshot
bq cp double-arbor-475907-s5:aero_dataset.flights \
     double-arbor-475907-s5:aero_dataset.flights_backup_$(date +%Y%m%d)
```

### Kafka Topic Backup

```bash
# Export topic data
kubectl exec kafka-0 -- kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic flights-raw \
  --from-beginning \
  --max-messages 10000 > backup.json
```

## Performance Tuning

### Kafka Producer

- Increase `batch.size` for higher throughput
- Adjust `linger.ms` for batching
- Use `compression.type=gzip` or `snappy`

### Spark

- Increase `spark.executor.memory` and `spark.executor.cores`
- Adjust `spark.sql.shuffle.partitions` based on data size
- Enable `spark.sql.adaptive.enabled=true`

### BigQuery

- Use partitioned tables with `timestamp` field
- Create materialized views for frequently accessed aggregations
- Use clustering for better query performance

## Monitoring Dashboards

Access monitoring dashboards:

- **Prefect**: http://localhost:4200
- **Spark**: http://localhost:8080
- **Kafka**: http://localhost:9092 (via Kafka Manager)
- **Grafana**: http://localhost:3000 (if configured)

## Support

For issues and questions:
- Check logs: `kubectl logs -f <pod-name>`
- Review documentation in `docs/`
- Check GitHub issues

