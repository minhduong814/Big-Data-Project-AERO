# Deployment Scripts

This directory contains scripts for deploying the AERO pipeline in different environments.

## Scripts

### `deploy-local.sh`
Deploys and starts all services locally using Docker Compose for development and testing.

**Usage:**
```bash
./scripts/deploy-local.sh
```

This script:
- Builds Docker images
- Starts infrastructure services (Kafka, Spark, Zookeeper, etc.)
- Waits for services to be ready
- Provides useful commands for running the pipeline

**Requirements:**
- Docker and Docker Compose installed
- GCP credentials file in `key/` directory

### `deploy-cloud.sh`
Deploys the pipeline to Google Cloud Platform (GKE or Cloud Run).

**Usage:**
```bash
# Deploy to GKE (default)
DEPLOYMENT_TYPE=gke ./scripts/deploy-cloud.sh

# Deploy to Cloud Run
DEPLOYMENT_TYPE=cloudrun ./scripts/deploy-cloud.sh

# With custom settings
GCP_PROJECT_ID=your-project \
GCP_REGION=us-central1 \
GKE_CLUSTER_NAME=my-cluster \
IMAGE_TAG=v1.0.0 \
./scripts/deploy-cloud.sh
```

**Environment Variables:**
- `GCP_PROJECT_ID`: GCP project ID (default: double-arbor-475907-s5)
- `GCP_REGION`: GCP region (default: us-central1)
- `GKE_CLUSTER_NAME`: GKE cluster name (default: aero-cluster)
- `K8S_NAMESPACE`: Kubernetes namespace (default: aero)
- `DEPLOYMENT_TYPE`: Deployment type - `gke` or `cloudrun` (default: gke)
- `IMAGE_TAG`: Docker image tag (default: latest)

**Requirements:**
- `gcloud` CLI installed and authenticated
- Docker installed
- GCP project with billing enabled
- Appropriate GCP permissions

## Environment Configuration

The scripts use environment-aware configuration:
- **Local**: Uses `config/pipeline_config.local.yaml`
- **Cloud**: Uses `config/pipeline_config.cloud.yaml`

You can also override the environment:
```bash
export AERO_ENVIRONMENT=local  # or 'cloud'
```

## Manual Pipeline Execution

After deploying locally, you can run the pipeline:

```bash
# Run entire pipeline
docker-compose -f orchestration/docker-compose.yml run --rm aero-pipeline \
    python src/main.py --mode all

# Run specific modes
docker-compose -f orchestration/docker-compose.yml run --rm aero-pipeline \
    python src/main.py --mode extract

docker-compose -f orchestration/docker-compose.yml run --rm aero-pipeline \
    python src/main.py --mode transform

docker-compose -f orchestration/docker-compose.yml run --rm aero-pipeline \
    python src/main.py --mode load

docker-compose -f orchestration/docker-compose.yml run --rm aero-pipeline \
    python src/main.py --mode visualize
```

## Troubleshooting

### Local Deployment

**Services not starting:**
```bash
# Check service status
docker-compose -f orchestration/docker-compose.yml ps

# View logs
docker-compose -f orchestration/docker-compose.yml logs -f [service-name]

# Restart services
docker-compose -f orchestration/docker-compose.yml restart
```

**Port conflicts:**
- Kafka: 9092, 9093
- Spark: 8080, 7077
- Schema Registry: 8081
- Kafka UI: 8082

If ports are in use, modify the port mappings in `orchestration/docker-compose.yml`.

### Cloud Deployment

**Authentication issues:**
```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

**Permission errors:**
Ensure your account has:
- Kubernetes Engine Admin
- Container Registry Service Agent
- BigQuery Admin
- Storage Admin

**Image push failures:**
```bash
# Enable Container Registry API
gcloud services enable containerregistry.googleapis.com

# Configure Docker authentication
gcloud auth configure-docker
```
