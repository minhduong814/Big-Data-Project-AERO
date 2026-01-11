# Big Data Project Deployment Guide

This guide covers deploying the Big Data Project using Docker and Kubernetes.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Local Development with Docker Compose](#local-development-with-docker-compose)
- [Building and Pushing Docker Images](#building-and-pushing-docker-images)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)

## Prerequisites

### Required Tools
- Docker (v20.10+)
- Docker Compose (v2.0+)
- kubectl (v1.25+)
- gcloud CLI (for GCP deployment)
- Kubernetes cluster (GKE, EKS, or local with minikube/kind)

### GCP Setup
```bash
# Authenticate with GCP
gcloud auth login

# Set your project
gcloud config set project double-arbor-475907-s5

# Configure Docker for GCR
gcloud auth configure-docker gcr.io
```

## Local Development with Docker Compose

### Start All Services
```bash
# Start all services in detached mode
docker-compose up -d

# View logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f producer
docker-compose logs -f consumer
```

### Access Services
- **Kafka UI**: http://localhost:8080
- **Schema Registry**: http://localhost:8081
- **Kafka Brokers**: localhost:9092, localhost:9093, localhost:9094

### Stop Services
```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

## Building and Pushing Docker Images

### Manual Build and Push
```bash
# Build the image
docker build -t gcr.io/double-arbor-475907-s5/big-data-project-aero:latest .

# Push to GCR
docker push gcr.io/double-arbor-475907-s5/big-data-project-aero:latest
```

### Using the Build Script
```bash
# Make the script executable
chmod +x build-and-push.sh

# Build and push with version tag
./build-and-push.sh v1.0.0

# Build and push as latest
./build-and-push.sh
```

## Kubernetes Deployment

### Step 1: Prepare Secrets

Create the GCP credentials secret:
```bash
kubectl create secret generic gcp-credentials \
    --from-file=key.json=key/double-arbor-475907-s5-a5863b0c230a.json \
    --namespace=default
```

Create image pull secret for GCR:
```bash
kubectl create secret docker-registry gcr-json-key \
    --docker-server=gcr.io \
    --docker-username=_json_key \
    --docker-password="$(cat key/double-arbor-475907-s5-a5863b0c230a.json)" \
    --docker-email=user@example.com \
    --namespace=default
```

### Step 2: Deploy Infrastructure

Deploy Kafka infrastructure:
```bash
kubectl apply -f kubernetes/kafka-infrastructure.yaml
```

Wait for Kafka to be ready:
```bash
kubectl wait --for=condition=ready pod -l app=kafka --timeout=300s
```

### Step 3: Deploy Application

Deploy the application:
```bash
kubectl apply -f kubernetes/deployment.yaml
```

Deploy Spark components:
```bash
kubectl apply -f kubernetes/spark-deployment.yaml
```

### Step 4: Using the Deployment Script

```bash
# Make the script executable
chmod +x deploy.sh

# Deploy to default namespace
./deploy.sh

# Deploy to specific namespace
./deploy.sh production my-namespace
```

### Verify Deployment

```bash
# Check all pods
kubectl get pods

# Check services
kubectl get services

# Check deployments
kubectl get deployments

# Check statefulsets
kubectl get statefulsets
```

## Monitoring and Troubleshooting

### View Logs

```bash
# Producer logs
kubectl logs -f deployment/kafka-producer

# Consumer logs
kubectl logs -f deployment/kafka-consumer

# Kafka logs
kubectl logs -f kafka-0

# Follow logs from multiple pods
kubectl logs -f -l app=kafka-producer
```

### Port Forwarding

Access services from your local machine:
```bash
# Kafka
kubectl port-forward svc/kafka-service 9092:9092

# Schema Registry
kubectl port-forward svc/schema-registry 8081:8081

# Producer service
kubectl port-forward svc/kafka-producer-service 8080:80
```

### Exec into Pods

```bash
# Access producer pod
kubectl exec -it deployment/kafka-producer -- /bin/bash

# Access Kafka pod
kubectl exec -it kafka-0 -- /bin/bash

# Run Kafka console consumer
kubectl exec -it kafka-0 -- kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic flights \
    --from-beginning
```

### Scale Deployments

```bash
# Scale producer
kubectl scale deployment kafka-producer --replicas=3

# Scale consumer
kubectl scale deployment kafka-consumer --replicas=2

# Scale Kafka brokers
kubectl scale statefulset kafka --replicas=5
```

### Check Resource Usage

```bash
# Pod resource usage
kubectl top pods

# Node resource usage
kubectl top nodes
```

### Troubleshooting Common Issues

#### Pods not starting
```bash
# Describe pod to see events
kubectl describe pod <pod-name>

# Check pod logs
kubectl logs <pod-name>

# Check previous logs if pod restarted
kubectl logs <pod-name> --previous
```

#### Connection issues
```bash
# Test Kafka connectivity from a pod
kubectl run -it --rm debug --image=confluentinc/cp-kafka:7.2.0 --restart=Never -- \
    kafka-broker-api-versions --bootstrap-server kafka-service:9092
```

#### Storage issues
```bash
# Check PVCs
kubectl get pvc

# Describe PVC
kubectl describe pvc <pvc-name>
```

## Cleanup

### Remove Kubernetes Resources
```bash
# Delete all deployments
kubectl delete -f kubernetes/deployment.yaml
kubectl delete -f kubernetes/kafka-infrastructure.yaml
kubectl delete -f kubernetes/spark-deployment.yaml

# Delete secrets
kubectl delete secret gcp-credentials gcr-json-key
```

### Remove Docker Compose Resources
```bash
# Stop and remove everything
docker-compose down -v --remove-orphans

# Remove images
docker-compose down --rmi all
```

## Production Considerations

### High Availability
- Use at least 3 Kafka brokers
- Set appropriate replication factors
- Configure pod anti-affinity rules
- Use multiple availability zones

### Security
- Enable TLS/SSL for Kafka
- Use RBAC for Kubernetes access
- Rotate credentials regularly
- Use network policies

### Monitoring
- Set up Prometheus for metrics
- Configure alerts for critical issues
- Use Grafana for visualization
- Enable centralized logging

### Backup
- Regular PVC snapshots
- Backup Kafka topics
- Store configuration in version control

## Additional Resources

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [Confluent Platform](https://docs.confluent.io/)
- [Google Cloud Documentation](https://cloud.google.com/docs)
