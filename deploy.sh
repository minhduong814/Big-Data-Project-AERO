#!/bin/bash

# Deploy the Big Data Project to Kubernetes
# Usage: ./deploy.sh [environment]

set -e

ENVIRONMENT=${1:-production}
NAMESPACE=${2:-default}

echo "================================="
echo "Deploying Big Data Project"
echo "================================="
echo "Environment: ${ENVIRONMENT}"
echo "Namespace: ${NAMESPACE}"
echo "================================="

# Create namespace if it doesn't exist
kubectl create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -

# Create GCP credentials secret
echo "Creating GCP credentials secret..."
if [ -f "key/double-arbor-475907-s5-a5863b0c230a.json" ]; then
    kubectl create secret generic gcp-credentials \
        --from-file=key.json=key/double-arbor-475907-s5-a5863b0c230a.json \
        --namespace=${NAMESPACE} \
        --dry-run=client -o yaml | kubectl apply -f -
else
    echo "Warning: GCP credentials file not found. Skipping secret creation."
fi

# Create image pull secret for GCR
echo "Creating image pull secret..."
kubectl create secret docker-registry gcr-json-key \
    --docker-server=gcr.io \
    --docker-username=_json_key \
    --docker-password="$(cat key/double-arbor-475907-s5-a5863b0c230a.json)" \
    --docker-email=user@example.com \
    --namespace=${NAMESPACE} \
    --dry-run=client -o yaml | kubectl apply -f - 2>/dev/null || echo "Image pull secret already exists"

# Deploy Kafka infrastructure
echo "Deploying Kafka infrastructure..."
kubectl apply -f kubernetes/kafka-infrastructure.yaml -n ${NAMESPACE}

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
kubectl wait --for=condition=ready pod -l app=kafka -n ${NAMESPACE} --timeout=300s || true

# Deploy application components
echo "Deploying application components..."
kubectl apply -f kubernetes/deployment.yaml -n ${NAMESPACE}

# Deploy Spark infrastructure
echo "Deploying Spark infrastructure..."
kubectl apply -f kubernetes/spark-deployment.yaml -n ${NAMESPACE}

echo "================================="
echo "Deployment completed!"
echo "================================="
echo ""
echo "Check deployment status:"
echo "  kubectl get pods -n ${NAMESPACE}"
echo ""
echo "View logs:"
echo "  kubectl logs -f deployment/kafka-producer -n ${NAMESPACE}"
echo "  kubectl logs -f deployment/kafka-consumer -n ${NAMESPACE}"
echo ""
echo "Port forward to access services locally:"
echo "  kubectl port-forward svc/kafka-service 9092:9092 -n ${NAMESPACE}"
echo "  kubectl port-forward svc/schema-registry 8081:8081 -n ${NAMESPACE}"
echo "================================="
