#!/bin/bash
#
# Cloud Deployment Script for Google Cloud Platform
# Deploys the AERO pipeline to GCP (GKE or Cloud Run)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Configuration
PROJECT_ID="${GCP_PROJECT_ID:-double-arbor-475907-s5}"
REGION="${GCP_REGION:-us-central1}"
CLUSTER_NAME="${GKE_CLUSTER_NAME:-aero-cluster}"
NAMESPACE="${K8S_NAMESPACE:-aero}"
IMAGE_NAME="gcr.io/${PROJECT_ID}/aero-pipeline"
IMAGE_TAG="${IMAGE_TAG:-latest}"

cd "$PROJECT_DIR"

echo "=========================================="
echo "AERO Pipeline - Cloud Deployment"
echo "=========================================="
echo "Project ID: $PROJECT_ID"
echo "Region: $REGION"
echo "Cluster: $CLUSTER_NAME"
echo "Namespace: $NAMESPACE"
echo "Image: $IMAGE_NAME:$IMAGE_TAG"
echo "=========================================="
echo ""

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "❌ Error: gcloud CLI is not installed"
    echo "Install it from: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Set project
echo "🔧 Setting GCP project..."
gcloud config set project "$PROJECT_ID"

# Enable required APIs
echo "🔧 Enabling required GCP APIs..."
gcloud services enable \
    container.googleapis.com \
    containerregistry.googleapis.com \
    bigquery.googleapis.com \
    storage-api.googleapis.com \
    compute.googleapis.com

# Build and push Docker image
echo "📦 Building Docker image..."
docker build -t "$IMAGE_NAME:$IMAGE_TAG" -f "$PROJECT_DIR/Dockerfile" "$PROJECT_DIR"

echo "📤 Pushing image to Google Container Registry..."
docker push "$IMAGE_NAME:$IMAGE_TAG"

# Check if we're deploying to GKE or Cloud Run
DEPLOYMENT_TYPE="${DEPLOYMENT_TYPE:-gke}"

if [ "$DEPLOYMENT_TYPE" == "gke" ]; then
    echo ""
    echo "🚀 Deploying to GKE..."
    
    # Check if cluster exists
    if ! gcloud container clusters describe "$CLUSTER_NAME" --region="$REGION" &> /dev/null; then
        echo "Creating GKE cluster..."
        gcloud container clusters create "$CLUSTER_NAME" \
            --region="$REGION" \
            --num-nodes=3 \
            --machine-type=e2-standard-4 \
            --enable-autoscaling \
            --min-nodes=1 \
            --max-nodes=5
    fi
    
    # Get cluster credentials
    echo "Getting cluster credentials..."
    gcloud container clusters get-credentials "$CLUSTER_NAME" --region="$REGION"
    
    # Create namespace
    kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
    
    # Create secrets
    if [ -f "$PROJECT_DIR/key/double-arbor-475907-s5-a5863b0c230a.json" ]; then
        echo "Creating GCP credentials secret..."
        kubectl create secret generic gcp-credentials \
            --from-file=key.json="$PROJECT_DIR/key/double-arbor-475907-s5-a5863b0c230a.json" \
            --namespace="$NAMESPACE" \
            --dry-run=client -o yaml | kubectl apply -f -
    fi
    
    # Deploy using kubectl
    if [ -d "$PROJECT_DIR/kubernetes" ]; then
        echo "Deploying Kubernetes resources..."
        # Update image in deployment files
        sed "s|IMAGE_PLACEHOLDER|$IMAGE_NAME:$IMAGE_TAG|g" "$PROJECT_DIR/kubernetes/deployment.yaml" | kubectl apply -f - -n "$NAMESPACE"
    else
        echo "⚠️  Kubernetes deployment files not found in kubernetes/ directory"
    fi
    
    echo ""
    echo "✅ Deployment to GKE completed!"
    echo ""
    echo "📊 Useful commands:"
    echo "   View pods:        kubectl get pods -n $NAMESPACE"
    echo "   View logs:        kubectl logs -f <pod-name> -n $NAMESPACE"
    echo "   View services:    kubectl get services -n $NAMESPACE"
    
elif [ "$DEPLOYMENT_TYPE" == "cloudrun" ]; then
    echo ""
    echo "🚀 Deploying to Cloud Run..."
    
    SERVICE_NAME="aero-pipeline"
    
    gcloud run deploy "$SERVICE_NAME" \
        --image "$IMAGE_NAME:$IMAGE_TAG" \
        --platform managed \
        --region "$REGION" \
        --set-env-vars "AERO_ENVIRONMENT=cloud,GCP_PROJECT_ID=$PROJECT_ID" \
        --memory 2Gi \
        --cpu 2 \
        --timeout 3600 \
        --max-instances 10 \
        --allow-unauthenticated
    
    echo ""
    echo "✅ Deployment to Cloud Run completed!"
    SERVICE_URL=$(gcloud run services describe "$SERVICE_NAME" --region="$REGION" --format="value(status.url)")
    echo "Service URL: $SERVICE_URL"
else
    echo "❌ Unknown deployment type: $DEPLOYMENT_TYPE"
    echo "Set DEPLOYMENT_TYPE to 'gke' or 'cloudrun'"
    exit 1
fi

echo ""
echo "✅ Cloud deployment completed!"
