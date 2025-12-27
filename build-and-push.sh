#!/bin/bash

# Build and push Docker image to Google Container Registry
# Usage: ./build-and-push.sh [version]

set -e

# Configuration
PROJECT_ID="double-arbor-475907-s5"
IMAGE_NAME="big-data-project-aero"
VERSION=${1:-latest}
REGISTRY="gcr.io"
FULL_IMAGE_NAME="${REGISTRY}/${PROJECT_ID}/${IMAGE_NAME}:${VERSION}"

echo "================================="
echo "Building Docker Image"
echo "================================="
echo "Project ID: ${PROJECT_ID}"
echo "Image Name: ${IMAGE_NAME}"
echo "Version: ${VERSION}"
echo "Full Image: ${FULL_IMAGE_NAME}"
echo "================================="

# Authenticate with GCP (if needed)
echo "Authenticating with GCP..."
gcloud auth configure-docker ${REGISTRY}

# Build the Docker image
echo "Building Docker image..."
docker build -t ${FULL_IMAGE_NAME} .

# Also tag as latest
if [ "$VERSION" != "latest" ]; then
    docker tag ${FULL_IMAGE_NAME} ${REGISTRY}/${PROJECT_ID}/${IMAGE_NAME}:latest
fi

# Push to GCR
echo "Pushing image to Google Container Registry..."
docker push ${FULL_IMAGE_NAME}

if [ "$VERSION" != "latest" ]; then
    docker push ${REGISTRY}/${PROJECT_ID}/${IMAGE_NAME}:latest
fi

echo "================================="
echo "Build and push completed successfully!"
echo "Image: ${FULL_IMAGE_NAME}"
echo "================================="
