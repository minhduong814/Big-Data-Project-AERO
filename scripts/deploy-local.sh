#!/bin/bash
#
# Local Development Deployment Script
# Starts all services using Docker Compose for local testing
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_DIR/orchestration/docker-compose.yml"

cd "$PROJECT_DIR"

echo "=========================================="
echo "AERO Pipeline - Local Development Setup"
echo "=========================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Set environment
export AERO_ENVIRONMENT=local

# Clean up existing containers to avoid port conflicts
echo "🧹 Cleaning up existing containers..."
docker compose -f "$COMPOSE_FILE" down 2>/dev/null || true

# Clear Docker build cache to ensure fresh build with updated dependencies
echo "🗑️  Clearing Docker build cache..."
docker builder prune -f > /dev/null 2>&1 || true
echo "   Build cache cleared"

echo "📦 Building Docker images (no cache)..."
docker compose -f "$COMPOSE_FILE" build --no-cache

echo ""
echo "🚀 Starting infrastructure services..."
docker compose -f "$COMPOSE_FILE" up -d zookeeper kafka schema-registry spark-master spark-worker kafka-ui

echo ""
echo "⏳ Waiting for services to be ready..."
sleep 10

# Check if Kafka is ready
echo "Checking Kafka..."
timeout=60
counter=0
while ! docker compose -f "$COMPOSE_FILE" exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    sleep 2
    counter=$((counter + 2))
    if [ $counter -ge $timeout ]; then
        echo "❌ Timeout waiting for Kafka to be ready"
        exit 1
    fi
done
echo "✅ Kafka is ready"

# Create Kafka topics if they don't exist
echo ""
echo "📝 Creating Kafka topics..."
docker compose -f "$COMPOSE_FILE" exec -T kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic flights-raw --partitions 3 --replication-factor 1 2>&1 || echo "   Topic flights-raw may already exist"
docker compose -f "$COMPOSE_FILE" exec -T kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic flights-processed --partitions 3 --replication-factor 1 2>&1 || echo "   Topic flights-processed may already exist"
docker compose -f "$COMPOSE_FILE" exec -T kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic flights-aggregated --partitions 3 --replication-factor 1 2>&1 || echo "   Topic flights-aggregated may already exist"
echo "✅ Kafka topics ready"

echo ""
echo "✅ Infrastructure services started!"
echo ""
echo "📊 Service URLs:"
echo "   - Kafka UI:        http://localhost:8082"
echo "   - Spark Master UI: http://localhost:8080"
echo "   - Spark Worker UI: http://localhost:8083"
echo "   - Schema Registry: http://localhost:8081"
echo ""
echo "🔧 Useful commands:"
echo "   View logs:         docker compose -f $COMPOSE_FILE logs -f [service-name]"
echo "   Stop services:     docker compose -f $COMPOSE_FILE down"
echo "   Run pipeline:      docker compose -f $COMPOSE_FILE run --rm aero-pipeline python src/main.py --mode all"
echo "   Run extract only:  docker compose -f $COMPOSE_FILE run --rm aero-pipeline python src/main.py --mode extract"
echo "   Run transform:     docker compose -f $COMPOSE_FILE run --rm aero-pipeline python src/main.py --mode transform"
echo "   Run load:          docker compose -f $COMPOSE_FILE run --rm aero-pipeline python src/main.py --mode load"
echo ""
echo "📝 To run the pipeline manually:"
echo "   docker compose -f $COMPOSE_FILE run --rm aero-pipeline python src/main.py --mode all"
echo ""
