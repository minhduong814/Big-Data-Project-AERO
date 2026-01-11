.PHONY: help install build up down logs test clean extract load transform visualize deploy

help:
	@echo "AERO Big Data Pipeline - Makefile Commands"
	@echo "==========================================="
	@echo "install        - Install Python dependencies"
	@echo "build          - Build Docker images"
	@echo "up             - Start all services"
	@echo "down           - Stop all services"
	@echo "logs           - View logs from all services"
	@echo "test           - Run tests"
	@echo "clean          - Clean up temporary files"
	@echo "extract        - Run data extraction"
	@echo "load           - Run data loading"
	@echo "transform      - Run data transformation"
	@echo "visualize      - Run visualization"
	@echo "pipeline       - Run complete pipeline"
	@echo "deploy         - Deploy to Kubernetes"

install:
	pip install -r requirements.txt

build:
	docker build -t aero-pipeline:latest .

up:
	cd orchestration && docker compose up -d
	@echo "Waiting for services to start..."
	sleep 10
	@echo "Services started. Access:"
	@echo "  - Prefect UI: http://localhost:4200"
	@echo "  - Spark UI: http://localhost:8080"
	@echo "  - Kafka: localhost:9092"

down:
	cd orchestration && docker compose down

logs:
	cd orchestration && docker compose logs -f

test:
	pytest test/ -v

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
	rm -rf dashboards/*.png

extract:
	python src/main.py --mode extract

load:
	python src/main.py --mode load --max-messages 1000

transform:
	python src/main.py --mode transform

visualize:
	python src/main.py --mode visualize

pipeline:
	python src/main.py --mode all

deploy:
	./build-and-push.sh
	kubectl apply -f kubernetes/kafka-infrastructure.yaml
	kubectl apply -f kubernetes/deployment.yaml
	kubectl apply -f kubernetes/spark-deployment.yaml

setup-gcp:
	gcloud config set project double-arbor-475907-s5
	gcloud auth application-default login
