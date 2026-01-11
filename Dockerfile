FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    default-jdk \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY config/ ./config/
COPY orchestration/ ./orchestration/

# Set Python path
ENV PYTHONPATH=/app

# Default command
CMD ["python", "src/extract/kafka_producer.py"]