FROM python:3.9-slim

# Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

# Copy requirements
COPY requirement.txt .
# Add missing requirements
RUN pip install --no-cache-dir -r requirement.txt && \
    pip install --no-cache-dir prefect google-cloud-storage google-cloud-bigquery

# Copy the rest of the application
COPY . .

# Set PYTHONPATH
ENV PYTHONPATH=/app

# Default command (can be overridden)
CMD ["python", "flow/collect_data_flow.py"]
