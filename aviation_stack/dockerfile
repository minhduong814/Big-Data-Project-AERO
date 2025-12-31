# Use the specific Python version requested
FROM python:3.10.18-slim-bullseye

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install OpenJDK and dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk-headless \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install PySpark and python-dotenv
RUN pip install --no-cache-dir pyspark==3.5.4 python-dotenv

# Create working directory
WORKDIR /app

RUN mkdir -p /tmp/checkpoints/aviation_stack/


# Copy the local jar file (ensure it's in your local ./jar folder)
COPY jar /app/jar

# Copy credentials and environment files
COPY .env /app/.env

COPY .credentials.json /app/.credentials.json

# Copy the script
COPY spark_consumer.py /app/main.py

# Create checkpoint directory

# Run the application
CMD ["python", "main.py"]
