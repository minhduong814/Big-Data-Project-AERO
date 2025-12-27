#!/bin/bash

# Installs the Spark BigQuery connector onto a Cloud Dataproc cluster.

set -euxo pipefail

install_kafka_connector() {
  local -r scala_version=$1
  local -r spark_version=$2

  if [[ -z "${SPARK_HOME}" ]]; then
    echo "ERROR: SPARK_HOME is not set. Please set SPARK_HOME before running this function."
    return 1
  fi

  local connector_dir="${SPARK_HOME}/jars"
  echo "Installing Spark Kafka connector and dependencies in ${connector_dir}..."

  # Ensure the target directory exists
  mkdir -p "${connector_dir}"

  # Define the URLs for required jar files
  declare -a jars=(
    "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${scala_version}/${spark_version}/spark-sql-kafka-0-10_${scala_version}-${spark_version}.jar"
    "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar"
    "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${scala_version}/${spark_version}/spark-token-provider-kafka-0-10_${scala_version}-${spark_version}.jar"
    "https://repo1.maven.org/maven2/org/apache/spark/spark-tags_${scala_version}/${spark_version}/spark-tags_${scala_version}-${spark_version}.jar"
    "https://repo1.maven.org/maven2/org/apache/spark/spark-sql_${scala_version}/${spark_version}/spark-sql_${scala_version}-${spark_version}.jar"
    "https://repo1.maven.org/maven2/org/apache/spark/spark-streaming_${scala_version}/${spark_version}/spark-streaming_${scala_version}-${spark_version}.jar"
  )

  # Download each jar file
  for jar_url in "${jars[@]}"; do
    echo "Downloading: ${jar_url}"
    wget -q -P "${connector_dir}" "${jar_url}" || {
      echo "ERROR: Failed to download ${jar_url}"
      return 1
    }
  done

  echo "Spark Kafka connector and dependencies installed successfully in ${connector_dir}."
}

# Fetch the Spark, Scala version
SPARK_VERSION=$(spark-submit --version 2>&1 | grep -o -P 'version \d+\.\d+\.\d+' | head -1 | awk '{print $2}')
SCALA_VERSION=$(spark-submit --version 2>&1 | grep -o -P 'Using Scala version \d+\.\d+' | awk '{print $4}')

# Call the function with dynamically fetched versions
install_kafka_connector "${SCALA_VERSION}" "${SPARK_VERSION}"