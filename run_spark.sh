#!/bin/bash

# Run Spark Structured Streaming Application
# This script should be executed from WSL

set -e

echo "=========================================="
echo "Spark Structured Streaming Application"
echo "=========================================="

# Navigate to project directory
cd /mnt/c/Users/athar/OneDrive/Desktop/spark-assignment

# Check if Kafka is accessible
echo "Checking Kafka connectivity..."
if nc -zv localhost 9092 2>&1 | grep -q "succeeded"; then
    echo "✓ Kafka is accessible at localhost:9092"
else
    echo "✗ Kafka is not accessible. Please start Docker Compose first."
    echo "  Run: docker compose up -d"
    exit 1
fi

# Run Spark application
echo ""
echo "Starting Spark Structured Streaming application..."
echo "=========================================="

/opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --driver-memory 1g \
  --executor-memory 1g \
  spark_app.py

echo ""
echo "Application stopped."
