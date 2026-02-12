#!/bin/bash

# Cleanup Script for Spark Streaming Output
# This script removes all generated Parquet files and checkpoints

set -e

echo "=========================================="
echo "Spark Streaming Output Cleanup"
echo "=========================================="

cd /mnt/c/Users/athar/OneDrive/Desktop/spark-streaming-kafka-assignment

# Check if output directory exists
if [ ! -d "./output" ]; then
    echo "✓ No output directory found - nothing to clean"
    exit 0
fi

echo ""
echo "Current output size:"
du -sh ./output 2>/dev/null || echo "Unable to calculate size"

echo ""
echo "WARNING: This will delete:"
echo "  - All Parquet files in ./output/"
echo "  - All checkpoints in ./output/checkpoints/"
echo ""
read -p "Are you sure you want to continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Cleanup cancelled."
    exit 0
fi

echo ""
echo "Removing output directory..."
rm -rf ./output

echo "✓ Output directory removed"
echo ""
echo "=========================================="
echo "Cleanup Complete!"
echo "=========================================="
echo ""
echo "Next run will start fresh from earliest Kafka offsets."
echo "To run the application again:"
echo "  ./run_spark.sh"
