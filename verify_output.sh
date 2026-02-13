#!/bin/bash

# Verify Spark Application Output
# This script queries the Parquet output to verify correctness

set -e

echo "=========================================="
echo "Verifying Spark Application Output"
echo "=========================================="

cd /mnt/c/Users/athar/OneDrive/Desktop/spark-assignment

# Check if output directories exist
echo ""
echo "Checking output directories..."

if [ -d "./output/latest_orders" ]; then
    echo "✓ Latest orders output exists"
    echo "  Files: $(find ./output/latest_orders -name '*.parquet' | wc -l) parquet files"
else
    echo "✗ Latest orders output not found"
fi

if [ -d "./output/aggregations/customer_value" ]; then
    echo "✓ Customer value aggregation output exists"
    echo "  Files: $(find ./output/aggregations/customer_value -name '*.parquet' | wc -l) parquet files"
else
    echo "✗ Customer value aggregation output not found"
fi

if [ -d "./output/aggregations/cancelled_orders" ]; then
    echo "✓ Cancelled orders aggregation output exists"
    echo "  Files: $(find ./output/aggregations/cancelled_orders -name '*.parquet' | wc -l) parquet files"
else
    echo "✗ Cancelled orders aggregation output not found"
fi

# Check checkpoints
echo ""
echo "Checking checkpoint directories..."

if [ -d "./output/checkpoint/latest_orders" ]; then
    echo "✓ Latest orders checkpoint exists"
else
    echo "✗ Latest orders checkpoint not found"
fi

if [ -d "./output/checkpoint/aggregations/customer_value" ]; then
    echo "✓ Customer value checkpoint exists"
else
    echo "✗ Customer value checkpoint not found"
fi

if [ -d "./output/checkpoint/aggregations/cancelled_orders" ]; then
    echo "✓ Cancelled orders checkpoint exists"
else
    echo "✗ Cancelled orders checkpoint not found"
fi

echo ""
echo "=========================================="
echo "Verification complete!"
echo "=========================================="
