#!/bin/bash

# Fix Kafka Hostname Resolution for WSL
# This script adds the 'kafka' hostname to /etc/hosts pointing to localhost

set -e

echo "=========================================="
echo "Fixing Kafka Hostname Resolution"
echo "=========================================="

# Check if 'kafka' entry already exists
if grep -q "127.0.0.1.*kafka" /etc/hosts; then
    echo "✓ 'kafka' hostname already configured in /etc/hosts"
else
    echo "Adding 'kafka' hostname to /etc/hosts..."
    echo "127.0.0.1 kafka" | sudo tee -a /etc/hosts > /dev/null
    echo "✓ Added 'kafka' hostname to /etc/hosts"
fi

echo ""
echo "Verifying configuration..."
grep "kafka" /etc/hosts

echo ""
echo "=========================================="
echo "Configuration complete!"
echo "=========================================="
echo ""
echo "You can now run the Spark application:"
echo "  ./run_spark.sh"
