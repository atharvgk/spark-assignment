"""
Improved Parquet verification script with error handling.
Handles cases where directories exist but no data files are present yet.
"""

from pyspark.sql import SparkSession
import sys
import os

# Initialize Spark
spark = SparkSession.builder.appName("QuickVerify").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("=" * 80)
print("Spark Streaming Application - Output Verification")
print("=" * 80)

def check_output(path, name):
    """Check if output exists and display sample data."""
    print(f"\n{name}:")
    
    # Check if directory exists
    if not os.path.exists(path):
        print(f"   ✗ Directory not found: {path}")
        return False
    
    # Allow Spark to read partitions automatically 
    # (Removed manual check for .parquet files in root dir which fails for partitioned data)
    
    try:
        df = spark.read.parquet(path)
        count = df.count()
        print(f"   ✓ Total records: {count}")
        
        if count > 0:
            print(f"\n   Sample (first 5 rows):")
            df.show(5, truncate=False)
        else:
            print(f"   ⚠ No records yet (windows may not have closed)")
        
        return True
    except Exception as e:
        print(f"   ✗ Error reading Parquet: {str(e)}")
        print(f"   → Files may be locked (application still writing)")
        return False

# Check all three outputs
results = []
results.append(check_output("./output/latest_orders", "1. Latest Orders"))
results.append(check_output("./output/aggregations/customer_value", "2. Customer Value Aggregation"))
results.append(check_output("./output/aggregations/cancelled_orders", "3. Cancelled Orders Count"))

print("\n" + "=" * 80)
if all(results):
    print("✓ Verification Complete - All outputs generated successfully!")
elif any(results):
    print("⚠ Partial Success - Some outputs are ready, others still processing")
else:
    print("⚠ No data available yet")
    print("\nPossible reasons:")
    print("  1. Application just started (wait 15-20 minutes for windows to close)")
    print("  2. Application is currently running (stop it first with Ctrl+C)")
    print("  3. Checkpoints exist but no data written yet")
print("=" * 80)

spark.stop()
