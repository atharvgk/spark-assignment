from pyspark.sql import SparkSession
from pyspark.sql.functions import(
    col, from_json, window, sum as _sum, count,
    to_timestamp, expr, row_number, struct, to_date
)
from pyspark.sql.types import(
    StructType, StructField, StringType,
    DoubleType, IntegerType, TimestampType
)
from pyspark.sql.window import Window

import logging
import os
import sys
import signal

# Suppress Py4J / logging noise
logging.getLogger().setLevel(logging.FATAL)

#configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "order_events"
CHECKPOINT_BASE_PATH = "./output/checkpoint"
OUTPUT_BASE_PATH = "./output"
WATERMARK_DELAY = "10 minutes"
TRIGGER_INTERVAL = "30 seconds"

#schema definition
#StructField(name,dataType,nullable)
order_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DoubleType(), False),
    StructField("event_time", StringType(), False)
])

#spark session initialization
#Note: The Kafka package is provided via spark-submit --packages flag.

def create_spark_session():
    return (
        SparkSession.builder
        .appName("OrderEventsStreamingPipeline")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )

#1)streaming ingestion & parsing -> Read and parse events from Kafka topic.
# Returns: DataFrame with parsed order events and event_time as TimestampType.

def read_kafka_stream(spark):
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    
    parsed_df = (
        kafka_df
        .select(from_json(col("value").cast("string"), order_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
    )
    
    return parsed_df

#2)stateful processing and correctness -> Latest State per Order
#maintain the latest state per order_id using aggregation.
# Returns: DataFrame with latest state per order_id.

def process_latest_state(parsed_df):
    from pyspark.sql.functions import max as _max
    
    deduped_df = (
        parsed_df
        .withWatermark("event_time", WATERMARK_DELAY)
        .dropDuplicates(["order_id", "event_time"])
    )
    
    latest_state_df = (
        deduped_df
        .groupBy(window(col("event_time"), "5 minutes"), col("order_id"))
        .agg(
            _max(struct(
                col("event_time"),
                col("customer_id"),
                col("product_id"),
                col("event_type"),
                col("quantity"),
                col("price")
            )).alias("latest_struct")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("order_id"),
            col("latest_struct.event_time").alias("event_time"),
            col("latest_struct.customer_id").alias("customer_id"),
            col("latest_struct.product_id").alias("product_id"),
            col("latest_struct.event_type").alias("event_type"),
            col("latest_struct.quantity").alias("quantity"),
            col("latest_struct.price").alias("price")
        )
        .withColumn("date", to_date(col("event_time")))
    )
    
    return latest_state_df

#3)windowed aggregations
#return customer_value_df, cancelled_count_df

def compute_windowed_aggregations(parsed_df):
    deduped_df = (
        parsed_df
        .withWatermark("event_time", WATERMARK_DELAY)
        .dropDuplicates(["order_id", "event_time"])
    )
    
    customer_value_df = (
        deduped_df
        .filter(col("event_type") != "CANCELLED")
        .withColumn("order_value", col("price") * col("quantity"))
        .groupBy(window(col("event_time"), "5 minutes"), col("customer_id"))
        .agg(_sum("order_value").alias("total_order_value"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("customer_id"),
            col("total_order_value")
        )
        .withColumn("date", to_date(col("window_start")))
    )
    
    cancelled_count_df = (
        deduped_df
        .filter(col("event_type") == "CANCELLED")
        .groupBy(window(col("event_time"), "5 minutes"))
        .agg(count("*").alias("cancelled_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("cancelled_count")
        )
        .withColumn("date", to_date(col("window_start")))
    )
    
    return customer_value_df, cancelled_count_df

#4)Output and storage
#write latest order state to parquet with checkpointing

def write_latest_state_stream(latest_state_df):
    query = (
        latest_state_df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", f"{OUTPUT_BASE_PATH}/latest_orders")
        .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/latest_orders")
        .partitionBy("date")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    return query

#write windowed aggregations to parquet with checkpointing
def write_aggregation_streams(customer_value_df, cancelled_count_df):
    # Customer value aggregation
    customer_query = (
        customer_value_df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", f"{OUTPUT_BASE_PATH}/aggregations/customer_value")
        .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/aggregations/customer_value")
        .partitionBy("date")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    # Cancelled orders count
    cancelled_query = (
        cancelled_count_df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", f"{OUTPUT_BASE_PATH}/aggregations/cancelled_orders")
        .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/aggregations/cancelled_orders")
        .partitionBy("date")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    
    return customer_query, cancelled_query

def main():
    print("=" * 80)
    print("Starting Spark Structured Streaming Application")
    print("=" * 80)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    #SIGINT handler (reliable Ctrl+C)
    def shutdown_handler(signum, frame):
        print("\n" + "=" * 80)
        print("Gracefully shutting down streaming queries...")
        print("=" * 80)

        try:
            spark.sparkContext.setLogLevel("FATAL")

            for query in spark.streams.active:
                try:
                    query.stop()
                except Exception:
                    pass

            spark.stop()

        except Exception:
            pass

        print("Shutdown complete.")

        sys.stderr = open(os.devnull, 'w')
        sys.stdout = open(os.devnull, 'w')

        os._exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"Watermark Delay: {WATERMARK_DELAY}")
    print(f"Trigger Interval: {TRIGGER_INTERVAL}")
    print(f"Output Path: {OUTPUT_BASE_PATH}")
    print(f"Checkpoint Path: {CHECKPOINT_BASE_PATH}")
    print("=" * 80)
    
    print("Reading from Kafka...")
    parsed_df = read_kafka_stream(spark)
    
    print("Setting up latest state stream...")
    latest_state_df = process_latest_state(parsed_df)
    latest_state_query = write_latest_state_stream(latest_state_df)
    
    print("Setting up windowed aggregation streams...")
    customer_value_df, cancelled_count_df = compute_windowed_aggregations(parsed_df)
    customer_query, cancelled_query = write_aggregation_streams(customer_value_df, cancelled_count_df)
    
    print("=" * 80)
    print("All streaming queries started successfully!")
    print("=" * 80)
    print("\nPress Ctrl+C to stop the application.")
    print("=" * 80)

    #No try/except needed now
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
