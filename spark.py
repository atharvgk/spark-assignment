from pyspark.sql import SparkSession
from pyspark.sql.functions import(
    col,from_json,window,sum as _sum,count,to_timestamp,expr,row_number
)
from pyspark.sql.types import(
    StructType,StructField,StringType,DoubleType,IntegerType,TimestampType
)
from pyspark.sql.window import Window

#configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "order_events"
CHECKPOINT_LOCATION = "./output/checkpoint"
OUTPUT_PATH = "./output"
WATERMARK_DELAY = "10 minutes"
TRIGGER_INTERVAL = "10 seconds"

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
    return (SparkSession.builder.appName("OrderEventsStreamingPipeline").config("spark.sql.shuffle.partitions", "4").config("spark.sql.streaming.schemaInference", "false").getOrCreate())

#1)streaming ingestion & parsing -> Read and parse events from Kafka topic.
# Returns: DataFrame with parsed order events and event_time as TimestampType.
def read_kafka_stream(spark):
    # Read from Kafka
    kafka_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load())
    
    # Parse JSON and convert event_time to timestamp
    parsed_df = (kafka_df
                 .select(from_json(col("value").cast("string"), order_schema).alias("data"))
                 .select("data.*")
                 .withColumn("event_time", to_timestamp(col("event_time"))))
    
    return parsed_df


#2)stateful processing and correctness -> Latest State per Order
#maintain the latest state per order_id using aggregation.
# Returns: DataFrame with latest state per order_id.
def process_latest_state(parsed_df):
    from pyspark.sql.functions import max as _max, first, struct
    
    # Apply watermark and deduplicate
    deduped_df = (parsed_df
                  .withWatermark("event_time", WATERMARK_DELAY)
                  .dropDuplicates(["order_id", "event_time"]))
    
    # Group by order_id and get the latest event
    # We use a 5-minute window to make this compatible with append mode
    latest_state_df = (deduped_df
                       .groupBy(
                           window(col("event_time"), "5 minutes"),
                           col("order_id")
                       )
                       .agg(
                           _max("event_time").alias("latest_event_time"),
                           first("customer_id").alias("customer_id"),
                           first("product_id").alias("product_id"),
                           first("event_type").alias("event_type"),
                           first("quantity").alias("quantity"),
                           first("price").alias("price")
                       )
                       .select(
                           col("window.start").alias("window_start"),
                           col("window.end").alias("window_end"),
                           col("order_id"),
                           col("latest_event_time").alias("event_time"),
                           col("customer_id"),
                           col("product_id"),
                           col("event_type"),
                           col("quantity"),
                           col("price")
                       ))
    
    return latest_state_df