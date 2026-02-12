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
