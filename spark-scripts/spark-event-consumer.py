import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType

# Load environment variables
dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"

# Initialize SparkContext and SparkSession
sparkcontext = pyspark.SparkContext.getOrCreate(
    conf=(pyspark.SparkConf().setAppName("DibimbingStreaming").setMaster(spark_host))
)
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# Define schema for the incoming data
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("furniture", StringType(), True),
    StructField("color", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("ts", LongType(), True)
])

# Ensure checkpoint directories exist and are writable
checkpoint_base_path = "/opt/spark-checkpoints"
os.makedirs(checkpoint_base_path, exist_ok=True)
hourly_checkpoint_path = os.path.join(checkpoint_base_path, "hourly")
minute_checkpoint_path = os.path.join(checkpoint_base_path, "minute")
ten_minute_checkpoint_path = os.path.join(checkpoint_base_path, "ten_minute")
os.makedirs(hourly_checkpoint_path, exist_ok=True)
os.makedirs(minute_checkpoint_path, exist_ok=True)
os.makedirs(ten_minute_checkpoint_path, exist_ok=True)

# Read the streaming data from Kafka
stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

# Transform the data: parse JSON and apply the schema
parsed_df = (
    stream_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("timestamp", (col("ts") / 1000).cast(TimestampType()))
)

# Aggregate for hourly, minute, and 10 minutes total purchase
hourly_agg = (
    parsed_df
    .groupBy(window(col("timestamp"), "1 hour"))
    .sum("price")
    .withColumnRenamed("sum(price)", "total_purchase")
)

minute_agg = (
    parsed_df
    .groupBy(window(col("timestamp"), "1 minute"))
    .sum("price")
    .withColumnRenamed("sum(price)", "total_purchase")
)

ten_minute_agg = (
    parsed_df
    .groupBy(window(col("timestamp"), "10 minutes"))
    .sum("price")
    .withColumnRenamed("sum(price)", "total_purchase")
)

# Write the aggregated data to the console
hourly_query = (
    hourly_agg
    .selectExpr("window.start as timestamp", "total_purchase")
    .writeStream
    .queryName("hourly_agg")
    .format("console")
    .option("checkpointLocation", hourly_checkpoint_path)
    .outputMode("complete")
    .start()
)

minute_query = (
    minute_agg
    .selectExpr("window.start as timestamp", "total_purchase")
    .writeStream
    .queryName("minute_agg")
    .format("console")
    .option("checkpointLocation", minute_checkpoint_path)
    .outputMode("complete")
    .start()
)

ten_minute_query = (
    ten_minute_agg
    .selectExpr("window.start as timestamp", "total_purchase")
    .writeStream
    .queryName("ten_minute_agg")
    .format("console")
    .option("checkpointLocation", ten_minute_checkpoint_path)
    .outputMode("complete")
    .start()
)

hourly_query.awaitTermination()
minute_query.awaitTermination()
ten_minute_query.awaitTermination()
