import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Dibimbing") \
    .master("local") \
    .config("spark.jars", "/opt/airflow/postgresql-42.2.18.jar") \
    .getOrCreate()

# Define schema for purchase events
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

# Read streaming data from Kafka
raw_streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "purchasing_events") \
    .load()

# Parse JSON data
parsed_streaming_df = raw_streaming_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert timestamp to TimestampType
parsed_streaming_df = parsed_streaming_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Aggregate total purchase per hour
hourly_agg = parsed_streaming_df.groupBy(window(col("timestamp"), "1 hour")) \
    .agg({'price': 'sum'}) \
    .withColumnRenamed("sum(price)", "total_purchase")

# Write results to console
query = hourly_agg.selectExpr("window.start as timestamp", "total_purchase as running_total") \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
