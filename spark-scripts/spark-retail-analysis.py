import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, when, avg, dense_rank
from pyspark.sql.window import Window

# Configuration
postgres_host = "dataeng-postgres"
postgres_dw_db = "warehouse"
postgres_user = "user"
postgres_password = "password"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RetailAnalysis") \
    .config("spark.jars", "/opt/airflow/postgresql-42.2.18.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# JDBC properties
jdbc_url = f"jdbc:postgresql://{postgres_host}/{postgres_dw_db}"
jdbc_properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified",
}

# Read retail data from Postgres
retail_df = spark.read.jdbc(jdbc_url, "public.retail", properties=jdbc_properties)

# Count rows with null CustomerID
null_customer_count = retail_df.filter(col("CustomerID").isNull()).count()
print(f"Number of rows with null CustomerID: {null_customer_count}")

# Separate data with and without CustomerID
retail_with_id_df = retail_df.filter(col("CustomerID").isNotNull())
retail_without_id_df = retail_df.filter(col("CustomerID").isNull())

def calculate_monthly_revenue(df):
    return df.groupBy("InvoiceDate").agg(sum(col("Quantity") * col("UnitPrice")).alias("MonthlyRevenue"))

def calculate_customer_retention(df):
    customer_orders = df.groupBy("CustomerID", "InvoiceDate").count()
    return customer_orders.groupBy("CustomerID").agg(count(when(col("count") > 1, True)).alias("RetentionCount"))

def calculate_churn_rate(df, retention_df):
    total_customers = df.select("CustomerID").distinct().count()
    churn_customers = retention_df.filter(col("RetentionCount") == 0).count()
    return churn_customers / total_customers

def calculate_average_order_value(df):
    return df.groupBy("CustomerID").agg(avg(col("Quantity") * col("UnitPrice")).alias("AverageOrderValue"))

def rank_customers_by_revenue(df):
    customer_revenue = df.groupBy("CustomerID").agg(sum(col("Quantity") * col("UnitPrice")).alias("TotalRevenue"))
    window = Window.orderBy(col("TotalRevenue").desc())
    return customer_revenue.withColumn("Rank", dense_rank().over(window))

# Perform business-based analysis on data with CustomerID
monthly_revenue = calculate_monthly_revenue(retail_with_id_df)
customer_retention = calculate_customer_retention(retail_with_id_df)
churn_rate = calculate_churn_rate(retail_with_id_df, customer_retention)
average_order_value = calculate_average_order_value(retail_with_id_df)
customer_ranking = rank_customers_by_revenue(retail_with_id_df)

# Show results for data with CustomerID
print(f"Churn Rate: {churn_rate:.2%}")
print("Monthly Revenue for Customers with Valid CustomerID:")
monthly_revenue.show()

print("Customer Retention for Customers with Valid CustomerID:")
customer_retention.show()

print("Average Order Value for Customers with Valid CustomerID:")
average_order_value.show()

print("Customer Ranking by Total Revenue for Customers with Valid CustomerID:")
customer_ranking.show()

# Analyze data without CustomerID
print("Monthly Revenue for Transactions without CustomerID:")
monthly_revenue_null = calculate_monthly_revenue(retail_without_id_df)
monthly_revenue_null.show()

# Save results to Postgres
monthly_revenue.write.jdbc(jdbc_url, "monthly_revenue", mode="overwrite", properties=jdbc_properties)
customer_retention.write.jdbc(jdbc_url, "customer_retention", mode="overwrite", properties=jdbc_properties)
average_order_value.write.jdbc(jdbc_url, "average_order_value", mode="overwrite", properties=jdbc_properties)
customer_ranking.write.jdbc(jdbc_url, "customer_ranking", mode="overwrite", properties=jdbc_properties)
monthly_revenue_null.write.jdbc(jdbc_url, "monthly_revenue_null", mode="overwrite", properties=jdbc_properties)

# Stop Spark session
spark.stop()