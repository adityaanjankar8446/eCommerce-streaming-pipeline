from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = (
    SparkSession.builder
    .appName("Silver_Transformations")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark Session started")

# ── Silver Orders ────────────────────────────────────────
print("Processing silver orders...")

orders_df = (
    spark.readStream
    .format("delta")
    .load("/opt/data/delta/bronze/orders")
)

silver_orders = (
    orders_df
    # Cast date string to timestamp
    .withColumn("order_date", to_timestamp(col("order_date")))
    # Extract date parts
    .withColumn("order_year",  year(col("order_date")))
    .withColumn("order_month", month(col("order_date")))
    .withColumn("order_day",   dayofmonth(col("order_date")))
    # Add ingestion timestamp
    .withColumn("ingested_at", current_timestamp())
    # Filter nulls on key fields
    .filter(
        col("order_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("product_id").isNotNull()
    )
)



(
    silver_orders.writeStream
    .format("delta")
    .option("checkpointLocation", "/opt/checkpoints/silver_orders")
    .outputMode("append")
    .trigger(availableNow=True)
    .start("/opt/data/delta/silver/orders")
    .awaitTermination()
)
print("✅ Silver orders complete")

# ── Silver Customers ─────────────────────────────────────
print("Processing silver customers...")

customers_df = (
    spark.readStream
    .format("delta")
    .load("/opt/data/delta/bronze/customers")
)

silver_customers = (
    customers_df
    # Combine first + last name
    .withColumn("full_name",
        concat(initcap(col("first_name")), lit(" "), initcap(col("last_name")))
    )
    # Standardize city and street casing
    .withColumn("city",   initcap(col("city")))
    .withColumn("street", initcap(col("street")))
    # Validate email
    .withColumn("is_valid_email", col("email").contains("@"))
    # Add ingestion timestamp
    .withColumn("ingested_at", current_timestamp())
    # Filter nulls
    .filter(col("customer_id").isNotNull())
)



(
    silver_customers.writeStream
    .format("delta")
    .option("checkpointLocation", "/opt/checkpoints/silver_customers")
    .outputMode("append")
    .trigger(availableNow=True)
    .start("/opt/data/delta/silver/customers")
    .awaitTermination()
)
print("✅ Silver customers complete")

# ── Silver Products ──────────────────────────────────────
print("Processing silver products...")

products_df = (
    spark.readStream
    .format("delta")
    .load("/opt/data/delta/bronze/products")
)

silver_products = (
    products_df
    # Cast price to Decimal
    .withColumn("price", col("price").cast(DecimalType(10, 2)))
    # Standardize category
    .withColumn("category", initcap(col("category")))
    # Add price category
    .withColumn("price_category",
        when(col("price") < 20,  "Budget")
        .when(col("price") < 100, "Mid-Range")
        .otherwise("Premium")
    )
    # Add rating category
    .withColumn("rating_category",
        when(col("rating_rate") >= 4.5, "Excellent")
        .when(col("rating_rate") >= 3.5, "Good")
        .when(col("rating_rate") >= 2.5, "Average")
        .otherwise("Poor")
    )
    # Add ingestion timestamp
    .withColumn("ingested_at", current_timestamp())
    # Filter nulls
    .filter(col("product_id").isNotNull())
)




(
    silver_products.writeStream
    .format("delta")
    .option("checkpointLocation", "/opt/checkpoints/silver_products")
    .outputMode("append")
    .trigger(availableNow=True)
    .start("/opt/data/delta/silver/products")
    .awaitTermination()
)
print("✅ Silver products complete")

print("✅ All silver tables complete")
spark.stop()