from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = (
    SparkSession.builder
    .appName("Bronze_Ingestion")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark Session started")

# ── Orders ──────────────────────────────────────────────
orders_schema = StructType([
    StructField("id", LongType()),
    StructField("userId", LongType()),
    StructField("date", StringType()),
    StructField("products", ArrayType(StructType([
        StructField("productId", LongType()),
        StructField("quantity", LongType())
    ])))
])

orders_df = (
    spark.readStream
    .format("json")
    .schema(orders_schema)
    .load("/opt/data/raw/orders")        # ← updated
)

orders_df = (
    orders_df
    .select(
        col("id").alias("order_id"),
        col("userId").alias("customer_id"),
        col("date").alias("order_date"),
        explode(col("products")).alias("product")
    )
    .select("order_id", "customer_id", "order_date",
            col("product.productId").alias("product_id"),
            col("product.quantity").alias("quantity"))
)

(
    orders_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/opt/checkpoints/bronze_orders")   # ← updated
    .outputMode("append")
    .trigger(availableNow=True)
    .start("/opt/data/delta/bronze/orders")                            # ← updated
    .awaitTermination()
)
print("✅ Bronze orders complete")

# ── Customers ────────────────────────────────────────────
customers_schema = StructType([
    StructField("id", LongType()),
    StructField("email", StringType()),
    StructField("username", StringType()),
    StructField("phone", StringType()),
    StructField("name", StructType([
        StructField("firstname", StringType()),
        StructField("lastname", StringType())
    ])),
    StructField("address", StructType([
        StructField("city", StringType()),
        StructField("street", StringType()),
        StructField("number", LongType()),
        StructField("zipcode", StringType()),
        StructField("geolocation", StructType([
            StructField("lat", StringType()),
            StructField("long", StringType())
        ]))
    ]))
])

customers_df = (
    spark.readStream
    .format("json")
    .schema(customers_schema)
    .load("/opt/data/raw/customers")     # ← updated
    .select(
        col("id").alias("customer_id"),
        col("username"),
        col("email"),
        col("phone"),
        col("name.firstname").alias("first_name"),
        col("name.lastname").alias("last_name"),
        col("address.city").alias("city"),
        col("address.street").alias("street"),
        col("address.number").alias("street_number"),
        col("address.zipcode").alias("zipcode")
    )
)

(
    customers_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/opt/checkpoints/bronze_customers")  # ← updated
    .outputMode("append")
    .trigger(availableNow=True)
    .start("/opt/data/delta/bronze/customers")                           # ← updated
    .awaitTermination()
)
print("✅ Bronze customers complete")

# ── Products ─────────────────────────────────────────────
products_schema = StructType([
    StructField("id", LongType()),
    StructField("title", StringType()),
    StructField("price", DoubleType()),
    StructField("description", StringType()),
    StructField("category", StringType()),
    StructField("image", StringType()),
    StructField("rating", StructType([
        StructField("rate", DoubleType()),
        StructField("count", LongType())
    ]))
])

products_df = (
    spark.readStream
    .format("json")
    .schema(products_schema)
    .load("/opt/data/raw/products")      # ← updated
    .select(
        col("id").alias("product_id"),
        col("title"), col("price"),
        col("description"), col("category"),
        col("image"),
        col("rating.rate").alias("rating_rate"),
        col("rating.count").alias("rating_count")
    )
)

(
    products_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/opt/checkpoints/bronze_products")   # ← updated
    .outputMode("append")
    .trigger(availableNow=True)
    .start("/opt/data/delta/bronze/products")                            # ← updated
    .awaitTermination()
)
print("✅ Bronze products complete")

print("✅ All bronze tables complete")
spark.stop()